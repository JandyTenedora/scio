/*
 * Copyright 2024 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.beam.sdk.extensions.smb;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.Keying;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.Predicate;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;

/**
 * API for reading and writing Iceberg tables as sorted-bucket files.
 *
 * <p>Bridges Iceberg's {@code bucket(N, column)} partition transform with SMB's shuffle-free merge
 * join engine. An Iceberg table partitioned with {@code bucket(N, user_id)} and sorted by {@code
 * user_id ASC} produces files structurally identical to SMB files: bucketed by hash, sorted within
 * each bucket. This class lets Beam/Scio pipelines read such tables as SMB inputs for zero-shuffle
 * joins with other SMB sources.
 *
 * <h3>Read example</h3>
 *
 * <pre>{@code
 * Table table = catalog.loadTable(TableIdentifier.of("db", "events"));
 * Schema avroSchema = AvroSchemaUtil.convert(table.schema(), "events");
 *
 * IcebergSortedBucketIO.read(new TupleTag<>("events"), avroSchema)
 *     .fromTable(table, "user_id")
 * }</pre>
 *
 * <h3>Write example</h3>
 *
 * <pre>{@code
 * IcebergSortedBucketIO.write(String.class, "user_id", avroSchema)
 *     .to(outputPath)
 *     .withNumBuckets(4096)
 * }</pre>
 */
public class IcebergSortedBucketIO {
  private static final String DEFAULT_SUFFIX = ".parquet";

  static {
    try {
      Class.forName("org.apache.iceberg.Table");
    } catch (ClassNotFoundException e) {
      throw new MissingImplementationException("iceberg", e);
    }
  }

  /** Returns a new {@link Read} for Iceberg tables read as Avro GenericRecords. */
  public static Read read(TupleTag<GenericRecord> tupleTag, Schema avroSchema) {
    return new AutoValue_IcebergSortedBucketIO_Read.Builder()
        .setTupleTag(tupleTag)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setAvroSchema(avroSchema)
        .build();
  }

  /** Returns a new {@link Write} for writing Iceberg-compatible sorted-bucket files. */
  public static <K1> Write<K1, Void, GenericRecord> write(
      Class<K1> keyClass, String keyField, Schema avroSchema) {
    return new AutoValue_IcebergSortedBucketIO_Write.Builder<K1, Void, GenericRecord>()
        .setNumShards(SortedBucketIO.DEFAULT_NUM_SHARDS)
        .setHashType(HashType.ICEBERG)
        .setSorterMemoryMb(SortedBucketIO.DEFAULT_SORTER_MEMORY_MB)
        .setFilenamePrefix(SortedBucketIO.DEFAULT_FILENAME_PREFIX)
        .setKeyClassPrimary(keyClass)
        .setKeyClassSecondary(null)
        .setKeyFieldPrimary(keyField)
        .setKeyFieldSecondary(null)
        .setKeyCacheSize(0)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setAvroSchema(avroSchema)
        .build();
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Read
  ////////////////////////////////////////////////////////////////////////////////

  @AutoValue
  public abstract static class Read extends SortedBucketIO.Read<GenericRecord> {
    abstract String getFilenameSuffix();

    abstract Schema getAvroSchema();

    @Nullable
    abstract IcebergTableConfig getTableConfig();

    @Nullable
    abstract Predicate<GenericRecord> getPredicate();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTupleTag(TupleTag<GenericRecord> tupleTag);

      abstract Builder setFilenameSuffix(String filenameSuffix);

      abstract Builder setAvroSchema(Schema avroSchema);

      abstract Builder setTableConfig(IcebergTableConfig tableConfig);

      abstract Builder setPredicate(Predicate<GenericRecord> predicate);

      abstract Read build();
    }

    /**
     * Configures this read to load files from an Iceberg table.
     *
     * @param table the Iceberg table (must use bucket partitioning)
     * @param bucketKeyField the field name used in the bucket partition transform
     */
    public Read fromTable(Table table, String bucketKeyField) {
      IcebergTableConfig config = IcebergTableConfig.fromTable(table, bucketKeyField);
      return toBuilder().setTableConfig(config).build();
    }

    public Read withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    public Read withPredicate(Predicate<GenericRecord> predicate) {
      return toBuilder().setPredicate(predicate).build();
    }

    @Override
    public BucketedInput<GenericRecord> toBucketedInput(Keying keying) {
      IcebergTableConfig tableConfig = getTableConfig();
      if (tableConfig != null) {
        return new IcebergBucketedInput(
            keying, getTupleTag(), tableConfig, getAvroSchema(), getFilenameSuffix(), getPredicate());
      }
      throw new IllegalStateException(
          "IcebergSortedBucketIO.Read requires .fromTable() to be configured");
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Write
  ////////////////////////////////////////////////////////////////////////////////

  @AutoValue
  public abstract static class Write<K1, K2, T extends GenericRecord>
      extends SortedBucketIO.Write<K1, K2, T> {
    @Nullable
    abstract String getKeyFieldPrimary();

    @Nullable
    abstract String getKeyFieldSecondary();

    abstract Schema getAvroSchema();

    abstract Builder<K1, K2, T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K1, K2, T extends GenericRecord> {
      abstract Builder<K1, K2, T> setNumBuckets(int numBuckets);

      abstract Builder<K1, K2, T> setNumShards(int numShards);

      abstract Builder<K1, K2, T> setKeyClassPrimary(Class<K1> keyClassPrimary);

      abstract Builder<K1, K2, T> setKeyClassSecondary(Class<K2> keyClassSecondary);

      abstract Builder<K1, K2, T> setHashType(HashType hashType);

      abstract Builder<K1, K2, T> setOutputDirectory(ResourceId outputDirectory);

      abstract Builder<K1, K2, T> setTempDirectory(ResourceId tempDirectory);

      abstract Builder<K1, K2, T> setFilenameSuffix(String filenameSuffix);

      abstract Builder<K1, K2, T> setSorterMemoryMb(int sorterMemoryMb);

      abstract Builder<K1, K2, T> setFilenamePrefix(String filenamePrefix);

      abstract Builder<K1, K2, T> setKeyFieldPrimary(String keyFieldPrimary);

      abstract Builder<K1, K2, T> setKeyFieldSecondary(String keyFieldSecondary);

      abstract Builder<K1, K2, T> setAvroSchema(Schema avroSchema);

      abstract Builder<K1, K2, T> setKeyCacheSize(int cacheSize);

      abstract Write<K1, K2, T> build();
    }

    public Write<K1, K2, T> to(String outputDirectory) {
      return toBuilder()
          .setOutputDirectory(FileSystems.matchNewResource(outputDirectory, true))
          .build();
    }

    public Write<K1, K2, T> withNumBuckets(int numBuckets) {
      return toBuilder().setNumBuckets(numBuckets).build();
    }

    public Write<K1, K2, T> withNumShards(int numShards) {
      return toBuilder().setNumShards(numShards).build();
    }

    public Write<K1, K2, T> withTempDirectory(String tempDirectory) {
      return toBuilder()
          .setTempDirectory(FileSystems.matchNewResource(tempDirectory, true))
          .build();
    }

    public Write<K1, K2, T> withFilenamePrefix(String filenamePrefix) {
      return toBuilder().setFilenamePrefix(filenamePrefix).build();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public BucketMetadata<K1, K2, T> getBucketMetadata() {
      try {
        return (BucketMetadata)
            new IcebergBucketMetadata<>(
                getNumBuckets(),
                getNumShards(),
                getKeyClassPrimary(),
                getKeyFieldPrimary(),
                getKeyClassSecondary(),
                getKeyFieldSecondary(),
                getAvroSchema());
      } catch (CannotProvideCoderException | Coder.NonDeterministicException e) {
        throw new IllegalStateException(e);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public FileOperations<T> getFileOperations() {
      return (FileOperations<T>)
          ParquetAvroFileOperations.of(getAvroSchema());
    }

  }

  ////////////////////////////////////////////////////////////////////////////////
  // Iceberg table config (serializable snapshot of table metadata)
  ////////////////////////////////////////////////////////////////////////////////

  @AutoValue
  abstract static class IcebergTableConfig implements Serializable {
    abstract int numBuckets();

    abstract String bucketKeyField();

    abstract Map<Integer, List<String>> bucketToFiles();

    static IcebergTableConfig fromTable(Table table, String bucketKeyField) {
      PartitionSpec spec = table.spec();

      PartitionField bucketField = null;
      for (PartitionField field : spec.fields()) {
        if (field.transform().toString().startsWith("bucket[")) {
          String sourceFieldName =
              table.schema().findField(field.sourceId()).name();
          if (sourceFieldName.equals(bucketKeyField)) {
            bucketField = field;
            break;
          }
        }
      }
      if (bucketField == null) {
        throw new IllegalArgumentException(
            String.format(
                "Table %s has no bucket partition on field '%s'. "
                    + "Partition spec: %s",
                table.name(), bucketKeyField, spec));
      }

      String transformStr = bucketField.transform().toString();
      int numBuckets =
          Integer.parseInt(
              transformStr.substring(
                  transformStr.indexOf('[') + 1, transformStr.indexOf(']')));

      Map<Integer, List<String>> bucketToFiles = new HashMap<>();
      try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
        for (FileScanTask task : tasks) {
          DataFile dataFile = task.file();
          int bucketId =
              dataFile.partition().get(spec.fields().indexOf(bucketField), Integer.class);
          bucketToFiles
              .computeIfAbsent(bucketId, k -> new ArrayList<>())
              .add(dataFile.location());
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to scan Iceberg table manifests", e);
      }

      return new AutoValue_IcebergSortedBucketIO_IcebergTableConfig(
          numBuckets, bucketKeyField, bucketToFiles);
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Custom BucketedInput that discovers files from Iceberg manifests
  ////////////////////////////////////////////////////////////////////////////////

  static class IcebergBucketedInput extends BucketedInput<GenericRecord> {
    private final IcebergTableConfig tableConfig;
    private final Schema avroSchema;
    private final String filenameSuffix;

    IcebergBucketedInput(
        Keying keying,
        TupleTag<GenericRecord> tupleTag,
        IcebergTableConfig tableConfig,
        Schema avroSchema,
        String filenameSuffix,
        @Nullable Predicate<GenericRecord> predicate) {
      super(
          keying,
          tupleTag,
          buildDirectoryMap(tableConfig, avroSchema, filenameSuffix),
          predicate);
      this.tableConfig = tableConfig;
      this.avroSchema = avroSchema;
      this.filenameSuffix = filenameSuffix;
    }

    private static Map<String, KV<String, FileOperations<GenericRecord>>> buildDirectoryMap(
        IcebergTableConfig tableConfig, Schema avroSchema, String filenameSuffix) {
      FileOperations<GenericRecord> fileOps = ParquetAvroFileOperations.of(avroSchema);

      Map<String, KV<String, FileOperations<GenericRecord>>> dirs = new HashMap<>();
      for (Map.Entry<Integer, List<String>> entry : tableConfig.bucketToFiles().entrySet()) {
        for (String filePath : entry.getValue()) {
          String parentDir = filePath.substring(0, filePath.lastIndexOf('/'));
          dirs.putIfAbsent(parentDir, KV.of(filenameSuffix, fileOps));
        }
      }

      if (dirs.isEmpty()) {
        throw new IllegalStateException("No data files found in Iceberg table");
      }
      return dirs;
    }

    @Override
    public BucketMetadataUtil.SourceMetadata<GenericRecord> getSourceMetadata() {
      if (sourceMetadata == null) {
        sourceMetadata = buildIcebergSourceMetadata();
      }
      return sourceMetadata;
    }

    private BucketMetadataUtil.SourceMetadata<GenericRecord> buildIcebergSourceMetadata() {
      int numBuckets = tableConfig.numBuckets();
      int maxFilesPerBucket =
          tableConfig.bucketToFiles().values().stream()
              .mapToInt(List::size)
              .max()
              .orElse(1);

      try {
        IcebergBucketMetadata<String, Void, GenericRecord> metadata =
            new IcebergBucketMetadata<>(
                numBuckets, maxFilesPerBucket, String.class, tableConfig.bucketKeyField(), avroSchema);

        IcebergFileAssignment fileAssignment =
            new IcebergFileAssignment(tableConfig.bucketToFiles(), numBuckets, maxFilesPerBucket);

        Map<ResourceId, BucketMetadataUtil.SourceMetadataValue<GenericRecord>> mapping =
            new HashMap<>();
        ResourceId syntheticDir =
            FileSystems.matchNewResource("iceberg://" + tableConfig.bucketKeyField(), true);
        mapping.put(
            syntheticDir,
            new BucketMetadataUtil.SourceMetadataValue<>(metadata, fileAssignment));

        return new BucketMetadataUtil.SourceMetadata<>(mapping);
      } catch (CannotProvideCoderException | Coder.NonDeterministicException e) {
        throw new RuntimeException("Failed to create IcebergBucketMetadata", e);
      }
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // FileAssignment that maps bucket IDs to Iceberg data file paths
  ////////////////////////////////////////////////////////////////////////////////

  static class IcebergFileAssignment extends FileAssignment {
    private final Map<Integer, List<String>> bucketToFiles;
    private final int numBuckets;
    private final int maxShards;

    IcebergFileAssignment(
        Map<Integer, List<String>> bucketToFiles, int numBuckets, int maxShards) {
      super(
          FileSystems.matchNewResource("iceberg://virtual", true),
          SortedBucketIO.DEFAULT_FILENAME_PREFIX,
          ".parquet",
          false);
      this.bucketToFiles = bucketToFiles;
      this.numBuckets = numBuckets;
      this.maxShards = maxShards;
    }

    @Override
    ResourceId forBucket(BucketShardId id, int maxNumBuckets, int maxNumShards) {
      List<String> files = bucketToFiles.get(id.getBucketId());
      if (files == null || files.isEmpty()) {
        return super.forBucket(id, maxNumBuckets, maxNumShards);
      }
      int shardIdx = id.getShardId();
      if (shardIdx >= files.size()) {
        return super.forBucket(id, maxNumBuckets, maxNumShards);
      }
      String filePath = files.get(shardIdx);
      return FileSystems.matchNewResource(filePath, false);
    }
  }
}
