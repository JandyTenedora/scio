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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for {@link IcebergSortedBucketIO}. */
public class IcebergSortedBucketIOTest {
  @Rule public final TemporaryFolder warehouse = new TemporaryFolder();
  @Rule public final TemporaryFolder outputFolder = new TemporaryFolder();

  static final Schema AVRO_SCHEMA =
      SchemaBuilder.record("User")
          .namespace("org.apache.beam.sdk.extensions.smb.iceberg")
          .fields()
          .requiredString("user_id")
          .requiredInt("age")
          .endRecord();

  static final org.apache.iceberg.Schema ICEBERG_SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "user_id", Types.StringType.get()),
          Types.NestedField.required(2, "age", Types.IntegerType.get()));

  @Test
  public void testWriteSerializable() {
    SerializableUtils.ensureSerializable(
        IcebergSortedBucketIO.write(String.class, "user_id", AVRO_SCHEMA)
            .to(outputFolder.getRoot().getAbsolutePath())
            .withNumBuckets(16));
  }

  @Test
  public void testTableConfigFromBucketPartitionedTable() throws IOException {
    HadoopCatalog catalog = createCatalog();
    Table table = createBucketedTable(catalog, "partitioned", 16);
    writeTestData(table, 100);

    IcebergSortedBucketIO.IcebergTableConfig config =
        IcebergSortedBucketIO.IcebergTableConfig.fromTable(table, "user_id");

    Assert.assertEquals(16, config.numBuckets());
    Assert.assertEquals("user_id", config.bucketKeyField());
    Assert.assertFalse(config.bucketToFiles().isEmpty());

    int totalFiles = config.bucketToFiles().values().stream()
        .mapToInt(List::size)
        .sum();
    Assert.assertTrue("Expected at least one data file", totalFiles > 0);

    for (int bucketId : config.bucketToFiles().keySet()) {
      Assert.assertTrue(
          "Bucket ID " + bucketId + " out of range", bucketId >= 0 && bucketId < 16);
    }
  }

  @Test
  public void testTableConfigFailsWithoutBucketPartition() throws IOException {
    HadoopCatalog catalog = createCatalog();
    Table table = catalog.createTable(
        TableIdentifier.of("db", "unpartitioned"),
        ICEBERG_SCHEMA);

    Assert.assertThrows(IllegalArgumentException.class, () ->
        IcebergSortedBucketIO.IcebergTableConfig.fromTable(table, "user_id"));
  }

  @Test
  public void testReadFromIcebergTable() throws IOException {
    HadoopCatalog catalog = createCatalog();
    Table table = createBucketedTable(catalog, "readable", 8);
    writeTestData(table, 50);

    TupleTag<GenericRecord> tag = new TupleTag<>("users");
    IcebergSortedBucketIO.Read read =
        IcebergSortedBucketIO.read(tag, AVRO_SCHEMA).fromTable(table, "user_id");

    SortedBucketSource.BucketedInput<GenericRecord> input =
        read.toBucketedInput(SortedBucketSource.Keying.PRIMARY);
    Assert.assertNotNull(input);

    BucketMetadataUtil.SourceMetadata<GenericRecord> sourceMeta = input.getSourceMetadata();
    Assert.assertNotNull(sourceMeta);
    Assert.assertEquals(8, sourceMeta.leastNumBuckets());
  }

  @Test
  public void testReadRequiresFromTable() {
    TupleTag<GenericRecord> tag = new TupleTag<>("users");
    IcebergSortedBucketIO.Read read = IcebergSortedBucketIO.read(tag, AVRO_SCHEMA);

    Assert.assertThrows(IllegalStateException.class, () ->
        read.toBucketedInput(SortedBucketSource.Keying.PRIMARY));
  }

  @Test
  public void testBucketConsistency() throws Exception {
    int numBuckets = 16;
    HadoopCatalog catalog = createCatalog();
    Table table = createBucketedTable(catalog, "consistent", numBuckets);
    writeTestData(table, 200);

    IcebergSortedBucketIO.IcebergTableConfig config =
        IcebergSortedBucketIO.IcebergTableConfig.fromTable(table, "user_id");

    Assert.assertEquals(numBuckets, config.numBuckets());
    Assert.assertFalse(
        "Table should have files after writing data",
        config.bucketToFiles().isEmpty());
  }

  // ---- helpers ----

  private HadoopCatalog createCatalog() {
    Configuration hadoopConf = new Configuration();
    HadoopCatalog catalog = new HadoopCatalog();
    catalog.setConf(hadoopConf);
    Map<String, String> props = new HashMap<>();
    props.put("warehouse", warehouse.getRoot().getAbsolutePath());
    catalog.initialize("test", props);
    return catalog;
  }

  private Table createBucketedTable(HadoopCatalog catalog, String name, int numBuckets) {
    return catalog.createTable(
        TableIdentifier.of("db", name),
        ICEBERG_SCHEMA,
        PartitionSpec.builderFor(ICEBERG_SCHEMA)
            .bucket("user_id", numBuckets)
            .build());
  }

  private void writeTestData(Table table, int numRecords) throws IOException {
    PartitionSpec spec = table.spec();
    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema());

    Map<Integer, List<Record>> byBucket = new HashMap<>();
    for (int i = 0; i < numRecords; i++) {
      Record record = org.apache.iceberg.data.GenericRecord.create(table.schema());
      record.setField("user_id", "user_" + String.format("%05d", i));
      record.setField("age", 20 + (i % 50));

      PartitionKey key = new PartitionKey(spec, table.schema());
      key.partition(record);
      int bucketId = key.get(0, Integer.class);

      byBucket.computeIfAbsent(bucketId, k -> new ArrayList<>()).add(record);
    }

    for (Map.Entry<Integer, List<Record>> entry : byBucket.entrySet()) {
      int bucketId = entry.getKey();
      List<Record> records = entry.getValue();

      String filePath = table.location() + "/data/bucket-" + bucketId + "/data-0.parquet";
      OutputFile outputFile = table.io().newOutputFile(filePath);

      PartitionKey partitionKey = new PartitionKey(spec, table.schema());
      partitionKey.partition(records.get(0));

      DataWriter<Record> writer = appenderFactory.newDataWriter(
          table.encryption().encrypt(outputFile),
          FileFormat.PARQUET,
          partitionKey);

      try {
        for (Record record : records) {
          writer.write(record);
        }
      } finally {
        writer.close();
      }

      DataFile dataFile = writer.toDataFile();
      table.newAppend().appendFile(dataFile).commit();
    }
  }
}
