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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link IcebergSortedBucketIO}.
 *
 * <p>These tests verify the SMB integration logic (file assignment, source metadata, bucket
 * configuration) without writing actual Iceberg data files. Tests that exercise
 * {@code IcebergTableConfig.fromTable()} require Avro 1.12+ (Iceberg 1.10 dependency) and
 * should be run with {@code -Davro.version=1.12.0}.
 */
public class IcebergSortedBucketIOTest {
  @Rule public final TemporaryFolder outputFolder = new TemporaryFolder();

  static final Schema AVRO_SCHEMA =
      SchemaBuilder.record("User")
          .namespace("org.apache.beam.sdk.extensions.smb.iceberg")
          .fields()
          .requiredString("user_id")
          .requiredInt("age")
          .endRecord();

  @Test
  public void testWriteSerializable() {
    SerializableUtils.ensureSerializable(
        IcebergSortedBucketIO.write(String.class, "user_id", AVRO_SCHEMA)
            .to(outputFolder.getRoot().getAbsolutePath())
            .withNumBuckets(16));
  }

  @Test
  public void testReadRequiresFromTable() {
    TupleTag<GenericRecord> tag = new TupleTag<>("users");
    IcebergSortedBucketIO.Read read = IcebergSortedBucketIO.read(tag, AVRO_SCHEMA);

    Assert.assertThrows(IllegalStateException.class, () ->
        read.toBucketedInput(SortedBucketSource.Keying.PRIMARY));
  }

  @Test
  public void testBucketedInputFromConfig() {
    Map<Integer, List<String>> bucketToFiles = new HashMap<>();
    bucketToFiles.put(0, Arrays.asList("gs://bucket/data/b0/file1.parquet"));
    bucketToFiles.put(1, Arrays.asList("gs://bucket/data/b1/file1.parquet",
                                        "gs://bucket/data/b1/file2.parquet"));
    bucketToFiles.put(3, Arrays.asList("gs://bucket/data/b3/file1.parquet"));

    IcebergSortedBucketIO.IcebergTableConfig config =
        new AutoValue_IcebergSortedBucketIO_IcebergTableConfig(4, "user_id", bucketToFiles);

    TupleTag<GenericRecord> tag = new TupleTag<>("users");
    IcebergSortedBucketIO.IcebergBucketedInput input =
        new IcebergSortedBucketIO.IcebergBucketedInput(
            SortedBucketSource.Keying.PRIMARY, tag, config, AVRO_SCHEMA, ".parquet", null);

    BucketMetadataUtil.SourceMetadata<GenericRecord> sourceMeta = input.getSourceMetadata();
    Assert.assertNotNull(sourceMeta);
    Assert.assertEquals(4, sourceMeta.leastNumBuckets());
  }

  @Test
  public void testFileAssignmentResolvesCorrectPaths() {
    Map<Integer, List<String>> bucketToFiles = new HashMap<>();
    bucketToFiles.put(0, Arrays.asList("gs://bucket/data/b0/file1.parquet"));
    bucketToFiles.put(1, Arrays.asList("gs://bucket/data/b1/file1.parquet",
                                        "gs://bucket/data/b1/file2.parquet"));
    bucketToFiles.put(2, Arrays.asList("gs://bucket/data/b2/file1.parquet"));

    IcebergSortedBucketIO.IcebergFileAssignment assignment =
        new IcebergSortedBucketIO.IcebergFileAssignment(bucketToFiles, 4, 2);

    // Bucket 0, shard 0 → file1.parquet
    ResourceId b0s0 = assignment.forBucket(BucketShardId.of(0, 0), 4, 2);
    Assert.assertTrue(b0s0.toString().contains("b0/file1.parquet"));

    // Bucket 1, shard 0 → file1.parquet
    ResourceId b1s0 = assignment.forBucket(BucketShardId.of(1, 0), 4, 2);
    Assert.assertTrue(b1s0.toString().contains("b1/file1.parquet"));

    // Bucket 1, shard 1 → file2.parquet (second file in bucket 1)
    ResourceId b1s1 = assignment.forBucket(BucketShardId.of(1, 1), 4, 2);
    Assert.assertTrue(b1s1.toString().contains("b1/file2.parquet"));

    // Bucket 2, shard 0 → file1.parquet
    ResourceId b2s0 = assignment.forBucket(BucketShardId.of(2, 0), 4, 2);
    Assert.assertTrue(b2s0.toString().contains("b2/file1.parquet"));
  }

  @Test
  public void testFileAssignmentFallsBackForMissingBuckets() {
    Map<Integer, List<String>> bucketToFiles = new HashMap<>();
    bucketToFiles.put(0, Arrays.asList("gs://bucket/data/b0/file1.parquet"));

    IcebergSortedBucketIO.IcebergFileAssignment assignment =
        new IcebergSortedBucketIO.IcebergFileAssignment(bucketToFiles, 4, 1);

    // Bucket 3 has no files — falls back to SMB default naming
    ResourceId b3 = assignment.forBucket(BucketShardId.of(3, 0), 4, 1);
    Assert.assertNotNull(b3);
  }

  @Test
  public void testFileAssignmentFallsBackForExcessShards() {
    Map<Integer, List<String>> bucketToFiles = new HashMap<>();
    bucketToFiles.put(0, Arrays.asList("gs://bucket/data/b0/file1.parquet"));

    IcebergSortedBucketIO.IcebergFileAssignment assignment =
        new IcebergSortedBucketIO.IcebergFileAssignment(bucketToFiles, 4, 2);

    // Bucket 0, shard 1 — only 1 file exists, falls back
    ResourceId b0s1 = assignment.forBucket(BucketShardId.of(0, 1), 4, 2);
    Assert.assertNotNull(b0s1);
  }

  @Test
  public void testConfigNumBucketsAndKeyField() {
    Map<Integer, List<String>> bucketToFiles = new HashMap<>();
    bucketToFiles.put(0, Arrays.asList("/data/file.parquet"));

    IcebergSortedBucketIO.IcebergTableConfig config =
        new AutoValue_IcebergSortedBucketIO_IcebergTableConfig(256, "event_id", bucketToFiles);

    Assert.assertEquals(256, config.numBuckets());
    Assert.assertEquals("event_id", config.bucketKeyField());
    Assert.assertEquals(1, config.bucketToFiles().size());
  }

  @Test
  public void testSourceMetadataUsesIcebergHashType() {
    Map<Integer, List<String>> bucketToFiles = new HashMap<>();
    bucketToFiles.put(0, Arrays.asList("/tmp/data/file.parquet"));

    IcebergSortedBucketIO.IcebergTableConfig config =
        new AutoValue_IcebergSortedBucketIO_IcebergTableConfig(16, "user_id", bucketToFiles);

    TupleTag<GenericRecord> tag = new TupleTag<>("test");
    IcebergSortedBucketIO.IcebergBucketedInput input =
        new IcebergSortedBucketIO.IcebergBucketedInput(
            SortedBucketSource.Keying.PRIMARY, tag, config, AVRO_SCHEMA, ".parquet", null);

    BucketMetadataUtil.SourceMetadata<GenericRecord> sourceMeta = input.getSourceMetadata();
    BucketMetadata<?, ?, GenericRecord> metadata =
        sourceMeta.mapping.values().iterator().next().metadata;

    Assert.assertEquals(BucketMetadata.HashType.ICEBERG, metadata.getHashType());
    Assert.assertEquals(16, metadata.getNumBuckets());
  }
}
