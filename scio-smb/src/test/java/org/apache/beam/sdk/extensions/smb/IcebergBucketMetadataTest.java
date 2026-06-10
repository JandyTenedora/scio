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

import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.junit.Assert;
import org.junit.Test;

/** Unit tests for {@link IcebergBucketMetadata}. */
public class IcebergBucketMetadataTest {

  static final Schema USER_SCHEMA =
      SchemaBuilder.record("User")
          .namespace("org.apache.beam.sdk.extensions.smb.iceberg")
          .fields()
          .requiredString("user_id")
          .requiredInt("age")
          .optionalString("name")
          .endRecord();

  @Test
  public void testExtractKeyPrimary() throws Exception {
    IcebergBucketMetadata<String, Void, GenericRecord> metadata =
        new IcebergBucketMetadata<>(16, 1, String.class, "user_id", USER_SCHEMA);

    GenericRecord record =
        new GenericRecordBuilder(USER_SCHEMA)
            .set("user_id", "abc123")
            .set("age", 25)
            .set("name", "Alice")
            .build();

    Assert.assertEquals("abc123", metadata.extractKeyPrimary(record));
  }

  @Test
  public void testExtractKeyPrimaryInteger() throws Exception {
    IcebergBucketMetadata<Integer, Void, GenericRecord> metadata =
        new IcebergBucketMetadata<>(16, 1, Integer.class, "age", USER_SCHEMA);

    GenericRecord record =
        new GenericRecordBuilder(USER_SCHEMA)
            .set("user_id", "abc123")
            .set("age", 42)
            .set("name", "Bob")
            .build();

    Assert.assertEquals((Integer) 42, metadata.extractKeyPrimary(record));
  }

  @Test
  public void testUsesIcebergHashType() throws Exception {
    IcebergBucketMetadata<String, Void, GenericRecord> metadata =
        new IcebergBucketMetadata<>(16, 1, String.class, "user_id", USER_SCHEMA);

    Assert.assertEquals(HashType.ICEBERG, metadata.getHashType());
  }

  @Test
  public void testBucketIdMatchesIcebergBucketTransform() throws Exception {
    int numBuckets = 16;
    IcebergBucketMetadata<String, Void, GenericRecord> metadata =
        new IcebergBucketMetadata<>(numBuckets, 1, String.class, "user_id", USER_SCHEMA);

    GenericRecord record =
        new GenericRecordBuilder(USER_SCHEMA)
            .set("user_id", "test_user")
            .set("age", 30)
            .build();

    int smbBucketId = metadata.getBucketId(metadata.getKeyBytesPrimary(record));

    // Iceberg bucket transform: murmur3_32(encode(value)) & Integer.MAX_VALUE % N
    byte[] keyBytes = "test_user".getBytes(StandardCharsets.UTF_8);
    int murmurHash = Hashing.murmur3_32_fixed().hashBytes(keyBytes).asInt();
    int icebergBucketId = (murmurHash & Integer.MAX_VALUE) % numBuckets;

    Assert.assertEquals(icebergBucketId, smbBucketId);
  }

  @Test
  public void testSerializationRoundTrip() throws Exception {
    IcebergBucketMetadata<String, Void, GenericRecord> metadata =
        new IcebergBucketMetadata<>(
            0, 64, 1, String.class, "user_id", null, null,
            BucketMetadata.serializeHashType(HashType.ICEBERG),
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);

    BucketMetadata<String, Void, GenericRecord> copy = BucketMetadata.from(metadata.toString());
    Assert.assertEquals(metadata.getVersion(), copy.getVersion());
    Assert.assertEquals(metadata.getNumBuckets(), copy.getNumBuckets());
    Assert.assertEquals(metadata.getNumShards(), copy.getNumShards());
    Assert.assertEquals(metadata.getKeyClass(), copy.getKeyClass());
    Assert.assertEquals(metadata.getHashType(), copy.getHashType());
  }

  @Test
  public void testCompatibleWithAvroBucketMetadata() throws Exception {
    IcebergBucketMetadata<String, Void, GenericRecord> icebergMeta =
        new IcebergBucketMetadata<>(16, 1, String.class, "user_id", USER_SCHEMA);

    Assert.assertTrue(
        icebergMeta.compatibleMetadataTypes().contains(AvroBucketMetadata.class));
    Assert.assertTrue(
        icebergMeta.compatibleMetadataTypes().contains(ParquetBucketMetadata.class));
  }

  @Test
  public void testSecondaryKey() throws Exception {
    IcebergBucketMetadata<String, Integer, GenericRecord> metadata =
        new IcebergBucketMetadata<>(
            16, 1, String.class, "user_id", Integer.class, "age", USER_SCHEMA);

    GenericRecord record =
        new GenericRecordBuilder(USER_SCHEMA)
            .set("user_id", "abc123")
            .set("age", 25)
            .set("name", "Alice")
            .build();

    Assert.assertEquals("abc123", metadata.extractKeyPrimary(record));
    Assert.assertEquals((Integer) 25, metadata.extractKeySecondary(record));
  }

  @Test
  public void testBucketDistribution() throws Exception {
    int numBuckets = 16;
    IcebergBucketMetadata<String, Void, GenericRecord> metadata =
        new IcebergBucketMetadata<>(numBuckets, 1, String.class, "user_id", USER_SCHEMA);

    int[] bucketCounts = new int[numBuckets];
    for (int i = 0; i < 1000; i++) {
      GenericRecord record =
          new GenericRecordBuilder(USER_SCHEMA)
              .set("user_id", "user_" + i)
              .set("age", i)
              .build();
      int bucketId = metadata.getBucketId(metadata.getKeyBytesPrimary(record));
      Assert.assertTrue(bucketId >= 0 && bucketId < numBuckets);
      bucketCounts[bucketId]++;
    }

    // Verify all buckets get at least some records (statistical — very unlikely to fail)
    for (int count : bucketCounts) {
      Assert.assertTrue("Expected all buckets to have records, got 0", count > 0);
    }
  }
}
