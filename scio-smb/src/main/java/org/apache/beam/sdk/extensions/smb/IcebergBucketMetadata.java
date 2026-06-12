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

import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

/**
 * {@link BucketMetadata} for Iceberg tables read as Avro {@link GenericRecord}s.
 *
 * <p>Uses {@link HashType#ICEBERG} to produce bucket assignments compatible with Iceberg's {@code
 * bucket(N, column)} partition transform. This means an Iceberg table partitioned with {@code
 * bucket(N, user_id)} produces the same bucket IDs as SMB files written with this metadata, enabling
 * shuffle-free joins between Iceberg tables and standard SMB sources.
 *
 * <p>Optionally tracks the Iceberg <b>field ID</b> ({@code icebergFieldId}) for the bucket key
 * column. The field ID corresponds to Iceberg's internal column ID from the table schema and
 * survives column renames and other schema evolution operations. When present, downstream code can
 * resolve the key by ID instead of by name, providing resilience against schema changes.
 */
public class IcebergBucketMetadata<K1, K2, V extends IndexedRecord>
    extends BucketMetadata<K1, K2, V> {

  @JsonProperty private final String keyField;

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final String keyFieldSecondary;

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final Integer icebergFieldId;

  @JsonIgnore private final AtomicReference<int[]> keyPath = new AtomicReference<>();
  @JsonIgnore private final AtomicReference<int[]> keyPathSecondary = new AtomicReference<>();

  public IcebergBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K1> keyClassPrimary,
      String keyField,
      Class<K2> keyClassSecondary,
      String keyFieldSecondary,
      Schema schema)
      throws CannotProvideCoderException, Coder.NonDeterministicException {
    this(numBuckets, numShards, keyClassPrimary, keyField, keyClassSecondary,
        keyFieldSecondary, schema, null);
  }

  public IcebergBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K1> keyClassPrimary,
      String keyField,
      Class<K2> keyClassSecondary,
      String keyFieldSecondary,
      Schema schema,
      Integer icebergFieldId)
      throws CannotProvideCoderException, Coder.NonDeterministicException {
    this(
        BucketMetadata.CURRENT_VERSION,
        numBuckets,
        numShards,
        keyClassPrimary,
        AvroUtils.validateKeyField(keyField, keyClassPrimary, schema),
        keyClassSecondary,
        keyFieldSecondary == null
            ? null
            : AvroUtils.validateKeyField(keyFieldSecondary, keyClassSecondary, schema),
        BucketMetadata.serializeHashType(HashType.ICEBERG),
        SortedBucketIO.DEFAULT_FILENAME_PREFIX,
        icebergFieldId);
  }

  public IcebergBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K1> keyClassPrimary,
      String keyField,
      Schema schema)
      throws CannotProvideCoderException, Coder.NonDeterministicException {
    this(numBuckets, numShards, keyClassPrimary, keyField, null, null, schema, null);
  }

  public IcebergBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K1> keyClassPrimary,
      String keyField,
      Schema schema,
      Integer icebergFieldId)
      throws CannotProvideCoderException, Coder.NonDeterministicException {
    this(numBuckets, numShards, keyClassPrimary, keyField, null, null, schema, icebergFieldId);
  }

  @JsonCreator
  IcebergBucketMetadata(
      @JsonProperty("version") int version,
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("numShards") int numShards,
      @JsonProperty("keyClass") Class<K1> keyClassPrimary,
      @JsonProperty("keyField") String keyField,
      @Nullable @JsonProperty("keyClassSecondary") Class<K2> keyClassSecondary,
      @Nullable @JsonProperty("keyFieldSecondary") String keyFieldSecondary,
      @JsonProperty("hashType") String hashType,
      @JsonProperty(value = "filenamePrefix", required = false) String filenamePrefix,
      @Nullable @JsonProperty("icebergFieldId") Integer icebergFieldId)
      throws CannotProvideCoderException, Coder.NonDeterministicException {
    super(
        version,
        numBuckets,
        numShards,
        keyClassPrimary,
        keyClassSecondary,
        hashType,
        filenamePrefix);
    verify(
        (keyClassSecondary != null && keyFieldSecondary != null)
            || (keyClassSecondary == null && keyFieldSecondary == null));
    this.keyField = keyField;
    this.keyFieldSecondary = keyFieldSecondary;
    this.icebergFieldId = icebergFieldId;
  }

  @Override
  public Map<Class<?>, Coder<?>> coderOverrides() {
    return AvroUtils.coderOverrides();
  }

  @Override
  int hashPrimaryKeyMetadata() {
    return Objects.hash(keyField, AvroUtils.castToComparableStringClass(getKeyClass()));
  }

  @Override
  int hashSecondaryKeyMetadata() {
    return Objects.hash(
        keyFieldSecondary, AvroUtils.castToComparableStringClass(getKeyClassSecondary()));
  }

  @Override
  public Set<Class<? extends BucketMetadata>> compatibleMetadataTypes() {
    return ImmutableSet.of(AvroBucketMetadata.class, ParquetBucketMetadata.class);
  }

  @Override
  public K1 extractKeyPrimary(V value) {
    int[] path = keyPath.get();
    if (path == null) {
      path = AvroUtils.toKeyPath(keyField, getKeyClass(), value.getSchema());
      keyPath.compareAndSet(null, path);
    }
    return AvroBucketMetadata.extractKey(getKeyClass(), path, value);
  }

  @Override
  public K2 extractKeySecondary(V value) {
    verifyNotNull(keyFieldSecondary);
    verifyNotNull(getKeyClassSecondary());
    int[] path = keyPathSecondary.get();
    if (path == null) {
      path = AvroUtils.toKeyPath(keyFieldSecondary, getKeyClassSecondary(), value.getSchema());
      keyPathSecondary.compareAndSet(null, path);
    }
    return AvroBucketMetadata.extractKey(getKeyClassSecondary(), path, value);
  }

  @Override
  public void populateDisplayData(Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("keyFieldPrimary", keyField));
    if (keyFieldSecondary != null)
      builder.add(DisplayData.item("keyFieldSecondary", keyFieldSecondary));
    if (icebergFieldId != null)
      builder.add(DisplayData.item("icebergFieldId", icebergFieldId));
    builder.add(DisplayData.item("hashType", "ICEBERG"));
  }

  @Override
  <OtherKeyType> boolean keyClassMatches(Class<OtherKeyType> requestedReadType) {
    return super.keyClassMatches(requestedReadType)
        || AvroUtils.castToComparableStringClass(getKeyClass()) == requestedReadType
        || AvroUtils.castToComparableStringClass(requestedReadType) == getKeyClass();
  }

  @Override
  <OtherKeyType> boolean keyClassSecondaryMatches(Class<OtherKeyType> requestedReadType) {
    return super.keyClassSecondaryMatches(requestedReadType)
        || AvroUtils.castToComparableStringClass(getKeyClassSecondary()) == requestedReadType
        || AvroUtils.castToComparableStringClass(requestedReadType) == getKeyClassSecondary();
  }
}
