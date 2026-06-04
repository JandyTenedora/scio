/*
 * Copyright 2019 Spotify AB.
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
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.smb.BucketShardId.BucketShardIdCoder;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
import org.apache.beam.sdk.io.fs.MoveOptions.StandardMoveOptions;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An alternative to {@link SortedBucketSink} that eliminates the {@link
 * org.apache.beam.sdk.transforms.GroupByKey} shuffle by having each worker locally partition, sort,
 * and write its own data.
 *
 * <p>Each worker hashes its records to bucket IDs, buffers them per bucket, sorts each bucket's
 * buffer in memory, and writes directly to shard files. Multiple workers may write to the same
 * logical shard — collisions are resolved by merge-sorting temp files during finalization.
 *
 * <p>The output is standard SMB format, fully compatible with {@link SortedBucketSource} and {@link
 * org.apache.beam.sdk.extensions.smb.SortedBucketIO.Read}. No changes to the read path are
 * required.
 *
 * @param <K1> the type of the primary sort key
 * @param <K2> the type of the secondary sort key (Void if not used)
 * @param <V> the type of the values
 */
public class MapSideSortedBucketSink<K1, K2, V> extends PTransform<PCollection<V>, WriteResult> {
  private static final Logger LOG = LoggerFactory.getLogger(MapSideSortedBucketSink.class);
  private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger(0);
  static final byte[] NULL_SORT_KEY = new byte[0];

  private final BucketMetadata<K1, K2, V> bucketMetadata;
  private final SMBFilenamePolicy filenamePolicy;
  private final ResourceId tempDirectory;
  private final FileOperations<V> fileOperations;
  private final int numShards;
  private final int keyCacheSize;

  public MapSideSortedBucketSink(
      BucketMetadata<K1, K2, V> bucketMetadata,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      String filenameSuffix,
      FileOperations<V> fileOperations,
      int numShards) {
    this(bucketMetadata, outputDirectory, tempDirectory, filenameSuffix, fileOperations, numShards, 0);
  }

  public MapSideSortedBucketSink(
      BucketMetadata<K1, K2, V> bucketMetadata,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      String filenameSuffix,
      FileOperations<V> fileOperations,
      int numShards,
      int keyCacheSize) {
    Preconditions.checkArgument(numShards > 0, "numShards must be positive, got %s", numShards);
    this.bucketMetadata = bucketMetadata;
    this.filenamePolicy =
        new SMBFilenamePolicy(outputDirectory, bucketMetadata.getFilenamePrefix(), filenameSuffix);
    this.tempDirectory = tempDirectory;
    this.fileOperations = fileOperations;
    this.numShards = numShards;
    this.keyCacheSize = keyCacheSize;
  }

  @Override
  public WriteResult expand(PCollection<V> input) {
    Preconditions.checkArgument(
        input.isBounded() == IsBounded.BOUNDED,
        "MapSideSortedBucketSink cannot be applied to a non-bounded PCollection");

    final Coder<V> valueCoder = input.getCoder();

    Preconditions.checkArgument(
        bucketMetadata.getNumShards() == numShards,
        "BucketMetadata numShards (%s) must match MapSideSortedBucketSink numShards (%s). "
            + "Construct your BucketMetadata with the desired numShards.",
        bucketMetadata.getNumShards(),
        numShards);

    final PCollection<KV<BucketShardId, ResourceId>> writtenFiles =
        input.apply(
            "MapSideBucketWrite",
            ParDo.of(
                new MapSideBucketWriteDoFn<>(
                    bucketMetadata,
                    filenamePolicy.forTempFiles(tempDirectory),
                    fileOperations,
                    valueCoder,
                    numShards)));

    return writtenFiles.apply(
        "FinalizeTempFiles",
        new FinalizeBuckets<>(
            filenamePolicy.forTempFiles(tempDirectory).getDirectory(),
            filenamePolicy.forDestination(),
            bucketMetadata,
            fileOperations,
            valueCoder));
  }

  /**
   * Each DoFn instance buffers records per bucket, sorts locally, and writes to temp shard files.
   * No GroupByKey — purely element-wise with state flushed on FinishBundle.
   */
  static class MapSideBucketWriteDoFn<K1, K2, V>
      extends DoFn<V, KV<BucketShardId, ResourceId>> {

    private final BucketMetadata<K1, K2, V> bucketMetadata;
    private final FileAssignment tempFileAssignment;
    private final FileOperations<V> fileOperations;
    private final Coder<V> valueCoder;
    private final int numShards;
    private final Comparator<byte[]> bytesComparator = UnsignedBytes.lexicographicalComparator();

    private final Counter recordsWritten;
    private final Counter bucketsWritten;

    private transient int shardId;
    private transient Map<Integer, List<byte[]>> buffers;

    MapSideBucketWriteDoFn(
        BucketMetadata<K1, K2, V> bucketMetadata,
        FileAssignment tempFileAssignment,
        FileOperations<V> fileOperations,
        Coder<V> valueCoder,
        int numShards) {
      this.bucketMetadata = bucketMetadata;
      this.tempFileAssignment = tempFileAssignment;
      this.fileOperations = fileOperations;
      this.valueCoder = valueCoder;
      this.numShards = numShards;
      this.recordsWritten =
          Metrics.counter(MapSideSortedBucketSink.class, "mapSide-recordsWritten");
      this.bucketsWritten =
          Metrics.counter(MapSideSortedBucketSink.class, "mapSide-bucketsWritten");
    }

    @Setup
    public void setup() {
      shardId = INSTANCE_COUNTER.getAndIncrement() % numShards;
    }

    @StartBundle
    public void startBundle() {
      buffers = new HashMap<>();
    }

    @ProcessElement
    public void processElement(@Element V record) throws IOException {
      final byte[] keyBytes = bucketMetadata.getKeyBytesPrimary(record);
      final int bucketId =
          keyBytes != null ? bucketMetadata.getBucketId(keyBytes) : BucketShardId.ofNullKey().getBucketId();
      final byte[] encodedRecord = CoderUtils.encodeToByteArray(valueCoder, record);
      buffers.computeIfAbsent(bucketId, k -> new ArrayList<>()).add(encodedRecord);
      recordsWritten.inc();
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) throws IOException {
      for (Map.Entry<Integer, List<byte[]>> entry : buffers.entrySet()) {
        final int bucketId = entry.getKey();
        final List<byte[]> records = entry.getValue();

        records.sort(
            (a, b) -> {
              try {
                final V va = CoderUtils.decodeFromByteArray(valueCoder, a);
                final V vb = CoderUtils.decodeFromByteArray(valueCoder, b);
                final byte[] ka = bucketMetadata.getKeyBytesPrimary(va);
                final byte[] kb = bucketMetadata.getKeyBytesPrimary(vb);
                return bytesComparator.compare(
                    ka != null ? ka : NULL_SORT_KEY,
                    kb != null ? kb : NULL_SORT_KEY);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });

        final BucketShardId bucketShardId = BucketShardId.of(bucketId, shardId);
        final ResourceId tmpFile = tempFileAssignment.forBucket(bucketShardId, bucketMetadata);

        try (FileOperations.Writer<V> writer = fileOperations.createWriter(tmpFile)) {
          for (byte[] encodedRecord : records) {
            writer.write(CoderUtils.decodeFromByteArray(valueCoder, encodedRecord));
          }
        }

        c.output(
            KV.of(bucketShardId, tmpFile),
            org.joda.time.Instant.now(),
            org.apache.beam.sdk.transforms.windowing.GlobalWindow.INSTANCE);
        bucketsWritten.inc();
      }

      buffers.clear();
    }
  }

  /**
   * Collects all written temp files and moves them to final destinations. Handles shard collisions
   * by merge-sorting multiple temp files for the same (bucket, shard). Writes empty files for any
   * (bucket, shard) gaps.
   */
  static class FinalizeBuckets<K1, K2, V>
      extends PTransform<PCollection<KV<BucketShardId, ResourceId>>, WriteResult> {

    private final ResourceId tempDirectory;
    private final FileAssignment dstFileAssignment;
    private final BucketMetadata<K1, K2, V> bucketMetadata;
    private final FileOperations<V> fileOperations;
    private final Coder<V> valueCoder;

    FinalizeBuckets(
        ResourceId tempDirectory,
        FileAssignment dstFileAssignment,
        BucketMetadata<K1, K2, V> bucketMetadata,
        FileOperations<V> fileOperations,
        Coder<V> valueCoder) {
      this.tempDirectory = tempDirectory;
      this.dstFileAssignment = dstFileAssignment;
      this.bucketMetadata = bucketMetadata;
      this.fileOperations = fileOperations;
      this.valueCoder = valueCoder;
    }

    @Override
    public WriteResult expand(PCollection<KV<BucketShardId, ResourceId>> input) {
      final PCollectionView<Map<BucketShardId, Iterable<ResourceId>>> writtenBuckets =
          input.apply("WrittenBucketShardIds", View.asMultimap());

      final TupleTag<KV<BucketShardId, ResourceId>> bucketsTag = new TupleTag<>("writtenBuckets");
      final TupleTag<ResourceId> metadataTag = new TupleTag<>("writtenMetadata");

      PCollectionTuple result =
          input
              .getPipeline()
              .apply("InitializeFinalize", Create.of(0))
              .apply(
                  "PopulateFinalDst",
                  ParDo.of(
                          new DoFn<Integer, KV<BucketShardId, ResourceId>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) throws IOException {
                              finalizeFiles(
                                  tempDirectory,
                                  bucketMetadata,
                                  c.sideInput(writtenBuckets),
                                  dstFileAssignment,
                                  fileOperations,
                                  valueCoder,
                                  bucketDst -> c.output(bucketsTag, bucketDst),
                                  metadataDst -> c.output(metadataTag, metadataDst));
                            }
                          })
                      .withSideInputs(writtenBuckets)
                      .withOutputTags(bucketsTag, TupleTagList.of(metadataTag)));

      return new WriteResult(
          input.getPipeline(),
          result.get(metadataTag).setCoder(ResourceIdCoder.of()),
          result.get(bucketsTag).setCoder(KvCoder.of(BucketShardIdCoder.of(), ResourceIdCoder.of())));
    }

    static <K1, K2, V> void finalizeFiles(
        ResourceId tempDirectory,
        BucketMetadata<K1, K2, V> bucketMetadata,
        Map<BucketShardId, Iterable<ResourceId>> writtenTmpBuckets,
        FileAssignment dstFileAssignment,
        FileOperations<V> fileOperations,
        Coder<V> valueCoder,
        java.util.function.Consumer<KV<BucketShardId, ResourceId>> bucketDstConsumer,
        java.util.function.Consumer<ResourceId> metadataDstConsumer)
        throws IOException {

      final List<ResourceId> srcFiles = new ArrayList<>();
      final List<ResourceId> dstFiles = new ArrayList<>();
      final List<ResourceId> filesToDelete = new ArrayList<>();

      final Set<BucketShardId> allBucketShardIds = bucketMetadata.getAllBucketShardIds();
      allBucketShardIds.add(BucketShardId.ofNullKey());

      for (BucketShardId id : allBucketShardIds) {
        final ResourceId finalDst = dstFileAssignment.forBucket(id, bucketMetadata);
        final Iterable<ResourceId> tmpFiles = writtenTmpBuckets.get(id);

        if (tmpFiles == null) {
          // No data for this (bucket, shard) — write empty file
          fileOperations.createWriter(finalDst).close();
          bucketDstConsumer.accept(KV.of(id, finalDst));

        } else {
          List<ResourceId> tmpFileList = new ArrayList<>();
          tmpFiles.forEach(tmpFileList::add);

          if (tmpFileList.size() == 1) {
            // Single temp file — rename to final destination
            srcFiles.add(tmpFileList.get(0));
            dstFiles.add(finalDst);
            bucketDstConsumer.accept(KV.of(id, finalDst));

          } else {
            // Multiple temp files (shard collision) — merge-sort into final file
            mergeSortFiles(tmpFileList, finalDst, bucketMetadata, fileOperations, valueCoder);
            bucketDstConsumer.accept(KV.of(id, finalDst));
            filesToDelete.addAll(tmpFileList);
          }
        }
      }

      LOG.info(
          "Moving {} bucket files into {}", srcFiles.size(), dstFileAssignment.getDirectory());
      FileSystems.rename(
          srcFiles,
          dstFiles,
          StandardMoveOptions.IGNORE_MISSING_FILES,
          StandardMoveOptions.SKIP_IF_DESTINATION_EXISTS);

      if (!filesToDelete.isEmpty()) {
        FileSystems.delete(filesToDelete, StandardMoveOptions.IGNORE_MISSING_FILES);
      }

      // Write metadata
      final ResourceId metadataDst =
          SortedBucketSink.RenameBuckets.writeMetadataFile(
              dstFileAssignment.forMetadata(), bucketMetadata);
      metadataDstConsumer.accept(metadataDst);

      // Clean up temp directory
      final List<ResourceId> remainingTempFiles =
          FileSystems.match(Collections.singletonList(tempDirectory.toString() + "*")).stream()
              .flatMap(m -> {
                try {
                  return m.metadata().stream().map(meta -> meta.resourceId());
                } catch (IOException e) {
                  return java.util.stream.Stream.empty();
                }
              })
              .collect(Collectors.toList());
      FileSystems.delete(remainingTempFiles, StandardMoveOptions.IGNORE_MISSING_FILES);
      FileSystems.delete(
          Collections.singletonList(tempDirectory), StandardMoveOptions.IGNORE_MISSING_FILES);
    }

    private static <K1, K2, V> void mergeSortFiles(
        List<ResourceId> tmpFiles,
        ResourceId finalDst,
        BucketMetadata<K1, K2, V> bucketMetadata,
        FileOperations<V> fileOperations,
        Coder<V> valueCoder)
        throws IOException {
      final Comparator<byte[]> bytesComparator = UnsignedBytes.lexicographicalComparator();

      // Open iterators on all temp files
      final List<java.util.PriorityQueue<KV<byte[], V>>> sources = new ArrayList<>();
      // Use a priority queue for k-way merge
      java.util.PriorityQueue<KV<byte[], KV<Integer, V>>> mergeQueue =
          new java.util.PriorityQueue<>(
              (a, b) -> bytesComparator.compare(a.getKey(), b.getKey()));

      @SuppressWarnings("unchecked")
      final List<java.util.Iterator<V>> iterators = new ArrayList<>();
      for (ResourceId tmpFile : tmpFiles) {
        iterators.add(fileOperations.iterator(tmpFile));
      }

      // Seed the priority queue with the first record from each iterator
      for (int i = 0; i < iterators.size(); i++) {
        if (iterators.get(i).hasNext()) {
          V record = iterators.get(i).next();
          byte[] keyBytes = bucketMetadata.getKeyBytesPrimary(record);
          mergeQueue.add(
              KV.of(
                  keyBytes != null ? keyBytes : NULL_SORT_KEY,
                  KV.of(i, record)));
        }
      }

      // Write merged output
      try (FileOperations.Writer<V> writer = fileOperations.createWriter(finalDst)) {
        while (!mergeQueue.isEmpty()) {
          KV<byte[], KV<Integer, V>> head = mergeQueue.poll();
          int sourceIdx = head.getValue().getKey();
          V record = head.getValue().getValue();
          writer.write(record);

          // Advance the iterator that produced this record
          if (iterators.get(sourceIdx).hasNext()) {
            V next = iterators.get(sourceIdx).next();
            byte[] keyBytes = bucketMetadata.getKeyBytesPrimary(next);
            mergeQueue.add(
                KV.of(
                    keyBytes != null ? keyBytes : NULL_SORT_KEY,
                    KV.of(sourceIdx, next)));
          }
        }
      }
    }
  }
}
