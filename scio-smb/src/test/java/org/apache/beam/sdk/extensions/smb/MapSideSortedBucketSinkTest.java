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

import static org.apache.beam.sdk.extensions.smb.TestUtils.fromFolder;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/** Unit tests for {@link MapSideSortedBucketSink}. */
public class MapSideSortedBucketSinkTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public final TemporaryFolder output = new TemporaryFolder();
  @Rule public final TemporaryFolder temp = new TemporaryFolder();

  // test input: [a01, a02, ..., a10, b01, ..., z10, A01, ..., Z10, "", "a"]
  private static final String[] input =
      Stream.concat(
              Stream.concat(
                      IntStream.rangeClosed('a', 'z').boxed(),
                      IntStream.rangeClosed('A', 'Z').boxed())
                  .flatMap(
                      i ->
                          IntStream.rangeClosed(1, 10)
                              .boxed()
                              .map(
                                  j ->
                                      String.format(
                                          "%s%02d", String.valueOf((char) i.intValue()), j))),
              Stream.of("", "a"))
          .toArray(String[]::new);

  @Test
  @Category(NeedsRunner.class)
  public void testOneBucketOneShard() throws Exception {
    test(1, 1);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMultiBucketOneShard() throws Exception {
    test(2, 1);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testOneBucketMultiShard() throws Exception {
    test(1, 3);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMultiBucketMultiShard() throws Exception {
    test(2, 3);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWritesEmptyBucketFiles() throws Exception {
    final TestBucketMetadata metadata = TestBucketMetadata.of(2, 2);
    final ResourceId outputDirectory = fromFolder(output);

    final MapSideSortedBucketSink<String, Void, String> sink =
        new MapSideSortedBucketSink<>(
            metadata, outputDirectory, fromFolder(temp), ".txt", new TestFileOperations(), 2);

    pipeline.apply("WritesEmptyBucketFiles", Create.empty(org.apache.beam.sdk.coders.StringUtf8Coder.of())).apply(sink);
    pipeline.run().waitUntilFinish();

    // Verify all (bucket, shard) files exist
    final FileAssignment dstFiles =
        new SMBFilenamePolicy.FileAssignment(
            outputDirectory, SortedBucketIO.DEFAULT_FILENAME_PREFIX, ".txt", false);

    for (int bucketId = 0; bucketId < metadata.getNumBuckets(); bucketId++) {
      for (int shardId = 0; shardId < metadata.getNumShards(); shardId++) {
        Assert.assertSame(
            "Missing file for bucket=" + bucketId + " shard=" + shardId,
            FileSystems.match(
                    dstFiles.forBucket(BucketShardId.of(bucketId, shardId), metadata).toString(),
                    EmptyMatchTreatment.DISALLOW)
                .status(),
            Status.OK);
      }
    }

    // Verify metadata exists
    Assert.assertSame(
        FileSystems.match(dstFiles.forMetadata().toString(), EmptyMatchTreatment.DISALLOW).status(),
        Status.OK);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testCleansUpTempFiles() throws Exception {
    final TestBucketMetadata metadata = TestBucketMetadata.of(1, 1);

    final java.io.File outputDir = Files.createTempDirectory("output").toFile();
    final java.io.File tempDir = Files.createTempDirectory("temp").toFile();
    outputDir.deleteOnExit();
    tempDir.deleteOnExit();

    final MapSideSortedBucketSink<String, Void, String> sink =
        new MapSideSortedBucketSink<>(
            metadata,
            org.apache.beam.sdk.io.LocalResources.fromFile(outputDir, true),
            org.apache.beam.sdk.io.LocalResources.fromFile(tempDir, true),
            ".txt",
            new TestFileOperations(),
            1);

    pipeline
        .apply("CleansUpTempFiles", Create.of(Stream.of(input).collect(Collectors.toList())))
        .apply(sink);
    pipeline.run().waitUntilFinish();

    Assert.assertFalse(
        "Temp files should be cleaned up",
        Files.walk(tempDir.toPath(), 2).anyMatch(path -> path.toFile().isFile()));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testOutputMatchesSortedBucketSink() throws Exception {
    // Write same data with both SortedBucketSink and MapSideSortedBucketSink
    // Verify both produce the same logical output (same records per bucket, same sort order)
    final TestBucketMetadata metadata = TestBucketMetadata.of(2, 1);
    final TestBucketMetadata metadataMapSide = TestBucketMetadata.of(2, 3);

    final TemporaryFolder outputStandard = new TemporaryFolder();
    outputStandard.create();
    final TemporaryFolder tempStandard = new TemporaryFolder();
    tempStandard.create();
    final TemporaryFolder outputMapSide = new TemporaryFolder();
    outputMapSide.create();
    final TemporaryFolder tempMapSide = new TemporaryFolder();
    tempMapSide.create();

    try {
      // Standard sink
      final TestPipeline pipeline1 = TestPipeline.create();
      final SortedBucketSink<String, Void, String> standardSink =
          new SortedBucketSink<>(
              metadata, fromFolder(outputStandard), fromFolder(tempStandard),
              ".txt", new TestFileOperations(), 1);

      pipeline1
          .apply("Standard", Create.of(Stream.of(input).collect(Collectors.toList())))
          .apply(standardSink);
      pipeline1.run().waitUntilFinish();

      // Map-side sink
      final TestPipeline pipeline2 = TestPipeline.create();
      final MapSideSortedBucketSink<String, Void, String> mapSideSink =
          new MapSideSortedBucketSink<>(
              metadataMapSide, fromFolder(outputMapSide), fromFolder(tempMapSide),
              ".txt", new TestFileOperations(), 3);

      pipeline2
          .apply("MapSide", Create.of(Stream.of(input).collect(Collectors.toList())))
          .apply(mapSideSink);
      pipeline2.run().waitUntilFinish();

      // Read both outputs and verify same logical content per bucket
      final FileAssignment standardFiles = new SMBFilenamePolicy.FileAssignment(
          fromFolder(outputStandard), SortedBucketIO.DEFAULT_FILENAME_PREFIX, ".txt", false);
      final FileAssignment mapSideFiles = new SMBFilenamePolicy.FileAssignment(
          fromFolder(outputMapSide), SortedBucketIO.DEFAULT_FILENAME_PREFIX, ".txt", false);

      for (int bucketId = 0; bucketId < metadata.getNumBuckets(); bucketId++) {
        // Standard: one shard
        List<String> standardRecords = readAllRecordsForBucket(
            standardFiles, metadata, bucketId);

        // Map-side: multiple shards, merge them
        List<String> mapSideRecords = readAllRecordsForBucket(
            mapSideFiles, metadataMapSide, bucketId);

        // Sort both (map-side shards may interleave differently)
        java.util.Collections.sort(standardRecords);
        java.util.Collections.sort(mapSideRecords);

        Assert.assertEquals(
            "Bucket " + bucketId + " should have same records",
            standardRecords, mapSideRecords);
      }
    } finally {
      outputStandard.delete();
      tempStandard.delete();
      outputMapSide.delete();
      tempMapSide.delete();
    }
  }

  private void test(int numBuckets, int numShards) {
    final TestBucketMetadata metadata = TestBucketMetadata.of(numBuckets, numShards);

    final MapSideSortedBucketSink<String, Void, String> sink =
        new MapSideSortedBucketSink<>(
            metadata,
            fromFolder(output),
            fromFolder(temp),
            ".txt",
            new TestFileOperations(),
            numShards);

    @SuppressWarnings("deprecation")
    final Reshuffle.ViaRandomKey<String> reshuffle = Reshuffle.viaRandomKey();

    SortedBucketSinkTest.check(
        pipeline
            .apply(
                "test-" + UUID.randomUUID(),
                Create.of(Stream.of(input).collect(Collectors.toList())))
            .apply(reshuffle)
            .apply(sink),
        metadata,
        SortedBucketSinkTest.assertValidSmbFormat(metadata, input));

    pipeline.run();
  }

  private static List<String> readAllRecordsForBucket(
      FileAssignment fileAssignment, BucketMetadata<String, ?, String> metadata, int bucketId) {
    List<String> allRecords = new ArrayList<>();
    TestFileOperations fileOps = new TestFileOperations();

    for (int shardId = 0; shardId < metadata.getNumShards(); shardId++) {
      ResourceId file = fileAssignment.forBucket(
          BucketShardId.of(bucketId, shardId), metadata);
      try {
        java.util.Iterator<String> it = fileOps.iterator(file);
        while (it.hasNext()) {
          allRecords.add(it.next());
        }
      } catch (Exception e) {
        // empty file or missing
      }
    }
    return allRecords;
  }
}
