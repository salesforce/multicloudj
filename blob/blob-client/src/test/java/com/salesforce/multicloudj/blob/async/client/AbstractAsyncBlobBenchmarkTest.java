package com.salesforce.multicloudj.blob.async.client;

import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStore;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.MultipartUploadResponse;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMH benchmarks for async blob operations via {@link AsyncBucketClient}.
 *
 * <p>Covers single-object upload/download (small/medium/large), write-read-delete lifecycle,
 * multipart upload, metadata, list, listPage, copy, and directory upload/download.
 *
 * <p>Single-object benchmarks use class-level defaults (3 warmup x 2s + 5 measurement x 3s).
 * Directory benchmarks override with method-level annotations (1 warmup x 60s + 5 measurement
 * x 60s) because each op takes seconds to minutes.
 *
 * <p>Intentionally avoids {@code @Setup(Level.Invocation)} because JMH does not reliably invoke
 * it on every worker thread in Throughput mode. Per-invocation resources are set up inside the
 * benchmark methods themselves.
 *
 * <p>Directory benchmarks use only 1 warmup iteration (vs JMH's recommended 3-5) because the
 * bottleneck is network/cloud IO rather than JVM hot-path compilation — C2 optimizations save
 * microseconds while each operation takes seconds.
 */
@BenchmarkMode({Mode.SampleTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(AbstractAsyncBlobBenchmarkTest.THREAD_COUNT)
public abstract class AbstractAsyncBlobBenchmarkTest {

  private static final Logger log = LoggerFactory.getLogger(AbstractAsyncBlobBenchmarkTest.class);

  protected static final int SMALL_FILE = 1024; // 1KB
  protected static final int MEDIUM_FILE = 1024 * 1024; // 1MB
  protected static final int LARGE_FILE = 10 * 1024 * 1024; // 10MB

  // Sized to keep per-invocation request rate under S3/GCS per-prefix limits at THREAD_COUNT=4.
  protected static final int DIR_SMALL_COUNT = 50;
  protected static final int DIR_MEDIUM_COUNT = 10;
  protected static final int DIR_LARGE_COUNT = 5;

  protected static final int PART_SIZE = 5 * 1024 * 1024;

  static final int THREAD_COUNT = 4;
  private static final long OP_TIMEOUT_SECONDS = 600;

  protected AsyncBucketClient asyncClient;

  protected Path dirTempRoot;
  protected Path dirSmallSource;
  protected Path dirMediumSource;
  protected Path dirLargeSource;

  protected List<String> dirSmallDownloadPrefixes;
  protected List<String> dirMediumDownloadPrefixes;
  protected List<String> dirLargeDownloadPrefixes;

  private final ThreadLocal<String> threadUploadPrefix = new ThreadLocal<>();

  // All per-thread upload prefixes ever assigned, so trial teardown can delete them.
  private final ConcurrentLinkedQueue<String> allUploadPrefixes = new ConcurrentLinkedQueue<>();

  // Single-object benchmark state
  private static final int SMALL_COUNT = 100;
  private static final int MEDIUM_COUNT = 20;
  private static final int LARGE_COUNT = 5;

  private byte[] smallBlob;
  private byte[] mediumBlob;
  private byte[] largeBlob;
  private List<String> smallKeys;
  private List<String> mediumKeys;
  private List<String> largeKeys;
  private String copySourceKey;

  private final AtomicInteger nextUploadSmallId = new AtomicInteger(0);
  private final AtomicInteger nextUploadMediumId = new AtomicInteger(0);
  private final AtomicInteger nextUploadLargeId = new AtomicInteger(0);
  private final AtomicInteger nextWriteReadDeleteId = new AtomicInteger(0);
  private final AtomicInteger nextMultipartUploadId = new AtomicInteger(0);
  private final AtomicInteger nextCopyId = new AtomicInteger(0);

  private String bucketName;
  private Harness harness;

  public interface Harness extends AutoCloseable {
    AsyncBlobStore createAsyncBlobStore();
  }

  protected abstract static class BaseHarnessImpl implements Harness {
    private AsyncBlobStore store;

    protected abstract AsyncBlobStore buildStore();

    @Override
    public final AsyncBlobStore createAsyncBlobStore() {
      try {
        store = buildStore();
        return store;
      } catch (Exception e) {
        throw new RuntimeException("Failed to create async blob store", e);
      }
    }

    @Override
    public final void close() {
      if (store != null) {
        try {
          store.close();
        } catch (Exception e) {
          throw new RuntimeException("Failed to close async blob store", e);
        }
      }
    }
  }

  protected abstract Harness createHarness();

  /** Returns provider ID for JMH result file naming, e.g., "aws", "gcp". */
  protected abstract String getProviderId();

  @Setup(Level.Trial)
  public void setupBenchmark() {
    log.info("Creating {} async blob store", getProviderId());
    try {
      harness = createHarness();
      AsyncBlobStore store = harness.createAsyncBlobStore();
      bucketName = store.getBucket();
      asyncClient = new AsyncBucketClient(store);
      cleanupSingleObjectData();
      setupDirectoryData();
      setupSingleObjectData();
    } catch (IOException e) {
      throw new RuntimeException("Failed to setup async benchmark", e);
    }
  }

  @TearDown(Level.Trial)
  public void teardownBenchmark() {
    try {
      teardownDirectoryData();
      cleanupSingleObjectData();
    } finally {
      threadUploadPrefix.remove();
      if (harness != null) {
        try {
          harness.close();
        } catch (Exception e) {
          log.warn("Failed to close {} async blob store", getProviderId(), e);
        }
      }
    }
  }

  @Benchmark
  @Warmup(iterations = 1, time = 60, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 5, time = 60, timeUnit = TimeUnit.SECONDS)
  public void benchmarkUploadDirectorySmall(Blackhole bh) {
    runDirectoryUpload(bh, dirSmallSource);
  }

  @Benchmark
  @Warmup(iterations = 1, time = 60, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 5, time = 60, timeUnit = TimeUnit.SECONDS)
  public void benchmarkUploadDirectoryMedium(Blackhole bh) {
    runDirectoryUpload(bh, dirMediumSource);
  }

  @Benchmark
  @Warmup(iterations = 1, time = 60, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 5, time = 60, timeUnit = TimeUnit.SECONDS)
  public void benchmarkUploadDirectoryLarge(Blackhole bh) {
    runDirectoryUpload(bh, dirLargeSource);
  }

  @Benchmark
  @Warmup(iterations = 1, time = 60, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 5, time = 60, timeUnit = TimeUnit.SECONDS)
  public void benchmarkDownloadDirectorySmall(Blackhole bh) throws IOException {
    runDirectoryDownload(bh, pickRandom(dirSmallDownloadPrefixes));
  }

  @Benchmark
  @Warmup(iterations = 1, time = 60, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 5, time = 60, timeUnit = TimeUnit.SECONDS)
  public void benchmarkDownloadDirectoryMedium(Blackhole bh) throws IOException {
    runDirectoryDownload(bh, pickRandom(dirMediumDownloadPrefixes));
  }

  @Benchmark
  @Warmup(iterations = 1, time = 60, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 5, time = 60, timeUnit = TimeUnit.SECONDS)
  public void benchmarkDownloadDirectoryLarge(Blackhole bh) throws IOException {
    runDirectoryDownload(bh, pickRandom(dirLargeDownloadPrefixes));
  }

  private void runDirectoryUpload(Blackhole bh, Path source) {
    DirectoryUploadRequest req =
        DirectoryUploadRequest.builder()
            .localSourceDirectory(source.toString())
            .prefix(threadUploadPrefix())
            .includeSubFolders(true)
            .build();
    bh.consume(
        asyncClient.uploadDirectory(req).orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join());
  }

  private void runDirectoryDownload(Blackhole bh, String prefix) throws IOException {
    Path dest = Files.createTempDirectory(dirTempRoot, "download-");
    try {
      DirectoryDownloadRequest req =
          DirectoryDownloadRequest.builder()
              .prefixToDownload(prefix)
              .localDestinationDirectory(dest.toString())
              .build();
      bh.consume(
          asyncClient
              .downloadDirectory(req)
              .orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
              .join());
    } finally {
      deleteLocalRecursive(dest);
    }
  }

  /** Returns this thread's upload prefix, creating and registering one on first use. */
  private String threadUploadPrefix() {
    String prefix = threadUploadPrefix.get();
    if (prefix == null) {
      prefix = "dir-bench-" + UUID.randomUUID() + "/";
      threadUploadPrefix.set(prefix);
      allUploadPrefixes.add(prefix);
    }
    return prefix;
  }

  private static String pickRandom(List<String> prefixes) {
    return prefixes.get(ThreadLocalRandom.current().nextInt(prefixes.size()));
  }

  private void setupDirectoryData() throws IOException {
    dirTempRoot = Files.createTempDirectory("mcj-async-dir-bench-");
    dirSmallSource = createLocalCorpus("small", DIR_SMALL_COUNT, SMALL_FILE);
    dirMediumSource = createLocalCorpus("medium", DIR_MEDIUM_COUNT, MEDIUM_FILE);
    dirLargeSource = createLocalCorpus("large", DIR_LARGE_COUNT, LARGE_FILE);

    dirSmallDownloadPrefixes = uploadCorpusShards(dirSmallSource, "small");
    dirMediumDownloadPrefixes = uploadCorpusShards(dirMediumSource, "medium");
    dirLargeDownloadPrefixes = uploadCorpusShards(dirLargeSource, "large");
  }

  private List<String> uploadCorpusShards(Path source, String sizeTag) {
    List<String> prefixes = new ArrayList<>(THREAD_COUNT);
    for (int i = 0; i < THREAD_COUNT; i++) {
      String prefix = "async-dir-bench-corpus-" + sizeTag + "-" + UUID.randomUUID() + "/";
      uploadCorpus(source, prefix);
      prefixes.add(prefix);
    }
    return prefixes;
  }

  private void teardownDirectoryData() {
    safeDeleteDirectoryPrefixes(dirSmallDownloadPrefixes);
    safeDeleteDirectoryPrefixes(dirMediumDownloadPrefixes);
    safeDeleteDirectoryPrefixes(dirLargeDownloadPrefixes);
    for (String prefix : allUploadPrefixes) {
      safeDeleteDirectoryPrefix(prefix);
    }
    allUploadPrefixes.clear();
    deleteLocalRecursive(dirTempRoot);
  }

  private void safeDeleteDirectoryPrefixes(List<String> prefixes) {
    if (prefixes == null) {
      return;
    }
    for (String prefix : prefixes) {
      safeDeleteDirectoryPrefix(prefix);
    }
  }

  private Path createLocalCorpus(String name, int count, int fileSize) throws IOException {
    Path dir = Files.createDirectory(dirTempRoot.resolve(name));
    Random rnd = new Random(42);
    byte[] buf = new byte[fileSize];
    for (int i = 0; i < count; i++) {
      rnd.nextBytes(buf);
      Files.write(dir.resolve("file_" + i + ".dat"), buf);
    }
    return dir;
  }

  private void uploadCorpus(Path source, String prefix) {
    DirectoryUploadRequest req =
        DirectoryUploadRequest.builder()
            .localSourceDirectory(source.toString())
            .prefix(prefix)
            .includeSubFolders(true)
            .build();
    asyncClient.uploadDirectory(req).join();
  }

  private void safeDeleteDirectoryPrefix(String prefix) {
    if (prefix == null || asyncClient == null) {
      return;
    }
    try {
      asyncClient.deleteDirectory(prefix).orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
    } catch (Exception e) {
      // Best-effort cleanup.
      log.warn("Failed to delete benchmark prefix {}", prefix, e);
    }
  }

  // ─── Single-object benchmark methods ───

  @Benchmark
  public void benchmarkUploadSmall(Blackhole bh) {
    String key = "bench-upload-small/" + nextUploadSmallId.incrementAndGet() + ".dat";
    UploadRequest request =
        new UploadRequest.Builder().withKey(key).withContentLength(smallBlob.length).build();
    UploadResponse response =
        asyncClient.upload(request, smallBlob)
            .orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
    bh.consume(response.getETag());
  }

  @Benchmark
  public void benchmarkUploadMedium(Blackhole bh) {
    String key = "bench-upload-medium/" + nextUploadMediumId.incrementAndGet() + ".dat";
    UploadRequest request =
        new UploadRequest.Builder().withKey(key).withContentLength(mediumBlob.length).build();
    UploadResponse response =
        asyncClient.upload(request, mediumBlob)
            .orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
    bh.consume(response.getETag());
  }

  // 10MB upload needs longer iterations to collect enough samples for stable P99.
  @Benchmark
  @Measurement(iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
  public void benchmarkUploadLarge(Blackhole bh) {
    String key = "bench-upload-large/" + nextUploadLargeId.incrementAndGet() + ".dat";
    UploadRequest request =
        new UploadRequest.Builder().withKey(key).withContentLength(largeBlob.length).build();
    UploadResponse response =
        asyncClient.upload(request, largeBlob)
            .orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
    bh.consume(response.getETag());
  }

  @Benchmark
  public void benchmarkDownloadSmall(Blackhole bh) {
    String key = pickRandom(smallKeys);
    ByteArray byteArray = new ByteArray();
    DownloadRequest request = new DownloadRequest.Builder().withKey(key).build();
    asyncClient.download(request, byteArray)
        .orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
    bh.consume(byteArray.getBytes());
  }

  @Benchmark
  public void benchmarkDownloadMedium(Blackhole bh) {
    String key = pickRandom(mediumKeys);
    ByteArray byteArray = new ByteArray();
    DownloadRequest request = new DownloadRequest.Builder().withKey(key).build();
    asyncClient.download(request, byteArray)
        .orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
    bh.consume(byteArray.getBytes());
  }

  // 10MB download needs longer iterations to collect enough samples for stable P99.
  @Benchmark
  @Measurement(iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
  public void benchmarkDownloadLarge(Blackhole bh) {
    String key = pickRandom(largeKeys);
    ByteArray byteArray = new ByteArray();
    DownloadRequest request = new DownloadRequest.Builder().withKey(key).build();
    asyncClient.download(request, byteArray)
        .orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
    bh.consume(byteArray.getBytes());
  }

  @Benchmark
  public void benchmarkWriteReadDelete(Blackhole bh) {
    String key = "bench-write-read-delete/" + nextWriteReadDeleteId.incrementAndGet() + ".dat";
    UploadRequest uploadRequest =
        new UploadRequest.Builder().withKey(key).withContentLength(smallBlob.length).build();
    UploadResponse uploadResponse =
        asyncClient.upload(uploadRequest, smallBlob)
            .orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
    bh.consume(uploadResponse.getETag());

    ByteArray byteArray = new ByteArray();
    DownloadRequest downloadRequest = new DownloadRequest.Builder().withKey(key).build();
    asyncClient.download(downloadRequest, byteArray)
        .orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
    bh.consume(byteArray.getBytes());

    asyncClient.delete(key, null).orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
  }

  // Explicit multipart API (initiate + uploadParts + complete).
  // 10MB serial upload needs longer iterations to collect enough samples for stable P99.
  @Benchmark
  @Measurement(iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
  public void benchmarkMultipartUpload(Blackhole bh) {
    String key = "bench-multipart/" + nextMultipartUploadId.incrementAndGet() + ".dat";
    MultipartUploadRequest request = new MultipartUploadRequest.Builder().withKey(key).build();
    MultipartUpload mpu =
        asyncClient.initiateMultipartUpload(request)
            .orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join();

    List<UploadPartResponse> partResponses = new ArrayList<>();
    int numParts = (int) Math.ceil((double) largeBlob.length / PART_SIZE);
    for (int partNum = 1; partNum <= numParts; partNum++) {
      int startIndex = (partNum - 1) * PART_SIZE;
      int endIndex = Math.min(startIndex + PART_SIZE, largeBlob.length);
      byte[] partData = Arrays.copyOfRange(largeBlob, startIndex, endIndex);
      MultipartPart part = new MultipartPart(partNum, partData);
      UploadPartResponse partResponse =
          asyncClient.uploadMultipartPart(mpu, part)
              .orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
      partResponses.add(partResponse);
    }

    MultipartUploadResponse completeResponse =
        asyncClient.completeMultipartUpload(mpu, partResponses)
            .orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
    bh.consume(completeResponse.getEtag());
  }

  // HEAD request only — file size does not affect latency, so a single small key suffices.
  @Benchmark
  public void benchmarkGetMetadata(Blackhole bh) {
    String key = pickRandom(smallKeys);
    BlobMetadata metadata =
        asyncClient.getMetadata(key, null)
            .orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
    bh.consume(metadata);
  }

  @Benchmark
  public void benchmarkList(Blackhole bh) {
    List<BlobInfo> results = new ArrayList<>();
    ListBlobsRequest request =
        ListBlobsRequest.builder().withPrefix("bench-download/small/").build();
    asyncClient.list(request, batch -> results.addAll(batch.getBlobs()))
        .orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
    bh.consume(results.size());
  }

  @Benchmark
  public void benchmarkListPage(Blackhole bh) {
    ListBlobsPageRequest request = ListBlobsPageRequest.builder()
        .withPrefix("bench-download/small/")
        .withMaxResults(20)
        .build();
    ListBlobsPageResponse response =
        asyncClient.listPage(request)
            .orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
    bh.consume(response.getBlobs().size());
  }

  @Benchmark
  public void benchmarkCopy(Blackhole bh) {
    String destKey = "bench-copy-dest/" + nextCopyId.incrementAndGet() + ".dat";
    CopyRequest request = CopyRequest.builder()
        .srcKey(copySourceKey)
        .destBucket(bucketName)
        .destKey(destKey)
        .build();
    CopyResponse response =
        asyncClient.copy(request)
            .orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
    bh.consume(response);
  }

  // ─── Single-object data setup/teardown helpers ───

  private void setupSingleObjectData() {
    Random rnd = new Random(42);
    smallBlob = new byte[SMALL_FILE];
    rnd.nextBytes(smallBlob);
    mediumBlob = new byte[MEDIUM_FILE];
    rnd.nextBytes(mediumBlob);
    largeBlob = new byte[LARGE_FILE];
    rnd.nextBytes(largeBlob);

    smallKeys = new ArrayList<>(SMALL_COUNT);
    for (int i = 0; i < SMALL_COUNT; i++) {
      String key = "bench-download/small/blob_" + i + ".dat";
      uploadBlob(key, smallBlob);
      smallKeys.add(key);
    }

    mediumKeys = new ArrayList<>(MEDIUM_COUNT);
    for (int i = 0; i < MEDIUM_COUNT; i++) {
      String key = "bench-download/medium/blob_" + i + ".dat";
      uploadBlob(key, mediumBlob);
      mediumKeys.add(key);
    }

    largeKeys = new ArrayList<>(LARGE_COUNT);
    for (int i = 0; i < LARGE_COUNT; i++) {
      String key = "bench-download/large/blob_" + i + ".dat";
      uploadBlob(key, largeBlob);
      largeKeys.add(key);
    }
    log.info("Uploaded {} large blobs, verifying existence...", LARGE_COUNT);
    for (String key : largeKeys) {
      asyncClient.getMetadata(key, null).orTimeout(60, TimeUnit.SECONDS).join();
    }
    log.info("All large blobs verified");

    copySourceKey = smallKeys.get(0);
  }

  private void uploadBlob(String key, byte[] data) {
    UploadRequest request =
        new UploadRequest.Builder().withKey(key).withContentLength(data.length).build();
    asyncClient.upload(request, data).orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
  }

  private void cleanupSingleObjectData() {
    if (asyncClient == null) {
      return;
    }
    String[] prefixes = {
        "bench-download/", "bench-upload-small/", "bench-upload-medium/",
        "bench-upload-large/", "bench-write-read-delete/", "bench-multipart/",
        "bench-copy-dest/"
    };
    for (String prefix : prefixes) {
      try {
        asyncClient.deleteDirectory(prefix)
            .orTimeout(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
      } catch (Exception e) {
        log.warn("Failed to cleanup prefix {}", prefix, e);
      }
    }
  }

  private static void deleteLocalRecursive(Path root) {
    if (root == null) {
      return;
    }
    try {
      if (!Files.exists(root)) {
        return;
      }
      try (var stream = Files.walk(root)) {
        stream
            .sorted(Comparator.reverseOrder())
            .forEach(
                p -> {
                  try {
                    Files.deleteIfExists(p);
                  } catch (IOException e) {
                    log.warn("Failed to delete local benchmark file {}", p, e);
                  }
                });
      }
    } catch (IOException e) {
      log.warn("Failed to walk local benchmark directory {} during cleanup", root, e);
    }
  }

  @Test
  @EnabledIfSystemProperty(named = "runBenchmarks", matches = "true")
  public void runBenchmarks() throws RunnerException {
    new Runner(
            new OptionsBuilder()
                .include(".*" + this.getClass().getName() + ".*")
                .resultFormat(ResultFormatType.JSON)
                .result("target/jmh-async-results-" + getProviderId() + ".json")
                .build())
        .run();
  }
}
