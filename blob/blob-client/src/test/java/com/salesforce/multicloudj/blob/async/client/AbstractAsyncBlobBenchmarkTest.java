package com.salesforce.multicloudj.blob.async.client;

import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStore;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
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

/**
 * JMH benchmarks for async directory upload/download via {@link AsyncBucketClient}.
 *
 * <p>Corpus: 100 x 1KB, 50 x 1MB, 10 x 10MB. Each size is uploaded to {@value #THREAD_COUNT}
 * distinct prefixes to spread concurrent GETs and stay under per-prefix rate limits.
 *
 * <p>Intentionally avoids {@code @Setup(Level.Invocation)} because JMH does not reliably invoke
 * it on every worker thread in Throughput mode. Per-invocation resources are set up inside the
 * benchmark methods themselves.
 */
@Disabled
@BenchmarkMode({Mode.SampleTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1, time = 60, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 60, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(AbstractAsyncBlobBenchmarkTest.THREAD_COUNT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractAsyncBlobBenchmarkTest {

  protected static final int SMALL_FILE = 1024; // 1KB
  protected static final int MEDIUM_FILE = 1024 * 1024; // 1MB
  protected static final int LARGE_FILE = 10 * 1024 * 1024; // 10MB

  // Sized to keep per-invocation request rate under S3/GCS per-prefix limits at THREAD_COUNT=4.
  protected static final int DIR_SMALL_COUNT = 100;
  protected static final int DIR_MEDIUM_COUNT = 50;
  protected static final int DIR_LARGE_COUNT = 10;

  static final int THREAD_COUNT = 4;

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

  private Harness harness;

  public interface Harness extends AutoCloseable {
    AsyncBlobStore createAsyncBlobStore();
  }

  protected abstract Harness createHarness();

  /** Returns provider ID for JMH result file naming, e.g., "aws", "gcp". */
  protected abstract String getProviderId();

  @Setup(Level.Trial)
  public void setupBenchmark() {
    try {
      harness = createHarness();
      AsyncBlobStore store = harness.createAsyncBlobStore();
      asyncClient = new AsyncBucketClient(store);
      setupDirectoryData();
    } catch (IOException e) {
      throw new RuntimeException("Failed to setup async benchmark", e);
    }
  }

  @TearDown(Level.Trial)
  public void teardownBenchmark() {
    try {
      teardownDirectoryData();
    } finally {
      if (harness != null) {
        try {
          harness.close();
        } catch (Exception ignored) {
          // Trial already ran; swallow so cleanup failure can't mask earlier errors.
        }
      }
    }
  }

  @Benchmark
  public void benchmarkUploadDirectorySmall(Blackhole bh) {
    runDirectoryUpload(bh, dirSmallSource);
  }

  @Benchmark
  public void benchmarkUploadDirectoryMedium(Blackhole bh) {
    runDirectoryUpload(bh, dirMediumSource);
  }

  @Benchmark
  public void benchmarkUploadDirectoryLarge(Blackhole bh) {
    runDirectoryUpload(bh, dirLargeSource);
  }

  @Benchmark
  public void benchmarkDownloadDirectorySmall(Blackhole bh) throws IOException {
    runDirectoryDownload(bh, pickRandom(dirSmallDownloadPrefixes));
  }

  @Benchmark
  public void benchmarkDownloadDirectoryMedium(Blackhole bh) throws IOException {
    runDirectoryDownload(bh, pickRandom(dirMediumDownloadPrefixes));
  }

  @Benchmark
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
    bh.consume(asyncClient.uploadDirectory(req).join());
  }

  private void runDirectoryDownload(Blackhole bh, String prefix) throws IOException {
    Path dest = Files.createTempDirectory(dirTempRoot, "download-");
    try {
      DirectoryDownloadRequest req =
          DirectoryDownloadRequest.builder()
              .prefixToDownload(prefix)
              .localDestinationDirectory(dest.toString())
              .build();
      bh.consume(asyncClient.downloadDirectory(req).join());
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
      asyncClient.deleteDirectory(prefix).join();
    } catch (Exception ignored) {
      // Best-effort cleanup; residual objects are acceptable for a benchmark run.
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
                  } catch (IOException ignored) {
                    // Best-effort cleanup of per-file delete.
                  }
                });
      }
    } catch (IOException ignored) {
      // Best-effort cleanup of temp tree walk.
    }
  }

  @Test
  public void runBenchmarks() throws RunnerException {
    new Runner(
            new OptionsBuilder()
                .include(".*" + this.getClass().getName() + ".*")
                .forks(1)
                .resultFormat(ResultFormatType.JSON)
                .result("target/jmh-async-directory-results-" + getProviderId() + ".json")
                .build())
        .run();
  }
}
