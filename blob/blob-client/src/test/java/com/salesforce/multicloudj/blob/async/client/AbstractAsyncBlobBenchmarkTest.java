package com.salesforce.multicloudj.blob.async.client;

import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStore;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
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

/**
 * JMH benchmarks for async directory upload/download via {@link AsyncBucketClient}.
 *
 * <p>Runs in {@link Mode#SampleTime}, {@link Mode#SingleShotTime} and {@link Mode#Throughput}.
 *
 * <p>Corpus: 100 x 1KB, 50 x 1MB, 10 x 10MB — kept under AWS/GCS rate limits.
 */
@BenchmarkMode({Mode.SingleShotTime, Mode.SampleTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1, time = 60, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 60, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractAsyncBlobBenchmarkTest {

  // File size constants
  protected static final int SMALL_FILE = 1024; // 1KB
  protected static final int MEDIUM_FILE = 1024 * 1024; // 1MB
  protected static final int LARGE_FILE = 10 * 1024 * 1024; // 10MB

  // Directory benchmark corpus sizes. Stay inside cloud provider limits (see class javadoc).
  protected static final int DIR_SMALL_COUNT = 100;
  protected static final int DIR_MEDIUM_COUNT = 50;
  protected static final int DIR_LARGE_COUNT = 10;

  protected AsyncBucketClient asyncClient;

  protected Path dirTempRoot;
  protected Path dirSmallSource;
  protected Path dirMediumSource;
  protected Path dirLargeSource;
  protected String dirSmallDownloadPrefix;
  protected String dirMediumDownloadPrefix;
  protected String dirLargeDownloadPrefix;

  // Per-invocation state, populated by invocationSetup() and cleaned by invocationTeardown().
  // Upload benchmarks write to uploadPrefix; download benchmarks write to downloadDest.
  private String uploadPrefix;
  private Path downloadDest;

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

  @Setup(Level.Invocation)
  public void invocationSetup() throws IOException {
    uploadPrefix = "dir-bench-" + UUID.randomUUID() + "/";
    downloadDest = Files.createTempDirectory(dirTempRoot, "download-");
  }

  @TearDown(Level.Invocation)
  public void invocationTeardown() {
    if (uploadPrefix != null) {
      safeDeleteDirectoryPrefix(uploadPrefix);
      uploadPrefix = null;
    }
    if (downloadDest != null) {
      deleteLocalRecursive(downloadDest);
      downloadDest = null;
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
  public void benchmarkDownloadDirectorySmall(Blackhole bh) {
    runDirectoryDownload(bh, dirSmallDownloadPrefix);
  }

  @Benchmark
  public void benchmarkDownloadDirectoryMedium(Blackhole bh) {
    runDirectoryDownload(bh, dirMediumDownloadPrefix);
  }

  @Benchmark
  public void benchmarkDownloadDirectoryLarge(Blackhole bh) {
    runDirectoryDownload(bh, dirLargeDownloadPrefix);
  }

  private void runDirectoryUpload(Blackhole bh, Path source) {
    DirectoryUploadRequest req =
        DirectoryUploadRequest.builder()
            .localSourceDirectory(source.toString())
            .prefix(uploadPrefix)
            .includeSubFolders(true)
            .build();
    bh.consume(asyncClient.uploadDirectory(req).join());
  }

  private void runDirectoryDownload(Blackhole bh, String prefix) {
    DirectoryDownloadRequest req =
        DirectoryDownloadRequest.builder()
            .prefixToDownload(prefix)
            .localDestinationDirectory(downloadDest.toString())
            .build();
    bh.consume(asyncClient.downloadDirectory(req).join());
  }

  private void setupDirectoryData() throws IOException {
    dirTempRoot = Files.createTempDirectory("mcj-async-dir-bench-");
    dirSmallSource = createLocalCorpus("small", DIR_SMALL_COUNT, SMALL_FILE);
    dirMediumSource = createLocalCorpus("medium", DIR_MEDIUM_COUNT, MEDIUM_FILE);
    dirLargeSource = createLocalCorpus("large", DIR_LARGE_COUNT, LARGE_FILE);

    dirSmallDownloadPrefix = "async-dir-bench-corpus-small-" + UUID.randomUUID() + "/";
    dirMediumDownloadPrefix = "async-dir-bench-corpus-medium-" + UUID.randomUUID() + "/";
    dirLargeDownloadPrefix = "async-dir-bench-corpus-large-" + UUID.randomUUID() + "/";

    uploadCorpus(dirSmallSource, dirSmallDownloadPrefix);
    uploadCorpus(dirMediumSource, dirMediumDownloadPrefix);
    uploadCorpus(dirLargeSource, dirLargeDownloadPrefix);
  }

  private void teardownDirectoryData() {
    safeDeleteDirectoryPrefix(dirSmallDownloadPrefix);
    safeDeleteDirectoryPrefix(dirMediumDownloadPrefix);
    safeDeleteDirectoryPrefix(dirLargeDownloadPrefix);
    deleteLocalRecursive(dirTempRoot);
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

  /** Launches the JMH suite. Opt in with {@code -DrunBenchmarks=true} */
  @Test
  @EnabledIfSystemProperty(named = "runBenchmarks", matches = "true")
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
