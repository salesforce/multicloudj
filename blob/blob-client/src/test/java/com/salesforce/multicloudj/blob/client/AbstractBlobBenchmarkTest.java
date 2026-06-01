package com.salesforce.multicloudj.blob.client;

import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
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
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMH benchmarks for sync blob operations via {@link BucketClient}.
 *
 * <p>Covers single-object upload/download (small/medium/large), write-read-delete lifecycle,
 * multipart upload, metadata, list, listPage, and copy.
 *
 * <p>Each benchmark method performs exactly one logical operation per invocation so that JMH
 * reports true per-op latency and throughput. Pre-seeded data is used for read-path benchmarks
 * to avoid conflating write cost into read measurements.
 */
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractBlobBenchmarkTest {

  private static final Logger logger = LoggerFactory.getLogger(AbstractBlobBenchmarkTest.class);

  protected static final int SMALL_BLOB = 1024; // 1KB
  protected static final int MEDIUM_BLOB = 1024 * 1024; // 1MB
  protected static final int LARGE_BLOB = 10 * 1024 * 1024; // 10MB
  protected static final int PART_SIZE = 5 * 1024 * 1024; // 5MB per MPU part
  protected static final int LIST_PAGE_MAX_RESULTS = 20;

  protected static final String DOWNLOAD_BLOBS_PREFIX = "bench-download/";
  protected static final String SMALL_BLOBS_PREFIX = DOWNLOAD_BLOBS_PREFIX + "small/";
  protected static final String MEDIUM_BLOBS_PREFIX = DOWNLOAD_BLOBS_PREFIX + "medium/";
  protected static final String LARGE_BLOBS_PREFIX = DOWNLOAD_BLOBS_PREFIX + "large/";

  protected static final String UPLOAD_SMALL_PREFIX = "bench-upload-small/";
  protected static final String UPLOAD_MEDIUM_PREFIX = "bench-upload-medium/";
  protected static final String UPLOAD_LARGE_PREFIX = "bench-upload-large/";
  protected static final String WRITE_READ_DELETE_PREFIX = "bench-write-read-delete/";
  protected static final String MULTIPART_PREFIX = "bench-multipart/";
  protected static final String COPY_DEST_PREFIX = "bench-copy-dest/";

  private static final int SMALL_COUNT = 100;
  private static final int MEDIUM_COUNT = 20;
  private static final int LARGE_COUNT = 5;

  protected String bucketName;
  protected BucketClient bucketClient;

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

  private final ConcurrentLinkedQueue<String> copyDestKeys = new ConcurrentLinkedQueue<>();

  /**
   * Reads a required config value from OS environment first, then from -D system properties.
   * Fails fast with a clear error if neither is set — avoids silent misconfiguration producing
   * garbage benchmark results.
   */
  protected static String requireEnv(String name) {
    String value = System.getenv(name);
    if (StringUtils.isBlank(value)) {
      value = System.getProperty(name);
    }
    if (StringUtils.isBlank(value)) {
      throw new IllegalStateException("Required environment variable not set: " + name);
    }
    return value;
  }

  public interface Harness extends AutoCloseable {
    AbstractBlobStore createBlobStore();

    String getBucketName();
  }

  protected abstract Harness createHarness();

  protected abstract String getProviderId();

  private Harness harness;

  @Setup(Level.Trial)
  public void setupBenchmark() {
    logger.info("Creating {} sync blob store", getProviderId());
    try {
      harness = createHarness();
      bucketName = harness.getBucketName();

      AbstractBlobStore blobStore = harness.createBlobStore();
      bucketClient = new BucketClient(blobStore);

      cleanupBenchmarkData();
      setupTestData();
    } catch (Exception e) {
      throw new RuntimeException("Failed to setup benchmark", e);
    }
  }

  @TearDown(Level.Trial)
  public void teardownBenchmark() {
    try {
      cleanupBenchmarkData();
      if (harness != null) {
        harness.close();
      }
    } catch (Exception e) {
      throw new RuntimeException("Error closing harness", e);
    }
  }

  private void setupTestData() {
    Random rnd = new Random(42);
    smallBlob = new byte[SMALL_BLOB];
    rnd.nextBytes(smallBlob);
    mediumBlob = new byte[MEDIUM_BLOB];
    rnd.nextBytes(mediumBlob);
    largeBlob = new byte[LARGE_BLOB];
    rnd.nextBytes(largeBlob);

    smallKeys = new ArrayList<>(SMALL_COUNT);
    for (int i = 0; i < SMALL_COUNT; i++) {
      String key = SMALL_BLOBS_PREFIX + "blob_" + i + ".dat";
      uploadBlob(key, smallBlob);
      smallKeys.add(key);
    }

    mediumKeys = new ArrayList<>(MEDIUM_COUNT);
    for (int i = 0; i < MEDIUM_COUNT; i++) {
      String key = MEDIUM_BLOBS_PREFIX + "blob_" + i + ".dat";
      uploadBlob(key, mediumBlob);
      mediumKeys.add(key);
    }

    largeKeys = new ArrayList<>(LARGE_COUNT);
    for (int i = 0; i < LARGE_COUNT; i++) {
      String key = LARGE_BLOBS_PREFIX + "blob_" + i + ".dat";
      uploadBlob(key, largeBlob);
      largeKeys.add(key);
    }

    copySourceKey = smallKeys.get(0);
  }

  private void uploadBlob(String key, byte[] data) {
    try (InputStream inputStream = new ByteArrayInputStream(data)) {
      UploadRequest request =
          new UploadRequest.Builder().withKey(key).withContentLength(data.length).build();
      bucketClient.upload(request, inputStream);
    } catch (Exception e) {
      throw new RuntimeException("Failed to upload test blob: " + key, e);
    }
  }

  private void cleanupBenchmarkData() {
    if (bucketClient == null) {
      return;
    }
    String[] prefixes = {
        DOWNLOAD_BLOBS_PREFIX, UPLOAD_SMALL_PREFIX, UPLOAD_MEDIUM_PREFIX,
        UPLOAD_LARGE_PREFIX, WRITE_READ_DELETE_PREFIX, MULTIPART_PREFIX,
        COPY_DEST_PREFIX
    };
    for (String prefix : prefixes) {
      try {
        ListBlobsRequest listRequest = ListBlobsRequest.builder().withPrefix(prefix).build();
        Iterator<BlobInfo> iter = bucketClient.list(listRequest);
        while (iter.hasNext()) {
          bucketClient.delete(iter.next().getKey(), null);
        }
      } catch (Exception e) {
        logger.warn("Failed to cleanup prefix {}", prefix, e);
      }
    }
    for (String key : copyDestKeys) {
      try {
        bucketClient.delete(key, null);
      } catch (Exception e) {
        // best-effort
      }
    }
    copyDestKeys.clear();
  }

  @Benchmark
  @Threads(4)
  public void benchmarkUploadSmall(Blackhole bh) {
    String key = UPLOAD_SMALL_PREFIX + nextUploadSmallId.incrementAndGet() + ".dat";
    try (InputStream is = new ByteArrayInputStream(smallBlob)) {
      UploadRequest request =
          new UploadRequest.Builder().withKey(key).withContentLength(smallBlob.length).build();
      UploadResponse response = bucketClient.upload(request, is);
      bh.consume(response.getETag());
    } catch (Exception e) {
      throw new RuntimeException("Benchmark upload small failed", e);
    }
  }

  @Benchmark
  @Threads(2)
  public void benchmarkUploadMedium(Blackhole bh) {
    String key = UPLOAD_MEDIUM_PREFIX + nextUploadMediumId.incrementAndGet() + ".dat";
    try (InputStream is = new ByteArrayInputStream(mediumBlob)) {
      UploadRequest request =
          new UploadRequest.Builder().withKey(key).withContentLength(mediumBlob.length).build();
      UploadResponse response = bucketClient.upload(request, is);
      bh.consume(response.getETag());
    } catch (Exception e) {
      throw new RuntimeException("Benchmark upload medium failed", e);
    }
  }

  @Benchmark
  @Threads(1)
  @Measurement(iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
  public void benchmarkUploadLarge(Blackhole bh) {
    String key = UPLOAD_LARGE_PREFIX + nextUploadLargeId.incrementAndGet() + ".dat";
    try (InputStream is = new ByteArrayInputStream(largeBlob)) {
      UploadRequest request =
          new UploadRequest.Builder().withKey(key).withContentLength(largeBlob.length).build();
      UploadResponse response = bucketClient.upload(request, is);
      bh.consume(response.getETag());
    } catch (Exception e) {
      throw new RuntimeException("Benchmark upload large failed", e);
    }
  }

  @Benchmark
  @Threads(4)
  public void benchmarkDownloadSmall(Blackhole bh) {
    String key = pickRandom(smallKeys);
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      DownloadRequest request = new DownloadRequest.Builder().withKey(key).build();
      bucketClient.download(request, os);
      bh.consume(os.toByteArray());
    } catch (Exception e) {
      throw new RuntimeException("Benchmark download small failed", e);
    }
  }

  @Benchmark
  @Threads(2)
  public void benchmarkDownloadMedium(Blackhole bh) {
    String key = pickRandom(mediumKeys);
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      DownloadRequest request = new DownloadRequest.Builder().withKey(key).build();
      bucketClient.download(request, os);
      bh.consume(os.toByteArray());
    } catch (Exception e) {
      throw new RuntimeException("Benchmark download medium failed", e);
    }
  }

  @Benchmark
  @Threads(1)
  @Measurement(iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
  public void benchmarkDownloadLarge(Blackhole bh) {
    String key = pickRandom(largeKeys);
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      DownloadRequest request = new DownloadRequest.Builder().withKey(key).build();
      bucketClient.download(request, os);
      bh.consume(os.toByteArray());
    } catch (Exception e) {
      throw new RuntimeException("Benchmark download large failed", e);
    }
  }

  @Benchmark
  @Threads(4)
  public void benchmarkWriteReadDelete(Blackhole bh) {
    String key = WRITE_READ_DELETE_PREFIX + nextWriteReadDeleteId.incrementAndGet() + ".dat";

    try {
      try (InputStream is = new ByteArrayInputStream(smallBlob)) {
        UploadRequest uploadRequest =
            new UploadRequest.Builder().withKey(key).withContentLength(smallBlob.length).build();
        UploadResponse uploadResponse = bucketClient.upload(uploadRequest, is);
        bh.consume(uploadResponse.getETag());
      }

      try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
        DownloadRequest downloadRequest = new DownloadRequest.Builder().withKey(key).build();
        bucketClient.download(downloadRequest, os);
        bh.consume(os.toByteArray());
      }

      bucketClient.delete(key, null);
    } catch (Exception e) {
      throw new RuntimeException("Benchmark write-read-delete failed", e);
    }
  }

  @Benchmark
  @Threads(2)
  public void benchmarkMultipartUpload(Blackhole bh) {
    String key = MULTIPART_PREFIX + nextMultipartUploadId.incrementAndGet() + ".dat";

    try {
      MultipartUploadRequest request = new MultipartUploadRequest.Builder().withKey(key).build();
      MultipartUpload mpu = bucketClient.initiateMultipartUpload(request);

      List<UploadPartResponse> partResponses = new ArrayList<>();
      int numParts = (int) Math.ceil((double) largeBlob.length / PART_SIZE);
      for (int partNum = 1; partNum <= numParts; partNum++) {
        int startIndex = (partNum - 1) * PART_SIZE;
        int endIndex = Math.min(startIndex + PART_SIZE, largeBlob.length);
        byte[] partData = Arrays.copyOfRange(largeBlob, startIndex, endIndex);

        MultipartPart part = new MultipartPart(partNum, partData);
        UploadPartResponse partResponse = bucketClient.uploadMultipartPart(mpu, part);
        partResponses.add(partResponse);
      }

      MultipartUploadResponse completeResponse =
          bucketClient.completeMultipartUpload(mpu, partResponses);
      bh.consume(completeResponse.getEtag());
    } catch (Exception e) {
      throw new RuntimeException("Benchmark multipart upload failed", e);
    }
  }

  @Benchmark
  @Threads(4)
  public void benchmarkGetMetadata(Blackhole bh) {
    String key = pickRandom(smallKeys);
    try {
      BlobMetadata metadata = bucketClient.getMetadata(key, null);
      bh.consume(metadata);
    } catch (Exception e) {
      throw new RuntimeException("Benchmark get metadata failed", e);
    }
  }

  @Benchmark
  @Threads(4)
  public void benchmarkList(Blackhole bh) {
    try {
      ListBlobsRequest request = ListBlobsRequest.builder().withPrefix(SMALL_BLOBS_PREFIX).build();
      Iterator<BlobInfo> iter = bucketClient.list(request);
      int count = 0;
      while (iter.hasNext()) {
        bh.consume(iter.next());
        count++;
      }
      bh.consume(count);
    } catch (Exception e) {
      throw new RuntimeException("Benchmark list failed", e);
    }
  }

  @Benchmark
  @Threads(4)
  public void benchmarkListPage(Blackhole bh) {
    try {
      ListBlobsPageRequest request =
          ListBlobsPageRequest.builder()
              .withPrefix(SMALL_BLOBS_PREFIX)
              .withMaxResults(LIST_PAGE_MAX_RESULTS)
              .build();
      ListBlobsPageResponse response = bucketClient.listPage(request);
      bh.consume(response.getBlobs().size());
    } catch (Exception e) {
      throw new RuntimeException("Benchmark list page failed", e);
    }
  }

  @Benchmark
  @Threads(4)
  public void benchmarkCopy(Blackhole bh) {
    String destKey = COPY_DEST_PREFIX + nextCopyId.incrementAndGet() + ".dat";
    try {
      CopyRequest request =
          CopyRequest.builder()
              .srcKey(copySourceKey)
              .destBucket(bucketName)
              .destKey(destKey)
              .build();
      CopyResponse response = bucketClient.copy(request);
      bh.consume(response);
      copyDestKeys.add(destKey);
    } catch (Exception e) {
      throw new RuntimeException("Benchmark copy failed", e);
    }
  }

  private static String pickRandom(List<String> keys) {
    return keys.get(ThreadLocalRandom.current().nextInt(keys.size()));
  }

  @Test
  @EnabledIfSystemProperty(named = "runBenchmarks", matches = "true")
  public void runBenchmarks() throws RunnerException {
    List<String> forwardedArgs = new ArrayList<>();
    for (String key : System.getProperties().stringPropertyNames()) {
      if (key.startsWith("BLOB_BENCHMARK_")) {
        forwardedArgs.add("-D" + key + "=" + System.getProperty(key));
      }
    }

    Options opt =
        new OptionsBuilder()
            .include(".*" + this.getClass().getName() + ".*")
            .forks(1)
            .resultFormat(ResultFormatType.JSON)
            .result("target/jmh-sync-results-" + getProviderId() + ".json")
            .jvmArgsAppend(forwardedArgs.toArray(new String[0]))
            .build();

    new Runner(opt).run();
  }
}
