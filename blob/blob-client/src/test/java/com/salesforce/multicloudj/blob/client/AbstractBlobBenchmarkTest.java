package com.salesforce.multicloudj.blob.client;

import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.MultipartUploadResponse;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Disabled;

/**
 * Abstract JMH benchmark class for Blob operations
 */
@Disabled
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)

public abstract class AbstractBlobBenchmarkTest {

    // Blob size constants
    protected static final int SMALL_BLOB = 1024;           // 1KB
    protected static final int MEDIUM_BLOB = 1024 * 1024;   // 1MB
    protected static final int LARGE_BLOB = 10 * 1024 * 1024; // 10MB
    protected static final int PART_SIZE = 5 * 1024 * 1024;

    // Test data
    protected String bucketName;
    protected List<String> blobKeys;
    protected List<byte[]> testBlobs;
    protected Random random;
    protected BucketClient bucketClient;

    private final AtomicInteger nextPutId = new AtomicInteger(0);
    private final AtomicInteger nextGetId = new AtomicInteger(0);
    private final AtomicInteger nextBatchPutId = new AtomicInteger(0);
    private final AtomicInteger nextBatchGetId = new AtomicInteger(0);
    private final AtomicInteger nextWriteReadDeleteId = new AtomicInteger(0);
    private final AtomicInteger nextMultipartUploadId = new AtomicInteger(0);

    // Harness interface
    public interface Harness extends AutoCloseable {
        AbstractBlobStore<?> createBlobStore();
        String getBucketName();
    }

    protected abstract Harness createHarness();
    private Harness harness;

    @Setup(Level.Trial)
    public void setupBenchmark() {
        try {
            harness = createHarness();
            
            bucketName = harness.getBucketName();
            blobKeys = new ArrayList<>();
            testBlobs = new ArrayList<>();
            random = new Random(42); 

            AbstractBlobStore<?> blobStore = harness.createBlobStore();
            bucketClient = new BucketClient(blobStore);
            cleanupTestData();
            generateTestBlobs();
            setupTestData();
        } catch (Exception e) {
            throw new RuntimeException("Failed to setup benchmark", e);
        }
    }

    @TearDown(Level.Trial)
    public void teardownBenchmark() {
        try {
            cleanupTestData();

            if (harness != null) {
                harness.close();
            }
        } catch (Exception e) {
            throw new RuntimeException("Error closing harness", e);
        }
    }

    /**
     * Generate test blobs of various sizes
     */
    private void generateTestBlobs() {
        // Generate small blobs
        for (int i = 0; i < 100; i++) {
            byte[] blob = createBlob(SMALL_BLOB);
            blobKeys.add("small/blob_" + i + ".dat");
            testBlobs.add(blob);
        }

        // Generate medium blobs
        for (int i = 0; i < 20; i++) {
            byte[] blob = createBlob(MEDIUM_BLOB);
            blobKeys.add("medium/blob_" + i + ".dat");
            testBlobs.add(blob);
        }

        // Generate large blobs
        for (int i = 0; i < 5; i++) {
            byte[] blob = createBlob(LARGE_BLOB);
            blobKeys.add("large/blob_" + i + ".dat");
            testBlobs.add(blob);
        }
    }

    /**
     * Setup pre-populated test data
     */
    private void setupTestData() {
        try {
            for (int i = 0; i < blobKeys.size(); i++) {
                String key = blobKeys.get(i);
                byte[] blob = testBlobs.get(i);

                try (InputStream inputStream = new ByteArrayInputStream(blob)) {
                    UploadRequest request = new UploadRequest.Builder()
                            .withKey(key)
                            .withContentLength(blob.length)
                            .build();
                    bucketClient.upload(request, inputStream);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to setup test data", e);
        }
    }

    /**
     * Cleanup test data
     */
    private void cleanupTestData() {
        if (blobKeys == null || bucketClient == null) {
            return;
        }
        
        for (String key : blobKeys) {
            try {
                bucketClient.delete(key, null);
            } catch (Exception e) {
            }
        }
    }

    @Benchmark
    @Threads(4)
    public void benchmarkSingleActionPut(Blackhole bh) {
        benchmarkSingleActionPut(bh, 10);
    }

    private void benchmarkSingleActionPut(Blackhole bh, int n) {
        final String baseKey = "benchmarksingleaction-put-";

        try {
            for (int i = 0; i < n; i++) {
                String key = baseKey + nextPutId.incrementAndGet();
                byte[] blobData = createBlob(SMALL_BLOB);

                try (InputStream inputStream = new ByteArrayInputStream(blobData)) {
                    UploadRequest request = new UploadRequest.Builder()
                            .withKey(key)
                            .withContentLength(blobData.length)
                            .build();
                    UploadResponse response = bucketClient.upload(request, inputStream);
                    bh.consume(response.getETag());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Benchmark single action put failed", e);
        }
    }

    /**
     * Single Action Get
     */
    @Benchmark
    @Threads(4)
    public void benchmarkSingleActionGet(Blackhole bh) {
        benchmarkSingleActionGet(bh, 10);
    }

    private void benchmarkSingleActionGet(Blackhole bh, int n) {
        final String baseKey = "benchmarksingleaction-get-";

        try {
            // Pre-populate data
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                String key = baseKey + nextGetId.incrementAndGet();
                keys.add(key);
                byte[] blobData = createBlob(SMALL_BLOB);

                try (InputStream inputStream = new ByteArrayInputStream(blobData)) {
                    UploadRequest request = new UploadRequest.Builder()
                            .withKey(key)
                            .withContentLength(blobData.length)
                            .build();
                    bucketClient.upload(request, inputStream);
                }
            }
            for (String key : keys) {
                try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                    DownloadRequest request = new DownloadRequest.Builder()
                            .withKey(key)
                            .build();
                    DownloadResponse response = bucketClient.download(request, outputStream);
                    bh.consume(outputStream.toByteArray());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Benchmark single action get failed", e);
        }
    }

    /**
     * Batch Action Put
     */
    @Benchmark
    @Threads(4)
    public void benchmarkActionListPut(Blackhole bh) {
        benchmarkActionListPut(bh, 50);
    }

    private void benchmarkActionListPut(Blackhole bh, int n) {
        final String baseKey = "benchmarkactionlist-put-";

        try {
            List<String> etags = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                String key = baseKey + nextBatchPutId.incrementAndGet();
                byte[] blobData = createBlob(SMALL_BLOB);

                try (InputStream inputStream = new ByteArrayInputStream(blobData)) {
                    UploadRequest request = new UploadRequest.Builder()
                            .withKey(key)
                            .withContentLength(blobData.length)
                            .build();
                    UploadResponse response = bucketClient.upload(request, inputStream);
                    etags.add(response.getETag());
                }
            }
            bh.consume(etags.size());
        } catch (Exception e) {
            throw new RuntimeException("Benchmark action list put failed", e);
        }
    }

    /**
     * Batch Action Get
     */
    @Benchmark
    @Threads(4)
    public void benchmarkActionListGet(Blackhole bh) {
        benchmarkActionListGet(bh, 100);
    }

    private void benchmarkActionListGet(Blackhole bh, int n) {
        final String baseKey = "benchmarkactionlist-get-";

        try {
            // Pre-populate data
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                String key = baseKey + nextBatchGetId.incrementAndGet();
                keys.add(key);
                byte[] blobData = createBlob(SMALL_BLOB);

                try (InputStream inputStream = new ByteArrayInputStream(blobData)) {
                    UploadRequest request = new UploadRequest.Builder()
                            .withKey(key)
                            .withContentLength(blobData.length)
                            .build();
                    bucketClient.upload(request, inputStream);
                }
            }

            // Benchmark batched Get operations
            List<Integer> results = new ArrayList<>();
            for (String key : keys) {
                try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                    DownloadRequest request = new DownloadRequest.Builder()
                            .withKey(key)
                            .build();
                    bucketClient.download(request, outputStream);
                    results.add(outputStream.size());
                }
            }
            bh.consume(results.size());
        } catch (Exception e) {
            throw new RuntimeException("Benchmark action list get failed", e);
        }
    }

    /**
     * Multipart Upload
     */
    @Benchmark
    @Threads(2)
    public void benchmarkMultipartUpload(Blackhole bh) {
        benchmarkMultipartUpload(bh, 10);
    }
    private void benchmarkMultipartUpload(Blackhole bh, int n) {
    final String baseKey = "benchmarkmultipartupload-";
    final byte[] content = createBlob(LARGE_BLOB);
    try {
        for (int i = 0; i < n; i++) {
            String key = baseKey + nextMultipartUploadId.incrementAndGet();

            MultipartUploadRequest request = new MultipartUploadRequest.Builder()
                    .withKey(key)
                    .build();
            MultipartUpload mpu = bucketClient.initiateMultipartUpload(request);
            bh.consume(mpu.getId());
            
            List<UploadPartResponse> partResponses = new ArrayList<>();
            
            int numParts = (int) Math.ceil((double) content.length / PART_SIZE);
            for (int partNum = 1; partNum <= numParts; partNum++) {
                int startIndex = (partNum - 1) * PART_SIZE;
                int endIndex = Math.min(startIndex + PART_SIZE, content.length);
                byte[] partData = Arrays.copyOfRange(content, startIndex, endIndex);
                
                MultipartPart part = new MultipartPart(partNum, partData);
                UploadPartResponse partResponse = bucketClient.uploadMultipartPart(mpu, part);
                partResponses.add(partResponse);
                bh.consume(partResponse.getEtag());
            }

            MultipartUploadResponse completeResponse = bucketClient.completeMultipartUpload(mpu, partResponses);
            bh.consume(completeResponse.getEtag());
        }
    } catch (Exception e) {
        throw new RuntimeException("Benchmark multipart upload failed", e);
    }
}
    /**
     Write-Read-Delete Benchmark
     */
    @Benchmark
    @Threads(4)
    public void benchmarkWriteReadDelete(Blackhole bh) {
        final String baseKey = "writereaddeletebenchmark-blob-";
        final byte[] content = createBlob(SMALL_BLOB); 
        
        try {
            String key = baseKey + nextWriteReadDeleteId.incrementAndGet();
            
            // Write operation
            try (InputStream inputStream = new ByteArrayInputStream(content)) {
                UploadRequest uploadRequest = new UploadRequest.Builder()
                        .withKey(key)
                        .withContentLength(content.length)
                        .build();
                UploadResponse uploadResponse = bucketClient.upload(uploadRequest, inputStream);
                bh.consume(uploadResponse.getETag());
            }
            
            // Read operation
            byte[] readData;
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                DownloadRequest downloadRequest = new DownloadRequest.Builder()
                        .withKey(key)
                        .build();
                DownloadResponse downloadResponse = bucketClient.download(downloadRequest, outputStream);
                readData = outputStream.toByteArray();
                bh.consume(downloadResponse);
            }
            
            // Verify content match
            if (!Arrays.equals(readData, content)) {
                throw new RuntimeException("Read data didn't match written data");
            }
            
            // Delete operation
            bucketClient.delete(key, null);
            
        } catch (Exception e) {
            throw new RuntimeException("Benchmark write-read-delete failed", e);
        }
    }

    /**
     * Benchmark downloads using pre-populated test data
     */
    @Benchmark
    public void benchmarkDownloadFromTestData(Blackhole bh) {
        try {
            String key = getRandomBlobKey();

            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                DownloadRequest request = new DownloadRequest.Builder()
                        .withKey(key)
                        .build();
                DownloadResponse response = bucketClient.download(request, outputStream);
                bh.consume(outputStream.toByteArray());
            }
        } catch (Exception e) {
            throw new RuntimeException("Benchmark download from test data failed", e);
        }
    }

    /**
     * Benchmark metadata operations on pre-populated data
     */
    @Benchmark
    public void benchmarkGetMetadataFromTestData(Blackhole bh) {
        try {
            String key = getRandomBlobKey();
            BlobMetadata metadata = bucketClient.getMetadata(key, null);
            bh.consume(metadata);
        } catch (Exception e) {
            throw new RuntimeException("Benchmark get metadata failed", e);
        }
    }

    /**
     * Benchmark small blob downloads using helper method
     */
    @Benchmark
    @Threads(4)
    public void benchmarkDownloadSmallBlobs(Blackhole bh) {
        benchmarkDownloadByPrefix(bh, "small/");
    }

    /**
     * Benchmark medium blob downloads using helper method
     */
    @Benchmark
    @Threads(4)
    public void benchmarkDownloadMediumBlobs(Blackhole bh) {
        benchmarkDownloadByPrefix(bh, "medium/");
    }

    /**
     * Benchmark large blob downloads using helper method
     */
    @Benchmark
    @Threads(4)
    public void benchmarkDownloadLargeBlobs(Blackhole bh) {
        benchmarkDownloadByPrefix(bh, "large/");
    }

    /**
     * Generic helper method for downloading blobs by prefix
     */
    private void benchmarkDownloadByPrefix(Blackhole bh, String prefix) {
        try {
            String key = getRandomBlobKeyWithPrefix(prefix);

            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                DownloadRequest request = new DownloadRequest.Builder()
                        .withKey(key)
                        .build();
                DownloadResponse response = bucketClient.download(request, outputStream);
                bh.consume(outputStream.toByteArray());
            }
        } catch (Exception e) {
            throw new RuntimeException("Benchmark download " + prefix + " blobs failed", e);
        }
    }

    /**
     * Create a blob of specified size with random data
     */
    protected byte[] createBlob(int size) {
        byte[] blob = new byte[size];
        random.nextBytes(blob);
        return blob;
    }

    /**
     * Generate a unique blob key
     */
    protected String generateUniqueBlobKey(String prefix) {
        return prefix + "/blob_" + UUID.randomUUID().toString() + ".dat";
    }

    /**
     * Get a random blob key from pre-populated test data
     */
    protected String getRandomBlobKey() {
        if (blobKeys.isEmpty()) {
            return generateUniqueBlobKey("fallback");
        }
        int index = random.nextInt(blobKeys.size());
        return blobKeys.get(index);
    }

    /**
     * Get a random blob key with specific prefix
     */
    protected String getRandomBlobKeyWithPrefix(String prefix) {
        List<String> filteredKeys = blobKeys.stream()
                .filter(key -> key.startsWith(prefix))
                .collect(Collectors.toList()); 

        if (filteredKeys.isEmpty()) {
            return generateUniqueBlobKey(prefix);
        }

        return filteredKeys.get(random.nextInt(filteredKeys.size()));
    }

    /**
     * Get random blob data from test blobs
     */
    protected byte[] getRandomBlob() {
        if (testBlobs.isEmpty()) {
            return createBlob(SMALL_BLOB);
        }
        return testBlobs.get(random.nextInt(testBlobs.size()));
    }


    /**
     * JUnit test method to run JMH benchmarks
     */
    @Test
    public void runBenchmarks() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + this.getClass().getName() + ".*")
                .forks(1)
                .warmupIterations(3)
                .measurementIterations(5)
                .resultFormat(ResultFormatType.JSON)
                .result("target/jmh-results.json")
                .build();

        new Runner(opt).run();
    }
} 