package com.salesforce.multicloudj.blob.gcp.async;

import com.google.cloud.storage.Storage;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsBatch;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.MultipartUploadResponse;
import com.salesforce.multicloudj.blob.driver.PresignedOperation;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.blob.gcp.GcpTransformerSupplier;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GcpAsyncBlobStoreTest {

    @Mock
    private AbstractBlobStore<?> mockBlobStore;

    @Mock
    private Storage mockStorage;

    private ExecutorService executorService;
    private GcpAsyncBlobStore gcpAsyncBlobStore;

    private static final String TEST_BUCKET = "test-bucket";
    private static final String TEST_REGION = "us-central1";
    private static final String TEST_KEY = "test-key";
    private static final String TEST_VERSION_ID = "test-version";

    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(2);
        gcpAsyncBlobStore = new GcpAsyncBlobStore(mockBlobStore, executorService);
    }

    @AfterEach
    void tearDown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }

    @Test
    void testGetters() {
        when(mockBlobStore.getBucket()).thenReturn(TEST_BUCKET);
        when(mockBlobStore.getRegion()).thenReturn(TEST_REGION);
        when(mockBlobStore.getProviderId()).thenReturn(GcpConstants.PROVIDER_ID);

        assertEquals(TEST_BUCKET, gcpAsyncBlobStore.getBucket());
        assertEquals(TEST_REGION, gcpAsyncBlobStore.getRegion());
        assertEquals(GcpConstants.PROVIDER_ID, gcpAsyncBlobStore.getProviderId());
        assertEquals(mockBlobStore, gcpAsyncBlobStore.getBlobStore());
        assertEquals(executorService, gcpAsyncBlobStore.getExecutorService());
    }

    @Test
    void testConstructorWithNullExecutor() {
        GcpAsyncBlobStore asyncStore = new GcpAsyncBlobStore(mockBlobStore, null);
        assertNotNull(asyncStore.getExecutorService());
        // Should use ForkJoinPool.commonPool() when null is passed
    }

    @Test
    void testUploadWithInputStream() throws Exception {
        // Given
        UploadRequest uploadRequest = UploadRequest.builder().withKey(TEST_KEY).build();
        UploadResponse expectedResponse = UploadResponse.builder()
                .key(TEST_KEY)
                .eTag("test-etag")
                .build();
        ByteArrayInputStream inputStream = new ByteArrayInputStream("test content".getBytes());

        when(mockBlobStore.upload(uploadRequest, inputStream)).thenReturn(expectedResponse);

        // When
        CompletableFuture<UploadResponse> result = gcpAsyncBlobStore.upload(uploadRequest, inputStream);

        // Then
        UploadResponse actualResponse = result.get(5, TimeUnit.SECONDS);
        assertEquals(expectedResponse, actualResponse);
        verify(mockBlobStore).upload(uploadRequest, inputStream);
    }

    @Test
    void testUploadWithByteArray() throws Exception {
        // Given
        UploadRequest uploadRequest = UploadRequest.builder().withKey(TEST_KEY).build();
        UploadResponse expectedResponse = UploadResponse.builder()
                .key(TEST_KEY)
                .eTag("test-etag")
                .build();
        byte[] content = "test content".getBytes();

        when(mockBlobStore.upload(uploadRequest, content)).thenReturn(expectedResponse);

        // When
        CompletableFuture<UploadResponse> result = gcpAsyncBlobStore.upload(uploadRequest, content);

        // Then
        UploadResponse actualResponse = result.get(5, TimeUnit.SECONDS);
        assertEquals(expectedResponse, actualResponse);
        verify(mockBlobStore).upload(uploadRequest, content);
    }

    @Test
    void testUploadWithFile() throws Exception {
        // Given
        UploadRequest uploadRequest = UploadRequest.builder().withKey(TEST_KEY).build();
        UploadResponse expectedResponse = UploadResponse.builder()
                .key(TEST_KEY)
                .eTag("test-etag")
                .build();
        File testFile = mock(File.class);

        when(mockBlobStore.upload(uploadRequest, testFile)).thenReturn(expectedResponse);

        // When
        CompletableFuture<UploadResponse> result = gcpAsyncBlobStore.upload(uploadRequest, testFile);

        // Then
        UploadResponse actualResponse = result.get(5, TimeUnit.SECONDS);
        assertEquals(expectedResponse, actualResponse);
        verify(mockBlobStore).upload(uploadRequest, testFile);
    }

    @Test
    void testUploadWithPath() throws Exception {
        // Given
        UploadRequest uploadRequest = UploadRequest.builder().withKey(TEST_KEY).build();
        UploadResponse expectedResponse = UploadResponse.builder()
                .key(TEST_KEY)
                .eTag("test-etag")
                .build();
        Path testPath = Paths.get("/test/path");

        when(mockBlobStore.upload(uploadRequest, testPath)).thenReturn(expectedResponse);

        // When
        CompletableFuture<UploadResponse> result = gcpAsyncBlobStore.upload(uploadRequest, testPath);

        // Then
        UploadResponse actualResponse = result.get(5, TimeUnit.SECONDS);
        assertEquals(expectedResponse, actualResponse);
        verify(mockBlobStore).upload(uploadRequest, testPath);
    }

    @Test
    void testDownloadWithOutputStream() throws Exception {
        // Given
        DownloadRequest downloadRequest = DownloadRequest.builder().withKey(TEST_KEY).build();
        DownloadResponse expectedResponse = DownloadResponse.builder()
                .key(TEST_KEY)
                .metadata(BlobMetadata.builder()
                        .eTag("test-etag")
                        .build())
                .build();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        when(mockBlobStore.download(downloadRequest, outputStream)).thenReturn(expectedResponse);

        // When
        CompletableFuture<DownloadResponse> result = gcpAsyncBlobStore.download(downloadRequest, outputStream);

        // Then
        DownloadResponse actualResponse = result.get(5, TimeUnit.SECONDS);
        assertEquals(expectedResponse, actualResponse);
        verify(mockBlobStore).download(downloadRequest, outputStream);
    }

    @Test
    void testDownloadWithByteArray() throws Exception {
        // Given
        DownloadRequest downloadRequest = DownloadRequest.builder().withKey(TEST_KEY).build();
        DownloadResponse expectedResponse = DownloadResponse.builder()
                .key(TEST_KEY)
                .metadata(BlobMetadata.builder()
                        .eTag("test-etag")
                        .build())
                .build();
        ByteArray byteArray = new ByteArray();

        when(mockBlobStore.download(downloadRequest, byteArray)).thenReturn(expectedResponse);

        // When
        CompletableFuture<DownloadResponse> result = gcpAsyncBlobStore.download(downloadRequest, byteArray);

        // Then
        DownloadResponse actualResponse = result.get(5, TimeUnit.SECONDS);
        assertEquals(expectedResponse, actualResponse);
        verify(mockBlobStore).download(downloadRequest, byteArray);
    }

    @Test
    void testDeleteWithKeyAndVersion() throws Exception {
        // Given
        doNothing().when(mockBlobStore).delete(TEST_KEY, TEST_VERSION_ID);

        // When
        CompletableFuture<Void> result = gcpAsyncBlobStore.delete(TEST_KEY, TEST_VERSION_ID);

        // Then
        result.get(5, TimeUnit.SECONDS);
        verify(mockBlobStore).delete(TEST_KEY, TEST_VERSION_ID);
    }

    @Test
    void testDeleteWithCollection() throws Exception {
        // Given
        Collection<BlobIdentifier> identifiers = List.of(
                new BlobIdentifier(TEST_KEY, TEST_VERSION_ID)
        );
        doNothing().when(mockBlobStore).delete(identifiers);

        // When
        CompletableFuture<Void> result = gcpAsyncBlobStore.delete(identifiers);

        // Then
        result.get(5, TimeUnit.SECONDS);
        verify(mockBlobStore).delete(identifiers);
    }

    @Test
    void testCopy() throws Exception {
        // Given
        CopyRequest copyRequest = CopyRequest.builder()
                .srcKey("source-key")
                .destKey("dest-key")
                .build();
        CopyResponse expectedResponse = CopyResponse.builder()
                .key("dest-key")
                .eTag("copy-etag")
                .build();

        when(mockBlobStore.copy(copyRequest)).thenReturn(expectedResponse);

        // When
        CompletableFuture<CopyResponse> result = gcpAsyncBlobStore.copy(copyRequest);

        // Then
        CopyResponse actualResponse = result.get(5, TimeUnit.SECONDS);
        assertEquals(expectedResponse, actualResponse);
        verify(mockBlobStore).copy(copyRequest);
    }

    @Test
    void testGetMetadata() throws Exception {
        // Given
        BlobMetadata expectedMetadata = BlobMetadata.builder()
                .key(TEST_KEY)
                .eTag("test-etag")
                .objectSize(1024L)
                .build();

        when(mockBlobStore.getMetadata(TEST_KEY, TEST_VERSION_ID)).thenReturn(expectedMetadata);

        // When
        CompletableFuture<BlobMetadata> result = gcpAsyncBlobStore.getMetadata(TEST_KEY, TEST_VERSION_ID);

        // Then
        BlobMetadata actualMetadata = result.get(5, TimeUnit.SECONDS);
        assertEquals(expectedMetadata, actualMetadata);
        verify(mockBlobStore).getMetadata(TEST_KEY, TEST_VERSION_ID);
    }

    @Test
    void testList() throws Exception {
        // Given
        ListBlobsRequest listRequest = ListBlobsRequest.builder().withPrefix("test-").build();
        
        // Create mock iterator with test data
        BlobInfo blob1 = new BlobInfo.Builder().withKey("test-1").withObjectSize(100L).build();
        BlobInfo blob2 = new BlobInfo.Builder().withKey("test-2").withObjectSize(200L).build();
        
        @SuppressWarnings("unchecked")
        Iterator<BlobInfo> mockIterator = mock(Iterator.class);
        when(mockIterator.hasNext()).thenReturn(true, true, false);
        when(mockIterator.next()).thenReturn(blob1, blob2);
        when(mockBlobStore.list(listRequest)).thenReturn(mockIterator);

        // Consumer to capture batches
        AtomicInteger batchCount = new AtomicInteger(0);
        Consumer<ListBlobsBatch> consumer = batch -> {
            batchCount.incrementAndGet();
            assertNotNull(batch);
            assertNotNull(batch.getBlobs());
        };

        // When
        CompletableFuture<Void> result = gcpAsyncBlobStore.list(listRequest, consumer);

        // Then
        result.get(5, TimeUnit.SECONDS);
        assertTrue(batchCount.get() > 0);
        verify(mockBlobStore).list(listRequest);
    }

    @Test
    void testGeneratePresignedUrl() throws Exception {
        // Given
        PresignedUrlRequest presignedRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.UPLOAD)
                .key(TEST_KEY)
                .duration(Duration.ofHours(1))
                .build();
        URL expectedUrl = new URL("https://example.com/signed-url");

        when(mockBlobStore.generatePresignedUrl(presignedRequest)).thenReturn(expectedUrl);

        // When
        CompletableFuture<URL> result = gcpAsyncBlobStore.generatePresignedUrl(presignedRequest);

        // Then
        URL actualUrl = result.get(5, TimeUnit.SECONDS);
        assertEquals(expectedUrl, actualUrl);
        verify(mockBlobStore).generatePresignedUrl(presignedRequest);
    }

    @Test
    void testDoesObjectExist() throws Exception {
        // Given
        when(mockBlobStore.doesObjectExist(TEST_KEY, TEST_VERSION_ID)).thenReturn(true);

        // When
        CompletableFuture<Boolean> result = gcpAsyncBlobStore.doesObjectExist(TEST_KEY, TEST_VERSION_ID);

        // Then
        Boolean exists = result.get(5, TimeUnit.SECONDS);
        assertTrue(exists);
        verify(mockBlobStore).doesObjectExist(TEST_KEY, TEST_VERSION_ID);
    }

    @Test
    void testGetTags() throws Exception {
        // Given
        Map<String, String> expectedTags = Map.of("key1", "value1", "key2", "value2");
        when(mockBlobStore.getTags(TEST_KEY)).thenReturn(expectedTags);

        // When
        CompletableFuture<Map<String, String>> result = gcpAsyncBlobStore.getTags(TEST_KEY);

        // Then
        Map<String, String> actualTags = result.get(5, TimeUnit.SECONDS);
        assertEquals(expectedTags, actualTags);
        verify(mockBlobStore).getTags(TEST_KEY);
    }

    @Test
    void testSetTags() throws Exception {
        // Given
        Map<String, String> tags = Map.of("key1", "value1", "key2", "value2");
        doNothing().when(mockBlobStore).setTags(TEST_KEY, tags);

        // When
        CompletableFuture<Void> result = gcpAsyncBlobStore.setTags(TEST_KEY, tags);

        // Then
        result.get(5, TimeUnit.SECONDS);
        verify(mockBlobStore).setTags(TEST_KEY, tags);
    }

    @Test
    void testMultipartUploadOperations() throws Exception {
        // Test initiate multipart upload
        MultipartUploadRequest mpuRequest = new MultipartUploadRequest.Builder()
                .withKey(TEST_KEY)
                .build();
        MultipartUpload expectedMpu = MultipartUpload.builder()
                .bucket(TEST_BUCKET)
                .key(TEST_KEY)
                .id("upload-id")
                .build();
        when(mockBlobStore.initiateMultipartUpload(mpuRequest)).thenReturn(expectedMpu);

        CompletableFuture<MultipartUpload> initiateResult = gcpAsyncBlobStore.initiateMultipartUpload(mpuRequest);
        MultipartUpload actualMpu = initiateResult.get(5, TimeUnit.SECONDS);
        assertEquals(expectedMpu, actualMpu);

        // Test upload part
        MultipartPart part = new MultipartPart(1, "test data".getBytes());
        UploadPartResponse expectedPartResponse = new UploadPartResponse(1, "part-etag", 9);
        when(mockBlobStore.uploadMultipartPart(expectedMpu, part)).thenReturn(expectedPartResponse);

        CompletableFuture<UploadPartResponse> uploadPartResult = gcpAsyncBlobStore.uploadMultipartPart(expectedMpu, part);
        UploadPartResponse actualPartResponse = uploadPartResult.get(5, TimeUnit.SECONDS);
        assertEquals(expectedPartResponse, actualPartResponse);

        // Test complete multipart upload
        List<UploadPartResponse> parts = List.of(expectedPartResponse);
        MultipartUploadResponse expectedCompleteResponse = new MultipartUploadResponse("final-etag");
        when(mockBlobStore.completeMultipartUpload(expectedMpu, parts)).thenReturn(expectedCompleteResponse);

        CompletableFuture<MultipartUploadResponse> completeResult = gcpAsyncBlobStore.completeMultipartUpload(expectedMpu, parts);
        MultipartUploadResponse actualCompleteResponse = completeResult.get(5, TimeUnit.SECONDS);
        assertEquals(expectedCompleteResponse, actualCompleteResponse);

        // Test list multipart upload
        when(mockBlobStore.listMultipartUpload(expectedMpu)).thenReturn(parts);

        CompletableFuture<List<UploadPartResponse>> listResult = gcpAsyncBlobStore.listMultipartUpload(expectedMpu);
        List<UploadPartResponse> actualParts = listResult.get(5, TimeUnit.SECONDS);
        assertEquals(parts, actualParts);

        // Test abort multipart upload
        doNothing().when(mockBlobStore).abortMultipartUpload(expectedMpu);

        CompletableFuture<Void> abortResult = gcpAsyncBlobStore.abortMultipartUpload(expectedMpu);
        abortResult.get(5, TimeUnit.SECONDS);

        // Verify all calls
        verify(mockBlobStore).initiateMultipartUpload(mpuRequest);
        verify(mockBlobStore).uploadMultipartPart(expectedMpu, part);
        verify(mockBlobStore).completeMultipartUpload(expectedMpu, parts);
        verify(mockBlobStore).listMultipartUpload(expectedMpu);
        verify(mockBlobStore).abortMultipartUpload(expectedMpu);
    }

    @Test
    void testGetException() {
        // Given
        Throwable testException = new RuntimeException("Test exception");
        Class<? extends SubstrateSdkException> expectedExceptionClass = SubstrateSdkException.class;
        doReturn(expectedExceptionClass).when(mockBlobStore).getException(testException);

        // When
        Class<? extends SubstrateSdkException> actualExceptionClass = gcpAsyncBlobStore.getException(testException);

        // Then
        assertEquals(expectedExceptionClass, actualExceptionClass);
        verify(mockBlobStore).getException(testException);
    }

    @Test
    void testAsyncExecutionUsesProvidedExecutor() throws Exception {
        // Given
        UploadRequest uploadRequest = UploadRequest.builder().withKey(TEST_KEY).build();
        UploadResponse expectedResponse = UploadResponse.builder()
                .key(TEST_KEY)
                .eTag("test-etag")
                .build();
        byte[] content = "test content".getBytes();

        // Mock the blob store to take some time and capture the thread name
        AtomicInteger callCount = new AtomicInteger(0);
        when(mockBlobStore.upload(any(UploadRequest.class), any(byte[].class)))
                .thenAnswer(invocation -> {
                    callCount.incrementAndGet();
                    // Verify we're running on an executor thread, not the main test thread
                    String threadName = Thread.currentThread().getName();
                    assertTrue(threadName.startsWith("pool-"), 
                            "Expected to run on thread pool, but was: " + threadName);
                    return expectedResponse;
                });

        // When
        CompletableFuture<UploadResponse> result = gcpAsyncBlobStore.upload(uploadRequest, content);

        // Then
        UploadResponse actualResponse = result.get(5, TimeUnit.SECONDS);
        assertEquals(expectedResponse, actualResponse);
        assertEquals(1, callCount.get());
        verify(mockBlobStore).upload(uploadRequest, content);
    }

    @Test
    void testBuilder() {
        // Given
        var builder = (GcpAsyncBlobStore.Builder)new GcpAsyncBlobStore.Builder()
                .withStorage(mockStorage)
                .withTransformerSupplier(new GcpTransformerSupplier())
                .withExecutorService(executorService)
                .withBucket(TEST_BUCKET)
                .withRegion(TEST_REGION);

        // Then
        assertEquals(GcpConstants.PROVIDER_ID, builder.getProviderId());
        assertEquals(mockStorage, builder.getStorage());
        assertNotNull(builder.getTransformerSupplier());
        assertEquals(TEST_BUCKET, builder.getBucket());
        assertEquals(TEST_REGION, builder.getRegion());
        assertEquals(executorService, builder.getExecutorService());
    }

    @Test
    void testBuilderBuild() {
        // Note: This test would require more complex mocking of GcpBlobStoreBuilder.build()
        // For now, we'll just test that the builder can be constructed with the required parameters
        var builder = new GcpAsyncBlobStore.Builder()
                .withStorage(mockStorage)
                .withBucket(TEST_BUCKET)
                .withRegion(TEST_REGION);

        assertNotNull(builder);
        assertEquals(GcpConstants.PROVIDER_ID, builder.getProviderId());
    }

    @Test
    void testExceptionHandlingInAsyncOperations() {
        // Given
        UploadRequest uploadRequest = UploadRequest.builder().withKey(TEST_KEY).build();
        byte[] content = "test content".getBytes();
        RuntimeException expectedException = new RuntimeException("Test exception");
        
        when(mockBlobStore.upload(uploadRequest, content)).thenThrow(expectedException);

        // When
        CompletableFuture<UploadResponse> result = gcpAsyncBlobStore.upload(uploadRequest, content);

        // Then
        assertThrows(Exception.class, () -> result.get(5, TimeUnit.SECONDS));
        verify(mockBlobStore).upload(uploadRequest, content);
    }
} 