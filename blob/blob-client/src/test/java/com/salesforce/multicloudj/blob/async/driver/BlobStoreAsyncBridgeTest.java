package com.salesforce.multicloudj.blob.async.driver;

import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
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
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import org.junit.jupiter.api.AfterEach;

import java.util.concurrent.ExecutionException;
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
class BlobStoreAsyncBridgeTest {

    @Mock
    private AbstractBlobStore<?> mockBlobStore;

    private ExecutorService executorService;
    private BlobStoreAsyncBridge asyncWrapper;

    private static final String TEST_BUCKET = "test-bucket";
    private static final String TEST_REGION = "us-west-2";
    private static final String TEST_PROVIDER_ID = "test-provider";
    private static final String TEST_KEY = "test-key";
    private static final String TEST_VERSION_ID = "test-version";

    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(2);
        asyncWrapper = new BlobStoreAsyncBridge(mockBlobStore, executorService);
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
        when(mockBlobStore.getProviderId()).thenReturn(TEST_PROVIDER_ID);

        assertEquals(TEST_BUCKET, asyncWrapper.getBucket());
        assertEquals(TEST_REGION, asyncWrapper.getRegion());
        assertEquals(TEST_PROVIDER_ID, asyncWrapper.getProviderId());
        assertEquals(mockBlobStore, asyncWrapper.getBlobStore());
        assertEquals(executorService, asyncWrapper.getExecutorService());
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
        CompletableFuture<UploadResponse> result = asyncWrapper.upload(uploadRequest, inputStream);

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
        CompletableFuture<UploadResponse> result = asyncWrapper.upload(uploadRequest, content);

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
        CompletableFuture<UploadResponse> result = asyncWrapper.upload(uploadRequest, testFile);

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
        CompletableFuture<UploadResponse> result = asyncWrapper.upload(uploadRequest, testPath);

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
        CompletableFuture<DownloadResponse> result = asyncWrapper.download(downloadRequest, outputStream);

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
        CompletableFuture<DownloadResponse> result = asyncWrapper.download(downloadRequest, byteArray);

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
        CompletableFuture<Void> result = asyncWrapper.delete(TEST_KEY, TEST_VERSION_ID);

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
        CompletableFuture<Void> result = asyncWrapper.delete(identifiers);

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
        CompletableFuture<CopyResponse> result = asyncWrapper.copy(copyRequest);

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
        CompletableFuture<BlobMetadata> result = asyncWrapper.getMetadata(TEST_KEY, TEST_VERSION_ID);

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
        CompletableFuture<Void> result = asyncWrapper.list(listRequest, consumer);

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
        CompletableFuture<URL> result = asyncWrapper.generatePresignedUrl(presignedRequest);

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
        CompletableFuture<Boolean> result = asyncWrapper.doesObjectExist(TEST_KEY, TEST_VERSION_ID);

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
        CompletableFuture<Map<String, String>> result = asyncWrapper.getTags(TEST_KEY);

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
        CompletableFuture<Void> result = asyncWrapper.setTags(TEST_KEY, tags);

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

        CompletableFuture<MultipartUpload> initiateResult = asyncWrapper.initiateMultipartUpload(mpuRequest);
        MultipartUpload actualMpu = initiateResult.get(5, TimeUnit.SECONDS);
        assertEquals(expectedMpu, actualMpu);

        // Test upload part
        MultipartPart part = new MultipartPart(1, "test data".getBytes());
        UploadPartResponse expectedPartResponse = new UploadPartResponse(1, "part-etag", 9);
        when(mockBlobStore.uploadMultipartPart(expectedMpu, part)).thenReturn(expectedPartResponse);

        CompletableFuture<UploadPartResponse> uploadPartResult = asyncWrapper.uploadMultipartPart(expectedMpu, part);
        UploadPartResponse actualPartResponse = uploadPartResult.get(5, TimeUnit.SECONDS);
        assertEquals(expectedPartResponse, actualPartResponse);

        // Test complete multipart upload
        List<UploadPartResponse> parts = List.of(expectedPartResponse);
        MultipartUploadResponse expectedCompleteResponse = new MultipartUploadResponse("final-etag");
        when(mockBlobStore.completeMultipartUpload(expectedMpu, parts)).thenReturn(expectedCompleteResponse);

        CompletableFuture<MultipartUploadResponse> completeResult = asyncWrapper.completeMultipartUpload(expectedMpu, parts);
        MultipartUploadResponse actualCompleteResponse = completeResult.get(5, TimeUnit.SECONDS);
        assertEquals(expectedCompleteResponse, actualCompleteResponse);

        // Test list multipart upload
        when(mockBlobStore.listMultipartUpload(expectedMpu)).thenReturn(parts);

        CompletableFuture<List<UploadPartResponse>> listResult = asyncWrapper.listMultipartUpload(expectedMpu);
        List<UploadPartResponse> actualParts = listResult.get(5, TimeUnit.SECONDS);
        assertEquals(parts, actualParts);

        // Test abort multipart upload
        doNothing().when(mockBlobStore).abortMultipartUpload(expectedMpu);

        CompletableFuture<Void> abortResult = asyncWrapper.abortMultipartUpload(expectedMpu);
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
        Class<? extends SubstrateSdkException> actualExceptionClass = asyncWrapper.getException(testException);

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
        CompletableFuture<UploadResponse> result = asyncWrapper.upload(uploadRequest, content);

        // Then
        UploadResponse actualResponse = result.get(5, TimeUnit.SECONDS);
        assertEquals(expectedResponse, actualResponse);
        assertEquals(1, callCount.get());
        verify(mockBlobStore).upload(uploadRequest, content);
    }

    @Test
    void doDownloadDirectory() throws Exception {
        // Given
        DirectoryDownloadRequest request = mock(DirectoryDownloadRequest.class);
        DirectoryDownloadResponse expectedResponse = mock(DirectoryDownloadResponse.class);
        when(mockBlobStore.downloadDirectory(request)).thenReturn(expectedResponse);

        // When
        CompletableFuture<DirectoryDownloadResponse> result = asyncWrapper.downloadDirectory(request);

        // Then
        DirectoryDownloadResponse actualResponse = result.get();
        assertEquals(expectedResponse, actualResponse);
        verify(mockBlobStore).downloadDirectory(request);
    }

    @Test
    void doDeleteDirectory() throws Exception {
        // Given
        String prefix = "files";
        doNothing().when(mockBlobStore).deleteDirectory(prefix);

        // When
        CompletableFuture<Void> result = asyncWrapper.deleteDirectory(prefix);

        // Then
        result.get(); // Should complete without exception
        verify(mockBlobStore).deleteDirectory(prefix);
    }

    @Test
    void testDownloadDirectory_UnsupportedOperation() throws Exception {
        // Given
        DirectoryDownloadRequest request = mock(DirectoryDownloadRequest.class);
        when(mockBlobStore.downloadDirectory(request))
                .thenThrow(new UnsupportedOperationException("Directory download not supported"));

        // When
        CompletableFuture<DirectoryDownloadResponse> result = asyncWrapper.downloadDirectory(request);

        // Then
        ExecutionException exception = assertThrows(ExecutionException.class, () -> {
            result.get();
        });
        assertTrue(exception.getCause() instanceof UnsupportedOperationException);
        verify(mockBlobStore).downloadDirectory(request);
    }

    @Test
    void testDeleteDirectory_UnsupportedOperation() throws Exception {
        // Given
        String prefix = "test-prefix";
        doThrow(new UnsupportedOperationException("Directory delete not supported"))
                .when(mockBlobStore).deleteDirectory(prefix);

        // When
        CompletableFuture<Void> result = asyncWrapper.deleteDirectory(prefix);

        // Then
        ExecutionException exception = assertThrows(ExecutionException.class, () -> {
            result.get();
        });
        assertTrue(exception.getCause() instanceof UnsupportedOperationException);
        verify(mockBlobStore).deleteDirectory(prefix);
    }

} 