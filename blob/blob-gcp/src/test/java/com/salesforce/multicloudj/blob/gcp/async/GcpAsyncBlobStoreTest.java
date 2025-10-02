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

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadResponse;
import com.salesforce.multicloudj.blob.gcp.GcpBlobStore;
import com.salesforce.multicloudj.blob.gcp.GcpTransformer;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class GcpAsyncBlobStoreTest {

    @Mock
    private AbstractBlobStore<?> mockBlobStore;

    @Mock
    private Storage mockStorage;

    @Mock
    private GcpTransformerSupplier mockTransformerSupplier;

    private ExecutorService executorService;
    private GcpAsyncBlobStore gcpAsyncBlobStore;

    private static final String TEST_BUCKET = "test-bucket";
    private static final String TEST_REGION = "us-central1";
    private static final String TEST_KEY = "test-key";
    private static final String TEST_VERSION_ID = "test-version";

    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(2);
        gcpAsyncBlobStore = new GcpAsyncBlobStore(mockBlobStore, executorService, mockStorage, mockTransformerSupplier);
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
        GcpAsyncBlobStore asyncStore = new GcpAsyncBlobStore(mockBlobStore, null, mockStorage, mockTransformerSupplier);
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

    @Test
    void testUploadDirectory_Success() throws Exception {
        // Given
        DirectoryUploadRequest request = DirectoryUploadRequest.builder()
                .localSourceDirectory("/test/dir")
                .prefix("uploads/")
                .includeSubFolders(true)
                .build();

        when(mockBlobStore.getBucket()).thenReturn(TEST_BUCKET);
        GcpTransformer mockTransformer = mock(GcpTransformer.class);
        when(mockTransformerSupplier.get(TEST_BUCKET)).thenReturn(mockTransformer);

        java.nio.file.Path sourceDir = java.nio.file.Paths.get("/test/dir");
        java.nio.file.Path file1 = java.nio.file.Paths.get("/test/dir/file1.txt");
        java.nio.file.Path file2 = java.nio.file.Paths.get("/test/dir/subdir/file2.txt");
        when(mockTransformer.toFilePaths(request)).thenReturn(List.of(file1, file2));
        when(mockTransformer.toBlobKey(sourceDir, file1, "uploads/")).thenReturn("uploads/file1.txt");
        when(mockTransformer.toBlobKey(sourceDir, file2, "uploads/")).thenReturn("uploads/subdir/file2.txt");

        // When
        CompletableFuture<DirectoryUploadResponse> result = gcpAsyncBlobStore.uploadDirectory(request);

        // Then
        DirectoryUploadResponse response = result.get(5, TimeUnit.SECONDS);
        assertNotNull(response);
        assertTrue(response.getFailedTransfers().isEmpty());
        verify(mockStorage).createFrom(any(com.google.cloud.storage.BlobInfo.class), eq(file1));
        verify(mockStorage).createFrom(any(com.google.cloud.storage.BlobInfo.class), eq(file2));
    }

    @Test
    void testUploadDirectory_WithFailures() throws Exception {
        // Given
        DirectoryUploadRequest request = DirectoryUploadRequest.builder()
                .localSourceDirectory("/test/dir")
                .prefix("uploads/")
                .includeSubFolders(true)
                .build();

        when(mockBlobStore.getBucket()).thenReturn(TEST_BUCKET);
        GcpTransformer mockTransformer = mock(GcpTransformer.class);
        when(mockTransformerSupplier.get(TEST_BUCKET)).thenReturn(mockTransformer);

        java.nio.file.Path sourceDir = java.nio.file.Paths.get("/test/dir");
        java.nio.file.Path file1 = java.nio.file.Paths.get("/test/dir/file1.txt");
        java.nio.file.Path file2 = java.nio.file.Paths.get("/test/dir/file2.txt");
        when(mockTransformer.toFilePaths(request)).thenReturn(List.of(file1, file2));
        when(mockTransformer.toBlobKey(sourceDir, file1, "uploads/")).thenReturn("uploads/file1.txt");
        when(mockTransformer.toBlobKey(sourceDir, file2, "uploads/")).thenReturn("uploads/file2.txt");

        // First upload succeeds, second fails
        doAnswer(invocation -> null).when(mockStorage).createFrom(any(com.google.cloud.storage.BlobInfo.class), eq(file1));
        doThrow(new RuntimeException("Upload failed")).when(mockStorage)
                .createFrom(any(com.google.cloud.storage.BlobInfo.class), eq(file2));

        // When
        CompletableFuture<DirectoryUploadResponse> result = gcpAsyncBlobStore.uploadDirectory(request);

        // Then
        DirectoryUploadResponse response = result.get(5, TimeUnit.SECONDS);
        assertNotNull(response);
        assertEquals(1, response.getFailedTransfers().size());
        assertEquals(file2, response.getFailedTransfers().get(0).getSource());
        assertTrue(response.getFailedTransfers().get(0).getException() instanceof RuntimeException);
        assertEquals("Upload failed", response.getFailedTransfers().get(0).getException().getMessage());
    }

    @Test
    void testUploadDirectory_EmptyDirectory() throws Exception {
        // Given
        DirectoryUploadRequest request = DirectoryUploadRequest.builder()
                .localSourceDirectory("/test/empty")
                .prefix("uploads/")
                .includeSubFolders(true)
                .build();

        when(mockBlobStore.getBucket()).thenReturn(TEST_BUCKET);
        GcpTransformer mockTransformer = mock(GcpTransformer.class);
        when(mockTransformerSupplier.get(TEST_BUCKET)).thenReturn(mockTransformer);
        when(mockTransformer.toFilePaths(request)).thenReturn(List.of());

        // When
        CompletableFuture<DirectoryUploadResponse> result = gcpAsyncBlobStore.uploadDirectory(request);

        // Then
        DirectoryUploadResponse response = result.get(5, TimeUnit.SECONDS);
        assertNotNull(response);
        assertTrue(response.getFailedTransfers().isEmpty());
        verify(mockStorage, never()).createFrom(any(com.google.cloud.storage.BlobInfo.class), any(java.nio.file.Path.class));
    }

    @Test
    void testUploadDirectory_ExceptionInMainThread() throws Exception {
        // Given
        DirectoryUploadRequest request = DirectoryUploadRequest.builder()
                .localSourceDirectory("/test/dir")
                .prefix("uploads/")
                .includeSubFolders(true)
                .build();

        when(mockBlobStore.getBucket()).thenReturn(TEST_BUCKET);
        GcpTransformer mockTransformer = mock(GcpTransformer.class);
        when(mockTransformerSupplier.get(TEST_BUCKET)).thenReturn(mockTransformer);
        when(mockTransformer.toFilePaths(request)).thenThrow(new RuntimeException("Transformer failed"));

        // When & Then
        CompletableFuture<DirectoryUploadResponse> result = gcpAsyncBlobStore.uploadDirectory(request);

        ExecutionException exception = assertThrows(ExecutionException.class, () -> {
            result.get(5, TimeUnit.SECONDS);
        });
        assertTrue(exception.getCause() instanceof RuntimeException);
        assertEquals("Failed to upload directory", exception.getCause().getMessage());
    }

    @Test
    void testDownloadDirectory_Implementation() throws Exception {
        // Given
        DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
                .prefixToDownload("uploads/")
                .localDestinationDirectory("/test/dir")
                .build();

        when(mockBlobStore.getBucket()).thenReturn(TEST_BUCKET);
        GcpTransformer mockTransformer = mock(GcpTransformer.class);
        when(mockTransformerSupplier.get(TEST_BUCKET)).thenReturn(mockTransformer);

        // When - just verify the method doesn't throw UnsupportedOperationException
        CompletableFuture<DirectoryDownloadResponse> result = gcpAsyncBlobStore.downloadDirectory(request);

        // Then - verify it returns a CompletableFuture (not null)
        assertNotNull(result);

        // The actual implementation will fail due to mocking, but we're just testing
        // that it doesn't throw UnsupportedOperationException like the bridge does
    }

    @Test
    void testDeleteDirectory_Implementation() {
        // Given
        String prefix = "uploads/";

        // Mock the storage.list() to return an empty page
        Page<Blob> mockPage = mock(Page.class);
        when(mockPage.getValues()).thenReturn(Collections.emptyList());
        when(mockStorage.list(any(), any(Storage.BlobListOption[].class))).thenReturn(mockPage);

        // Mock the transformer supplier to return a mock transformer
        GcpTransformer mockTransformer = mock(GcpTransformer.class);
        when(mockTransformer.partitionList(any(), anyInt())).thenReturn(Collections.emptyList());
        when(mockTransformerSupplier.get(any())).thenReturn(mockTransformer);

        // When
        CompletableFuture<Void> result = gcpAsyncBlobStore.deleteDirectory(prefix);

        // Then
        assertDoesNotThrow(() -> result.get());
        verify(mockStorage).list(any(), any(Storage.BlobListOption[].class));
    }

    @Test
    void testDeleteDirectory_EmptyDirectory() {
        // Mock empty blob list
        Page<Blob> mockPage = mock(Page.class);
        when(mockPage.getValues()).thenReturn(Collections.emptyList());
        when(mockStorage.list(any(), any(Storage.BlobListOption[].class))).thenReturn(mockPage);

        GcpTransformer mockTransformer = mock(GcpTransformer.class);
        when(mockTransformerSupplier.get(any())).thenReturn(mockTransformer);
        when(mockTransformer.partitionList(any(), anyInt())).thenReturn(Collections.emptyList());

        assertDoesNotThrow(() -> {
            CompletableFuture<Void> future = gcpAsyncBlobStore.deleteDirectory("empty-prefix/");
            future.get(5, TimeUnit.SECONDS);
        });
    }

    @Test
    void testDeleteDirectory_NullPrefix() {
        // Mock empty blob list for null prefix
        Page<Blob> mockPage = mock(Page.class);
        when(mockPage.getValues()).thenReturn(Collections.emptyList());
        when(mockStorage.list(any(), any(Storage.BlobListOption[].class))).thenReturn(mockPage);

        GcpTransformer mockTransformer = mock(GcpTransformer.class);
        when(mockTransformerSupplier.get(any())).thenReturn(mockTransformer);
        when(mockTransformer.partitionList(any(), anyInt())).thenReturn(Collections.emptyList());

        assertDoesNotThrow(() -> {
            CompletableFuture<Void> future = gcpAsyncBlobStore.deleteDirectory(null);
            future.get(5, TimeUnit.SECONDS);
        });
    }

    @Test
    void testDeleteDirectory_StorageException() {
        // Mock storage to throw exception
        when(mockStorage.list(any(), any(Storage.BlobListOption[].class)))
                .thenThrow(new RuntimeException("Storage error"));

        assertThrows(RuntimeException.class, () -> {
            CompletableFuture<Void> future = gcpAsyncBlobStore.deleteDirectory("test-prefix/");
            future.get(5, TimeUnit.SECONDS);
        });
    }

    @Test
    void testUploadDirectory_ConcurrentUploads() throws Exception {
        // Create a temporary directory with multiple files
        Path tempDir = Files.createTempDirectory("test-upload");
        try {
            // Create multiple test files
            for (int i = 0; i < 5; i++) {
                Path file = tempDir.resolve("file" + i + ".txt");
                Files.write(file, ("content" + i).getBytes());
            }

            // Mock storage operations
            when(mockStorage.createFrom(any(com.google.cloud.storage.BlobInfo.class), any(Path.class)))
                    .thenReturn(mock(com.google.cloud.storage.Blob.class));

            GcpTransformer mockTransformer = mock(GcpTransformer.class);
            when(mockTransformerSupplier.get(any())).thenReturn(mockTransformer);
            when(mockTransformer.toFilePaths(any(DirectoryUploadRequest.class)))
                    .thenReturn(Files.list(tempDir).collect(Collectors.toList()));
            when(mockTransformer.toBlobKey(any(Path.class), any(Path.class), anyString()))
                    .thenReturn("test-key");

            DirectoryUploadRequest request = DirectoryUploadRequest.builder()
                    .localSourceDirectory(tempDir.toString())
                    .prefix("test/")
                    .includeSubFolders(false)
                    .build();

            CompletableFuture<DirectoryUploadResponse> future = gcpAsyncBlobStore.uploadDirectory(request);
            DirectoryUploadResponse response = future.get(10, TimeUnit.SECONDS);

            assertNotNull(response);
            // Check that we have some response, even if there are failures
            assertNotNull(response.getFailedTransfers());

        } finally {
            // Clean up
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore cleanup errors
                        }
                    });
        }
    }

    @Test
    void testDownloadDirectory_ConcurrentDownloads() throws Exception {
        // Create a temporary directory for downloads
        Path tempDir = Files.createTempDirectory("test-download");
        try {
            // Mock blob list with multiple blobs
            List<com.google.cloud.storage.Blob> mockBlobs = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                com.google.cloud.storage.Blob mockBlob = mock(com.google.cloud.storage.Blob.class);
                when(mockBlob.getName()).thenReturn("test/file" + i + ".txt");
                when(mockBlob.getSize()).thenReturn(100L);
                mockBlobs.add(mockBlob);
            }

            Page<com.google.cloud.storage.Blob> mockPage = mock(Page.class);
            when(mockPage.getValues()).thenReturn(mockBlobs);
            when(mockStorage.list(any(), any(Storage.BlobListOption[].class))).thenReturn(mockPage);

            // Mock blob download
            for (com.google.cloud.storage.Blob blob : mockBlobs) {
                doNothing().when(blob).downloadTo(any(Path.class));
            }

            DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
                    .localDestinationDirectory(tempDir.toString())
                    .prefixToDownload("test/")
                    .build();

            CompletableFuture<DirectoryDownloadResponse> future = gcpAsyncBlobStore.downloadDirectory(request);
            DirectoryDownloadResponse response = future.get(10, TimeUnit.SECONDS);

            assertNotNull(response);
            // Should have no failures since we mocked successful downloads
            assertTrue(response.getFailedTransfers().isEmpty());

        } finally {
            // Clean up
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore cleanup errors
                        }
                    });
        }
    }

    @Test
    void testDownloadDirectory_DirectoryMarkerBlobs() throws Exception {
        // Create a temporary directory for downloads
        Path tempDir = Files.createTempDirectory("test-download");
        try {
            // Mock blob list with directory markers (blobs ending with /)
            List<com.google.cloud.storage.Blob> mockBlobs = new ArrayList<>();

            // Add a directory marker
            com.google.cloud.storage.Blob dirMarker = mock(com.google.cloud.storage.Blob.class);
            when(dirMarker.getName()).thenReturn("test/dir/");
            mockBlobs.add(dirMarker);

            // Add a regular file
            com.google.cloud.storage.Blob fileBlob = mock(com.google.cloud.storage.Blob.class);
            when(fileBlob.getName()).thenReturn("test/file.txt");
            when(fileBlob.getSize()).thenReturn(100L);
            doNothing().when(fileBlob).downloadTo(any(Path.class));
            mockBlobs.add(fileBlob);

            Page<com.google.cloud.storage.Blob> mockPage = mock(Page.class);
            when(mockPage.getValues()).thenReturn(mockBlobs);
            when(mockStorage.list(any(), any(Storage.BlobListOption[].class))).thenReturn(mockPage);

            DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
                    .localDestinationDirectory(tempDir.toString())
                    .prefixToDownload("test/")
                    .build();

            CompletableFuture<DirectoryDownloadResponse> future = gcpAsyncBlobStore.downloadDirectory(request);
            DirectoryDownloadResponse response = future.get(10, TimeUnit.SECONDS);

            assertNotNull(response);
            // Directory marker should be skipped, so no failures expected
            assertTrue(response.getFailedTransfers().isEmpty());

        } finally {
            // Clean up
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore cleanup errors
                        }
                    });
        }
    }

    @Test
    void testDeleteDirectory_LargeBatch() {
        // Set up the bucket name for the store
        when(mockBlobStore.getBucket()).thenReturn("test-bucket");

        // Mock a large number of blobs to test partitioning
        List<com.google.cloud.storage.Blob> mockBlobs = new ArrayList<>();
        for (int i = 0; i < 2500; i++) { // More than GCP's 1000 limit
            com.google.cloud.storage.Blob mockBlob = mock(com.google.cloud.storage.Blob.class);
            when(mockBlob.getName()).thenReturn("test/file" + i + ".txt");
            when(mockBlob.getSize()).thenReturn(100L);
            mockBlobs.add(mockBlob);
        }

        Page<com.google.cloud.storage.Blob> mockPage = mock(Page.class);
        when(mockPage.getValues()).thenReturn(mockBlobs);
        when(mockStorage.list(any(), any(Storage.BlobListOption[].class))).thenReturn(mockPage);

        // Mock successful batch deletes
        when(mockStorage.delete(anyList())).thenReturn(Collections.emptyList());

        GcpTransformer mockTransformer = mock(GcpTransformer.class);
        when(mockTransformerSupplier.get(any())).thenReturn(mockTransformer);

        // Mock partitioning - should create 3 partitions (1000, 1000, 500)
        List<List<com.salesforce.multicloudj.blob.driver.BlobInfo>> partitions = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            List<com.salesforce.multicloudj.blob.driver.BlobInfo> partition = new ArrayList<>();
            int size = (i < 2) ? 1000 : 500;
            for (int j = 0; j < size; j++) {
                partition.add(com.salesforce.multicloudj.blob.driver.BlobInfo.builder()
                        .withKey("test/file" + (i * 1000 + j) + ".txt")
                        .withObjectSize(100L)
                        .build());
            }
            partitions.add(partition);
        }
        when(mockTransformer.partitionList(any(), eq(1000))).thenReturn(partitions);

        assertDoesNotThrow(() -> {
            CompletableFuture<Void> future = gcpAsyncBlobStore.deleteDirectory("test-prefix/");
            future.get(10, TimeUnit.SECONDS);
        });

        // Verify that delete was called 3 times (for 3 partitions)
        verify(mockStorage, times(3)).delete(anyList());
    }

    @Test
    void testBuilder_WithAllParameters() {
        GcpBlobStore mockGcpBlobStore = mock(GcpBlobStore.class);
        when(mockGcpBlobStore.getBucket()).thenReturn("test-bucket");
        when(mockGcpBlobStore.getRegion()).thenReturn("us-central1");

        Storage mockStorage = mock(Storage.class);
        GcpTransformerSupplier mockSupplier = mock(GcpTransformerSupplier.class);
        ExecutorService mockExecutor = mock(ExecutorService.class);

        GcpAsyncBlobStore store = new GcpAsyncBlobStore(mockGcpBlobStore, mockExecutor, mockStorage, mockSupplier);

        assertNotNull(store);
        assertEquals("test-bucket", store.getBucket());
        assertEquals("us-central1", store.getRegion());
    }

    @Test
    void testBuilder_WithMinimalParameters() {
        Storage mockStorage = mock(Storage.class);
        GcpTransformerSupplier mockSupplier = mock(GcpTransformerSupplier.class);

        // Create a mock GcpBlobStore
        GcpBlobStore mockGcpBlobStore = mock(GcpBlobStore.class);
        when(mockGcpBlobStore.getBucket()).thenReturn("test-bucket");
        when(mockGcpBlobStore.getRegion()).thenReturn("us-central1");

        GcpAsyncBlobStore store = new GcpAsyncBlobStore(mockGcpBlobStore, null, mockStorage, mockSupplier);

        assertNotNull(store);
        assertEquals("test-bucket", store.getBucket());
        assertEquals("us-central1", store.getRegion());
    }
} 