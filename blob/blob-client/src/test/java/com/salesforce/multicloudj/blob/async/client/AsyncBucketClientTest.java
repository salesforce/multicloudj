package com.salesforce.multicloudj.blob.async.client;

import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStore;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStoreProvider;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.FailedBlobUpload;
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
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;

import static com.salesforce.multicloudj.blob.async.driver.TestAsyncBlobStore.PROVIDER_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AsyncBucketClientTest {

    private AsyncBlobStore mockBlobStore;
    private StsCredentials creds;
    private AsyncBucketClient client;

    private MockedStatic<ProviderSupplier> providerSupplier;
    private MockedStatic<ExceptionHandler> mockedExceptionHandler;

    @BeforeEach
    void setup() {
        mockedExceptionHandler = mockStatic(ExceptionHandler.class);
        mockedExceptionHandler
                .when(() -> ExceptionHandler.handleAndPropagate(any(), any()))
                .thenThrow(UnAuthorizedException.class);

        mockBlobStore = mock(AsyncBlobStore.class);
        providerSupplier = mockStatic(ProviderSupplier.class);
        AsyncBlobStoreProvider.Builder mockBuilder = mock(AsyncBlobStoreProvider.Builder.class);
        when(mockBuilder.build()).thenReturn(mockBlobStore);
        providerSupplier.when(() -> ProviderSupplier.findAsyncBuilder(PROVIDER_ID)).thenReturn(mockBuilder);
        creds = new StsCredentials("keyId", "keySecret", "token");
        CredentialsOverrider credsOverrider = new CredentialsOverrider
                .Builder(CredentialsType.SESSION)
                .withSessionCredentials(creds)
                .build();

        Properties properties = new Properties();
        properties.setProperty("bucket", "bucket-1");

        client = AsyncBucketClient
                .builder(PROVIDER_ID)
                .withBucket("bucket-1")
                .withRegion("us-west-1")
                .withCredentialsOverrider(credsOverrider)
                .withEndpoint(URI.create("https://blob.endpoint.com"))
                .withProxyEndpoint(URI.create("https://proxy.endpoint.com:443"))
                .withMaxConnections(100)
                .withSocketTimeout(Duration.ofSeconds(60))
                .withIdleConnectionTimeout(Duration.ofMinutes(10))
                .withProperties(properties)
                .withExecutorService(ForkJoinPool.commonPool())
                .build();
    }

    @AfterEach
    void testdown() {
        if (providerSupplier != null) {
            providerSupplier.close();
        }

        if (mockedExceptionHandler != null) {
            mockedExceptionHandler.close();
        }
    }

    // Shorthand method to clean up verbose code:
    <T> CompletableFuture<T> future(T value) {
        return CompletableFuture.completedFuture(value);
    }

    CompletableFuture<Void> futureVoid() {
        return future(null);
    }

    <T extends Throwable> void assertFailed(CompletableFuture<?> future, Class<T> expectedType) {
        assertTrue(future.isCompletedExceptionally());

        // when calling .get() on a CompletableFuture, it will throw an ExecutionException
        // but the root error will be the cause of the thrown exception, so we must unpack the
        // thrown exception in order to extract the cause, which should be and UnAuthorizedException:
        ExecutionException error = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(expectedType, error.getCause());
    }

    @Test
    void testUploadInputStream() throws ExecutionException, InterruptedException {
        UploadResponse expectedResponse = UploadResponse.builder().eTag("eTag-1").build();
        when(mockBlobStore.upload(any(), any(InputStream.class))).thenReturn(future(expectedResponse));

        byte[] content = "Test data".getBytes();
        InputStream inputStream = new ByteArrayInputStream(content);
        UploadRequest request = UploadRequest.builder()
                .withKey("object-1")
                .withContentLength(content.length)
                .build();

        UploadResponse actualResponse = client.upload(request, inputStream).get();
        verify(mockBlobStore, times(1)).upload(eq(request), eq(inputStream));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    void testUploadByteArray() throws ExecutionException, InterruptedException {
        UploadResponse expectedResponse = UploadResponse.builder().eTag("eTag-1").build();
        when(mockBlobStore.upload(any(), any(byte[].class))).thenReturn(future(expectedResponse));

        byte[] content = "Test data".getBytes();
        UploadRequest request = UploadRequest.builder()
                .withKey("object-1")
                .withContentLength(content.length)
                .build();

        UploadResponse actualResponse = client.upload(request, content).get();
        verify(mockBlobStore, times(1)).upload(eq(request), eq(content));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    void testUploadFile() throws ExecutionException, InterruptedException {
        UploadResponse expectedResponse = UploadResponse.builder().eTag("eTag-1").build();
        when(mockBlobStore.upload(any(), any(File.class))).thenReturn(future(expectedResponse));

        File file = new File("test.txt");
        UploadRequest request = UploadRequest.builder()
                .withKey("object-1")
                .withContentLength(1024L)
                .build();

        UploadResponse actualResponse = client.upload(request, file).get();
        verify(mockBlobStore, times(1)).upload(eq(request), eq(file));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    void testUploadPath() throws ExecutionException, InterruptedException {
        UploadResponse expectedResponse = UploadResponse.builder().eTag("eTag-1").build();
        when(mockBlobStore.upload(any(), any(Path.class))).thenReturn(future(expectedResponse));

        Path path = Paths.get("test.txt");
        UploadRequest request = UploadRequest.builder()
                .withKey("object-1")
                .withContentLength(1024L)
                .build();

        UploadResponse actualResponse = client.upload(request, path).get();
        verify(mockBlobStore, times(1)).upload(eq(request), eq(path));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    void testUploadThrowsFutureException() {
        CompletableFuture<UploadResponse> failure = CompletableFuture.failedFuture(new RuntimeException());
        when(mockBlobStore.upload(any(), any(InputStream.class))).thenReturn(failure);
        when(mockBlobStore.upload(any(), any(byte[].class))).thenReturn(failure);
        when(mockBlobStore.upload(any(), any(File.class))).thenReturn(failure);
        when(mockBlobStore.upload(any(), any(Path.class))).thenReturn(failure);

        var result = client.upload(mock(UploadRequest.class), mock(InputStream.class));
        assertFailed(result, UnAuthorizedException.class);
        result = client.upload(mock(UploadRequest.class), "Test data".getBytes());
        assertFailed(result, UnAuthorizedException.class);
        result = client.upload(mock(UploadRequest.class), new File("test.txt"));
        assertFailed(result, UnAuthorizedException.class);
        result = client.upload(mock(UploadRequest.class), Paths.get("test.txt"));
        assertFailed(result, UnAuthorizedException.class);
    }

    @Test
    void testDownloadOutputStream() throws ExecutionException, InterruptedException {
        OutputStream outputStream = mock(OutputStream.class);
        DownloadRequest request = new DownloadRequest.Builder().withKey("object-1").build();
        DownloadResponse response = mock(DownloadResponse.class);
        when(mockBlobStore.download(any(), any(OutputStream.class))).thenReturn(future(response));
        client.download(request, outputStream).get();
        verify(mockBlobStore, times(1)).download(eq(request), eq(outputStream));
    }

    @Test
    void testDownloadByteArrayWrapper() throws ExecutionException, InterruptedException {
        ByteArray byteArray = new ByteArray();
        DownloadRequest request = new DownloadRequest.Builder().withKey("object-1").build();
        DownloadResponse response = mock(DownloadResponse.class);
        when(mockBlobStore.download(any(), any(ByteArray.class))).thenReturn(future(response));
        client.download(request, byteArray).get();
        verify(mockBlobStore, times(1)).download(eq(request), eq(byteArray));
    }

    @Test
    void testDownloadFile() throws ExecutionException, InterruptedException {
        File file = new File("testFile.txt");
        DownloadRequest request = new DownloadRequest.Builder().withKey("object-1").build();
        DownloadResponse response = mock(DownloadResponse.class);
        when(mockBlobStore.download(any(), any(File.class))).thenReturn(future(response));
        client.download(request, file).get();
        verify(mockBlobStore, times(1)).download(eq(request), eq(file));
    }

    @Test
    void testDownloadPath() throws ExecutionException, InterruptedException {
        Path output = mock(Path.class);
        DownloadRequest request = new DownloadRequest.Builder().withKey("object-1").build();
        DownloadResponse response = mock(DownloadResponse.class);
        when(mockBlobStore.download(any(), any(Path.class))).thenReturn(future(response));
        client.download(request, output).get();
        verify(mockBlobStore, times(1)).download(eq(request), eq(output));
    }

    @Test
    void testDownloadThrowsException() {
        CompletableFuture<DownloadResponse> failure = CompletableFuture.failedFuture(new RuntimeException());
        when(mockBlobStore.download(any(), any(OutputStream.class))).thenReturn(failure);
        when(mockBlobStore.download(any(), any(ByteArray.class))).thenReturn(failure);
        when(mockBlobStore.download(any(), any(File.class))).thenReturn(failure);
        when(mockBlobStore.download(any(), any(Path.class))).thenReturn(failure);

        DownloadRequest request = mock(DownloadRequest.class);
        CompletableFuture<DownloadResponse> result = client.download(request, mock(OutputStream.class));
        assertFailed(result, UnAuthorizedException.class);
        result = client.download(request, mock(ByteArray.class));
        assertFailed(result, UnAuthorizedException.class);
        result = client.download(request, mock(File.class));
        assertFailed(result, UnAuthorizedException.class);
        result = client.download(request, mock(Path.class));
        assertFailed(result, UnAuthorizedException.class);
    }

    @Test
    void testDelete() throws ExecutionException, InterruptedException {
        when(mockBlobStore.delete(anyString(), anyString())).thenReturn(futureVoid());
        client.delete("object-1", "version-1").get();
        verify(mockBlobStore, times(1)).delete(eq("object-1"), eq("version-1"));
    }

    @Test
    void testBulkDelete() {
        List<BlobIdentifier> objects = List.of(new BlobIdentifier("object-1","version-1"),
                new BlobIdentifier("object-2","version-2"),
                new BlobIdentifier("object-3","version-3"));
        when(mockBlobStore.delete(objects)).thenReturn(futureVoid());
        client.delete(objects);
        verify(mockBlobStore, times(1)).delete(eq(objects));
    }

    @Test
    void testDeleteThrowsException() {
        CompletableFuture<Void> failure = CompletableFuture.failedFuture(new RuntimeException());
        when(mockBlobStore.delete(anyString(), anyString())).thenReturn(failure);
        when(mockBlobStore.delete(any())).thenReturn(failure);

        assertFailed(client.delete("object-1", "version-1"), UnAuthorizedException.class);
        assertFailed(client.delete(List.of(new BlobIdentifier("object-1", "version-1"))), UnAuthorizedException.class);
    }

    @Test
    void testCopy() throws ExecutionException, InterruptedException {
        Instant now = Instant.now();

        String destKey = "dest-object-1";
        CopyResponse expectedResponse = CopyResponse.builder()
                .key(destKey)
                .versionId("version-1")
                .eTag("eTag-3")
                .lastModified(now)
                .build();
        CopyRequest request = CopyRequest
                .builder()
                .srcKey("src-object-1")
                .srcVersionId("version-1")
                .destKey(destKey)
                .destBucket("dest-bucket-1")
                .build();
        when(mockBlobStore.copy(any())).thenReturn(future(expectedResponse));

        CopyResponse actualResponse = client
                .copy(request)
                .get();
        verify(mockBlobStore, times(1)).copy(request);
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    void testCopyThrowsException() {
        CompletableFuture<CopyResponse> failure = CompletableFuture.failedFuture(new RuntimeException());
        when(mockBlobStore.copy(any())).thenReturn(failure);

        CopyRequest request = CopyRequest
                .builder()
                .srcKey("src-object-1")
                .srcVersionId("version-1")
                .destKey("dest-object-1")
                .destBucket("dest-bucket-1")
                .build();
        var result = client.copy(request);
        assertFailed(result, UnAuthorizedException.class);
    }

    @Test
    void testGetMetadata() throws ExecutionException, InterruptedException {
        Instant now = Instant.now();
        BlobMetadata expectedBlobInfo = BlobMetadata.builder()
                .key("object-1")
                .versionId("version-1")
                .eTag("eTag-1")
                .objectSize(10)
                .metadata(Map.of("key-1", "value-1"))
                .lastModified(now)
                .build();

        when(mockBlobStore.getMetadata(any(), any())).thenReturn(future(expectedBlobInfo));
        BlobMetadata actualBlobMetadata = client.getMetadata("object-1", "version-1").get();
        verify(mockBlobStore, times(1)).getMetadata(eq("object-1"), eq("version-1"));
        assertEquals("object-1", actualBlobMetadata.getKey());
        assertEquals("version-1", actualBlobMetadata.getVersionId());
        assertEquals("eTag-1", actualBlobMetadata.getETag());
        assertEquals(10, actualBlobMetadata.getObjectSize());
        assertEquals("value-1", actualBlobMetadata.getMetadata().get("key-1"));
        assertEquals(now, actualBlobMetadata.getLastModified());
    }

    @Test
    void testGetMetadataThrowsException() {
        CompletableFuture<BlobMetadata> failure = CompletableFuture.failedFuture(new RuntimeException());
        when(mockBlobStore.getMetadata(any(), any())).thenReturn(failure);

        assertFailed(client.getMetadata("object-1", "version-1"), UnAuthorizedException.class);
    }

    @Test
    void testList() {
        ListBlobsRequest request = new ListBlobsRequest.Builder().build();
        Consumer<ListBlobsBatch> consumer = batch -> {};
        when(mockBlobStore.list(request, consumer)).thenReturn(futureVoid());
        client.list(request, consumer);
        verify(mockBlobStore, times(1)).list(request, consumer);
    }

    @Test
    void testListThrowsException() {
        ListBlobsRequest request = new ListBlobsRequest.Builder().build();
        Consumer<ListBlobsBatch> consumer = batch -> {};
        CompletableFuture<Void> failure = CompletableFuture.failedFuture(new RuntimeException());
        when(mockBlobStore.list(request, consumer)).thenReturn(failure);

        assertFailed(client.list(request, consumer), UnAuthorizedException.class);
    }

    @Test
    void testInitiateMultipartUpload() {
        MultipartUploadRequest request = new MultipartUploadRequest.Builder().withKey("object-1").build();
        doReturn(future(mock(MultipartUpload.class))).when(mockBlobStore).initiateMultipartUpload(request);
        client.initiateMultipartUpload(request);
        verify(mockBlobStore, times(1)).initiateMultipartUpload(request);
    }

    @Test
    void testInitiateMultipartUploadException() {
        MultipartUploadRequest request = new MultipartUploadRequest.Builder().withKey("object-1").build();
        CompletableFuture<Void> failure = CompletableFuture.failedFuture(new RuntimeException());
        doReturn(failure).when(mockBlobStore).initiateMultipartUpload(request);
        assertFailed(client.initiateMultipartUpload(request), UnAuthorizedException.class);
    }

    @Test
    void testUploadMultipartPart() {
        MultipartUpload multipartUpload = new MultipartUpload("bucket-1", "object-1", "mpu-id");
        MultipartPart multipartPart = new MultipartPart(1, null, 0);
        doReturn(future(mock(UploadPartResponse.class))).when(mockBlobStore).uploadMultipartPart(multipartUpload, multipartPart);
        client.uploadMultipartPart(multipartUpload, multipartPart);
        verify(mockBlobStore, times(1)).uploadMultipartPart(multipartUpload, multipartPart);
    }

    @Test
    void testUploadMultipartPartException() {
        MultipartUpload multipartUpload = new MultipartUpload("bucket-1", "object-1", "mpu-id");
        MultipartPart multipartPart = new MultipartPart(1, null, 0);
        CompletableFuture<Void> failure = CompletableFuture.failedFuture(new RuntimeException());
        doReturn(failure).when(mockBlobStore).uploadMultipartPart(multipartUpload, multipartPart);
        assertFailed(client.uploadMultipartPart(multipartUpload, multipartPart), UnAuthorizedException.class);
    }

    @Test
    void testCompleteMultipartUpload() {
        MultipartUpload multipartUpload = new MultipartUpload("bucket-1", "object-1", "mpu-id");
        List<UploadPartResponse> listOfParts = List.of(new UploadPartResponse(1, "etag", 0));
        doReturn(future(mock(MultipartUploadResponse.class))).when(mockBlobStore).completeMultipartUpload(multipartUpload, listOfParts);
        client.completeMultipartUpload(multipartUpload, listOfParts);
        verify(mockBlobStore, times(1)).completeMultipartUpload(multipartUpload, listOfParts);
    }

    @Test
    void testCompleteMultipartUploadException() {
        MultipartUpload multipartUpload = new MultipartUpload("bucket-1", "object-1", "mpu-id");
        List<UploadPartResponse> listOfParts = List.of(new UploadPartResponse(1, "etag", 0));
        CompletableFuture<Void> failure = CompletableFuture.failedFuture(new RuntimeException());
        doReturn(failure).when(mockBlobStore).completeMultipartUpload(multipartUpload, listOfParts);
        assertFailed(client.completeMultipartUpload(multipartUpload, listOfParts), UnAuthorizedException.class);
    }

    @Test
    void testListMultipartUpload() {
        MultipartUpload multipartUpload = new MultipartUpload("bucket-1", "object-1", "mpu-id");
        doReturn(future(mock(List.class))).when(mockBlobStore).listMultipartUpload(multipartUpload);
        client.listMultipartUpload(multipartUpload);
        verify(mockBlobStore, times(1)).listMultipartUpload(multipartUpload);
    }

    @Test
    void testListMultipartUploadException() {
        MultipartUpload multipartUpload = new MultipartUpload("bucket-1", "object-1", "mpu-id");
        when(mockBlobStore.listMultipartUpload(multipartUpload)).thenThrow(RuntimeException.class);
        CompletableFuture<Void> failure = CompletableFuture.failedFuture(new RuntimeException());
        doReturn(failure).when(mockBlobStore).listMultipartUpload(multipartUpload);
        assertFailed(client.listMultipartUpload(multipartUpload), UnAuthorizedException.class);
    }

    @Test
    void testAbortMultipartUpload() {
        MultipartUpload multipartUpload = new MultipartUpload("bucket-1", "object-1", "mpu-id");
        doReturn(futureVoid()).when(mockBlobStore).abortMultipartUpload(multipartUpload);
        client.abortMultipartUpload(multipartUpload);
        verify(mockBlobStore, times(1)).abortMultipartUpload(multipartUpload);
    }

    @Test
    void testAbortMultipartUploadException() {
        MultipartUpload multipartUpload = new MultipartUpload("bucket-1", "object-1", "mpu-id");
        CompletableFuture<Void> failure = CompletableFuture.failedFuture(new RuntimeException());
        doReturn(failure).when(mockBlobStore).abortMultipartUpload(multipartUpload);
        assertFailed(client.abortMultipartUpload(multipartUpload), UnAuthorizedException.class);
    }

    @Test
    void testGetTags() {
        Map<String, String> tags = Map.of("key1", "value1", "key2", "value2");
        doReturn(future(tags)).when(mockBlobStore).getTags(any());
        client.getTags("object-1");
        verify(mockBlobStore, times(1)).getTags("object-1");
    }

    @Test
    void testGetTagsException() {
        CompletableFuture<Void> failure = CompletableFuture.failedFuture(new RuntimeException());
        doReturn(failure).when(mockBlobStore).getTags("object-1");
        assertFailed(client.getTags("object-1"), UnAuthorizedException.class);
    }

    @Test
    void testSetTags() {
        Map<String, String> tags = Map.of("key1", "value1", "key2", "value2");
        doReturn(futureVoid()).when(mockBlobStore).setTags(any(), any());
        client.setTags("object-1", tags);
        verify(mockBlobStore, times(1)).setTags("object-1", tags);
    }

    @Test
    void testSetTagsException() {
        Map<String, String> tags = Map.of("key1", "value1", "key2", "value2");
        CompletableFuture<Void> failure = CompletableFuture.failedFuture(new RuntimeException());
        doReturn(failure).when(mockBlobStore).setTags("object-1", tags);
        assertFailed(client.setTags("object-1", tags), UnAuthorizedException.class);
    }

    @Test
    void testGeneratePresignedUrl() {
        PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.DOWNLOAD)
                .key("object-1")
                .duration(Duration.ofMinutes(10))
                .build();
        CompletableFuture<Void> failure = CompletableFuture.failedFuture(new RuntimeException());
        doReturn(failure).when(mockBlobStore).generatePresignedUrl(presignedUrlRequest);
        assertFailed(client.generatePresignedUrl(presignedUrlRequest), UnAuthorizedException.class);
    }

    @Test
    void testDoDoesObjectExist() throws ExecutionException, InterruptedException {
        doReturn(CompletableFuture.completedFuture(true)).when(mockBlobStore).doesObjectExist(any(), any());
        boolean result = client.doesObjectExist("object-1", "version-1").get();
        assertTrue(result);

        CompletableFuture<Boolean> failure = CompletableFuture.failedFuture(new RuntimeException());
        doReturn(failure).when(mockBlobStore).doesObjectExist(any(), any());
        assertFailed(client.doesObjectExist("object-1", "version-1"), UnAuthorizedException.class);
    }

    @Test
    void testDownloadDirectory() throws ExecutionException, InterruptedException {
        DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
                .prefixToDownload("prefix-1")
                .localDestinationDirectory("/home/files")
                .prefixesToExclude(List.of("abc", "xyz"))
                .build();

        DirectoryDownloadResponse expectedResponse = mock(DirectoryDownloadResponse.class);
        when(mockBlobStore.downloadDirectory(any())).thenReturn(future(expectedResponse));
        DirectoryDownloadResponse actualResponse = client.downloadDirectory(request).get();
        verify(mockBlobStore, times(1)).downloadDirectory(eq(request));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    void testUploadDirectory() throws ExecutionException, InterruptedException {
        DirectoryUploadRequest request = DirectoryUploadRequest.builder()
                .localSourceDirectory("/home/files")
                .prefix("abc")
                .includeSubFolders(true)
                .build();

        FailedBlobUpload response1 = mock(FailedBlobUpload.class);
        FailedBlobUpload response2 = mock(FailedBlobUpload.class);
        DirectoryUploadResponse expectedResponse = DirectoryUploadResponse.builder()
                .failedTransfers(List.of(response1, response2))
                .build();

        when(mockBlobStore.uploadDirectory(any())).thenReturn(future(expectedResponse));
        DirectoryUploadResponse actualResponse = client.uploadDirectory(request).get();
        verify(mockBlobStore, times(1)).uploadDirectory(eq(request));
        assertEquals(expectedResponse, actualResponse);
    }
}
