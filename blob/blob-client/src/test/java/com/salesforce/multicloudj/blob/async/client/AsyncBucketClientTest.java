package com.salesforce.multicloudj.blob.async.client;

import static com.salesforce.multicloudj.blob.async.driver.TestAsyncBlobStore.PROVIDER_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStore;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStoreProvider;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.Checksum;
import com.salesforce.multicloudj.blob.driver.ChecksumMethod;
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
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.MultipartUploadResponse;
import com.salesforce.multicloudj.blob.driver.ObjectLockConfiguration;
import com.salesforce.multicloudj.blob.driver.ObjectLockInfo;
import com.salesforce.multicloudj.blob.driver.PresignedOperation;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.RetentionMode;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.observability.OperationContext;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.slf4j.MDC;

public class AsyncBucketClientTest {

  private AsyncBlobStore mockBlobStore;
  private StsCredentials creds;
  private AsyncBucketClient client;

  private MockedStatic<ProviderSupplier> providerSupplier;

  @BeforeEach
  void setup() {
    mockBlobStore = mock(AsyncBlobStore.class);
    doReturn(new UnAuthorizedException()).when(mockBlobStore).mapException(any());
    providerSupplier = mockStatic(ProviderSupplier.class);
    AsyncBlobStoreProvider.Builder mockBuilder = mock(AsyncBlobStoreProvider.Builder.class);
    when(mockBuilder.build()).thenReturn(mockBlobStore);
    providerSupplier
        .when(() -> ProviderSupplier.findAsyncBuilder(PROVIDER_ID))
        .thenReturn(mockBuilder);
    creds = new StsCredentials("keyId", "keySecret", "token");
    CredentialsOverrider credsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(creds)
            .build();

    Properties properties = new Properties();
    properties.setProperty("bucket", "bucket-1");

    client =
        AsyncBucketClient.builder(PROVIDER_ID)
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

  private static OperationContext fullContext() {
    return OperationContext.builder()
        .correlationId("req-abc-123")
        .serviceId("svc-42")
        .tenantId("tenant-7")
        .build();
  }

  private static Map<String, String> snapshotObservabilityMdc() {
    Map<String, String> snapshot = new HashMap<>();
    snapshot.put("correlation_id", MDC.get("correlation_id"));
    snapshot.put("service_id", MDC.get("service_id"));
    snapshot.put("tenant_id", MDC.get("tenant_id"));
    return snapshot;
  }

  private static void assertContextPropagated(Map<String, String> capturedMdc) {
    assertEquals("req-abc-123", capturedMdc.get("correlation_id"));
    assertEquals("svc-42", capturedMdc.get("service_id"));
    assertEquals("tenant-7", capturedMdc.get("tenant_id"));
  }

  @Test
  void testUploadInputStream() throws ExecutionException, InterruptedException {
    UploadResponse expectedResponse = UploadResponse.builder().eTag("eTag-1").build();
    when(mockBlobStore.upload(any(), any(InputStream.class))).thenReturn(future(expectedResponse));

    byte[] content = "Test data".getBytes();
    InputStream inputStream = new ByteArrayInputStream(content);
    UploadRequest request =
        UploadRequest.builder().withKey("object-1").withContentLength(content.length).build();

    UploadResponse actualResponse = client.upload(request, inputStream).get();
    verify(mockBlobStore, times(1))
        .upload(argThat(uploadRequestEnrichedWith("object-1")), eq(inputStream));
    assertEquals(expectedResponse, actualResponse);
  }

  @Test
  void testUploadByteArray() throws ExecutionException, InterruptedException {
    UploadResponse expectedResponse = UploadResponse.builder().eTag("eTag-1").build();
    when(mockBlobStore.upload(any(), any(byte[].class))).thenReturn(future(expectedResponse));

    byte[] content = "Test data".getBytes();
    UploadRequest request =
        UploadRequest.builder().withKey("object-1").withContentLength(content.length).build();

    UploadResponse actualResponse = client.upload(request, content).get();
    verify(mockBlobStore, times(1))
        .upload(argThat(uploadRequestEnrichedWith("object-1")), eq(content));
    assertEquals(expectedResponse, actualResponse);
  }

  @Test
  void testUploadFile() throws ExecutionException, InterruptedException {
    UploadResponse expectedResponse = UploadResponse.builder().eTag("eTag-1").build();
    when(mockBlobStore.upload(any(), any(File.class))).thenReturn(future(expectedResponse));

    File file = new File("test.txt");
    UploadRequest request =
        UploadRequest.builder().withKey("object-1").withContentLength(1024L).build();

    UploadResponse actualResponse = client.upload(request, file).get();
    verify(mockBlobStore, times(1))
        .upload(argThat(uploadRequestEnrichedWith("object-1")), eq(file));
    assertEquals(expectedResponse, actualResponse);
  }

  @Test
  void testUploadPath() throws ExecutionException, InterruptedException {
    UploadResponse expectedResponse = UploadResponse.builder().eTag("eTag-1").build();
    when(mockBlobStore.upload(any(), any(Path.class))).thenReturn(future(expectedResponse));

    Path path = Paths.get("test.txt");
    UploadRequest request =
        UploadRequest.builder().withKey("object-1").withContentLength(1024L).build();

    UploadResponse actualResponse = client.upload(request, path).get();
    verify(mockBlobStore, times(1))
        .upload(argThat(uploadRequestEnrichedWith("object-1")), eq(path));
    assertEquals(expectedResponse, actualResponse);
  }

  /**
   * Matches an {@link UploadRequest} whose key matches and whose {@link
   * com.salesforce.multicloudj.common.observability.OperationContext} has been populated by the
   * SDK with a non-null correlation id (so the provider's transformer can persist it on the
   * blob's stored metadata under {@code BlobMetadataKeys.CORRELATION_ID}).
   */
  private static org.mockito.ArgumentMatcher<UploadRequest> uploadRequestEnrichedWith(String key) {
    return req ->
        req != null
            && key.equals(req.getKey())
            && req.getOperationContext() != null
            && req.getOperationContext().getCorrelationId() != null;
  }

  @Test
  void testUploadThrowsFutureException() {
    CompletableFuture<UploadResponse> failure =
        CompletableFuture.failedFuture(new RuntimeException());
    when(mockBlobStore.upload(any(), any(InputStream.class))).thenReturn(failure);
    when(mockBlobStore.upload(any(), any(byte[].class))).thenReturn(failure);
    when(mockBlobStore.upload(any(), any(File.class))).thenReturn(failure);
    when(mockBlobStore.upload(any(), any(Path.class))).thenReturn(failure);
    UploadRequest request = UploadRequest.builder().withKey("object-1").build();

    var result = client.upload(request, mock(InputStream.class));
    assertFailed(result, UnAuthorizedException.class);
    result = client.upload(request, "Test data".getBytes());
    assertFailed(result, UnAuthorizedException.class);
    result = client.upload(request, new File("test.txt"));
    assertFailed(result, UnAuthorizedException.class);
    result = client.upload(request, Paths.get("test.txt"));
    assertFailed(result, UnAuthorizedException.class);
  }

  @Test
  void testDownloadOutputStream() throws ExecutionException, InterruptedException {
    OutputStream outputStream = mock(OutputStream.class);
    DownloadRequest request = new DownloadRequest.Builder().withKey("object-1").build();
    DownloadResponse response = DownloadResponse.builder().key("object-1").build();
    when(mockBlobStore.download(any(), any(OutputStream.class))).thenReturn(future(response));
    client.download(request, outputStream).get();
    verify(mockBlobStore, times(1)).download(eq(request), eq(outputStream));
  }

  @Test
  void testDownloadByteArrayWrapper() throws ExecutionException, InterruptedException {
    ByteArray byteArray = new ByteArray();
    DownloadRequest request = new DownloadRequest.Builder().withKey("object-1").build();
    DownloadResponse response = DownloadResponse.builder().key("object-1").build();
    when(mockBlobStore.download(any(), any(ByteArray.class))).thenReturn(future(response));
    client.download(request, byteArray).get();
    verify(mockBlobStore, times(1)).download(eq(request), eq(byteArray));
  }

  @Test
  void testDownloadFile() throws ExecutionException, InterruptedException {
    File file = new File("testFile.txt");
    DownloadRequest request = new DownloadRequest.Builder().withKey("object-1").build();
    DownloadResponse response = DownloadResponse.builder().key("object-1").build();
    when(mockBlobStore.download(any(), any(File.class))).thenReturn(future(response));
    client.download(request, file).get();
    verify(mockBlobStore, times(1)).download(eq(request), eq(file));
  }

  @Test
  void testDownloadPath() throws ExecutionException, InterruptedException {
    Path output = mock(Path.class);
    DownloadRequest request = new DownloadRequest.Builder().withKey("object-1").build();
    DownloadResponse response = DownloadResponse.builder().key("object-1").build();
    when(mockBlobStore.download(any(), any(Path.class))).thenReturn(future(response));
    client.download(request, output).get();
    verify(mockBlobStore, times(1)).download(eq(request), eq(output));
  }

  @Test
  void testDownloadThrowsException() {
    CompletableFuture<DownloadResponse> failure =
        CompletableFuture.failedFuture(new RuntimeException());
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
    List<BlobIdentifier> objects =
        List.of(
            new BlobIdentifier("object-1", "version-1"),
            new BlobIdentifier("object-2", "version-2"),
            new BlobIdentifier("object-3", "version-3"));
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
    assertFailed(
        client.delete(List.of(new BlobIdentifier("object-1", "version-1"))),
        UnAuthorizedException.class);
  }

  @Test
  void testCopy() throws ExecutionException, InterruptedException {
    Instant now = Instant.now();

    String destKey = "dest-object-1";
    CopyResponse expectedResponse =
        CopyResponse.builder()
            .key(destKey)
            .versionId("version-1")
            .eTag("eTag-3")
            .lastModified(now)
            .build();
    CopyRequest request =
        CopyRequest.builder()
            .srcKey("src-object-1")
            .srcVersionId("version-1")
            .destKey(destKey)
            .destBucket("dest-bucket-1")
            .build();
    when(mockBlobStore.copy(any())).thenReturn(future(expectedResponse));

    CopyResponse actualResponse = client.copy(request).get();
    verify(mockBlobStore, times(1)).copy(request);
    assertEquals(expectedResponse, actualResponse);
  }

  @Test
  void testCopyThrowsException() {
    CompletableFuture<CopyResponse> failure =
        CompletableFuture.failedFuture(new RuntimeException());
    when(mockBlobStore.copy(any())).thenReturn(failure);

    CopyRequest request =
        CopyRequest.builder()
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
    BlobMetadata expectedBlobInfo =
        BlobMetadata.builder()
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
    CompletableFuture<BlobMetadata> failure =
        CompletableFuture.failedFuture(new RuntimeException());
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
    MultipartUploadRequest request =
        new MultipartUploadRequest.Builder().withKey("object-1").build();
    doReturn(future(mock(MultipartUpload.class)))
        .when(mockBlobStore)
        .initiateMultipartUpload(any(MultipartUploadRequest.class));
    client.initiateMultipartUpload(request);
    // The client forwards a request enriched with the resolved OperationContext, so match by key
    // rather than by object identity.
    verify(mockBlobStore, times(1))
        .initiateMultipartUpload(
            argThat((MultipartUploadRequest req) -> "object-1".equals(req.getKey())));
  }

  @Test
  void testInitiateMultipartUploadException() {
    MultipartUploadRequest request =
        new MultipartUploadRequest.Builder().withKey("object-1").build();
    CompletableFuture<Void> failure = CompletableFuture.failedFuture(new RuntimeException());
    doReturn(failure)
        .when(mockBlobStore)
        .initiateMultipartUpload(any(MultipartUploadRequest.class));
    assertFailed(client.initiateMultipartUpload(request), UnAuthorizedException.class);
  }

  @Test
  void testUploadMultipartPart() {
    MultipartUpload multipartUpload =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();
    MultipartPart multipartPart = new MultipartPart(1, null, 0);
    doReturn(future(mock(UploadPartResponse.class)))
        .when(mockBlobStore)
        .uploadMultipartPart(multipartUpload, multipartPart);
    client.uploadMultipartPart(multipartUpload, multipartPart);
    verify(mockBlobStore, times(1)).uploadMultipartPart(multipartUpload, multipartPart);
  }

  @Test
  void testUploadMultipartPartException() {
    MultipartUpload multipartUpload =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();
    MultipartPart multipartPart = new MultipartPart(1, null, 0);
    CompletableFuture<Void> failure = CompletableFuture.failedFuture(new RuntimeException());
    doReturn(failure).when(mockBlobStore).uploadMultipartPart(multipartUpload, multipartPart);
    assertFailed(
        client.uploadMultipartPart(multipartUpload, multipartPart), UnAuthorizedException.class);
  }

  @Test
  void testCompleteMultipartUpload() {
    MultipartUpload multipartUpload =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();
    List<UploadPartResponse> listOfParts = List.of(new UploadPartResponse(1, "etag", 0));
    doReturn(future(mock(MultipartUploadResponse.class)))
        .when(mockBlobStore)
        .completeMultipartUpload(multipartUpload, listOfParts);
    client.completeMultipartUpload(multipartUpload, listOfParts);
    verify(mockBlobStore, times(1)).completeMultipartUpload(multipartUpload, listOfParts);
  }

  @Test
  void testCompleteMultipartUploadException() {
    MultipartUpload multipartUpload =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();
    List<UploadPartResponse> listOfParts = List.of(new UploadPartResponse(1, "etag", 0));
    CompletableFuture<Void> failure = CompletableFuture.failedFuture(new RuntimeException());
    doReturn(failure).when(mockBlobStore).completeMultipartUpload(multipartUpload, listOfParts);
    assertFailed(
        client.completeMultipartUpload(multipartUpload, listOfParts), UnAuthorizedException.class);
  }

  @Test
  void testListMultipartUpload() {
    MultipartUpload multipartUpload =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();
    doReturn(future(mock(List.class))).when(mockBlobStore).listMultipartUpload(multipartUpload);
    client.listMultipartUpload(multipartUpload);
    verify(mockBlobStore, times(1)).listMultipartUpload(multipartUpload);
  }

  @Test
  void testListMultipartUploadException() {
    MultipartUpload multipartUpload =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();
    when(mockBlobStore.listMultipartUpload(multipartUpload)).thenThrow(RuntimeException.class);
    CompletableFuture<Void> failure = CompletableFuture.failedFuture(new RuntimeException());
    doReturn(failure).when(mockBlobStore).listMultipartUpload(multipartUpload);
    assertFailed(client.listMultipartUpload(multipartUpload), UnAuthorizedException.class);
  }

  @Test
  void testAbortMultipartUpload() {
    MultipartUpload multipartUpload =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();
    doReturn(futureVoid()).when(mockBlobStore).abortMultipartUpload(multipartUpload);
    client.abortMultipartUpload(multipartUpload);
    verify(mockBlobStore, times(1)).abortMultipartUpload(multipartUpload);
  }

  @Test
  void testAbortMultipartUploadException() {
    MultipartUpload multipartUpload =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();
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

  // ---- OperationContext propagation tests --------------------------------
  // Each captures the observability MDC at the moment the driver is invoked (when the tracer has
  // set it) and asserts every id in the supplied OperationContext reached the MDC. Passing null
  // for the context in the client overload would regress these to an empty correlation id.

  @Test
  void testDeleteWithOperationContext() throws ExecutionException, InterruptedException {
    Map<String, String> captured = new HashMap<>();
    doAnswer(
            invocation -> {
              captured.putAll(snapshotObservabilityMdc());
              return futureVoid();
            })
        .when(mockBlobStore)
        .delete(eq("object-1"), eq("version-1"));
    client.delete("object-1", "version-1", fullContext()).get();
    verify(mockBlobStore, times(1)).delete(eq("object-1"), eq("version-1"));
    assertContextPropagated(captured);
  }

  @Test
  void testBulkDeleteWithOperationContext() throws ExecutionException, InterruptedException {
    List<BlobIdentifier> objects = List.of(new BlobIdentifier("object-1", "version-1"));
    Map<String, String> captured = new HashMap<>();
    doAnswer(
            invocation -> {
              captured.putAll(snapshotObservabilityMdc());
              return futureVoid();
            })
        .when(mockBlobStore)
        .delete(eq(objects));
    client.delete(objects, fullContext()).get();
    verify(mockBlobStore, times(1)).delete(eq(objects));
    assertContextPropagated(captured);
  }

  @Test
  void testGetMetadataWithOperationContext() throws ExecutionException, InterruptedException {
    Map<String, String> captured = new HashMap<>();
    doAnswer(
            invocation -> {
              captured.putAll(snapshotObservabilityMdc());
              return future(BlobMetadata.builder().key("object-1").build());
            })
        .when(mockBlobStore)
        .getMetadata(eq("object-1"), eq("version-1"));
    client.getMetadata("object-1", "version-1", fullContext()).get();
    verify(mockBlobStore, times(1)).getMetadata(eq("object-1"), eq("version-1"));
    assertContextPropagated(captured);
  }

  /**
   * The client stamps the resolved correlationId onto the {@link BlobMetadata} returned by the
   * driver by rebuilding via {@code toBuilder()}. Populate every field to a distinct non-default
   * value and assert that every field survives the rebuild — the only field that should differ is
   * {@code correlationId}, which is overwritten with the caller's OperationContext value. Guards
   * against a field being silently dropped from the rebuild path.
   */
  @Test
  void testGetMetadataPreservesAllFieldsWhenStampingCorrelationId()
      throws ExecutionException, InterruptedException {
    Instant lastModified = Instant.parse("2026-01-15T10:30:00Z");
    Instant createdTime = Instant.parse("2026-01-10T08:00:00Z");
    Instant retainUntil = Instant.parse("2027-01-01T00:00:00Z");
    byte[] md5 = new byte[] {1, 2, 3, 4};
    Map<String, String> userMetadata = Map.of("meta-a", "value-a", "meta-b", "value-b");
    ObjectLockInfo lockInfo =
        ObjectLockInfo.builder()
            .mode(RetentionMode.GOVERNANCE)
            .retainUntilDate(retainUntil)
            .legalHold(true)
            .useEventBasedHold(true)
            .build();
    Checksum checksum =
        Checksum.builder().algorithm(ChecksumMethod.CRC32C).value("chk-value").build();
    BlobMetadata fromDriver =
        BlobMetadata.builder()
            .key("object-1")
            .versionId("v1")
            .eTag("etag-1")
            .objectSize(42L)
            .metadata(userMetadata)
            .lastModified(lastModified)
            .createdTime(createdTime)
            .md5(md5)
            .contentType("application/octet-stream")
            .objectLockInfo(lockInfo)
            .checksum(checksum)
            .correlationId("driver-supplied-id")
            .build();
    when(mockBlobStore.getMetadata("object-1", "v1")).thenReturn(future(fromDriver));

    BlobMetadata actual = client.getMetadata("object-1", "v1", fullContext()).get();

    assertEquals(fromDriver.getKey(), actual.getKey());
    assertEquals(fromDriver.getVersionId(), actual.getVersionId());
    assertEquals(fromDriver.getETag(), actual.getETag());
    assertEquals(fromDriver.getObjectSize(), actual.getObjectSize());
    assertEquals(fromDriver.getMetadata(), actual.getMetadata());
    assertEquals(fromDriver.getLastModified(), actual.getLastModified());
    assertEquals(fromDriver.getCreatedTime(), actual.getCreatedTime());
    assertEquals(fromDriver.getMd5(), actual.getMd5());
    assertEquals(fromDriver.getContentType(), actual.getContentType());
    assertEquals(fromDriver.getObjectLockInfo(), actual.getObjectLockInfo());
    assertEquals(fromDriver.getChecksum(), actual.getChecksum());
    assertEquals("req-abc-123", actual.getCorrelationId());
  }

  /**
   * The client stamps the resolved correlationId onto the {@link UploadResponse} returned by the
   * driver by rebuilding via {@code toBuilder()}. Populate every field to a distinct non-default
   * value and assert that every field survives the rebuild — the only field that should differ is
   * {@code correlationId}, which is overwritten with the caller's OperationContext value. Guards
   * against a field being silently dropped from the rebuild path.
   */
  @Test
  void testUploadPreservesAllFieldsWhenStampingCorrelationId()
      throws ExecutionException, InterruptedException {
    UploadResponse fromDriver =
        UploadResponse.builder()
            .key("object-1")
            .versionId("v1")
            .eTag("etag-1")
            .checksumValue("chk-value")
            .correlationId("driver-supplied-id")
            .build();
    when(mockBlobStore.upload(any(UploadRequest.class), any(byte[].class)))
        .thenReturn(future(fromDriver));
    UploadRequest request =
        UploadRequest.builder().withKey("object-1").withOperationContext(fullContext()).build();

    UploadResponse actual = client.upload(request, "test data".getBytes()).get();

    assertEquals(fromDriver.getKey(), actual.getKey());
    assertEquals(fromDriver.getVersionId(), actual.getVersionId());
    assertEquals(fromDriver.getETag(), actual.getETag());
    assertEquals(fromDriver.getChecksumValue(), actual.getChecksumValue());
    assertEquals("req-abc-123", actual.getCorrelationId());
  }

  /**
   * The client stamps the resolved correlationId onto the {@link DownloadResponse} returned by
   * the driver — both at the top level and on the nested {@link BlobMetadata}. Populate every
   * field to a distinct non-default value and assert that every field survives the rebuild, that
   * the top-level correlationId is overwritten with the caller's OperationContext value, and that
   * the nested metadata's correlationId is also stamped. The nested rebuild is intentional — a
   * plain {@code toBuilder().correlationId(...)} would shallow-copy the driver's original
   * (unstamped) metadata. Guards against a field being silently dropped from the rebuild path.
   */
  @Test
  void testDownloadPreservesAllFieldsWhenStampingCorrelationId()
      throws ExecutionException, InterruptedException {
    BlobMetadata nestedMetadata =
        BlobMetadata.builder()
            .key("object-1")
            .versionId("v1")
            .eTag("etag-1")
            .correlationId("driver-supplied-md-id")
            .build();
    InputStream inputStream = mock(InputStream.class);
    DownloadResponse fromDriver =
        DownloadResponse.builder()
            .key("object-1")
            .metadata(nestedMetadata)
            .inputStream(inputStream)
            .correlationId("driver-supplied-dl-id")
            .build();
    when(mockBlobStore.download(any(DownloadRequest.class))).thenReturn(future(fromDriver));
    DownloadRequest request =
        new DownloadRequest.Builder()
            .withKey("object-1")
            .withOperationContext(fullContext())
            .build();

    DownloadResponse actual = client.download(request).get();

    assertEquals(fromDriver.getKey(), actual.getKey());
    assertNotNull(actual.getMetadata());
    assertEquals(nestedMetadata.getKey(), actual.getMetadata().getKey());
    assertEquals(nestedMetadata.getVersionId(), actual.getMetadata().getVersionId());
    assertEquals(nestedMetadata.getETag(), actual.getMetadata().getETag());
    assertEquals("req-abc-123", actual.getMetadata().getCorrelationId());
    assertEquals(inputStream, actual.getInputStream());
    assertEquals("req-abc-123", actual.getCorrelationId());
  }

  @Test
  void testGetTagsWithOperationContext() throws ExecutionException, InterruptedException {
    Map<String, String> captured = new HashMap<>();
    doAnswer(
            invocation -> {
              captured.putAll(snapshotObservabilityMdc());
              return future(Map.of("key1", "value1"));
            })
        .when(mockBlobStore)
        .getTags(eq("object-1"));
    client.getTags("object-1", fullContext()).get();
    verify(mockBlobStore, times(1)).getTags(eq("object-1"));
    assertContextPropagated(captured);
  }

  @Test
  void testUploadMultipartPartWithOperationContext()
      throws ExecutionException, InterruptedException {
    MultipartUpload mpu =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();
    MultipartPart mpp = new MultipartPart(1, null, 0);
    Map<String, String> captured = new HashMap<>();
    doAnswer(
            invocation -> {
              captured.putAll(snapshotObservabilityMdc());
              return future(mock(UploadPartResponse.class));
            })
        .when(mockBlobStore)
        .uploadMultipartPart(eq(mpu), eq(mpp));
    client.uploadMultipartPart(mpu, mpp, fullContext()).get();
    verify(mockBlobStore, times(1)).uploadMultipartPart(eq(mpu), eq(mpp));
    assertContextPropagated(captured);
  }

  @Test
  void testCompleteMultipartUploadWithOperationContext()
      throws ExecutionException, InterruptedException {
    MultipartUpload mpu =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();
    List<UploadPartResponse> parts = List.of(new UploadPartResponse(1, "etag", 0));
    Map<String, String> captured = new HashMap<>();
    doAnswer(
            invocation -> {
              captured.putAll(snapshotObservabilityMdc());
              return future(mock(MultipartUploadResponse.class));
            })
        .when(mockBlobStore)
        .completeMultipartUpload(eq(mpu), eq(parts));
    client.completeMultipartUpload(mpu, parts, fullContext()).get();
    verify(mockBlobStore, times(1)).completeMultipartUpload(eq(mpu), eq(parts));
    assertContextPropagated(captured);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testListMultipartUploadWithOperationContext()
      throws ExecutionException, InterruptedException {
    MultipartUpload mpu =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();
    Map<String, String> captured = new HashMap<>();
    doAnswer(
            invocation -> {
              captured.putAll(snapshotObservabilityMdc());
              return future(mock(List.class));
            })
        .when(mockBlobStore)
        .listMultipartUpload(eq(mpu));
    client.listMultipartUpload(mpu, fullContext()).get();
    verify(mockBlobStore, times(1)).listMultipartUpload(eq(mpu));
    assertContextPropagated(captured);
  }

  @Test
  void testAbortMultipartUploadWithOperationContext()
      throws ExecutionException, InterruptedException {
    MultipartUpload mpu =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();
    Map<String, String> captured = new HashMap<>();
    doAnswer(
            invocation -> {
              captured.putAll(snapshotObservabilityMdc());
              return futureVoid();
            })
        .when(mockBlobStore)
        .abortMultipartUpload(eq(mpu));
    client.abortMultipartUpload(mpu, fullContext()).get();
    verify(mockBlobStore, times(1)).abortMultipartUpload(eq(mpu));
    assertContextPropagated(captured);
  }

  @Test
  void testListPageWithOperationContext() throws ExecutionException, InterruptedException {
    ListBlobsPageRequest request =
        new ListBlobsPageRequest.Builder().withOperationContext(fullContext()).build();
    Map<String, String> captured = new HashMap<>();
    doAnswer(
            invocation -> {
              captured.putAll(snapshotObservabilityMdc());
              return future(new ListBlobsPageResponse(List.of(), false, null));
            })
        .when(mockBlobStore)
        .listPage(eq(request));
    client.listPage(request).get();
    verify(mockBlobStore, times(1)).listPage(eq(request));
    assertContextPropagated(captured);
  }

  @Test
  void testInitiateMultipartUploadWithOperationContext()
      throws ExecutionException, InterruptedException {
    MultipartUploadRequest request =
        new MultipartUploadRequest.Builder()
            .withKey("object-1")
            .withOperationContext(fullContext())
            .build();
    Map<String, String> captured = new HashMap<>();
    doAnswer(
            invocation -> {
              captured.putAll(snapshotObservabilityMdc());
              return future(mock(MultipartUpload.class));
            })
        .when(mockBlobStore)
        .initiateMultipartUpload(any(MultipartUploadRequest.class));
    client.initiateMultipartUpload(request).get();
    verify(mockBlobStore, times(1)).initiateMultipartUpload(any(MultipartUploadRequest.class));
    assertContextPropagated(captured);
  }

  /**
   * The initiateMultipartUpload path carries the resolved OperationContext into the request
   * forwarded to the driver, so the driver's transformer can stamp the correlation id onto the
   * multipart object's metadata (matching the upload path).
   */
  @Test
  void testInitiateMultipartUploadEnrichesRequestWithResolvedContext()
      throws ExecutionException, InterruptedException {
    // Populate every field the client's withResolvedContext copies so we can assert none is
    // silently dropped while the request is rebuilt to swap in the resolved OperationContext.
    // A dropped field (e.g. metadata or tags) would defeat the metadata stamping this path adds.
    Map<String, String> metadata = Map.of("meta-1", "meta-value-1");
    Map<String, String> tags = Map.of("tag-1", "tag-value-1");
    ObjectLockConfiguration objectLock =
        ObjectLockConfiguration.builder()
            .mode(RetentionMode.GOVERNANCE)
            .retainUntilDate(Instant.parse("2030-01-01T00:00:00Z"))
            .legalHold(true)
            .build();
    MultipartUploadRequest request =
        new MultipartUploadRequest.Builder()
            .withKey("object-1")
            .withMetadata(metadata)
            .withTags(tags)
            .withKmsKeyId("kms-key-1")
            .withUseKmsManagedKey(true)
            .withChecksumEnabled(true)
            .withChecksumAlgorithm(ChecksumMethod.SHA256)
            .withObjectLock(objectLock)
            .withContentType("application/json")
            .withOperationContext(fullContext())
            .build();
    doReturn(future(mock(MultipartUpload.class)))
        .when(mockBlobStore)
        .initiateMultipartUpload(any(MultipartUploadRequest.class));

    client.initiateMultipartUpload(request).get();

    ArgumentCaptor<MultipartUploadRequest> captor =
        ArgumentCaptor.forClass(MultipartUploadRequest.class);
    verify(mockBlobStore, times(1)).initiateMultipartUpload(captor.capture());
    MultipartUploadRequest forwarded = captor.getValue();

    // The resolved context is swapped in.
    assertEquals("req-abc-123", forwarded.getOperationContext().getCorrelationId());
    // Every other field must survive the rebuild unchanged.
    assertEquals("object-1", forwarded.getKey());
    assertEquals(metadata, forwarded.getMetadata());
    assertEquals(tags, forwarded.getTags());
    assertEquals("kms-key-1", forwarded.getKmsKeyId());
    assertTrue(forwarded.isUseKmsManagedKey());
    assertTrue(forwarded.isChecksumEnabled());
    assertEquals(ChecksumMethod.SHA256, forwarded.getChecksumAlgorithm());
    assertSame(objectLock, forwarded.getObjectLock());
    assertEquals("application/json", forwarded.getContentType());
  }

  /**
   * Directly exercises the multipart rebuild branch of {@code withResolvedContext}: when the
   * resolved context differs from the request's own context, the request is rebuilt from scratch.
   * Every field the builder copies must be preserved — a dropped field (e.g. metadata or tags)
   * would silently defeat the metadata stamping this path adds. The higher-level client test can
   * short-circuit past this branch, so it is asserted here explicitly.
   */
  @Test
  void testWithResolvedContextMultipartRebuildPreservesAllFields() {
    Map<String, String> metadata = Map.of("meta-1", "meta-value-1");
    Map<String, String> tags = Map.of("tag-1", "tag-value-1");
    ObjectLockConfiguration objectLock =
        ObjectLockConfiguration.builder()
            .mode(RetentionMode.GOVERNANCE)
            .retainUntilDate(Instant.parse("2030-01-01T00:00:00Z"))
            .legalHold(true)
            .build();
    MultipartUploadRequest request =
        new MultipartUploadRequest.Builder()
            .withKey("object-1")
            .withMetadata(metadata)
            .withTags(tags)
            .withKmsKeyId("kms-key-1")
            .withUseKmsManagedKey(true)
            .withChecksumEnabled(true)
            .withChecksumAlgorithm(ChecksumMethod.SHA256)
            .withObjectLock(objectLock)
            .withContentType("application/json")
            .withOperationContext(OperationContext.builder().correlationId("original").build())
            .build();

    // A distinct context instance forces the rebuild branch (not the identity short-circuit).
    OperationContext resolved = fullContext();
    MultipartUploadRequest rebuilt = AsyncBucketClient.withResolvedContext(request, resolved);

    assertSame(resolved, rebuilt.getOperationContext());
    assertEquals("object-1", rebuilt.getKey());
    assertEquals(metadata, rebuilt.getMetadata());
    assertEquals(tags, rebuilt.getTags());
    assertEquals("kms-key-1", rebuilt.getKmsKeyId());
    assertTrue(rebuilt.isUseKmsManagedKey());
    assertTrue(rebuilt.isChecksumEnabled());
    assertEquals(ChecksumMethod.SHA256, rebuilt.getChecksumAlgorithm());
    assertSame(objectLock, rebuilt.getObjectLock());
    assertEquals("application/json", rebuilt.getContentType());
  }

  /**
   * When the resolved context is the very same instance already on the request, {@code
   * withResolvedContext} returns the request unchanged rather than rebuilding it.
   */
  @Test
  void testWithResolvedContextMultipartReturnsSameInstanceWhenContextUnchanged() {
    OperationContext ctx = fullContext();
    MultipartUploadRequest request =
        new MultipartUploadRequest.Builder().withKey("object-1").withOperationContext(ctx).build();

    assertSame(request, AsyncBucketClient.withResolvedContext(request, ctx));
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
    PresignedUrlRequest presignedUrlRequest =
        PresignedUrlRequest.builder()
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
    doReturn(CompletableFuture.completedFuture(true))
        .when(mockBlobStore)
        .doesObjectExist(any(), any());
    boolean result = client.doesObjectExist("object-1", "version-1").get();
    assertTrue(result);

    CompletableFuture<Boolean> failure = CompletableFuture.failedFuture(new RuntimeException());
    doReturn(failure).when(mockBlobStore).doesObjectExist(any(), any());
    assertFailed(client.doesObjectExist("object-1", "version-1"), UnAuthorizedException.class);
  }

  @Test
  void testDoesBucketExist_ReturnsTrue() throws ExecutionException, InterruptedException {
    doReturn(CompletableFuture.completedFuture(true)).when(mockBlobStore).doesBucketExist();
    boolean result = client.doesBucketExist().get();
    verify(mockBlobStore, times(1)).doesBucketExist();
    assertTrue(result);
  }

  @Test
  void testDoesBucketExist_ReturnsFalse() throws ExecutionException, InterruptedException {
    doReturn(CompletableFuture.completedFuture(false)).when(mockBlobStore).doesBucketExist();
    boolean result = client.doesBucketExist().get();
    verify(mockBlobStore, times(1)).doesBucketExist();
    assertFalse(result);
  }

  @Test
  void testDoesBucketExist_ThrowsException() {
    CompletableFuture<Boolean> failure = CompletableFuture.failedFuture(new RuntimeException());
    doReturn(failure).when(mockBlobStore).doesBucketExist();
    assertFailed(client.doesBucketExist(), UnAuthorizedException.class);
    verify(mockBlobStore, times(1)).doesBucketExist();
  }

  @Test
  void testDownloadDirectory() throws ExecutionException, InterruptedException {
    DirectoryDownloadRequest request =
        DirectoryDownloadRequest.builder()
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
    DirectoryUploadRequest request =
        DirectoryUploadRequest.builder()
            .localSourceDirectory("/home/files")
            .prefix("abc")
            .includeSubFolders(true)
            .build();

    FailedBlobUpload response1 = mock(FailedBlobUpload.class);
    FailedBlobUpload response2 = mock(FailedBlobUpload.class);
    DirectoryUploadResponse expectedResponse =
        DirectoryUploadResponse.builder().failedTransfers(List.of(response1, response2)).build();

    when(mockBlobStore.uploadDirectory(any())).thenReturn(future(expectedResponse));
    DirectoryUploadResponse actualResponse = client.uploadDirectory(request).get();
    verify(mockBlobStore, times(1)).uploadDirectory(eq(request));
    assertEquals(expectedResponse, actualResponse);
  }

  @Test
  void testDeleteDirectory() throws ExecutionException, InterruptedException {
    String prefix = "files";
    when(mockBlobStore.deleteDirectory(any())).thenReturn(futureVoid());
    client.deleteDirectory(prefix).get();
    verify(mockBlobStore, times(1)).deleteDirectory(eq(prefix));
  }

  @Test
  void testBuilderWithParallelUDownloadsEnabledConfiguration() {
    AsyncBucketClient.Builder builder = AsyncBucketClient.builder(PROVIDER_ID);

    AsyncBucketClient asyncBucketClient =
        builder
            .withBucket("test-bucket")
            .withRegion("us-west-1")
            .withParallelDownloadsEnabled(true)
            .withTargetThroughputInGbps(12.12)
            .withMaxNativeMemoryLimitInBytes(21L)
            .build();

    assertInstanceOf(AsyncBucketClient.class, asyncBucketClient);
  }

  @Test
  void testBuilderWithParallelUploadsEnabledConfiguration() {
    AsyncBucketClient.Builder builder = AsyncBucketClient.builder(PROVIDER_ID);

    AsyncBucketClient client =
        builder
            .withBucket("test-bucket")
            .withRegion("us-west-1")
            .withThresholdBytes(5 * 1024 * 1024L)
            .withPartBufferSize(1024 * 1024L)
            .withParallelUploadsEnabled(true)
            .build();

    assertInstanceOf(AsyncBucketClient.class, client);
  }

  @Test
  void testUploadDirectory_WithException() throws ExecutionException, InterruptedException {
    DirectoryUploadRequest request =
        DirectoryUploadRequest.builder()
            .localSourceDirectory("/home/files")
            .prefix("abc")
            .includeSubFolders(true)
            .build();

    RuntimeException expectedException = new RuntimeException("Upload failed");
    when(mockBlobStore.uploadDirectory(any()))
        .thenReturn(CompletableFuture.failedFuture(expectedException));

    CompletableFuture<DirectoryUploadResponse> future = client.uploadDirectory(request);

    ExecutionException exception =
        assertThrows(
            ExecutionException.class,
            () -> {
              future.get();
            });
    assertTrue(exception.getCause() instanceof UnAuthorizedException);
    verify(mockBlobStore, times(1)).uploadDirectory(eq(request));
  }

  @Test
  void testDownloadDirectory_WithException() throws ExecutionException, InterruptedException {
    DirectoryDownloadRequest request =
        DirectoryDownloadRequest.builder()
            .prefixToDownload("prefix-1")
            .localDestinationDirectory("/home/files")
            .prefixesToExclude(List.of("abc", "xyz"))
            .build();

    RuntimeException expectedException = new RuntimeException("Download failed");
    when(mockBlobStore.downloadDirectory(any()))
        .thenReturn(CompletableFuture.failedFuture(expectedException));

    CompletableFuture<DirectoryDownloadResponse> future = client.downloadDirectory(request);

    ExecutionException exception =
        assertThrows(
            ExecutionException.class,
            () -> {
              future.get();
            });
    assertTrue(exception.getCause() instanceof UnAuthorizedException);
    verify(mockBlobStore, times(1)).downloadDirectory(eq(request));
  }

  @Test
  void testDeleteDirectory_WithException() throws ExecutionException, InterruptedException {
    String prefix = "files";
    RuntimeException expectedException = new RuntimeException("Delete failed");
    when(mockBlobStore.deleteDirectory(prefix))
        .thenReturn(CompletableFuture.failedFuture(expectedException));

    CompletableFuture<Void> future = client.deleteDirectory(prefix);

    ExecutionException exception =
        assertThrows(
            ExecutionException.class,
            () -> {
              future.get();
            });
    assertTrue(exception.getCause() instanceof UnAuthorizedException);
    verify(mockBlobStore, times(1)).deleteDirectory(eq(prefix));
  }

  @Test
  void testUploadDirectory_WithNullResponse() throws ExecutionException, InterruptedException {
    DirectoryUploadRequest request =
        DirectoryUploadRequest.builder()
            .localSourceDirectory("/home/files")
            .prefix("abc")
            .includeSubFolders(true)
            .build();

    when(mockBlobStore.uploadDirectory(any())).thenReturn(future(null));
    DirectoryUploadResponse actualResponse = client.uploadDirectory(request).get();
    verify(mockBlobStore, times(1)).uploadDirectory(eq(request));
    assertNull(actualResponse);
  }

  @Test
  void testDownloadDirectory_WithNullResponse() throws ExecutionException, InterruptedException {
    DirectoryDownloadRequest request =
        DirectoryDownloadRequest.builder()
            .prefixToDownload("prefix-1")
            .localDestinationDirectory("/home/files")
            .prefixesToExclude(List.of("abc", "xyz"))
            .build();

    when(mockBlobStore.downloadDirectory(any())).thenReturn(future(null));
    DirectoryDownloadResponse actualResponse = client.downloadDirectory(request).get();
    verify(mockBlobStore, times(1)).downloadDirectory(eq(request));
    assertNull(actualResponse);
  }

  @Test
  void testAsyncBucketClientBuilderWithRetryConfig() {
    RetryConfig retryConfig =
        RetryConfig.builder().maxAttempts(5).attemptTimeout(3000L).totalTimeout(10000L).build();

    AsyncBlobStoreProvider.Builder mockBuilder2 = mock(AsyncBlobStoreProvider.Builder.class);
    when(mockBuilder2.withBucket(any())).thenReturn(mockBuilder2);
    when(mockBuilder2.withRegion(any())).thenReturn(mockBuilder2);
    when(mockBuilder2.withRetryConfig(any())).thenReturn(mockBuilder2);
    when(mockBuilder2.build()).thenReturn(mockBlobStore);

    providerSupplier
        .when(() -> ProviderSupplier.findAsyncBuilder("test2"))
        .thenReturn(mockBuilder2);

    AsyncBucketClient testClient =
        AsyncBucketClient.builder("test2")
            .withBucket("test-bucket")
            .withRegion("us-east-1")
            .withRetryConfig(retryConfig)
            .build();

    verify(mockBuilder2, times(1)).withRetryConfig(retryConfig);
    assertInstanceOf(AsyncBucketClient.class, testClient);
  }

  @Test
  void testAsyncBucketClientBuilderWithUseSystemPropertyProxyValues() {
    AsyncBlobStoreProvider.Builder mockBuilder2 = mock(AsyncBlobStoreProvider.Builder.class);
    when(mockBuilder2.withBucket(any())).thenReturn(mockBuilder2);
    when(mockBuilder2.withRegion(any())).thenReturn(mockBuilder2);
    when(mockBuilder2.withUseSystemPropertyProxyValues(any())).thenReturn(mockBuilder2);
    when(mockBuilder2.build()).thenReturn(mockBlobStore);

    providerSupplier
        .when(() -> ProviderSupplier.findAsyncBuilder("test3"))
        .thenReturn(mockBuilder2);

    AsyncBucketClient testClient =
        AsyncBucketClient.builder("test3")
            .withBucket("test-bucket")
            .withRegion("us-east-1")
            .withUseSystemPropertyProxyValues(false)
            .build();

    verify(mockBuilder2, times(1)).withUseSystemPropertyProxyValues(false);
    assertInstanceOf(AsyncBucketClient.class, testClient);
  }

  @Test
  void testAsyncBucketClientBuilderWithUseEnvironmentVariableProxyValues() {
    AsyncBlobStoreProvider.Builder mockBuilder2 = mock(AsyncBlobStoreProvider.Builder.class);
    when(mockBuilder2.withBucket(any())).thenReturn(mockBuilder2);
    when(mockBuilder2.withRegion(any())).thenReturn(mockBuilder2);
    when(mockBuilder2.withUseEnvironmentVariableProxyValues(any())).thenReturn(mockBuilder2);
    when(mockBuilder2.build()).thenReturn(mockBlobStore);

    providerSupplier
        .when(() -> ProviderSupplier.findAsyncBuilder("test4"))
        .thenReturn(mockBuilder2);

    AsyncBucketClient testClient =
        AsyncBucketClient.builder("test4")
            .withBucket("test-bucket")
            .withRegion("us-east-1")
            .withUseEnvironmentVariableProxyValues(false)
            .build();

    verify(mockBuilder2, times(1)).withUseEnvironmentVariableProxyValues(false);
    assertInstanceOf(AsyncBucketClient.class, testClient);
  }

  @Test
  void testAsyncBucketClientBuilderWithProxyEndpointAndOverrideFlags() {
    AsyncBlobStoreProvider.Builder mockBuilder2 = mock(AsyncBlobStoreProvider.Builder.class);
    when(mockBuilder2.withBucket(any())).thenReturn(mockBuilder2);
    when(mockBuilder2.withRegion(any())).thenReturn(mockBuilder2);
    when(mockBuilder2.withProxyEndpoint(any())).thenReturn(mockBuilder2);
    when(mockBuilder2.withUseSystemPropertyProxyValues(any())).thenReturn(mockBuilder2);
    when(mockBuilder2.withUseEnvironmentVariableProxyValues(any())).thenReturn(mockBuilder2);
    when(mockBuilder2.build()).thenReturn(mockBlobStore);

    providerSupplier
        .when(() -> ProviderSupplier.findAsyncBuilder("test5"))
        .thenReturn(mockBuilder2);

    AsyncBucketClient testClient =
        AsyncBucketClient.builder("test5")
            .withBucket("test-bucket")
            .withRegion("us-east-1")
            .withProxyEndpoint(URI.create("https://proxy.example.com:443"))
            .withUseSystemPropertyProxyValues(true)
            .withUseEnvironmentVariableProxyValues(false)
            .build();

    verify(mockBuilder2, times(1)).withProxyEndpoint(URI.create("https://proxy.example.com:443"));
    verify(mockBuilder2, times(1)).withUseSystemPropertyProxyValues(true);
    verify(mockBuilder2, times(1)).withUseEnvironmentVariableProxyValues(false);
    assertInstanceOf(AsyncBucketClient.class, testClient);
  }
}
