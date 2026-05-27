package com.salesforce.multicloudj.blob.ali.async;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.aliyun.sdk.service.oss2.OSSAsyncClient;
import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.OperationOptions;
import com.aliyun.sdk.service.oss2.models.GetObjectRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectResult;
import com.aliyun.sdk.service.oss2.models.PutObjectRequest;
import com.aliyun.sdk.service.oss2.models.PutObjectResult;
import com.salesforce.multicloudj.blob.ali.AliTransformerSupplier;
import com.salesforce.multicloudj.blob.driver.BlobStoreValidator;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class AliAsyncBlobStoreTest {

  private static final String BUCKET = "test-bucket";
  private static final String REGION = "cn-shanghai";

  private OSSAsyncClient mockAsyncClient;
  private OSSClient mockSyncClient;
  private AliAsyncBlobStore store;
  private final BlobStoreValidator validator = new BlobStoreValidator();

  @BeforeEach
  void setup() {
    mockAsyncClient = mock(OSSAsyncClient.class);
    mockSyncClient = mock(OSSClient.class);
    store = new AliAsyncBlobStore(
        BUCKET, REGION, null, validator, mockAsyncClient,
        mockSyncClient, new AliTransformerSupplier(), null);
  }

  @Test
  void testUploadInputStream() throws Exception {
    PutObjectResult mockResult = mock(PutObjectResult.class);
    when(mockResult.versionId()).thenReturn("v1");
    when(mockResult.eTag()).thenReturn("\"etag123\"");
    when(mockAsyncClient.putObjectAsync(
        any(PutObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    UploadRequest request = UploadRequest.builder()
        .withKey("test-key")
        .withContentLength(11)
        .build();
    byte[] content = "hello world".getBytes(StandardCharsets.UTF_8);
    UploadResponse response = store.upload(request,
        new ByteArrayInputStream(content)).get();

    assertNotNull(response);
    assertEquals("test-key", response.getKey());
    assertEquals("v1", response.getVersionId());
  }

  @Test
  void testUploadBytes() throws Exception {
    PutObjectResult mockResult = mock(PutObjectResult.class);
    when(mockResult.versionId()).thenReturn("v2");
    when(mockResult.eTag()).thenReturn("\"etag456\"");
    when(mockAsyncClient.putObjectAsync(
        any(PutObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    UploadRequest request = UploadRequest.builder()
        .withKey("byte-key")
        .build();
    UploadResponse response = store.upload(request,
        "content".getBytes(StandardCharsets.UTF_8)).get();

    assertEquals("byte-key", response.getKey());
    assertEquals("v2", response.getVersionId());
  }

  @Test
  void testUploadFile(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("upload.txt");
    Files.writeString(file, "file content");

    PutObjectResult mockResult = mock(PutObjectResult.class);
    when(mockResult.versionId()).thenReturn("v3");
    when(mockResult.eTag()).thenReturn("\"etag789\"");
    when(mockAsyncClient.putObjectAsync(
        any(PutObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    UploadRequest request = UploadRequest.builder()
        .withKey("file-key")
        .build();
    UploadResponse response = store.upload(request, file.toFile()).get();

    assertEquals("file-key", response.getKey());
    assertEquals("v3", response.getVersionId());
  }

  @Test
  void testDownloadToOutputStream() throws Exception {
    byte[] content = "downloaded content".getBytes(StandardCharsets.UTF_8);
    GetObjectResult mockResult = mock(GetObjectResult.class);
    when(mockResult.body())
        .thenReturn(new ByteArrayInputStream(content));
    when(mockResult.contentLength()).thenReturn((long) content.length);
    when(mockResult.eTag()).thenReturn("\"etag-dl\"");
    when(mockAsyncClient.getObjectAsync(
        any(GetObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    DownloadRequest request = DownloadRequest.builder()
        .withKey("dl-key")
        .build();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DownloadResponse response =
        store.download(request, outputStream).get();

    assertNotNull(response);
    assertArrayEquals(content, outputStream.toByteArray());
  }

  @Test
  void testDownloadToFile(@TempDir Path tempDir) throws Exception {
    byte[] content = "file download".getBytes(StandardCharsets.UTF_8);
    GetObjectResult mockResult = mock(GetObjectResult.class);
    when(mockResult.body())
        .thenReturn(new ByteArrayInputStream(content));
    when(mockResult.contentLength()).thenReturn((long) content.length);
    when(mockResult.eTag()).thenReturn("\"etag-file\"");
    when(mockAsyncClient.getObjectAsync(
        any(GetObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    Path file = tempDir.resolve("download.txt");
    DownloadRequest request = DownloadRequest.builder()
        .withKey("dl-file-key")
        .build();
    DownloadResponse response =
        store.download(request, file).get();

    assertNotNull(response);
    assertArrayEquals(content, Files.readAllBytes(file));
  }

  @Test
  void testDownloadAsInputStream() throws Exception {
    byte[] content = "stream download".getBytes(StandardCharsets.UTF_8);
    GetObjectResult mockResult = mock(GetObjectResult.class);
    when(mockResult.body())
        .thenReturn(new ByteArrayInputStream(content));
    when(mockResult.contentLength()).thenReturn((long) content.length);
    when(mockResult.eTag()).thenReturn("\"etag-stream\"");
    when(mockAsyncClient.getObjectAsync(
        any(GetObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    DownloadRequest request = DownloadRequest.builder()
        .withKey("dl-stream-key")
        .build();
    DownloadResponse response = store.download(request).get();

    assertNotNull(response);
    assertNotNull(response.getInputStream());
    assertArrayEquals(content, response.getInputStream().readAllBytes());
  }
}
