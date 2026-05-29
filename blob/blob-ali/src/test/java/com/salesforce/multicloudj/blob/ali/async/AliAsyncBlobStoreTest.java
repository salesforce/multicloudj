package com.salesforce.multicloudj.blob.ali.async;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.aliyun.sdk.service.oss2.OSSAsyncClient;
import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.OperationOptions;
import com.aliyun.sdk.service.oss2.PresignOptions;
import com.aliyun.sdk.service.oss2.models.AbortMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.AbortMultipartUploadResult;
import com.aliyun.sdk.service.oss2.models.CommonPrefix;
import com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadResult;
import com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadResultXml;
import com.aliyun.sdk.service.oss2.models.CopyObjectRequest;
import com.aliyun.sdk.service.oss2.models.CopyObjectResult;
import com.aliyun.sdk.service.oss2.models.DeleteMultipleObjectsRequest;
import com.aliyun.sdk.service.oss2.models.DeleteMultipleObjectsResult;
import com.aliyun.sdk.service.oss2.models.DeleteObjectRequest;
import com.aliyun.sdk.service.oss2.models.DeleteObjectResult;
import com.aliyun.sdk.service.oss2.models.GetObjectMetaRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectResult;
import com.aliyun.sdk.service.oss2.models.GetObjectTaggingRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectTaggingResult;
import com.aliyun.sdk.service.oss2.models.HeadObjectRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectResult;
import com.aliyun.sdk.service.oss2.models.InitiateMultipartUpload;
import com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadResult;
import com.aliyun.sdk.service.oss2.models.ListObjectsV2Request;
import com.aliyun.sdk.service.oss2.models.ListObjectsV2Result;
import com.aliyun.sdk.service.oss2.models.ListPartsRequest;
import com.aliyun.sdk.service.oss2.models.ListPartsResult;
import com.aliyun.sdk.service.oss2.models.ObjectSummary;
import com.aliyun.sdk.service.oss2.models.Part;
import com.aliyun.sdk.service.oss2.models.PresignResult;
import com.aliyun.sdk.service.oss2.models.PutObjectRequest;
import com.aliyun.sdk.service.oss2.models.PutObjectResult;
import com.aliyun.sdk.service.oss2.models.PutObjectTaggingRequest;
import com.aliyun.sdk.service.oss2.models.PutObjectTaggingResult;
import com.aliyun.sdk.service.oss2.models.Tag;
import com.aliyun.sdk.service.oss2.models.TagSet;
import com.aliyun.sdk.service.oss2.models.Tagging;
import com.aliyun.sdk.service.oss2.models.UploadPartRequest;
import com.aliyun.sdk.service.oss2.models.UploadPartResult;
import com.aliyun.sdk.service.oss2.transfermanager.DownloadError;
import com.aliyun.sdk.service.oss2.transfermanager.DownloadResult;
import com.aliyun.sdk.service.oss2.transfermanager.Downloader;
import com.salesforce.multicloudj.blob.ali.AliTransformerSupplier;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.BlobStoreValidator;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsBatch;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageResponse;
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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

public class AliAsyncBlobStoreTest {

  private static final String BUCKET = "test-bucket";
  private static final String REGION = "cn-shanghai";

  private OSSAsyncClient mockAsyncClient;
  private OSSClient mockSyncClient;
  private Downloader mockDownloader;
  private AliAsyncBlobStore store;
  private final BlobStoreValidator validator = new BlobStoreValidator();

  @BeforeEach
  void setup() {
    mockAsyncClient = mock(OSSAsyncClient.class);
    mockSyncClient = mock(OSSClient.class);
    mockDownloader = mock(Downloader.class);
    store = new AliAsyncBlobStore(
        BUCKET, REGION, null, validator, mockAsyncClient,
        mockSyncClient, new AliTransformerSupplier(), null,
        mockDownloader);
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

  @Test
  void testUploadFailedAsyncClient() {
    RuntimeException cause = new RuntimeException("network error");
    when(mockAsyncClient.putObjectAsync(
        any(PutObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.failedFuture(cause));

    UploadRequest request = UploadRequest.builder()
        .withKey("fail-key")
        .withContentLength(5)
        .build();
    byte[] content = "hello".getBytes(StandardCharsets.UTF_8);

    ExecutionException ex = assertThrows(ExecutionException.class,
        () -> store.upload(request, content).get());
    assertNotNull(ex.getCause());
  }

  @Test
  void testDownloadFailedAsyncClient() {
    RuntimeException cause = new RuntimeException("connection refused");
    when(mockAsyncClient.getObjectAsync(
        any(GetObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.failedFuture(cause));

    DownloadRequest request = DownloadRequest.builder()
        .withKey("fail-key")
        .build();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    ExecutionException ex = assertThrows(ExecutionException.class,
        () -> store.download(request, outputStream).get());
    assertNotNull(ex.getCause());
  }

  @Test
  void testParallelDownloadToFile(@TempDir Path tempDir) throws Exception {
    HeadObjectResult mockHeadResult = mock(HeadObjectResult.class);
    when(mockHeadResult.contentLength()).thenReturn(1024L);
    when(mockHeadResult.eTag()).thenReturn("\"etag-parallel\"");
    when(mockHeadResult.versionId()).thenReturn("v-parallel");
    when(mockHeadResult.contentType()).thenReturn("application/octet-stream");
    when(mockAsyncClient.headObjectAsync(
        any(HeadObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockHeadResult));

    DownloadResult mockDownloadResult = new DownloadResult(1024L);
    when(mockDownloader.downloadFile(
        any(GetObjectRequest.class), anyString()))
        .thenReturn(mockDownloadResult);

    Path file = tempDir.resolve("parallel-dl.bin");
    DownloadRequest request = DownloadRequest.builder()
        .withKey("parallel-key")
        .withParallelDownload(true)
        .build();
    DownloadResponse response = store.download(request, file).get();

    assertNotNull(response);
    assertEquals("parallel-key", response.getKey());
    BlobMetadata metadata = response.getMetadata();
    assertNotNull(metadata);
    assertEquals("etag-parallel", metadata.getETag());
    assertEquals(1024L, metadata.getObjectSize());
    verify(mockDownloader).downloadFile(
        any(GetObjectRequest.class), anyString());
  }

  @Test
  void testParallelDownloadWithRangeFallsBackToNormal(
      @TempDir Path tempDir) throws Exception {
    byte[] content = "range content".getBytes(StandardCharsets.UTF_8);
    GetObjectResult mockResult = mock(GetObjectResult.class);
    when(mockResult.body())
        .thenReturn(new ByteArrayInputStream(content));
    when(mockResult.contentLength()).thenReturn((long) content.length);
    when(mockResult.eTag()).thenReturn("\"etag-range\"");
    when(mockAsyncClient.getObjectAsync(
        any(GetObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    Path file = tempDir.resolve("range-dl.bin");
    DownloadRequest request = DownloadRequest.builder()
        .withKey("range-key")
        .withParallelDownload(true)
        .withRange(0L, 10L)
        .build();
    DownloadResponse response = store.download(request, file).get();

    assertNotNull(response);
    verify(mockAsyncClient, never()).headObjectAsync(
        any(HeadObjectRequest.class), any(OperationOptions.class));
    assertTrue(Files.exists(file));
  }

  @Test
  void testParallelDownloadFailure(@TempDir Path tempDir) throws Exception {
    HeadObjectResult mockHeadResult = mock(HeadObjectResult.class);
    when(mockHeadResult.contentLength()).thenReturn(2048L);
    when(mockAsyncClient.headObjectAsync(
        any(HeadObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockHeadResult));

    when(mockDownloader.downloadFile(
        any(GetObjectRequest.class), anyString()))
        .thenThrow(new DownloadError("download failed",
            new RuntimeException("network error")));

    Path file = tempDir.resolve("fail-parallel.bin");
    DownloadRequest request = DownloadRequest.builder()
        .withKey("fail-parallel-key")
        .withParallelDownload(true)
        .build();

    ExecutionException ex = assertThrows(ExecutionException.class,
        () -> store.download(request, file).get());
    assertNotNull(ex.getCause());
  }

  @Test
  void testDeleteSingleObject() throws Exception {
    DeleteObjectResult mockResult = mock(DeleteObjectResult.class);
    when(mockAsyncClient.deleteObjectAsync(
        any(DeleteObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    store.delete("key-to-delete", null).get();
  }

  @Test
  void testDeleteMultipleObjects() throws Exception {
    DeleteMultipleObjectsResult mockResult =
        mock(DeleteMultipleObjectsResult.class);
    when(mockAsyncClient.deleteMultipleObjectsAsync(
        any(DeleteMultipleObjectsRequest.class),
        any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    List<BlobIdentifier> objects = List.of(
        new BlobIdentifier("key1", null),
        new BlobIdentifier("key2", "v1"));
    store.delete(objects).get();
  }

  @Test
  void testDeleteEmptyCollection() {
    assertThrows(IllegalArgumentException.class,
        () -> store.delete(List.of()));
  }

  @Test
  void testCopy() throws Exception {
    CopyObjectResult mockResult = mock(CopyObjectResult.class);
    when(mockResult.versionId()).thenReturn("v-copy");
    when(mockResult.eTag()).thenReturn("\"etag-copy\"");
    when(mockResult.lastModified()).thenReturn("Wed, 27 May 2026 00:00:00 GMT");
    when(mockAsyncClient.copyObjectAsync(
        any(CopyObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    CopyRequest request = CopyRequest.builder()
        .srcKey("src-key")
        .destBucket("dest-bucket")
        .destKey("dest-key")
        .build();
    CopyResponse response = store.copy(request).get();

    assertNotNull(response);
    assertEquals("dest-key", response.getKey());
    assertEquals("v-copy", response.getVersionId());
    assertEquals("etag-copy", response.getETag());
    assertNotNull(response.getLastModified());
  }

  @Test
  void testDeleteSingleObjectFailed() {
    RuntimeException cause = new RuntimeException("access denied");
    when(mockAsyncClient.deleteObjectAsync(
        any(DeleteObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.failedFuture(cause));

    ExecutionException ex = assertThrows(ExecutionException.class,
        () -> store.delete("forbidden-key", null).get());
    assertNotNull(ex.getCause());
  }

  @Test
  void testDeleteMultipleObjectsFailed() {
    RuntimeException cause = new RuntimeException("batch delete error");
    when(mockAsyncClient.deleteMultipleObjectsAsync(
        any(DeleteMultipleObjectsRequest.class),
        any(OperationOptions.class)))
        .thenReturn(CompletableFuture.failedFuture(cause));

    List<BlobIdentifier> objects = List.of(
        new BlobIdentifier("key1", null),
        new BlobIdentifier("key2", null));

    ExecutionException ex = assertThrows(ExecutionException.class,
        () -> store.delete(objects).get());
    assertNotNull(ex.getCause());
  }

  @Test
  void testCopyFailed() {
    RuntimeException cause = new RuntimeException("source not found");
    when(mockAsyncClient.copyObjectAsync(
        any(CopyObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.failedFuture(cause));

    CopyRequest request = CopyRequest.builder()
        .srcKey("missing-key")
        .destBucket("dest-bucket")
        .destKey("dest-key")
        .build();

    ExecutionException ex = assertThrows(ExecutionException.class,
        () -> store.copy(request).get());
    assertNotNull(ex.getCause());
  }

  @Test
  void testGetMetadata() throws Exception {
    HeadObjectResult mockResult = mock(HeadObjectResult.class);
    when(mockResult.versionId()).thenReturn("v-meta");
    when(mockResult.eTag()).thenReturn("\"etag-meta\"");
    when(mockResult.contentLength()).thenReturn(1024L);
    when(mockResult.lastModified()).thenReturn("Wed, 27 May 2026 00:00:00 GMT");
    when(mockResult.contentType()).thenReturn("application/octet-stream");
    when(mockResult.contentMd5()).thenReturn(null);
    when(mockResult.metadata()).thenReturn(Map.of());
    when(mockAsyncClient.headObjectAsync(
        any(HeadObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    BlobMetadata metadata = store.getMetadata("meta-key", null).get();

    assertNotNull(metadata);
    assertEquals("meta-key", metadata.getKey());
    assertEquals("v-meta", metadata.getVersionId());
    assertEquals("etag-meta", metadata.getETag());
    assertEquals(1024L, metadata.getObjectSize());
    assertEquals("application/octet-stream", metadata.getContentType());
    assertNotNull(metadata.getLastModified());
  }

  @Test
  void testDoesObjectExistTrue() throws Exception {
    when(mockAsyncClient.doesObjectExistAsync(
        any(GetObjectMetaRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(true));

    boolean exists = store.doesObjectExist("existing-key", null).get();
    assertEquals(true, exists);
  }

  @Test
  void testDoesObjectExistFalse() throws Exception {
    when(mockAsyncClient.doesObjectExistAsync(
        any(GetObjectMetaRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(false));

    boolean exists = store.doesObjectExist("missing-key", null).get();
    assertEquals(false, exists);
  }

  @Test
  void testDoesBucketExistTrue() throws Exception {
    when(mockAsyncClient.doesBucketExistAsync(BUCKET))
        .thenReturn(CompletableFuture.completedFuture(true));

    boolean exists = store.doesBucketExist().get();
    assertEquals(true, exists);
  }

  @Test
  void testDoesBucketExistFalse() throws Exception {
    when(mockAsyncClient.doesBucketExistAsync(BUCKET))
        .thenReturn(CompletableFuture.completedFuture(false));

    boolean exists = store.doesBucketExist().get();
    assertEquals(false, exists);
  }

  @Test
  void testGetMetadataFailed() {
    RuntimeException cause = new RuntimeException("object not found");
    when(mockAsyncClient.headObjectAsync(
        any(HeadObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.failedFuture(cause));

    ExecutionException ex = assertThrows(ExecutionException.class,
        () -> store.getMetadata("missing-key", null).get());
    assertNotNull(ex.getCause());
  }

  @Test
  void testDoesObjectExistFailed() {
    RuntimeException cause = new RuntimeException("access denied");
    when(mockAsyncClient.doesObjectExistAsync(
        any(GetObjectMetaRequest.class)))
        .thenReturn(CompletableFuture.failedFuture(cause));

    ExecutionException ex = assertThrows(ExecutionException.class,
        () -> store.doesObjectExist("forbidden-key", null).get());
    assertNotNull(ex.getCause());
  }

  @Test
  void testDoesBucketExistFailed() {
    RuntimeException cause = new RuntimeException("service unavailable");
    when(mockAsyncClient.doesBucketExistAsync(BUCKET))
        .thenReturn(CompletableFuture.failedFuture(cause));

    ExecutionException ex = assertThrows(ExecutionException.class,
        () -> store.doesBucketExist().get());
    assertNotNull(ex.getCause());
  }

  @Test
  void testListPage() throws Exception {
    ObjectSummary obj1 = mock(ObjectSummary.class);
    when(obj1.key()).thenReturn("prefix/file1.txt");
    when(obj1.size()).thenReturn(100L);
    ObjectSummary obj2 = mock(ObjectSummary.class);
    when(obj2.key()).thenReturn("prefix/file2.txt");
    when(obj2.size()).thenReturn(200L);

    CommonPrefix cp = mock(CommonPrefix.class);
    when(cp.prefix()).thenReturn("prefix/subdir/");

    ListObjectsV2Result mockResult = mock(ListObjectsV2Result.class);
    when(mockResult.contents()).thenReturn(List.of(obj1, obj2));
    when(mockResult.commonPrefixes()).thenReturn(List.of(cp));
    when(mockResult.isTruncated()).thenReturn(false);
    when(mockResult.nextContinuationToken()).thenReturn(null);
    when(mockAsyncClient.listObjectsV2Async(
        any(ListObjectsV2Request.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    ListBlobsPageRequest request = ListBlobsPageRequest.builder()
        .withPrefix("prefix/")
        .build();
    ListBlobsPageResponse response = store.listPage(request).get();

    assertNotNull(response);
    assertEquals(2, response.getBlobs().size());
    assertEquals("prefix/file1.txt", response.getBlobs().get(0).getKey());
    assertEquals(100L, response.getBlobs().get(0).getObjectSize());
    assertEquals("prefix/file2.txt", response.getBlobs().get(1).getKey());
    assertEquals(1, response.getCommonPrefixes().size());
    assertEquals("prefix/subdir/", response.getCommonPrefixes().get(0));
    assertEquals(false, response.isTruncated());
  }

  @Test
  void testListPageTruncated() throws Exception {
    ObjectSummary obj = mock(ObjectSummary.class);
    when(obj.key()).thenReturn("key1");
    when(obj.size()).thenReturn(50L);

    ListObjectsV2Result mockResult = mock(ListObjectsV2Result.class);
    when(mockResult.contents()).thenReturn(List.of(obj));
    when(mockResult.commonPrefixes()).thenReturn(null);
    when(mockResult.isTruncated()).thenReturn(true);
    when(mockResult.nextContinuationToken()).thenReturn("token-abc");
    when(mockAsyncClient.listObjectsV2Async(
        any(ListObjectsV2Request.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    ListBlobsPageRequest request = ListBlobsPageRequest.builder()
        .withPrefix("prefix/")
        .withMaxResults(1)
        .build();
    ListBlobsPageResponse response = store.listPage(request).get();

    assertEquals(true, response.isTruncated());
    assertEquals("token-abc", response.getNextPageToken());
    assertEquals(1, response.getBlobs().size());

    ArgumentCaptor<ListObjectsV2Request> captor =
        ArgumentCaptor.forClass(ListObjectsV2Request.class);
    verify(mockAsyncClient).listObjectsV2Async(
        captor.capture(), any(OperationOptions.class));
    ListObjectsV2Request captured = captor.getValue();
    assertEquals("prefix/", captured.prefix());
    assertEquals(1L, captured.maxKeys());
  }

  @Test
  void testList() throws Exception {
    ObjectSummary obj1 = mock(ObjectSummary.class);
    when(obj1.key()).thenReturn("a/1.txt");
    when(obj1.size()).thenReturn(10L);
    ObjectSummary obj2 = mock(ObjectSummary.class);
    when(obj2.key()).thenReturn("a/2.txt");
    when(obj2.size()).thenReturn(20L);

    ListObjectsV2Result page1 = mock(ListObjectsV2Result.class);
    when(page1.contents()).thenReturn(List.of(obj1));
    when(page1.commonPrefixes()).thenReturn(null);
    when(page1.isTruncated()).thenReturn(true);
    when(page1.nextContinuationToken()).thenReturn("token-2");

    ListObjectsV2Result page2 = mock(ListObjectsV2Result.class);
    when(page2.contents()).thenReturn(List.of(obj2));
    when(page2.commonPrefixes()).thenReturn(null);
    when(page2.isTruncated()).thenReturn(false);
    when(page2.nextContinuationToken()).thenReturn(null);

    when(mockAsyncClient.listObjectsV2Async(
        any(ListObjectsV2Request.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(page1))
        .thenReturn(CompletableFuture.completedFuture(page2));

    List<ListBlobsBatch> batches = new ArrayList<>();
    ListBlobsRequest request = ListBlobsRequest.builder()
        .withPrefix("a/")
        .build();
    store.list(request, batches::add).get();

    assertEquals(2, batches.size());
    assertEquals("a/1.txt", batches.get(0).getBlobs().get(0).getKey());
    assertEquals("a/2.txt", batches.get(1).getBlobs().get(0).getKey());
  }

  @Test
  void testListPageFailed() {
    RuntimeException cause = new RuntimeException("access denied");
    when(mockAsyncClient.listObjectsV2Async(
        any(ListObjectsV2Request.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.failedFuture(cause));

    ListBlobsPageRequest request = ListBlobsPageRequest.builder()
        .withPrefix("prefix/")
        .build();

    ExecutionException ex = assertThrows(ExecutionException.class,
        () -> store.listPage(request).get());
    assertNotNull(ex.getCause());
  }

  @Test
  void testListFailed() {
    RuntimeException cause = new RuntimeException("service unavailable");
    when(mockAsyncClient.listObjectsV2Async(
        any(ListObjectsV2Request.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.failedFuture(cause));

    List<ListBlobsBatch> batches = new ArrayList<>();
    ListBlobsRequest request = ListBlobsRequest.builder()
        .withPrefix("a/")
        .build();

    ExecutionException ex = assertThrows(ExecutionException.class,
        () -> store.list(request, batches::add).get());
    assertNotNull(ex.getCause());
  }

  @Test
  void testInitiateMultipartUpload() throws Exception {
    InitiateMultipartUpload upload = mock(InitiateMultipartUpload.class);
    when(upload.bucket()).thenReturn(BUCKET);
    when(upload.key()).thenReturn("mpu-key");
    when(upload.uploadId()).thenReturn("upload-id-123");

    InitiateMultipartUploadResult mockResult =
        mock(InitiateMultipartUploadResult.class);
    when(mockResult.initiateMultipartUpload()).thenReturn(upload);
    when(mockAsyncClient.initiateMultipartUploadAsync(
        any(InitiateMultipartUploadRequest.class),
        any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    MultipartUploadRequest request =
        new MultipartUploadRequest.Builder().withKey("mpu-key").build();
    MultipartUpload mpu = store.initiateMultipartUpload(request).get();

    assertNotNull(mpu);
    assertEquals(BUCKET, mpu.getBucket());
    assertEquals("mpu-key", mpu.getKey());
    assertEquals("upload-id-123", mpu.getId());
  }

  @Test
  void testUploadMultipartPart() throws Exception {
    UploadPartResult mockResult = mock(UploadPartResult.class);
    when(mockResult.eTag()).thenReturn("\"part-etag-1\"");
    when(mockAsyncClient.uploadPartAsync(
        any(UploadPartRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    MultipartUpload mpu = MultipartUpload.builder()
        .bucket(BUCKET)
        .key("mpu-key")
        .id("upload-id-123")
        .build();
    byte[] content = "part content".getBytes(StandardCharsets.UTF_8);
    MultipartPart mpp = new MultipartPart(
        1, new ByteArrayInputStream(content), content.length, null);
    UploadPartResponse response =
        store.uploadMultipartPart(mpu, mpp).get();

    assertNotNull(response);
    assertEquals(1, response.getPartNumber());
    assertEquals("part-etag-1", response.getEtag());
    assertEquals(content.length, response.getSizeInBytes());
  }

  @Test
  void testCompleteMultipartUpload() throws Exception {
    CompleteMultipartUploadResultXml xml =
        mock(CompleteMultipartUploadResultXml.class);
    when(xml.eTag()).thenReturn("\"final-etag\"");
    CompleteMultipartUploadResult mockResult =
        mock(CompleteMultipartUploadResult.class);
    when(mockResult.completeMultipartUpload()).thenReturn(xml);
    when(mockAsyncClient.completeMultipartUploadAsync(
        any(CompleteMultipartUploadRequest.class),
        any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    MultipartUpload mpu = MultipartUpload.builder()
        .bucket(BUCKET)
        .key("mpu-key")
        .id("upload-id-123")
        .build();
    List<UploadPartResponse> parts = List.of(
        new UploadPartResponse(1, "part-etag-1", 1024),
        new UploadPartResponse(2, "part-etag-2", 1024));
    MultipartUploadResponse response =
        store.completeMultipartUpload(mpu, parts).get();

    assertNotNull(response);
    assertEquals("final-etag", response.getEtag());
  }

  @Test
  void testListMultipartUpload() throws Exception {
    Part part1 = mock(Part.class);
    when(part1.partNumber()).thenReturn(1L);
    when(part1.eTag()).thenReturn("\"etag-p1\"");
    when(part1.size()).thenReturn(512L);
    Part part2 = mock(Part.class);
    when(part2.partNumber()).thenReturn(2L);
    when(part2.eTag()).thenReturn("\"etag-p2\"");
    when(part2.size()).thenReturn(1024L);

    ListPartsResult mockResult = mock(ListPartsResult.class);
    when(mockResult.parts()).thenReturn(List.of(part1, part2));
    when(mockAsyncClient.listPartsAsync(
        any(ListPartsRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    MultipartUpload mpu = MultipartUpload.builder()
        .bucket(BUCKET)
        .key("mpu-key")
        .id("upload-id-123")
        .build();
    List<UploadPartResponse> parts =
        store.listMultipartUpload(mpu).get();

    assertNotNull(parts);
    assertEquals(2, parts.size());
    assertEquals(1, parts.get(0).getPartNumber());
    assertEquals("etag-p1", parts.get(0).getEtag());
    assertEquals(512L, parts.get(0).getSizeInBytes());
    assertEquals(2, parts.get(1).getPartNumber());
    assertEquals("etag-p2", parts.get(1).getEtag());
    assertEquals(1024L, parts.get(1).getSizeInBytes());
  }

  @Test
  void testAbortMultipartUpload() throws Exception {
    AbortMultipartUploadResult mockResult =
        mock(AbortMultipartUploadResult.class);
    when(mockAsyncClient.abortMultipartUploadAsync(
        any(AbortMultipartUploadRequest.class),
        any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    MultipartUpload mpu = MultipartUpload.builder()
        .bucket(BUCKET)
        .key("mpu-key")
        .id("upload-id-123")
        .build();
    store.abortMultipartUpload(mpu).get();
  }

  @Test
  void testInitiateMultipartUploadFailed() {
    RuntimeException cause = new RuntimeException("access denied");
    when(mockAsyncClient.initiateMultipartUploadAsync(
        any(InitiateMultipartUploadRequest.class),
        any(OperationOptions.class)))
        .thenReturn(CompletableFuture.failedFuture(cause));

    MultipartUploadRequest request =
        new MultipartUploadRequest.Builder().withKey("fail-key").build();

    ExecutionException ex = assertThrows(ExecutionException.class,
        () -> store.initiateMultipartUpload(request).get());
    assertNotNull(ex.getCause());
  }

  @Test
  void testUploadMultipartPartFailed() {
    RuntimeException cause = new RuntimeException("network error");
    when(mockAsyncClient.uploadPartAsync(
        any(UploadPartRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.failedFuture(cause));

    MultipartUpload mpu = MultipartUpload.builder()
        .bucket(BUCKET)
        .key("mpu-key")
        .id("upload-id-123")
        .build();
    byte[] content = "part content".getBytes(StandardCharsets.UTF_8);
    MultipartPart mpp = new MultipartPart(
        1, new ByteArrayInputStream(content), content.length, null);

    ExecutionException ex = assertThrows(ExecutionException.class,
        () -> store.uploadMultipartPart(mpu, mpp).get());
    assertNotNull(ex.getCause());
  }

  @Test
  void testCompleteMultipartUploadFailed() {
    RuntimeException cause = new RuntimeException("invalid part order");
    when(mockAsyncClient.completeMultipartUploadAsync(
        any(CompleteMultipartUploadRequest.class),
        any(OperationOptions.class)))
        .thenReturn(CompletableFuture.failedFuture(cause));

    MultipartUpload mpu = MultipartUpload.builder()
        .bucket(BUCKET)
        .key("mpu-key")
        .id("upload-id-123")
        .build();
    List<UploadPartResponse> parts = List.of(
        new UploadPartResponse(1, "part-etag-1", 1024));

    ExecutionException ex = assertThrows(ExecutionException.class,
        () -> store.completeMultipartUpload(mpu, parts).get());
    assertNotNull(ex.getCause());
  }

  @Test
  void testListMultipartUploadFailed() {
    RuntimeException cause = new RuntimeException("upload not found");
    when(mockAsyncClient.listPartsAsync(
        any(ListPartsRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.failedFuture(cause));

    MultipartUpload mpu = MultipartUpload.builder()
        .bucket(BUCKET)
        .key("mpu-key")
        .id("upload-id-123")
        .build();

    ExecutionException ex = assertThrows(ExecutionException.class,
        () -> store.listMultipartUpload(mpu).get());
    assertNotNull(ex.getCause());
  }

  @Test
  void testAbortMultipartUploadFailed() {
    RuntimeException cause = new RuntimeException("upload already completed");
    when(mockAsyncClient.abortMultipartUploadAsync(
        any(AbortMultipartUploadRequest.class),
        any(OperationOptions.class)))
        .thenReturn(CompletableFuture.failedFuture(cause));

    MultipartUpload mpu = MultipartUpload.builder()
        .bucket(BUCKET)
        .key("mpu-key")
        .id("upload-id-123")
        .build();

    ExecutionException ex = assertThrows(ExecutionException.class,
        () -> store.abortMultipartUpload(mpu).get());
    assertNotNull(ex.getCause());
  }

  @Test
  void testGetTags() throws Exception {
    Tag tag1 = mock(Tag.class);
    when(tag1.key()).thenReturn("env");
    when(tag1.value()).thenReturn("prod");
    Tag tag2 = mock(Tag.class);
    when(tag2.key()).thenReturn("team");
    when(tag2.value()).thenReturn("platform");

    TagSet tagSet = mock(TagSet.class);
    when(tagSet.tags()).thenReturn(List.of(tag1, tag2));
    Tagging tagging = mock(Tagging.class);
    when(tagging.tagSet()).thenReturn(tagSet);

    GetObjectTaggingResult mockResult =
        mock(GetObjectTaggingResult.class);
    when(mockResult.tagging()).thenReturn(tagging);
    when(mockAsyncClient.getObjectTaggingAsync(
        any(GetObjectTaggingRequest.class),
        any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    Map<String, String> tags = store.getTags("tagged-key").get();

    assertNotNull(tags);
    assertEquals(2, tags.size());
    assertEquals("prod", tags.get("env"));
    assertEquals("platform", tags.get("team"));
  }

  @Test
  void testSetTags() throws Exception {
    PutObjectTaggingResult mockResult =
        mock(PutObjectTaggingResult.class);
    when(mockAsyncClient.putObjectTaggingAsync(
        any(PutObjectTaggingRequest.class),
        any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    Map<String, String> tags = Map.of("env", "staging", "version", "2");
    store.setTags("tag-key", tags).get();
  }

  @Test
  void testGeneratePresignedUrlDownload() throws Exception {
    PresignResult mockResult = mock(PresignResult.class);
    when(mockResult.url()).thenReturn("https://bucket.oss.aliyuncs.com/key?sig=abc");
    when(mockSyncClient.presign(
        any(GetObjectRequest.class), any(PresignOptions.class)))
        .thenReturn(mockResult);

    PresignedUrlRequest request = PresignedUrlRequest.builder()
        .key("presign-key")
        .type(PresignedOperation.DOWNLOAD)
        .duration(Duration.ofMinutes(15))
        .build();
    URL url = store.generatePresignedUrl(request).get();

    assertNotNull(url);
    assertEquals("https", url.getProtocol());
    assertEquals("bucket.oss.aliyuncs.com", url.getHost());
  }

  @Test
  void testGeneratePresignedUrlUpload() throws Exception {
    PresignResult mockResult = mock(PresignResult.class);
    when(mockResult.url()).thenReturn("https://bucket.oss.aliyuncs.com/upload-key?sig=xyz");
    when(mockSyncClient.presign(
        any(PutObjectRequest.class), any(PresignOptions.class)))
        .thenReturn(mockResult);

    PresignedUrlRequest request = PresignedUrlRequest.builder()
        .key("upload-key")
        .type(PresignedOperation.UPLOAD)
        .duration(Duration.ofMinutes(30))
        .build();
    URL url = store.generatePresignedUrl(request).get();

    assertNotNull(url);
    assertEquals("https", url.getProtocol());
  }

  @Test
  void testGetTagsFailed() {
    RuntimeException cause = new RuntimeException("object not found");
    when(mockAsyncClient.getObjectTaggingAsync(
        any(GetObjectTaggingRequest.class),
        any(OperationOptions.class)))
        .thenReturn(CompletableFuture.failedFuture(cause));

    ExecutionException ex = assertThrows(ExecutionException.class,
        () -> store.getTags("missing-key").get());
    assertNotNull(ex.getCause());
  }

  @Test
  void testSetTagsFailed() {
    RuntimeException cause = new RuntimeException("access denied");
    when(mockAsyncClient.putObjectTaggingAsync(
        any(PutObjectTaggingRequest.class),
        any(OperationOptions.class)))
        .thenReturn(CompletableFuture.failedFuture(cause));

    ExecutionException ex = assertThrows(ExecutionException.class,
        () -> store.setTags("forbidden-key", Map.of("k", "v")).get());
    assertNotNull(ex.getCause());
  }

  @Test
  void testGeneratePresignedUrlFailed() {
    when(mockSyncClient.presign(
        any(GetObjectRequest.class), any(PresignOptions.class)))
        .thenThrow(new RuntimeException("presign error"));

    PresignedUrlRequest request = PresignedUrlRequest.builder()
        .key("fail-key")
        .type(PresignedOperation.DOWNLOAD)
        .duration(Duration.ofMinutes(15))
        .build();

    ExecutionException ex = assertThrows(ExecutionException.class,
        () -> store.generatePresignedUrl(request).get());
    assertNotNull(ex.getCause());
  }

  @Test
  void testUploadDirectory(@TempDir Path tempDir) throws Exception {
    Files.writeString(tempDir.resolve("file1.txt"), "content1");
    Files.writeString(tempDir.resolve("file2.txt"), "content2");

    PutObjectResult mockResult = mock(PutObjectResult.class);
    when(mockResult.versionId()).thenReturn(null);
    when(mockResult.eTag()).thenReturn("\"etag\"");
    when(mockAsyncClient.putObjectAsync(
        any(PutObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    DirectoryUploadRequest request = DirectoryUploadRequest.builder()
        .localSourceDirectory(tempDir.toString())
        .prefix("upload-prefix")
        .includeSubFolders(true)
        .build();
    DirectoryUploadResponse response =
        store.uploadDirectory(request).get();

    assertNotNull(response);
    assertNotNull(response.getFailedTransfers());
    assertEquals(0, response.getFailedTransfers().size());
  }

  @Test
  void testUploadDirectoryWithSubFolders(@TempDir Path tempDir)
      throws Exception {
    Files.writeString(tempDir.resolve("root.txt"), "root");
    Path subDir = Files.createDirectory(tempDir.resolve("sub"));
    Files.writeString(subDir.resolve("nested.txt"), "nested");

    PutObjectResult mockResult = mock(PutObjectResult.class);
    when(mockResult.versionId()).thenReturn(null);
    when(mockResult.eTag()).thenReturn("\"etag\"");
    when(mockAsyncClient.putObjectAsync(
        any(PutObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResult));

    DirectoryUploadRequest requestWithSub = DirectoryUploadRequest.builder()
        .localSourceDirectory(tempDir.toString())
        .prefix("pfx")
        .includeSubFolders(true)
        .build();
    DirectoryUploadResponse responseWithSub =
        store.uploadDirectory(requestWithSub).get();
    assertEquals(0, responseWithSub.getFailedTransfers().size());

    DirectoryUploadRequest requestNoSub = DirectoryUploadRequest.builder()
        .localSourceDirectory(tempDir.toString())
        .prefix("pfx")
        .includeSubFolders(false)
        .build();
    DirectoryUploadResponse responseNoSub =
        store.uploadDirectory(requestNoSub).get();
    assertEquals(0, responseNoSub.getFailedTransfers().size());
  }

  @Test
  void testUploadDirectoryEmptyDir(@TempDir Path tempDir) throws Exception {
    DirectoryUploadRequest request = DirectoryUploadRequest.builder()
        .localSourceDirectory(tempDir.toString())
        .prefix("empty")
        .includeSubFolders(true)
        .build();
    DirectoryUploadResponse response =
        store.uploadDirectory(request).get();

    assertNotNull(response);
    assertEquals(0, response.getFailedTransfers().size());
  }

  @Test
  void testDownloadDirectory(@TempDir Path tempDir) throws Exception {
    ObjectSummary obj1 = mock(ObjectSummary.class);
    when(obj1.key()).thenReturn("prefix/file1.txt");
    when(obj1.size()).thenReturn(6L);
    ObjectSummary obj2 = mock(ObjectSummary.class);
    when(obj2.key()).thenReturn("prefix/sub/file2.txt");
    when(obj2.size()).thenReturn(7L);

    ListObjectsV2Result listResult = mock(ListObjectsV2Result.class);
    when(listResult.contents()).thenReturn(List.of(obj1, obj2));
    when(listResult.commonPrefixes()).thenReturn(null);
    when(listResult.isTruncated()).thenReturn(false);
    when(mockAsyncClient.listObjectsV2Async(
        any(ListObjectsV2Request.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(listResult));

    byte[] content = "hello1".getBytes(StandardCharsets.UTF_8);
    when(mockAsyncClient.getObjectAsync(
        any(GetObjectRequest.class), any(OperationOptions.class)))
        .thenAnswer(invocation -> {
          GetObjectResult result = mock(GetObjectResult.class);
          when(result.body()).thenReturn(
              new ByteArrayInputStream(content));
          when(result.contentLength()).thenReturn((long) content.length);
          when(result.eTag()).thenReturn("\"e\"");
          return CompletableFuture.completedFuture(result);
        });

    DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
        .prefixToDownload("prefix")
        .localDestinationDirectory(tempDir.toString())
        .build();
    DirectoryDownloadResponse response =
        store.downloadDirectory(request).get();

    assertNotNull(response);
    assertEquals(0, response.getFailedTransfers().size());
    assertTrue(Files.exists(tempDir.resolve("file1.txt")));
    assertTrue(Files.exists(tempDir.resolve("sub/file2.txt")));
  }

  @Test
  void testDownloadDirectorySkipsFolderMarkers(
      @TempDir Path tempDir) throws Exception {
    ObjectSummary realObj = mock(ObjectSummary.class);
    when(realObj.key()).thenReturn("dir/file.txt");
    when(realObj.size()).thenReturn(5L);
    ObjectSummary folderMarker = mock(ObjectSummary.class);
    when(folderMarker.key()).thenReturn("dir/subfolder/");
    when(folderMarker.size()).thenReturn(0L);

    ListObjectsV2Result listResult = mock(ListObjectsV2Result.class);
    when(listResult.contents()).thenReturn(List.of(realObj, folderMarker));
    when(listResult.commonPrefixes()).thenReturn(null);
    when(listResult.isTruncated()).thenReturn(false);
    when(mockAsyncClient.listObjectsV2Async(
        any(ListObjectsV2Request.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(listResult));

    byte[] content = "hello".getBytes(StandardCharsets.UTF_8);
    GetObjectResult getResult = mock(GetObjectResult.class);
    when(getResult.body()).thenReturn(new ByteArrayInputStream(content));
    when(getResult.contentLength()).thenReturn((long) content.length);
    when(getResult.eTag()).thenReturn("\"e\"");
    when(mockAsyncClient.getObjectAsync(
        any(GetObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(getResult));

    DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
        .prefixToDownload("dir")
        .localDestinationDirectory(tempDir.toString())
        .build();
    DirectoryDownloadResponse response =
        store.downloadDirectory(request).get();

    assertNotNull(response);
    assertEquals(0, response.getFailedTransfers().size());
    assertTrue(Files.exists(tempDir.resolve("file.txt")));
    assertArrayEquals(content, Files.readAllBytes(tempDir.resolve("file.txt")));
  }

  @Test
  void testDownloadDirectoryWithPrefixExclusions(
      @TempDir Path tempDir) throws Exception {
    ObjectSummary included = mock(ObjectSummary.class);
    when(included.key()).thenReturn("data/keep.txt");
    when(included.size()).thenReturn(4L);
    ObjectSummary excluded = mock(ObjectSummary.class);
    when(excluded.key()).thenReturn("data/logs/debug.log");
    when(excluded.size()).thenReturn(10L);

    ListObjectsV2Result listResult = mock(ListObjectsV2Result.class);
    when(listResult.contents()).thenReturn(List.of(included, excluded));
    when(listResult.commonPrefixes()).thenReturn(null);
    when(listResult.isTruncated()).thenReturn(false);
    when(mockAsyncClient.listObjectsV2Async(
        any(ListObjectsV2Request.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(listResult));

    byte[] content = "keep".getBytes(StandardCharsets.UTF_8);
    GetObjectResult getResult = mock(GetObjectResult.class);
    when(getResult.body()).thenReturn(new ByteArrayInputStream(content));
    when(getResult.contentLength()).thenReturn((long) content.length);
    when(getResult.eTag()).thenReturn("\"e\"");
    when(mockAsyncClient.getObjectAsync(
        any(GetObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(getResult));

    DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
        .prefixToDownload("data")
        .localDestinationDirectory(tempDir.toString())
        .prefixesToExclude(List.of("data/logs/"))
        .build();
    DirectoryDownloadResponse response =
        store.downloadDirectory(request).get();

    assertNotNull(response);
    assertEquals(0, response.getFailedTransfers().size());
    assertTrue(Files.exists(tempDir.resolve("keep.txt")));
    assertTrue(Files.notExists(tempDir.resolve("logs/debug.log")));
  }

  @Test
  void testDownloadDirectoryEmpty(@TempDir Path tempDir) throws Exception {
    ListObjectsV2Result listResult = mock(ListObjectsV2Result.class);
    when(listResult.contents()).thenReturn(List.of());
    when(listResult.commonPrefixes()).thenReturn(null);
    when(listResult.isTruncated()).thenReturn(false);
    when(mockAsyncClient.listObjectsV2Async(
        any(ListObjectsV2Request.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(listResult));

    DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
        .prefixToDownload("empty-prefix")
        .localDestinationDirectory(tempDir.toString())
        .build();
    DirectoryDownloadResponse response =
        store.downloadDirectory(request).get();

    assertNotNull(response);
    assertEquals(0, response.getFailedTransfers().size());
  }

  @Test
  void testDownloadDirectoryWithTransferLogging(
      @TempDir Path tempDir) throws Exception {
    ObjectSummary obj = mock(ObjectSummary.class);
    when(obj.key()).thenReturn("log/data.bin");
    when(obj.size()).thenReturn(1024L);

    ListObjectsV2Result listResult = mock(ListObjectsV2Result.class);
    when(listResult.contents()).thenReturn(List.of(obj));
    when(listResult.commonPrefixes()).thenReturn(null);
    when(listResult.isTruncated()).thenReturn(false);
    when(mockAsyncClient.listObjectsV2Async(
        any(ListObjectsV2Request.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(listResult));

    byte[] content = new byte[1024];
    GetObjectResult getResult = mock(GetObjectResult.class);
    when(getResult.body()).thenReturn(new ByteArrayInputStream(content));
    when(getResult.contentLength()).thenReturn(1024L);
    when(getResult.eTag()).thenReturn("\"e\"");
    when(mockAsyncClient.getObjectAsync(
        any(GetObjectRequest.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(getResult));

    DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
        .prefixToDownload("log")
        .localDestinationDirectory(tempDir.toString())
        .transferStatusLoggingEnabled(true)
        .build();
    DirectoryDownloadResponse response =
        store.downloadDirectory(request).get();

    assertNotNull(response);
    assertEquals(0, response.getFailedTransfers().size());
    assertEquals(1024L, response.getTotalBytesTransferred());
  }

  @Test
  void testDeleteDirectory() throws Exception {
    ObjectSummary obj1 = mock(ObjectSummary.class);
    when(obj1.key()).thenReturn("dir/file1.txt");
    when(obj1.size()).thenReturn(100L);
    ObjectSummary obj2 = mock(ObjectSummary.class);
    when(obj2.key()).thenReturn("dir/file2.txt");
    when(obj2.size()).thenReturn(200L);

    ListObjectsV2Result listResult = mock(ListObjectsV2Result.class);
    when(listResult.contents()).thenReturn(List.of(obj1, obj2));
    when(listResult.commonPrefixes()).thenReturn(null);
    when(listResult.isTruncated()).thenReturn(false);
    when(mockAsyncClient.listObjectsV2Async(
        any(ListObjectsV2Request.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(listResult));

    DeleteMultipleObjectsResult deleteResult =
        mock(DeleteMultipleObjectsResult.class);
    when(mockAsyncClient.deleteMultipleObjectsAsync(
        any(DeleteMultipleObjectsRequest.class),
        any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(deleteResult));

    store.deleteDirectory("dir/").get();

    verify(mockAsyncClient, times(1)).deleteMultipleObjectsAsync(
        any(DeleteMultipleObjectsRequest.class),
        any(OperationOptions.class));
  }

  @Test
  void testDeleteDirectoryEmpty() throws Exception {
    ListObjectsV2Result listResult = mock(ListObjectsV2Result.class);
    when(listResult.contents()).thenReturn(List.of());
    when(listResult.commonPrefixes()).thenReturn(null);
    when(listResult.isTruncated()).thenReturn(false);
    when(mockAsyncClient.listObjectsV2Async(
        any(ListObjectsV2Request.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(listResult));

    store.deleteDirectory("empty-dir/").get();

    verify(mockAsyncClient, never()).deleteMultipleObjectsAsync(
        any(DeleteMultipleObjectsRequest.class),
        any(OperationOptions.class));
  }

  @Test
  void testDeleteDirectoryWithPagination() throws Exception {
    ObjectSummary obj1 = mock(ObjectSummary.class);
    when(obj1.key()).thenReturn("dir/page1.txt");
    when(obj1.size()).thenReturn(50L);
    ObjectSummary obj2 = mock(ObjectSummary.class);
    when(obj2.key()).thenReturn("dir/page2.txt");
    when(obj2.size()).thenReturn(60L);

    ListObjectsV2Result firstPage = mock(ListObjectsV2Result.class);
    when(firstPage.contents()).thenReturn(List.of(obj1));
    when(firstPage.commonPrefixes()).thenReturn(null);
    when(firstPage.isTruncated()).thenReturn(true);
    when(firstPage.nextContinuationToken()).thenReturn("token1");

    ListObjectsV2Result secondPage = mock(ListObjectsV2Result.class);
    when(secondPage.contents()).thenReturn(List.of(obj2));
    when(secondPage.commonPrefixes()).thenReturn(null);
    when(secondPage.isTruncated()).thenReturn(false);

    when(mockAsyncClient.listObjectsV2Async(
        any(ListObjectsV2Request.class), any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(firstPage))
        .thenReturn(CompletableFuture.completedFuture(secondPage));

    DeleteMultipleObjectsResult deleteResult =
        mock(DeleteMultipleObjectsResult.class);
    when(mockAsyncClient.deleteMultipleObjectsAsync(
        any(DeleteMultipleObjectsRequest.class),
        any(OperationOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(deleteResult));

    store.deleteDirectory("dir/").get();

    verify(mockAsyncClient, times(2)).deleteMultipleObjectsAsync(
        any(DeleteMultipleObjectsRequest.class),
        any(OperationOptions.class));
  }
}
