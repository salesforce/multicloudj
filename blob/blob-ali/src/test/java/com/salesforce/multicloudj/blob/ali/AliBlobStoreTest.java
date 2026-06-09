package com.salesforce.multicloudj.blob.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.exceptions.OperationException;
import com.aliyun.sdk.service.oss2.exceptions.ServiceException;
import com.aliyun.sdk.service.oss2.models.HeadObjectRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectResult;
import com.aliyun.sdk.service.oss2.models.ListObjectVersionsRequest;
import com.aliyun.sdk.service.oss2.models.ListObjectVersionsResult;
import com.aliyun.sdk.service.oss2.models.ObjectVersion;
import com.aliyun.sdk.service.oss2.paginator.ListObjectVersionsIterable;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyFromRequest;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobVersionsRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.PresignedOperation;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.PresignedUrlResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.ali.AliConstants;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class AliBlobStoreTest {

  private OSSClient mockOssClient;
  private AliBlobStore ali;

  @BeforeEach
  void setup() {
    mockOssClient = mock(OSSClient.class);

    StsCredentials creds = new StsCredentials("key-1", "secret-1", "token-1");
    CredentialsOverrider credsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(creds)
            .build();
    ali =
        new AliBlobStore.Builder()
            .withClient(mockOssClient)
            .withBucket("bucket-1")
            .withRegion("cn-shanghai")
            .withEndpoint(URI.create("https://test.example.com"))
            .withProxyEndpoint(URI.create("http://proxy.example.com:80"))
            .withCredentialsOverrider(credsOverrider)
            .build();
  }

  @Test
  void testClose() throws Exception {
    ali.close();
    verify(mockOssClient, times(1)).close();
  }

  @Test
  void testProviderId() {
    assertEquals(AliConstants.PROVIDER_ID, ali.getProviderId());
  }

  @Test
  void testExceptionHandling() {
    ServiceException serviceException = mock(ServiceException.class);
    when(serviceException.errorCode()).thenReturn("AccessDenied");
    OperationException operationException = mock(OperationException.class);
    when(operationException.getCause()).thenReturn(serviceException);
    Class<?> cls = ali.getException(operationException);
    assertEquals(cls, UnAuthorizedException.class);

    cls = ali.getException(serviceException);
    assertEquals(cls, UnAuthorizedException.class);

    cls = ali.getException(new IllegalArgumentException("bad arg"));
    assertEquals(cls, InvalidArgumentException.class);

    cls = ali.getException(new IOException("Channel is closed"));
    assertEquals(cls, UnknownException.class);
  }

  @Test
  void testDoUploadInputStream() {
    doReturn(buildTestPutObjectResult())
        .when(mockOssClient).putObject(
            any(com.aliyun.sdk.service.oss2.models.PutObjectRequest.class), any());
    verifyUploadTestResults(ali.doUpload(getTestUploadRequest(), mock(InputStream.class)));
  }

  @Test
  void testDoUploadByteArray() {
    doReturn(buildTestPutObjectResult())
        .when(mockOssClient).putObject(
            any(com.aliyun.sdk.service.oss2.models.PutObjectRequest.class), any());
    verifyUploadTestResults(ali.doUpload(getTestUploadRequest(), new byte[1024]));
  }

  @Test
  void testDoUploadFile() throws IOException {
    doReturn(buildTestPutObjectResult())
        .when(mockOssClient).putObject(
            any(com.aliyun.sdk.service.oss2.models.PutObjectRequest.class), any());
    Path path = null;
    try {
      path = Files.createTempFile("tempFile", ".txt");
      try (BufferedWriter writer = Files.newBufferedWriter(path)) {
        writer.write(new char[1024]);
      }
      verifyUploadTestResults(ali.doUpload(getTestUploadRequest(), path.toFile()));
    } finally {
      if (path != null) {
        try {
          Files.deleteIfExists(path);
        } catch (IOException e) {
          Assertions.fail(e);
        }
      }
    }
  }

  @Test
  void testDoUploadPath() throws IOException {
    doReturn(buildTestPutObjectResult())
        .when(mockOssClient).putObject(
            any(com.aliyun.sdk.service.oss2.models.PutObjectRequest.class), any());
    Path path = Files.createTempFile("tempFile", ".txt");
    try (BufferedWriter writer = Files.newBufferedWriter(path)) {
      writer.write(new char[1024]);
    }
    verifyUploadTestResults(ali.doUpload(getTestUploadRequest(), path));
  }

  void verifyUploadTestResults(UploadResponse uploadResponse) {

    // Verify the parameters passed into the OSS SDK
    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.PutObjectRequest> putObjectRequestCaptor =
        ArgumentCaptor.forClass(com.aliyun.sdk.service.oss2.models.PutObjectRequest.class);
    verify(mockOssClient, times(1)).putObject(putObjectRequestCaptor.capture(), any());
    com.aliyun.sdk.service.oss2.models.PutObjectRequest actualRequest =
        putObjectRequestCaptor.getValue();
    assertEquals("object-1", actualRequest.key());
    assertEquals("bucket-1", actualRequest.bucket());
    assertEquals("tag-1=tag-value-1", actualRequest.tagging());
    assertEquals("value-1", actualRequest.metadata().get("key-1"));

    // Verify the mapping of the response into the UploadResponse object
    assertEquals("object-1", uploadResponse.getKey());
    assertEquals("version-1", uploadResponse.getVersionId());
    assertEquals("etag", uploadResponse.getETag());
  }

  @Test
  void testDoDownloadOutputStream() {
    Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    doReturn(buildTestGetObjectResult(now))
        .when(mockOssClient).getObject(
            any(com.aliyun.sdk.service.oss2.models.GetObjectRequest.class), any());
    verifyDownloadTestResults(
        ali.doDownload(getTestDownloadRequest(), mock(OutputStream.class)), now);
  }

  @Test
  void testDoDownloadInputStream() {
    Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    doReturn(buildTestGetObjectResult(now))
        .when(mockOssClient).getObject(
            any(com.aliyun.sdk.service.oss2.models.GetObjectRequest.class), any());
    verifyDownloadTestResults(ali.doDownload(getTestDownloadRequest()), now);
  }

  @Test
  void testDoDownloadByteArrayWrapper() {
    Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    doReturn(buildTestGetObjectResult(now))
        .when(mockOssClient).getObject(
            any(com.aliyun.sdk.service.oss2.models.GetObjectRequest.class), any());
    ByteArray byteArray = new ByteArray();
    verifyDownloadTestResults(ali.doDownload(getTestDownloadRequest(), byteArray), now);
    assertEquals("downloadedData", new String(byteArray.getBytes()));
  }

  @Test
  void testDoDownloadFile() {
    Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    doReturn(buildTestGetObjectResult(now))
        .when(mockOssClient).getObject(
            any(com.aliyun.sdk.service.oss2.models.GetObjectRequest.class), any());
    Path path = Path.of("tempFile.txt");
    try {
      Files.deleteIfExists(path);
      verifyDownloadTestResults(ali.doDownload(getTestDownloadRequest(), path.toFile()), now);
    } catch (IOException e) {
      Assertions.fail(e);
    } finally {
      try {
        Files.deleteIfExists(path);
      } catch (IOException e) {
        Assertions.fail(e);
      }
    }
  }

  @Test
  void testDoDownloadPath() {
    Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    doReturn(buildTestGetObjectResult(now))
        .when(mockOssClient).getObject(
            any(com.aliyun.sdk.service.oss2.models.GetObjectRequest.class), any());
    Path path = Path.of("tempPath.txt");
    try {
      Files.deleteIfExists(path);
      verifyDownloadTestResults(ali.doDownload(getTestDownloadRequest(), path), now);
    } catch (IOException e) {
      Assertions.fail(e);
    } finally {
      try {
        Files.deleteIfExists(path);
      } catch (IOException e) {
        Assertions.fail(e);
      }
    }
  }

  void verifyDownloadTestResults(DownloadResponse response, Instant now) {

    // Verify the parameters passed into the OSS SDK
    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.GetObjectRequest> getObjectRequestCaptor =
        ArgumentCaptor.forClass(com.aliyun.sdk.service.oss2.models.GetObjectRequest.class);
    verify(mockOssClient, times(1)).getObject(getObjectRequestCaptor.capture(), any());
    com.aliyun.sdk.service.oss2.models.GetObjectRequest actualGetObjectRequest =
        getObjectRequestCaptor.getValue();
    assertEquals("object-1", actualGetObjectRequest.key());
    assertEquals("bucket-1", actualGetObjectRequest.bucket());
    assertEquals("version-1", actualGetObjectRequest.versionId());
    assertEquals("bytes=10-110", actualGetObjectRequest.range());

    // Verify the response data is properly mapped into the DownloadResponse object
    assertEquals("object-1", response.getKey());
    assertEquals("object-1", response.getMetadata().getKey());
    assertEquals("version-1", response.getMetadata().getVersionId());
    assertEquals("etag1", response.getMetadata().getETag());
    assertEquals(Date.from(now), Date.from(response.getMetadata().getLastModified()));
    assertEquals(Map.of("key1", "value1", "key2", "value2"), response.getMetadata().getMetadata());
    assertEquals(100, response.getMetadata().getObjectSize());
  }

  @Test
  void testDoDelete() {
    ali.doDelete("object-1", "version-1");

    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.DeleteObjectRequest> captor =
        ArgumentCaptor.forClass(com.aliyun.sdk.service.oss2.models.DeleteObjectRequest.class);
    verify(mockOssClient, times(1)).deleteObject(captor.capture(),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class));
    com.aliyun.sdk.service.oss2.models.DeleteObjectRequest actual = captor.getValue();
    assertEquals("bucket-1", actual.bucket());
    assertEquals("object-1", actual.key());
    assertEquals("version-1", actual.versionId());

    ali.doDelete("object-1", null);
    verify(mockOssClient, times(2)).deleteObject(captor.capture(),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class));
    actual = captor.getValue();
    assertEquals("bucket-1", actual.bucket());
    assertEquals("object-1", actual.key());
    assertNull(actual.versionId());
  }

  @Test
  void testDoBulkDelete() {
    List<BlobIdentifier> objects =
        List.of(
            new BlobIdentifier("object-1", "version-1"),
            new BlobIdentifier("object-2", null),
            new BlobIdentifier("object-3", "version-3"),
            new BlobIdentifier("object-4", null));
    ali.doDelete(objects);

    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.DeleteMultipleObjectsRequest> captor =
        ArgumentCaptor.forClass(
            com.aliyun.sdk.service.oss2.models.DeleteMultipleObjectsRequest.class);
    verify(mockOssClient, times(1)).deleteMultipleObjects(captor.capture(),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class));
    com.aliyun.sdk.service.oss2.models.DeleteMultipleObjectsRequest actual = captor.getValue();
    assertEquals("bucket-1", actual.bucket());
    List<com.aliyun.sdk.service.oss2.models.ObjectIdentifier> ids = actual.delete().objects();
    assertEquals(4, ids.size());
    assertEquals("object-1", ids.get(0).key());
    assertEquals("version-1", ids.get(0).versionId());
    assertEquals("object-2", ids.get(1).key());
    assertNull(ids.get(1).versionId());
    assertEquals("object-3", ids.get(2).key());
    assertEquals("version-3", ids.get(2).versionId());
    assertEquals("object-4", ids.get(3).key());
    assertNull(ids.get(3).versionId());

    // Test edge cases
    ali.doDelete(List.of(new BlobIdentifier("object-1", "version-1")));
    ali.doDelete(List.of(new BlobIdentifier("object-1", null)));

    // Empty list should not call deleteMultipleObjects
    ali.doDelete(List.of());
    verify(mockOssClient, times(3)).deleteMultipleObjects(
        any(com.aliyun.sdk.service.oss2.models.DeleteMultipleObjectsRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class));
  }

  @Test
  void testDoCopy() {
    Instant now = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
    String lastModifiedRfc =
        java.time.ZonedDateTime.ofInstant(now, java.time.ZoneOffset.UTC)
            .format(java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME);

    com.aliyun.sdk.service.oss2.models.CopyObjectResult mockCopyResult =
        mock(com.aliyun.sdk.service.oss2.models.CopyObjectResult.class);
    when(mockCopyResult.versionId()).thenReturn("copyVersion-1");
    when(mockCopyResult.eTag()).thenReturn("\"eTag-1\"");
    when(mockCopyResult.lastModified()).thenReturn(null);
    when(mockOssClient.copyObject(
        any(com.aliyun.sdk.service.oss2.models.CopyObjectRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class)))
        .thenReturn(mockCopyResult);

    com.aliyun.sdk.service.oss2.models.HeadObjectResult mockHeadResult =
        mock(com.aliyun.sdk.service.oss2.models.HeadObjectResult.class);
    when(mockHeadResult.lastModified()).thenReturn(lastModifiedRfc);
    when(mockOssClient.headObject(
        any(com.aliyun.sdk.service.oss2.models.HeadObjectRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class)))
        .thenReturn(mockHeadResult);

    CopyRequest copyRequest =
        CopyRequest.builder()
            .srcKey("src-object-1")
            .srcVersionId("version-1")
            .destBucket("dest-bucket-1")
            .destKey("dest-object-1")
            .build();

    CopyResponse copyResponse = ali.doCopy(copyRequest);

    assertEquals("dest-object-1", copyResponse.getKey());
    assertEquals("copyVersion-1", copyResponse.getVersionId());
    assertEquals("eTag-1", copyResponse.getETag());
    assertEquals(now, copyResponse.getLastModified());

    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.CopyObjectRequest> captor =
        ArgumentCaptor.forClass(com.aliyun.sdk.service.oss2.models.CopyObjectRequest.class);
    verify(mockOssClient, times(1)).copyObject(captor.capture(), any());
    com.aliyun.sdk.service.oss2.models.CopyObjectRequest actual = captor.getValue();
    assertEquals("bucket-1", actual.sourceBucket());
    assertEquals("src-object-1", actual.sourceKey());
    assertEquals("version-1", actual.sourceVersionId());
    assertEquals("dest-bucket-1", actual.bucket());
    assertEquals("dest-object-1", actual.key());
  }

  @Test
  void testDoCopyFrom() {
    Instant now = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
    String lastModifiedRfc =
        java.time.ZonedDateTime.ofInstant(now, java.time.ZoneOffset.UTC)
            .format(java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME);

    com.aliyun.sdk.service.oss2.models.CopyObjectResult mockCopyResult =
        mock(com.aliyun.sdk.service.oss2.models.CopyObjectResult.class);
    when(mockCopyResult.versionId()).thenReturn("copyVersion-1");
    when(mockCopyResult.eTag()).thenReturn("\"eTag-1\"");
    when(mockCopyResult.lastModified()).thenReturn(null);
    when(mockOssClient.copyObject(
        any(com.aliyun.sdk.service.oss2.models.CopyObjectRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class)))
        .thenReturn(mockCopyResult);

    com.aliyun.sdk.service.oss2.models.HeadObjectResult mockHeadResult =
        mock(com.aliyun.sdk.service.oss2.models.HeadObjectResult.class);
    when(mockHeadResult.lastModified()).thenReturn(lastModifiedRfc);
    when(mockOssClient.headObject(
        any(com.aliyun.sdk.service.oss2.models.HeadObjectRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class)))
        .thenReturn(mockHeadResult);

    CopyFromRequest copyFromRequest =
        CopyFromRequest.builder()
            .srcBucket("src-bucket-1")
            .srcKey("src-object-1")
            .srcVersionId("version-1")
            .destKey("dest-object-1")
            .build();

    CopyResponse copyResponse = ali.doCopyFrom(copyFromRequest);

    assertEquals("dest-object-1", copyResponse.getKey());
    assertEquals("copyVersion-1", copyResponse.getVersionId());
    assertEquals("eTag-1", copyResponse.getETag());
    assertEquals(now, copyResponse.getLastModified());

    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.CopyObjectRequest> captor =
        ArgumentCaptor.forClass(com.aliyun.sdk.service.oss2.models.CopyObjectRequest.class);
    verify(mockOssClient, times(1)).copyObject(captor.capture(), any());
    com.aliyun.sdk.service.oss2.models.CopyObjectRequest actual = captor.getValue();
    assertEquals("src-bucket-1", actual.sourceBucket());
    assertEquals("src-object-1", actual.sourceKey());
    assertEquals("version-1", actual.sourceVersionId());
    assertEquals("bucket-1", actual.bucket());
    assertEquals("dest-object-1", actual.key());
  }

  @Test
  void testDoGetMetadata() {
    Map<String, String> metadataMap = Map.of("key1", "value1", "key2", "value2");
    HeadObjectResult mockResult = mock(HeadObjectResult.class);
    when(mockResult.versionId()).thenReturn("v1");
    when(mockResult.eTag()).thenReturn("etag");
    when(mockResult.contentLength()).thenReturn(1024L);
    when(mockResult.metadata()).thenReturn(metadataMap);
    when(mockResult.lastModified()).thenReturn("Sun, 18 May 2025 12:00:00 GMT");
    when(mockResult.contentType()).thenReturn("application/octet-stream");
    when(mockOssClient.headObject(any(HeadObjectRequest.class), any())).thenReturn(mockResult);

    BlobMetadata metadata = ali.doGetMetadata("object-1", "v1");

    ArgumentCaptor<HeadObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(HeadObjectRequest.class);
    verify(mockOssClient, times(1)).headObject(requestCaptor.capture(), any());

    HeadObjectRequest capturedRequest = requestCaptor.getValue();
    assertEquals("bucket-1", capturedRequest.bucket());
    assertEquals("object-1", capturedRequest.key());
    assertEquals("v1", capturedRequest.versionId());

    assertEquals("object-1", metadata.getKey());
    assertEquals("v1", metadata.getVersionId());
    assertEquals("etag", metadata.getETag());
    assertEquals(1024L, metadata.getObjectSize());
    assertEquals(metadataMap, metadata.getMetadata());
    assertNotNull(metadata.getLastModified());
    assertEquals("application/octet-stream", metadata.getContentType());
  }

  @Test
  void testDoListEmpty() {
    ListBlobsRequest request = new ListBlobsRequest.Builder().build();
    com.aliyun.sdk.service.oss2.models.ListObjectsV2Result mockResult =
        mock(com.aliyun.sdk.service.oss2.models.ListObjectsV2Result.class);
    when(mockOssClient.listObjectsV2(
        any(com.aliyun.sdk.service.oss2.models.ListObjectsV2Request.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class))).thenReturn(mockResult);
    when(mockResult.contents()).thenReturn(List.of());
    when(mockResult.nextContinuationToken()).thenReturn(null);

    Iterator<BlobInfo> iterator = ali.doList(request);
    assertThrows(
        NoSuchElementException.class,
        () -> {
          iterator.next();
        });
  }

  @Test
  void testDoList() {
    ListBlobsRequest request =
        new ListBlobsRequest.Builder().withPrefix("abc").withDelimiter("/").build();
    com.aliyun.sdk.service.oss2.models.ListObjectsV2Result mockResult =
        mock(com.aliyun.sdk.service.oss2.models.ListObjectsV2Result.class);
    when(mockOssClient.listObjectsV2(
        any(com.aliyun.sdk.service.oss2.models.ListObjectsV2Request.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class))).thenReturn(mockResult);
    List<com.aliyun.sdk.service.oss2.models.ObjectSummary> list = getObjectSummaryList();
    when(mockResult.contents()).thenReturn(list);
    when(mockResult.nextContinuationToken()).thenReturn(null);

    Iterator<BlobInfo> iterator = ali.doList(request);
    assertNotNull(iterator);

    int count = 1;
    while (iterator.hasNext()) {
      BlobInfo blobInfo = iterator.next();
      int current = count++;
      assertEquals("key-" + current, blobInfo.getKey());
      assertEquals(current, blobInfo.getObjectSize());
      assertEquals(
          BASE_LAST_MODIFIED.plusSeconds(current), blobInfo.getLastModified(),
          "doList should propagate the OSS ObjectSummary lastModified timestamp");
    }
  }

  // Base instant for deterministic lastModified values in the object-summary fixtures;
  // each summary i gets BASE_LAST_MODIFIED + i seconds so tests can assert exact timestamps.
  private static final java.time.Instant BASE_LAST_MODIFIED =
      java.time.Instant.parse("2026-01-01T00:00:00Z");

  private List<com.aliyun.sdk.service.oss2.models.ObjectSummary> getObjectSummaryList() {
    List<com.aliyun.sdk.service.oss2.models.ObjectSummary> list = new ArrayList<>();
    IntStream.range(1, 100)
        .forEach(
            (i) -> {
              com.aliyun.sdk.service.oss2.models.ObjectSummary summary =
                  com.aliyun.sdk.service.oss2.models.ObjectSummary.newBuilder()
                      .key("key-" + i)
                      .size((long) i)
                      .lastModified(BASE_LAST_MODIFIED.plusSeconds(i))
                      .build();
              list.add(summary);
            });
    return list;
  }

  @Test
  void testDoListPage() {
    ListBlobsPageRequest request =
        ListBlobsPageRequest.builder()
            .withPrefix("abc")
            .withDelimiter("/")
            .withPaginationToken("next-token")
            .withMaxResults(50)
            .build();

    com.aliyun.sdk.service.oss2.models.ListObjectsV2Result mockResult =
        mock(com.aliyun.sdk.service.oss2.models.ListObjectsV2Result.class);
    when(mockOssClient.listObjectsV2(
        any(com.aliyun.sdk.service.oss2.models.ListObjectsV2Request.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class))).thenReturn(mockResult);
    List<com.aliyun.sdk.service.oss2.models.ObjectSummary> list = getObjectSummaryList();
    when(mockResult.contents()).thenReturn(list);
    when(mockResult.commonPrefixes()).thenReturn(List.of());
    when(mockResult.isTruncated()).thenReturn(true);
    when(mockResult.nextContinuationToken()).thenReturn("next-page-token");

    ListBlobsPageResponse response = ali.listPage(request);

    // Verify the request is mapped to the SDK
    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.ListObjectsV2Request> requestCaptor =
        ArgumentCaptor.forClass(com.aliyun.sdk.service.oss2.models.ListObjectsV2Request.class);
    verify(mockOssClient, times(1)).listObjectsV2(requestCaptor.capture(),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class));
    com.aliyun.sdk.service.oss2.models.ListObjectsV2Request actualRequest =
        requestCaptor.getValue();
    assertEquals("bucket-1", actualRequest.bucket());
    assertEquals("abc", actualRequest.prefix());
    assertEquals("/", actualRequest.delimiter());
    assertEquals("next-token", actualRequest.continuationToken());
    assertEquals(50L, actualRequest.maxKeys());

    // Verify the response is mapped back properly
    assertNotNull(response);
    assertEquals(99, response.getBlobs().size());
    assertEquals(List.of(), response.getCommonPrefixes());
    assertEquals(true, response.isTruncated());
    assertEquals("next-page-token", response.getNextPageToken());

    // Verify first and last blob
    assertEquals("key-1", response.getBlobs().get(0).getKey());
    assertEquals(1, response.getBlobs().get(0).getObjectSize());
    assertEquals(BASE_LAST_MODIFIED.plusSeconds(1), response.getBlobs().get(0).getLastModified());
    assertEquals("key-99", response.getBlobs().get(98).getKey());
    assertEquals(99, response.getBlobs().get(98).getObjectSize());
    assertEquals(BASE_LAST_MODIFIED.plusSeconds(99), response.getBlobs().get(98).getLastModified());
  }

  @Test
  void testDoListPageEmpty() {
    ListBlobsPageRequest request = ListBlobsPageRequest.builder().build();
    com.aliyun.sdk.service.oss2.models.ListObjectsV2Result mockResult =
        mock(com.aliyun.sdk.service.oss2.models.ListObjectsV2Result.class);
    when(mockOssClient.listObjectsV2(
        any(com.aliyun.sdk.service.oss2.models.ListObjectsV2Request.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class))).thenReturn(mockResult);
    when(mockResult.contents()).thenReturn(List.of());
    when(mockResult.commonPrefixes()).thenReturn(List.of());
    when(mockResult.isTruncated()).thenReturn(false);
    when(mockResult.nextContinuationToken()).thenReturn(null);

    ListBlobsPageResponse response = ali.listPage(request);

    assertNotNull(response);
    assertEquals(0, response.getBlobs().size());
    assertEquals(List.of(), response.getCommonPrefixes());
    assertEquals(false, response.isTruncated());
    assertNull(response.getNextPageToken());
  }

  @Test
  void testDoListPage_WithCommonPrefixes() {
    ListBlobsPageRequest request =
        ListBlobsPageRequest.builder().withDelimiter("/").build();

    com.aliyun.sdk.service.oss2.models.ListObjectsV2Result mockResult =
        mock(com.aliyun.sdk.service.oss2.models.ListObjectsV2Result.class);
    when(mockOssClient.listObjectsV2(
        any(com.aliyun.sdk.service.oss2.models.ListObjectsV2Request.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class))).thenReturn(mockResult);
    when(mockResult.contents()).thenReturn(List.of());
    when(mockResult.commonPrefixes()).thenReturn(List.of(
        com.aliyun.sdk.service.oss2.models.CommonPrefix.newBuilder().prefix("dir1/").build(),
        com.aliyun.sdk.service.oss2.models.CommonPrefix.newBuilder().prefix("dir2/").build()));
    when(mockResult.isTruncated()).thenReturn(false);
    when(mockResult.nextContinuationToken()).thenReturn(null);

    ListBlobsPageResponse response = ali.listPage(request);

    assertNotNull(response);
    assertEquals(0, response.getBlobs().size());
    assertEquals(List.of("dir1/", "dir2/"), response.getCommonPrefixes());
    assertFalse(response.isTruncated());
  }

  @Test
  void testDoListPage_WithBothBlobsAndPrefixes() {
    ListBlobsPageRequest request =
        ListBlobsPageRequest.builder().withDelimiter("/").build();

    com.aliyun.sdk.service.oss2.models.ObjectSummary summary =
        com.aliyun.sdk.service.oss2.models.ObjectSummary.newBuilder()
            .key("root.txt")
            .size(100L)
            .build();

    com.aliyun.sdk.service.oss2.models.ListObjectsV2Result mockResult =
        mock(com.aliyun.sdk.service.oss2.models.ListObjectsV2Result.class);
    when(mockOssClient.listObjectsV2(
        any(com.aliyun.sdk.service.oss2.models.ListObjectsV2Request.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class))).thenReturn(mockResult);
    when(mockResult.contents()).thenReturn(List.of(summary));
    when(mockResult.commonPrefixes()).thenReturn(List.of(
        com.aliyun.sdk.service.oss2.models.CommonPrefix.newBuilder().prefix("dir1/").build()));
    when(mockResult.isTruncated()).thenReturn(false);
    when(mockResult.nextContinuationToken()).thenReturn(null);

    ListBlobsPageResponse response = ali.listPage(request);

    assertNotNull(response);
    assertEquals(1, response.getBlobs().size());
    assertEquals("root.txt", response.getBlobs().get(0).getKey());
    assertEquals(List.of("dir1/"), response.getCommonPrefixes());
  }

  @Test
  void testDoListPage_WithOnlyPrefixes() {
    ListBlobsPageRequest request =
        ListBlobsPageRequest.builder().withDelimiter("/").withMaxResults(5).build();

    com.aliyun.sdk.service.oss2.models.ListObjectsV2Result mockResult =
        mock(com.aliyun.sdk.service.oss2.models.ListObjectsV2Result.class);
    when(mockOssClient.listObjectsV2(
        any(com.aliyun.sdk.service.oss2.models.ListObjectsV2Request.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class))).thenReturn(mockResult);
    when(mockResult.contents()).thenReturn(List.of());
    when(mockResult.commonPrefixes()).thenReturn(List.of(
        com.aliyun.sdk.service.oss2.models.CommonPrefix.newBuilder().prefix("a/").build(),
        com.aliyun.sdk.service.oss2.models.CommonPrefix.newBuilder().prefix("b/").build(),
        com.aliyun.sdk.service.oss2.models.CommonPrefix.newBuilder().prefix("c/").build()));
    when(mockResult.isTruncated()).thenReturn(false);
    when(mockResult.nextContinuationToken()).thenReturn(null);

    ListBlobsPageResponse response = ali.listPage(request);

    assertNotNull(response);
    assertEquals(0, response.getBlobs().size());
    assertEquals(List.of("a/", "b/", "c/"), response.getCommonPrefixes());
    assertFalse(response.isTruncated());
  }

  @Test
  void testDoListPage_NullCommonPrefixes() {
    ListBlobsPageRequest request =
        ListBlobsPageRequest.builder().withDelimiter("/").build();

    com.aliyun.sdk.service.oss2.models.ObjectSummary summary =
        com.aliyun.sdk.service.oss2.models.ObjectSummary.newBuilder()
            .key("file.txt")
            .size(50L)
            .build();

    com.aliyun.sdk.service.oss2.models.ListObjectsV2Result mockResult =
        mock(com.aliyun.sdk.service.oss2.models.ListObjectsV2Result.class);
    when(mockOssClient.listObjectsV2(
        any(com.aliyun.sdk.service.oss2.models.ListObjectsV2Request.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class))).thenReturn(mockResult);
    when(mockResult.contents()).thenReturn(List.of(summary));
    when(mockResult.commonPrefixes()).thenReturn(null);
    when(mockResult.isTruncated()).thenReturn(false);
    when(mockResult.nextContinuationToken()).thenReturn(null);

    ListBlobsPageResponse response = ali.listPage(request);

    assertNotNull(response);
    assertEquals(1, response.getBlobs().size());
    assertEquals(List.of(), response.getCommonPrefixes());
  }

  @Test
  void testDoInitiateMultipartUpload() {
    com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadResult mockResult =
        mock(com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadResult.class);
    com.aliyun.sdk.service.oss2.models.InitiateMultipartUpload mockUpload =
        mock(com.aliyun.sdk.service.oss2.models.InitiateMultipartUpload.class);
    when(mockResult.initiateMultipartUpload()).thenReturn(mockUpload);
    when(mockUpload.bucket()).thenReturn("bucket-1");
    when(mockUpload.key()).thenReturn("object-1");
    when(mockUpload.uploadId()).thenReturn("mpu-id");
    when(mockOssClient.initiateMultipartUpload(
        any(com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class))).thenReturn(mockResult);
    Map<String, String> metadata = Map.of("key-1", "value-1");
    MultipartUploadRequest request =
        new MultipartUploadRequest.Builder().withKey("object-1").withMetadata(metadata).build();

    ali.initiateMultipartUpload(request);

    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadRequest> captor =
        ArgumentCaptor.forClass(
            com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadRequest.class);
    verify(mockOssClient, times(1)).initiateMultipartUpload(captor.capture(),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class));
    com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadRequest actualRequest =
        captor.getValue();
    assertEquals("object-1", actualRequest.key());
    assertEquals("bucket-1", actualRequest.bucket());
    assertEquals(metadata, actualRequest.metadata());
  }

  @Test
  void testDoInitiateMultipartUploadWithKms() {
    com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadResult mockResult =
        mock(com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadResult.class);
    com.aliyun.sdk.service.oss2.models.InitiateMultipartUpload mockUpload =
        mock(com.aliyun.sdk.service.oss2.models.InitiateMultipartUpload.class);
    when(mockResult.initiateMultipartUpload()).thenReturn(mockUpload);
    when(mockUpload.bucket()).thenReturn("bucket-1");
    when(mockUpload.key()).thenReturn("object-1");
    when(mockUpload.uploadId()).thenReturn("mpu-id");
    when(mockOssClient.initiateMultipartUpload(
        any(com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class))).thenReturn(mockResult);
    Map<String, String> metadata = Map.of("key-1", "value-1");
    String kmsKeyId = "test-kms-key-id";
    MultipartUploadRequest request =
        new MultipartUploadRequest.Builder()
            .withKey("object-1")
            .withMetadata(metadata)
            .withKmsKeyId(kmsKeyId)
            .build();

    MultipartUpload response = ali.initiateMultipartUpload(request);

    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadRequest> captor =
        ArgumentCaptor.forClass(
            com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadRequest.class);
    verify(mockOssClient, times(1)).initiateMultipartUpload(captor.capture(),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class));
    com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadRequest actualRequest =
        captor.getValue();
    assertEquals("object-1", actualRequest.key());
    assertEquals("bucket-1", actualRequest.bucket());
    assertEquals(metadata, actualRequest.metadata());
    assertEquals("KMS", actualRequest.serverSideEncryption());
    assertEquals(kmsKeyId, actualRequest.serverSideEncryptionKeyId());

    // Verify the response has KMS key
    assertEquals(kmsKeyId, response.getKmsKeyId());
  }

  @Test
  void testDoUploadMultipartPart() {
    com.aliyun.sdk.service.oss2.models.UploadPartResult mockResult =
        mock(com.aliyun.sdk.service.oss2.models.UploadPartResult.class);
    when(mockResult.eTag()).thenReturn("\"etag\"");
    when(mockOssClient.uploadPart(
        any(com.aliyun.sdk.service.oss2.models.UploadPartRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class))).thenReturn(mockResult);
    MultipartUpload multipartUpload =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();
    byte[] content = "This is test data".getBytes(StandardCharsets.UTF_8);
    MultipartPart multipartPart = new MultipartPart(1, content);

    ali.uploadMultipartPart(multipartUpload, multipartPart);

    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.UploadPartRequest> captor =
        ArgumentCaptor.forClass(com.aliyun.sdk.service.oss2.models.UploadPartRequest.class);
    verify(mockOssClient, times(1)).uploadPart(captor.capture(),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class));
    com.aliyun.sdk.service.oss2.models.UploadPartRequest actualRequest = captor.getValue();
    assertEquals("object-1", actualRequest.key());
    assertEquals("bucket-1", actualRequest.bucket());
    assertEquals("mpu-id", actualRequest.uploadId());
    assertEquals(1L, actualRequest.partNumber());
  }

  @Test
  void testDoCompleteMultipartUpload() {
    com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadResult mockResult =
        mock(com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadResult.class);
    com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadResultXml mockXml =
        mock(com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadResultXml.class);
    when(mockResult.completeMultipartUpload()).thenReturn(mockXml);
    when(mockXml.eTag()).thenReturn("\"result-etag\"");
    when(mockOssClient.completeMultipartUpload(
        any(com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class))).thenReturn(mockResult);
    MultipartUpload multipartUpload =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();
    List<com.salesforce.multicloudj.blob.driver.UploadPartResponse> listOfParts =
        List.of(new com.salesforce.multicloudj.blob.driver.UploadPartResponse(1, "etag", 0));

    ali.completeMultipartUpload(multipartUpload, listOfParts);

    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadRequest> captor =
        ArgumentCaptor.forClass(
            com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadRequest.class);
    verify(mockOssClient, times(1)).completeMultipartUpload(captor.capture(),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class));
    com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadRequest actualRequest =
        captor.getValue();
    assertEquals("object-1", actualRequest.key());
    assertEquals("bucket-1", actualRequest.bucket());
    assertEquals("mpu-id", actualRequest.uploadId());
    List<com.aliyun.sdk.service.oss2.models.Part> parts =
        actualRequest.completeMultipartUpload().parts();
    assertEquals(1, parts.size());
    assertEquals(1L, parts.get(0).partNumber());
    assertEquals("etag", parts.get(0).eTag());
  }

  @Test
  void testDoListMultipartUpload() {
    com.aliyun.sdk.service.oss2.models.ListPartsResult mockResult =
        mock(com.aliyun.sdk.service.oss2.models.ListPartsResult.class);
    when(mockResult.parts()).thenReturn(List.of());
    when(mockOssClient.listParts(
        any(com.aliyun.sdk.service.oss2.models.ListPartsRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class))).thenReturn(mockResult);
    MultipartUpload multipartUpload =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();

    ali.listMultipartUpload(multipartUpload);

    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.ListPartsRequest> captor =
        ArgumentCaptor.forClass(com.aliyun.sdk.service.oss2.models.ListPartsRequest.class);
    verify(mockOssClient, times(1)).listParts(captor.capture(),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class));
    com.aliyun.sdk.service.oss2.models.ListPartsRequest actualRequest = captor.getValue();
    assertEquals("object-1", actualRequest.key());
    assertEquals("bucket-1", actualRequest.bucket());
    assertEquals("mpu-id", actualRequest.uploadId());
  }

  @Test
  void testDoAbortMultipartUpload() {
    MultipartUpload multipartUpload =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();

    ali.abortMultipartUpload(multipartUpload);

    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.AbortMultipartUploadRequest> captor =
        ArgumentCaptor.forClass(
            com.aliyun.sdk.service.oss2.models.AbortMultipartUploadRequest.class);
    verify(mockOssClient, times(1)).abortMultipartUpload(captor.capture(),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class));
    com.aliyun.sdk.service.oss2.models.AbortMultipartUploadRequest actualRequest =
        captor.getValue();
    assertEquals("object-1", actualRequest.key());
    assertEquals("bucket-1", actualRequest.bucket());
    assertEquals("mpu-id", actualRequest.uploadId());
  }

  @Test
  void testDoGetTags() {
    com.aliyun.sdk.service.oss2.models.Tag tag1 =
        com.aliyun.sdk.service.oss2.models.Tag.newBuilder().key("key1").value("value1").build();
    com.aliyun.sdk.service.oss2.models.Tag tag2 =
        com.aliyun.sdk.service.oss2.models.Tag.newBuilder().key("key2").value("value2").build();
    com.aliyun.sdk.service.oss2.models.TagSet tagSet =
        com.aliyun.sdk.service.oss2.models.TagSet.newBuilder()
            .tags(List.of(tag1, tag2)).build();
    com.aliyun.sdk.service.oss2.models.Tagging tagging =
        com.aliyun.sdk.service.oss2.models.Tagging.newBuilder().tagSet(tagSet).build();
    com.aliyun.sdk.service.oss2.models.GetObjectTaggingResult mockResult =
        mock(com.aliyun.sdk.service.oss2.models.GetObjectTaggingResult.class);
    when(mockResult.tagging()).thenReturn(tagging);
    when(mockOssClient.getObjectTagging(
        any(com.aliyun.sdk.service.oss2.models.GetObjectTaggingRequest.class), any()))
        .thenReturn(mockResult);

    Map<String, String> tagsResult = ali.getTags("object-1");

    assertEquals(Map.of("key1", "value1", "key2", "value2"), tagsResult);
  }

  @Test
  void testDoSetTags() {
    Map<String, String> tags = Map.of("key1", "value1", "key2", "value2");
    ali.setTags("object-1", tags);

    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.PutObjectTaggingRequest> requestCaptor =
        ArgumentCaptor.forClass(
            com.aliyun.sdk.service.oss2.models.PutObjectTaggingRequest.class);
    verify(mockOssClient, times(1)).putObjectTagging(requestCaptor.capture(), any());

    com.aliyun.sdk.service.oss2.models.PutObjectTaggingRequest actualRequest =
        requestCaptor.getValue();
    assertEquals("bucket-1", actualRequest.bucket());
    assertEquals("object-1", actualRequest.key());
    List<com.aliyun.sdk.service.oss2.models.Tag> actualTags =
        actualRequest.tagging().tagSet().tags();
    Map<String, String> actualTagMap = actualTags.stream()
        .collect(java.util.stream.Collectors.toMap(
            com.aliyun.sdk.service.oss2.models.Tag::key,
            com.aliyun.sdk.service.oss2.models.Tag::value));
    assertEquals(tags, actualTagMap);
  }

  @Test
  void testDoGeneratePresignedUploadUrl() {
    com.aliyun.sdk.service.oss2.models.PresignResult mockResult =
        mock(com.aliyun.sdk.service.oss2.models.PresignResult.class);
    doReturn("https://bucket-1.oss-cn-shanghai.aliyuncs.com/object-1?signed=true")
        .when(mockResult).url();
    doReturn(mockResult).when(mockOssClient).presign(
        any(com.aliyun.sdk.service.oss2.models.PutObjectRequest.class),
        any(com.aliyun.sdk.service.oss2.PresignOptions.class));

    UploadRequest uploadRequest = getTestUploadRequest();
    Duration duration = Duration.ofHours(12);
    PresignedUrlRequest presignedUploadRequest =
        PresignedUrlRequest.builder()
            .type(PresignedOperation.UPLOAD)
            .key(uploadRequest.getKey())
            .metadata(uploadRequest.getMetadata())
            .tags(uploadRequest.getTags())
            .duration(duration)
            .build();

    PresignedUrlResponse result = ali.doPresign(presignedUploadRequest);

    assertNotNull(result);
    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.PutObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(com.aliyun.sdk.service.oss2.models.PutObjectRequest.class);
    verify(mockOssClient, times(1)).presign(requestCaptor.capture(),
        any(com.aliyun.sdk.service.oss2.PresignOptions.class));
    com.aliyun.sdk.service.oss2.models.PutObjectRequest actualRequest = requestCaptor.getValue();
    assertEquals("bucket-1", actualRequest.bucket());
    assertEquals("object-1", actualRequest.key());
    assertEquals("tag-1=tag-value-1", actualRequest.tagging());
    assertEquals("value-1", actualRequest.metadata().get("key-1"));
  }

  @Test
  void testDoGeneratePresignedDownloadUrl() {
    com.aliyun.sdk.service.oss2.models.PresignResult mockResult =
        mock(com.aliyun.sdk.service.oss2.models.PresignResult.class);
    doReturn("https://bucket-1.oss-cn-shanghai.aliyuncs.com/object-1?signed=true")
        .when(mockResult).url();
    doReturn(mockResult).when(mockOssClient).presign(
        any(com.aliyun.sdk.service.oss2.models.GetObjectRequest.class),
        any(com.aliyun.sdk.service.oss2.PresignOptions.class));

    Duration duration = Duration.ofHours(12);
    PresignedUrlRequest presignedDownloadRequest =
        PresignedUrlRequest.builder()
            .type(PresignedOperation.DOWNLOAD)
            .key("object-1")
            .duration(duration)
            .build();

    PresignedUrlResponse result = ali.doPresign(presignedDownloadRequest);

    assertNotNull(result);
    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.GetObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(com.aliyun.sdk.service.oss2.models.GetObjectRequest.class);
    verify(mockOssClient, times(1)).presign(requestCaptor.capture(),
        any(com.aliyun.sdk.service.oss2.PresignOptions.class));
    com.aliyun.sdk.service.oss2.models.GetObjectRequest actualRequest = requestCaptor.getValue();
    assertEquals("bucket-1", actualRequest.bucket());
    assertEquals("object-1", actualRequest.key());
  }

  @Test
  void testDoDoesObjectExist() {
    doReturn(true).when(mockOssClient).doesObjectExist(
        any(com.aliyun.sdk.service.oss2.models.GetObjectMetaRequest.class));

    boolean result = ali.doDoesObjectExist("object-1", "version-1");

    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.GetObjectMetaRequest> requestCaptor =
        ArgumentCaptor.forClass(
            com.aliyun.sdk.service.oss2.models.GetObjectMetaRequest.class);
    verify(mockOssClient, times(1)).doesObjectExist(requestCaptor.capture());
    com.aliyun.sdk.service.oss2.models.GetObjectMetaRequest actualRequest =
        requestCaptor.getValue();
    assertEquals("bucket-1", actualRequest.bucket());
    assertEquals("object-1", actualRequest.key());
    assertEquals("version-1", actualRequest.versionId());
    assertTrue(result);
  }

  @Test
  void testDoDoesBucketExist() {
    doReturn(true).when(mockOssClient).doesBucketExist("bucket-1");

    boolean result = ali.doDoesBucketExist();

    verify(mockOssClient, times(1)).doesBucketExist("bucket-1");
    assertTrue(result);
  }

  @Test
  void testDoDoesBucketExist_BucketDoesNotExist() {
    doReturn(false).when(mockOssClient).doesBucketExist("bucket-1");

    boolean result = ali.doDoesBucketExist();

    verify(mockOssClient, times(1)).doesBucketExist("bucket-1");
    assertFalse(result);
  }

  private UploadRequest getTestUploadRequest() {
    Map<String, String> metadata = Map.of("key-1", "value-1");
    Map<String, String> tags = Map.of("tag-1", "tag-value-1");
    return new UploadRequest.Builder()
        .withKey("object-1")
        .withContentLength(1024)
        .withMetadata(metadata)
        .withTags(tags)
        .build();
  }

  private com.aliyun.sdk.service.oss2.models.PutObjectResult buildTestPutObjectResult() {
    com.aliyun.sdk.service.oss2.models.PutObjectResult result =
        mock(com.aliyun.sdk.service.oss2.models.PutObjectResult.class);
    doReturn("version-1").when(result).versionId();
    doReturn("\"etag\"").when(result).eTag();
    return result;
  }

  private DownloadRequest getTestDownloadRequest() {
    return new DownloadRequest.Builder()
        .withKey("object-1")
        .withVersionId("version-1")
        .withRange(10L, 110L)
        .build();
  }

  private com.aliyun.sdk.service.oss2.models.GetObjectResult buildTestGetObjectResult(
      Instant now) {
    Map<String, String> metadataMap = Map.of("key1", "value1", "key2", "value2");
    InputStream inputStream = new ByteArrayInputStream("downloadedData".getBytes());
    String lastModifiedStr = ZonedDateTime.ofInstant(now, ZoneOffset.UTC)
        .format(DateTimeFormatter.RFC_1123_DATE_TIME);
    com.aliyun.sdk.service.oss2.models.GetObjectResult result =
        mock(com.aliyun.sdk.service.oss2.models.GetObjectResult.class);
    doReturn("version-1").when(result).versionId();
    doReturn("etag1").when(result).eTag();
    doReturn(lastModifiedStr).when(result).lastModified();
    doReturn(metadataMap).when(result).metadata();
    doReturn(100L).when(result).contentLength();
    doReturn(inputStream).when(result).body();
    doReturn("bytes=10-110").when(result).contentRange();
    doReturn("application/octet-stream").when(result).contentType();
    return result;
  }

  @Test
  void testGetObjectLock() {
    String key = "test-key";
    String versionId = "version-1";

    com.aliyun.sdk.service.oss2.models.GetObjectRetentionResult retentionResult =
        com.aliyun.sdk.service.oss2.models.GetObjectRetentionResult.newBuilder()
            .retention(com.aliyun.sdk.service.oss2.models.Retention.newBuilder()
                .mode(com.aliyun.sdk.service.oss2.models.ObjectRetentionModeType.GOVERNANCE)
                .retainUntilDate("2030-01-01T00:00:00Z")
                .build())
            .build();
    com.aliyun.sdk.service.oss2.models.GetObjectLegalHoldResult legalHoldResult =
        com.aliyun.sdk.service.oss2.models.GetObjectLegalHoldResult.newBuilder()
            .legalHold(com.aliyun.sdk.service.oss2.models.LegalHold.newBuilder()
                .status(com.aliyun.sdk.service.oss2.models.ObjectLegalHoldStatusType.ON)
                .build())
            .build();

    when(mockOssClient.getObjectRetention(any(), any())).thenReturn(retentionResult);
    when(mockOssClient.getObjectLegalHold(any(), any())).thenReturn(legalHoldResult);

    com.salesforce.multicloudj.blob.driver.ObjectLockInfo info =
        ali.getObjectLock(key, versionId);

    assertEquals(
        com.salesforce.multicloudj.blob.driver.RetentionMode.GOVERNANCE, info.getMode());
    assertEquals(Instant.parse("2030-01-01T00:00:00Z"), info.getRetainUntilDate());
    assertTrue(info.isLegalHold());
  }

  @Test
  void testGetObjectLockWithRetentionButNoLegalHold() {
    String key = "test-key";
    String versionId = "version-1";

    com.aliyun.sdk.service.oss2.models.GetObjectRetentionResult retentionResult =
        com.aliyun.sdk.service.oss2.models.GetObjectRetentionResult.newBuilder()
            .retention(com.aliyun.sdk.service.oss2.models.Retention.newBuilder()
                .mode(com.aliyun.sdk.service.oss2.models.ObjectRetentionModeType.GOVERNANCE)
                .retainUntilDate("2030-01-01T00:00:00Z")
                .build())
            .build();
    when(mockOssClient.getObjectRetention(any(), any())).thenReturn(retentionResult);

    // OSS returns 404 NoSuchObjectLegalHoldConfiguration when an object has retention
    // but no legal hold set. getObjectLock must treat this as "no legal hold", not fail.
    ServiceException serviceException = mock(ServiceException.class);
    when(serviceException.statusCode()).thenReturn(404);
    when(serviceException.errorCode()).thenReturn("NoSuchObjectLegalHoldConfiguration");
    OperationException operationException =
        new OperationException("GetObjectLegalHold", serviceException);
    when(mockOssClient.getObjectLegalHold(any(), any())).thenThrow(operationException);

    com.salesforce.multicloudj.blob.driver.ObjectLockInfo info =
        ali.getObjectLock(key, versionId);

    assertEquals(
        com.salesforce.multicloudj.blob.driver.RetentionMode.GOVERNANCE, info.getMode());
    assertEquals(Instant.parse("2030-01-01T00:00:00Z"), info.getRetainUntilDate());
    assertFalse(info.isLegalHold());
  }

  @Test
  void testUpdateLegalHold() {
    String key = "test-key";
    String versionId = "version-1";

    when(mockOssClient.putObjectLegalHold(any(), any()))
        .thenReturn(
            com.aliyun.sdk.service.oss2.models.PutObjectLegalHoldResult.newBuilder().build());

    ali.updateLegalHold(key, versionId, true);

    verify(mockOssClient).putObjectLegalHold(any(), any());
  }

  @Test
  void testDoUpdateObjectRetention() {
    String key = "test-key";
    String versionId = "version-1";

    com.aliyun.sdk.service.oss2.models.GetObjectRetentionResult currentResult =
        com.aliyun.sdk.service.oss2.models.GetObjectRetentionResult.newBuilder()
            .retention(com.aliyun.sdk.service.oss2.models.Retention.newBuilder()
                .mode(com.aliyun.sdk.service.oss2.models.ObjectRetentionModeType.GOVERNANCE)
                .retainUntilDate("2030-01-01T00:00:00Z")
                .build())
            .build();

    when(mockOssClient.getObjectRetention(any(), any())).thenReturn(currentResult);
    when(mockOssClient.putObjectRetention(any(), any()))
        .thenReturn(
            com.aliyun.sdk.service.oss2.models.PutObjectRetentionResult.newBuilder().build());

    com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig config =
        com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig.builder()
            .mode(com.salesforce.multicloudj.blob.driver.RetentionMode.GOVERNANCE)
            .retainUntilDate(Instant.parse("2031-01-01T00:00:00Z"))
            .bypassGovernanceRetention(false)
            .build();

    ali.updateObjectRetention(key, versionId, config);

    verify(mockOssClient).putObjectRetention(any(), any());
  }

  @Test
  void testDoUpdateObjectRetention_governanceToComplianceUpgrade_throwsUnsupported() {
    // OSS cannot upgrade an object's retention mode GOVERNANCE -> COMPLIANCE. Even with
    // bypass=true (which the shared ObjectRetentionRules allows for AWS/GCP), the Ali driver
    // must fail fast with a typed UnSupportedOperationException instead of issuing the
    // PutObjectRetention call and leaking OSS's 409 FileImmutable.
    String key = "test-key";
    String versionId = "version-1";

    com.aliyun.sdk.service.oss2.models.GetObjectRetentionResult currentResult =
        com.aliyun.sdk.service.oss2.models.GetObjectRetentionResult.newBuilder()
            .retention(com.aliyun.sdk.service.oss2.models.Retention.newBuilder()
                .mode(com.aliyun.sdk.service.oss2.models.ObjectRetentionModeType.GOVERNANCE)
                .retainUntilDate("2030-01-01T00:00:00Z")
                .build())
            .build();
    when(mockOssClient.getObjectRetention(any(), any())).thenReturn(currentResult);

    com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig config =
        com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig.builder()
            .mode(com.salesforce.multicloudj.blob.driver.RetentionMode.COMPLIANCE)
            .retainUntilDate(Instant.parse("2031-01-01T00:00:00Z"))
            .bypassGovernanceRetention(true)
            .build();

    assertThrows(UnSupportedOperationException.class,
        () -> ali.updateObjectRetention(key, versionId, config));

    // The upgrade is rejected before any PutObjectRetention call is made.
    verify(mockOssClient, never()).putObjectRetention(any(), any());
  }

  @Test
  void testDoUpdateObjectRetention_governanceSameModeExtend_isUnaffectedByUpgradeGuard() {
    // Regression safeguard: the GOVERNANCE -> COMPLIANCE upgrade guard must NOT interfere with a
    // same-mode GOVERNANCE update (extending the retain-until date). This path should proceed
    // normally and issue the PutObjectRetention call.
    String key = "test-key";
    String versionId = "version-1";

    com.aliyun.sdk.service.oss2.models.GetObjectRetentionResult currentResult =
        com.aliyun.sdk.service.oss2.models.GetObjectRetentionResult.newBuilder()
            .retention(com.aliyun.sdk.service.oss2.models.Retention.newBuilder()
                .mode(com.aliyun.sdk.service.oss2.models.ObjectRetentionModeType.GOVERNANCE)
                .retainUntilDate("2030-01-01T00:00:00Z")
                .build())
            .build();
    when(mockOssClient.getObjectRetention(any(), any())).thenReturn(currentResult);
    when(mockOssClient.putObjectRetention(any(), any()))
        .thenReturn(
            com.aliyun.sdk.service.oss2.models.PutObjectRetentionResult.newBuilder().build());

    com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig config =
        com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig.builder()
            .mode(com.salesforce.multicloudj.blob.driver.RetentionMode.GOVERNANCE)
            .retainUntilDate(Instant.parse("2031-01-01T00:00:00Z"))
            .build();

    ali.updateObjectRetention(key, versionId, config);

    // Guard does not fire for same-mode updates; the retention update is issued to OSS.
    verify(mockOssClient).putObjectRetention(any(), any());
  }

  @Test
  void testDoUpdateObjectRetention_complianceToGovernanceDowngrade_isUnaffectedByUpgradeGuard() {
    // Regression safeguard: a COMPLIANCE -> GOVERNANCE downgrade is rejected by the shared
    // ObjectRetentionRules (FailedPreconditionException), NOT by the OSS upgrade guard, and must
    // never reach OSS. Documents that the upgrade guard is scoped to the opposite direction only.
    String key = "test-key";
    String versionId = "version-1";

    com.aliyun.sdk.service.oss2.models.GetObjectRetentionResult currentResult =
        com.aliyun.sdk.service.oss2.models.GetObjectRetentionResult.newBuilder()
            .retention(com.aliyun.sdk.service.oss2.models.Retention.newBuilder()
                .mode(com.aliyun.sdk.service.oss2.models.ObjectRetentionModeType.COMPLIANCE)
                .retainUntilDate("2030-01-01T00:00:00Z")
                .build())
            .build();
    when(mockOssClient.getObjectRetention(any(), any())).thenReturn(currentResult);

    com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig config =
        com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig.builder()
            .mode(com.salesforce.multicloudj.blob.driver.RetentionMode.GOVERNANCE)
            .retainUntilDate(Instant.parse("2031-01-01T00:00:00Z"))
            .bypassGovernanceRetention(true)
            .build();

    assertThrows(
        com.salesforce.multicloudj.common.exceptions.FailedPreconditionException.class,
        () -> ali.updateObjectRetention(key, versionId, config));

    // Rejected by the shared rules before reaching OSS — not via the upgrade guard.
    verify(mockOssClient, never()).putObjectRetention(any(), any());
  }

  @Test
  void testGetObjectLock_nonexistentKey_throws() {
    String key = "no-such-key";
    String versionId = null;

    // OSS returns 404 with error code "NoSuchKey" when the object does not exist.
    // This must NOT be swallowed as "no configuration" — it should propagate as an error.
    ServiceException serviceException = mock(ServiceException.class);
    when(serviceException.statusCode()).thenReturn(404);
    when(serviceException.errorCode()).thenReturn("NoSuchKey");
    OperationException operationException =
        new OperationException("GetObjectRetention", serviceException);
    when(mockOssClient.getObjectRetention(any(), any())).thenThrow(operationException);

    assertThrows(OperationException.class, () -> ali.getObjectLock(key, versionId));
  }

  @Test
  void testGetObjectLock_noRetentionNoLegalHold_returnsDefaults() {
    String key = "test-key";
    String versionId = "version-1";

    // Both retention and legal hold return 404 NoSuchConfiguration — object exists but
    // has no lock configuration. Should return an ObjectLockInfo with null mode and false hold.
    ServiceException retentionException = mock(ServiceException.class);
    when(retentionException.statusCode()).thenReturn(404);
    when(retentionException.errorCode()).thenReturn("NoSuchObjectRetentionConfiguration");
    OperationException retentionOpException =
        new OperationException("GetObjectRetention", retentionException);
    when(mockOssClient.getObjectRetention(any(), any())).thenThrow(retentionOpException);

    ServiceException legalHoldException = mock(ServiceException.class);
    when(legalHoldException.statusCode()).thenReturn(404);
    when(legalHoldException.errorCode()).thenReturn("NoSuchObjectLegalHoldConfiguration");
    OperationException legalHoldOpException =
        new OperationException("GetObjectLegalHold", legalHoldException);
    when(mockOssClient.getObjectLegalHold(any(), any())).thenThrow(legalHoldOpException);

    com.salesforce.multicloudj.blob.driver.ObjectLockInfo info =
        ali.getObjectLock(key, versionId);

    assertNull(info.getMode());
    assertNull(info.getRetainUntilDate());
    assertFalse(info.isLegalHold());
  }

  @Test
  void testDoUpdateObjectRetention_noCurrentRetention_throwsFailedPrecondition() {
    String key = "test-key";
    String versionId = "version-1";

    // OSS returns 404 NoSuchObjectRetentionConfiguration for an object with no retention.
    // doUpdateObjectRetention catches this and passes null currentMode to ObjectRetentionRules,
    // which throws FailedPreconditionException.
    ServiceException serviceException = mock(ServiceException.class);
    when(serviceException.statusCode()).thenReturn(404);
    when(serviceException.errorCode()).thenReturn("NoSuchObjectRetentionConfiguration");
    OperationException operationException =
        new OperationException("GetObjectRetention", serviceException);
    when(mockOssClient.getObjectRetention(any(), any())).thenThrow(operationException);

    com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig config =
        com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig.builder()
            .mode(com.salesforce.multicloudj.blob.driver.RetentionMode.GOVERNANCE)
            .retainUntilDate(Instant.parse("2031-01-01T00:00:00Z"))
            .bypassGovernanceRetention(false)
            .build();

    assertThrows(
        com.salesforce.multicloudj.common.exceptions.FailedPreconditionException.class,
        () -> ali.updateObjectRetention(key, versionId, config));
  }

  @Test
  void testDoUpdateObjectRetention_complianceMode_shortenDate_throwsFailedPrecondition() {
    String key = "test-key";
    String versionId = "version-1";

    // Object currently has COMPLIANCE mode with retain-until 2035. Attempting to shorten
    // the date should throw FailedPreconditionException regardless of bypass flag.
    com.aliyun.sdk.service.oss2.models.GetObjectRetentionResult currentResult =
        com.aliyun.sdk.service.oss2.models.GetObjectRetentionResult.newBuilder()
            .retention(com.aliyun.sdk.service.oss2.models.Retention.newBuilder()
                .mode(com.aliyun.sdk.service.oss2.models.ObjectRetentionModeType.COMPLIANCE)
                .retainUntilDate("2035-01-01T00:00:00Z")
                .build())
            .build();
    when(mockOssClient.getObjectRetention(any(), any())).thenReturn(currentResult);

    com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig config =
        com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig.builder()
            .mode(com.salesforce.multicloudj.blob.driver.RetentionMode.COMPLIANCE)
            .retainUntilDate(Instant.parse("2030-01-01T00:00:00Z"))
            .bypassGovernanceRetention(true)
            .build();

    assertThrows(
        com.salesforce.multicloudj.common.exceptions.FailedPreconditionException.class,
        () -> ali.updateObjectRetention(key, versionId, config));
  }

  @Test
  void testDoUpdateObjectRetention_governanceMode_shortenWithoutBypass_throwsFailedPrecondition() {
    String key = "test-key";
    String versionId = "version-1";

    // Object currently has GOVERNANCE mode with retain-until 2035. Attempting to shorten
    // without bypass=true should throw FailedPreconditionException.
    com.aliyun.sdk.service.oss2.models.GetObjectRetentionResult currentResult =
        com.aliyun.sdk.service.oss2.models.GetObjectRetentionResult.newBuilder()
            .retention(com.aliyun.sdk.service.oss2.models.Retention.newBuilder()
                .mode(com.aliyun.sdk.service.oss2.models.ObjectRetentionModeType.GOVERNANCE)
                .retainUntilDate("2035-01-01T00:00:00Z")
                .build())
            .build();
    when(mockOssClient.getObjectRetention(any(), any())).thenReturn(currentResult);

    com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig config =
        com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig.builder()
            .mode(com.salesforce.multicloudj.blob.driver.RetentionMode.GOVERNANCE)
            .retainUntilDate(Instant.parse("2030-01-01T00:00:00Z"))
            .bypassGovernanceRetention(false)
            .build();

    assertThrows(
        com.salesforce.multicloudj.common.exceptions.FailedPreconditionException.class,
        () -> ali.updateObjectRetention(key, versionId, config));
  }

  @Test
  void testDoCompleteMultipartUpload_withObjectLock_appliesRetentionAndLegalHold() {
    // Verify that completing a multipart upload with an ObjectLockConfiguration
    // triggers putObjectRetention and putObjectLegalHold calls.
    com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadResult mockResult =
        mock(com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadResult.class);
    com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadResultXml mockXml =
        mock(com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadResultXml.class);
    when(mockResult.completeMultipartUpload()).thenReturn(mockXml);
    when(mockResult.versionId()).thenReturn("ver-123");
    when(mockXml.eTag()).thenReturn("\"result-etag\"");
    when(mockOssClient.completeMultipartUpload(
        any(com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class))).thenReturn(mockResult);
    when(mockOssClient.putObjectRetention(any(), any()))
        .thenReturn(
            com.aliyun.sdk.service.oss2.models.PutObjectRetentionResult.newBuilder().build());
    when(mockOssClient.putObjectLegalHold(any(), any()))
        .thenReturn(
            com.aliyun.sdk.service.oss2.models.PutObjectLegalHoldResult.newBuilder().build());

    com.salesforce.multicloudj.blob.driver.ObjectLockConfiguration lockConfig =
        com.salesforce.multicloudj.blob.driver.ObjectLockConfiguration.builder()
            .mode(com.salesforce.multicloudj.blob.driver.RetentionMode.GOVERNANCE)
            .retainUntilDate(Instant.parse("2100-01-01T00:00:00Z"))
            .legalHold(true)
            .build();

    MultipartUpload multipartUpload = MultipartUpload.builder()
        .bucket("bucket-1").key("object-1").id("mpu-id")
        .objectLock(lockConfig)
        .build();
    List<com.salesforce.multicloudj.blob.driver.UploadPartResponse> listOfParts =
        List.of(new com.salesforce.multicloudj.blob.driver.UploadPartResponse(1, "etag", 0));

    ali.completeMultipartUpload(multipartUpload, listOfParts);

    verify(mockOssClient).completeMultipartUpload(any(), any());
    verify(mockOssClient).putObjectRetention(any(), any());
    verify(mockOssClient).putObjectLegalHold(any(), any());
  }

  @Test
  void testDoCompleteMultipartUpload_withoutObjectLock_doesNotApplyRetention() {
    // Verify that completing a multipart upload WITHOUT ObjectLockConfiguration
    // does NOT call putObjectRetention or putObjectLegalHold.
    com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadResult mockResult =
        mock(com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadResult.class);
    com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadResultXml mockXml =
        mock(com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadResultXml.class);
    when(mockResult.completeMultipartUpload()).thenReturn(mockXml);
    when(mockXml.eTag()).thenReturn("\"result-etag\"");
    when(mockOssClient.completeMultipartUpload(
        any(com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class))).thenReturn(mockResult);

    MultipartUpload multipartUpload = MultipartUpload.builder()
        .bucket("bucket-1").key("object-1").id("mpu-id")
        .build();
    List<com.salesforce.multicloudj.blob.driver.UploadPartResponse> listOfParts =
        List.of(new com.salesforce.multicloudj.blob.driver.UploadPartResponse(1, "etag", 0));

    ali.completeMultipartUpload(multipartUpload, listOfParts);

    verify(mockOssClient).completeMultipartUpload(any(), any());
    verify(mockOssClient, org.mockito.Mockito.never()).putObjectRetention(any(), any());
    verify(mockOssClient, org.mockito.Mockito.never()).putObjectLegalHold(any(), any());
  }

  @Test
  void testListBlobVersions() {
    String key = "my-object";
    ObjectVersion v1 = ObjectVersion.newBuilder()
        .key(key).versionId("v1").eTag("etag1").size(100L)
        .lastModified(Instant.now()).build();
    ObjectVersion v2 = ObjectVersion.newBuilder()
        .key(key).versionId("v2").eTag("etag2").size(200L)
        .lastModified(Instant.now()).build();

    ListObjectVersionsResult result = mock(ListObjectVersionsResult.class);
    when(result.versions()).thenReturn(List.of(v1, v2));

    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(List.of(result).iterator());
    when(mockOssClient.listObjectVersionsPaginator(
        any(ListObjectVersionsRequest.class))).thenReturn(iterable);

    Iterator<BlobMetadata> iter = ali.listBlobVersions(
        ListBlobVersionsRequest.builder()
            .withKey(key).build());

    assertTrue(iter.hasNext());
    BlobMetadata first = iter.next();
    assertEquals(key, first.getKey());
    assertEquals("v1", first.getVersionId());
    assertEquals("etag1", first.getETag());
    assertEquals(100L, first.getObjectSize());

    assertTrue(iter.hasNext());
    BlobMetadata second = iter.next();
    assertEquals("v2", second.getVersionId());
    assertEquals(200L, second.getObjectSize());

    assertFalse(iter.hasNext());
  }

  @Test
  void testListBlobVersionsFiltersPrefixMatches() {
    String key = "obj-1";
    ObjectVersion matching = ObjectVersion.newBuilder()
        .key(key).versionId("v1").eTag("e1").size(10L)
        .lastModified(Instant.now()).build();
    ObjectVersion nonMatching = ObjectVersion.newBuilder()
        .key("obj-1-extra").versionId("v2").eTag("e2").size(20L)
        .lastModified(Instant.now()).build();

    ListObjectVersionsResult result = mock(ListObjectVersionsResult.class);
    when(result.versions()).thenReturn(List.of(matching, nonMatching));

    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(List.of(result).iterator());
    when(mockOssClient.listObjectVersionsPaginator(
        any(ListObjectVersionsRequest.class))).thenReturn(iterable);

    Iterator<BlobMetadata> iter = ali.listBlobVersions(
        ListBlobVersionsRequest.builder()
            .withKey(key).build());

    assertTrue(iter.hasNext());
    BlobMetadata metadata = iter.next();
    assertEquals(key, metadata.getKey());
    assertEquals("v1", metadata.getVersionId());
    assertFalse(iter.hasNext());
  }

  @Test
  void testListBlobVersionsMultiplePages() {
    String key = "paged-obj";
    ObjectVersion v1 = ObjectVersion.newBuilder()
        .key(key).versionId("v1").eTag("e1").size(10L)
        .lastModified(Instant.now()).build();
    ObjectVersion v2 = ObjectVersion.newBuilder()
        .key(key).versionId("v2").eTag("e2").size(20L)
        .lastModified(Instant.now()).build();

    ListObjectVersionsResult page1 = mock(ListObjectVersionsResult.class);
    when(page1.versions()).thenReturn(List.of(v1));
    ListObjectVersionsResult page2 = mock(ListObjectVersionsResult.class);
    when(page2.versions()).thenReturn(List.of(v2));

    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(List.of(page1, page2).iterator());
    when(mockOssClient.listObjectVersionsPaginator(
        any(ListObjectVersionsRequest.class))).thenReturn(iterable);

    Iterator<BlobMetadata> iter = ali.listBlobVersions(
        ListBlobVersionsRequest.builder()
            .withKey(key).build());
    List<BlobMetadata> all = new ArrayList<>();
    iter.forEachRemaining(all::add);

    assertEquals(2, all.size());
    assertEquals("v1", all.get(0).getVersionId());
    assertEquals("v2", all.get(1).getVersionId());
  }

  @Test
  void testListBlobVersionsEmpty() {
    String key = "no-versions";

    ListObjectVersionsResult result = mock(ListObjectVersionsResult.class);
    when(result.versions()).thenReturn(List.of());

    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(List.of(result).iterator());
    when(mockOssClient.listObjectVersionsPaginator(
        any(ListObjectVersionsRequest.class))).thenReturn(iterable);

    Iterator<BlobMetadata> iter = ali.listBlobVersions(
        ListBlobVersionsRequest.builder()
            .withKey(key).build());
    assertFalse(iter.hasNext());
    assertThrows(NoSuchElementException.class, iter::next);
  }

  // ---------- checkArchived (delete-marker / prior-version detection) ----------

  @Test
  void testDoDownload_checkArchived_deletedOnVersionedBucket_throwsWithArchiveInfo() {
    // OSS returns 404 NoSuchKey on a GET of a deleted versioned object, with the
    // x-oss-delete-marker:true header on the ServiceException. Behavior verified live
    // against a real bucket — see AliCheckArchivedSmokeIT (separate IT branch). With
    // checkArchived=true, the driver must call ListObjectVersions, capture the prior
    // ObjectVersion's id, and throw ResourceNotFoundException with ArchiveInfo populated.
    String key = "deleted-key";
    String priorVersionId = "v-prior-1";

    // 1. The OSS GET fails with 404 + the delete-marker header. Use a mocked OperationException
    //    rather than `new OperationException(...)` because that constructor performs a
    //    String.format on the cause, which interacts poorly with the JaCoCo agent on Mockito-spun
    //    ServiceException instances in this build environment.
    ServiceException service = mock(ServiceException.class);
    when(service.statusCode()).thenReturn(404);
    when(service.errorCode()).thenReturn("NoSuchKey");
    Map<String, String> headers = new java.util.HashMap<>();
    headers.put("x-oss-delete-marker", "true");
    when(service.headers()).thenReturn(headers);
    OperationException op = mock(OperationException.class);
    when(op.getCause()).thenReturn(service);
    when(mockOssClient.getObject(any(com.aliyun.sdk.service.oss2.models.GetObjectRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class)))
        .thenThrow(op);

    // 2. ListObjectVersions returns one prior ObjectVersion. Use the prior version's id (NOT
    //    the delete marker's id), since the conformance test re-downloads it for the bytes.
    ObjectVersion priorVersion = mock(ObjectVersion.class);
    when(priorVersion.versionId()).thenReturn(priorVersionId);
    when(priorVersion.key()).thenReturn(key);
    ListObjectVersionsResult listResult = mock(ListObjectVersionsResult.class);
    when(listResult.versions()).thenReturn(List.of(priorVersion));
    when(mockOssClient.listObjectVersions(any(ListObjectVersionsRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class)))
        .thenReturn(listResult);

    DownloadRequest request = DownloadRequest.builder()
        .withKey(key).withCheckArchived(true).build();

    com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException ex = assertThrows(
        com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException.class,
        () -> ali.doDownload(request, new java.io.ByteArrayOutputStream()));

    com.salesforce.multicloudj.common.exceptions.ArchiveInfo info = ex.getArchiveInfo();
    assertNotNull(info, "ArchiveInfo should be populated when a delete marker is detected");
    assertTrue(info.isArchived());
    assertEquals(priorVersionId, info.getVersionId());
  }

  @Test
  void testDoDownload_checkArchivedFalse_doesNotListVersionsOrChangeException() {
    // When checkArchived is OFF, the guard must be a complete no-op: no ListObjectVersions
    // call, and the original OSS exception type must propagate unchanged. This protects the
    // single-file download path from any regression introduced by the guard.
    String key = "deleted-key";

    ServiceException service = mock(ServiceException.class);
    when(service.statusCode()).thenReturn(404);
    when(service.errorCode()).thenReturn("NoSuchKey");
    Map<String, String> headers = new java.util.HashMap<>();
    headers.put("x-oss-delete-marker", "true");
    when(service.headers()).thenReturn(headers);
    OperationException op = mock(OperationException.class);
    when(op.getCause()).thenReturn(service);
    when(mockOssClient.getObject(any(com.aliyun.sdk.service.oss2.models.GetObjectRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class)))
        .thenThrow(op);

    // checkArchived defaults to false on DownloadRequest.
    DownloadRequest request = DownloadRequest.builder().withKey(key).build();

    // The original OSS-side failure surfaces (the existing path wraps it in RuntimeException);
    // the assertion is that the driver does NOT enrich it with ArchiveInfo and does NOT call
    // listObjectVersions.
    assertThrows(RuntimeException.class,
        () -> ali.doDownload(request, new java.io.ByteArrayOutputStream()));
    verify(mockOssClient, never()).listObjectVersions(any(ListObjectVersionsRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class));
  }

  @Test
  void testDoDownload_checkArchived_neverExisted_doesNotPopulateArchiveInfo() {
    // 404 with NO x-oss-delete-marker header -> the key never existed (vs. was deleted on a
    // versioned bucket). The driver must NOT call ListObjectVersions and must NOT throw a
    // ResourceNotFoundException with archived=true. The conformance test
    // testDownload_checkArchived_neverExisted accepts either a null ArchiveInfo or
    // archived=false; we satisfy it by simply propagating the original exception unchanged.
    String key = "never-existed-key";

    ServiceException service = mock(ServiceException.class);
    when(service.statusCode()).thenReturn(404);
    when(service.errorCode()).thenReturn("NoSuchKey");
    when(service.headers()).thenReturn(new java.util.HashMap<>()); // no delete-marker header
    OperationException op = mock(OperationException.class);
    when(op.getCause()).thenReturn(service);
    when(mockOssClient.getObject(any(com.aliyun.sdk.service.oss2.models.GetObjectRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class)))
        .thenThrow(op);

    DownloadRequest request = DownloadRequest.builder()
        .withKey(key).withCheckArchived(true).build();

    // No ResourceNotFoundException with archive info — guard short-circuits and the original
    // 404-driven RuntimeException propagates.
    assertThrows(RuntimeException.class,
        () -> ali.doDownload(request, new java.io.ByteArrayOutputStream()));
    verify(mockOssClient, never()).listObjectVersions(any(ListObjectVersionsRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class));
  }
}
