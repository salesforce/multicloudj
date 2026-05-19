package com.salesforce.multicloudj.blob.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.internal.OSSHeaders;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ListPartsRequest;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PartListing;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.models.HeadObjectRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectResult;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyFromRequest;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.PresignedOperation;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

public class AliBlobStoreTest {

  private MockedStatic<OSSClientBuilder> staticMockBuilder;

  private OSS mockOssClient;
  private OSSClient mockOssV2Client;
  private AliBlobStore ali;

  @BeforeEach
  void setup() {
    mockOssClient = mock(OSS.class);
    mockOssV2Client = mock(OSSClient.class);
    staticMockBuilder = mockStatic(OSSClientBuilder.class);
    OSSClientBuilder.OSSClientBuilderImpl mockBuilder =
        mock(OSSClientBuilder.OSSClientBuilderImpl.class);

    staticMockBuilder.when(OSSClientBuilder::create).thenReturn(mockBuilder);
    when(mockBuilder.region(any())).thenReturn(mockBuilder);
    when(mockBuilder.endpoint(any())).thenReturn(mockBuilder);
    when(mockBuilder.clientConfiguration(any())).thenReturn(mockBuilder);
    when(mockBuilder.credentialsProvider(any())).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockOssClient);

    StsCredentials creds = new StsCredentials("key-1", "secret-1", "token-1");
    CredentialsOverrider credsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(creds)
            .build();
    ali =
        new AliBlobStore.Builder()
            .withBucket("bucket-1")
            .withRegion("cn-shanghai")
            .withEndpoint(URI.create("https://test.example.com"))
            .withProxyEndpoint(URI.create("http://proxy.example.com:80"))
            .withCredentialsOverrider(credsOverrider)
            .withSocketTimeout(Duration.ofMinutes(1))
            .withIdleConnectionTimeout(Duration.ofMinutes(5))
            .withMaxConnections(100)
            .build();
    credsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE).withRole("role").build();
    ali =
        new AliBlobStore.Builder()
            .withV2Client(mockOssV2Client)
            .withBucket("bucket-1")
            .withRegion("cn-shanghai")
            .withCredentialsOverrider(credsOverrider)
            .build();
  }

  @AfterEach
  void teardown() {
    if (staticMockBuilder != null) {
      staticMockBuilder.close();
    }
  }

  @Test
  void testProviderId() {
    assertEquals(AliConstants.PROVIDER_ID, ali.getProviderId());
  }

  @Test
  void testExceptionHandling() {
    OSSException ossException = new OSSException("", "AccessDenied", "", "", "", "", "");
    Class<?> cls = ali.getException(ossException);
    assertEquals(cls, UnAuthorizedException.class);

    ClientException clientException = new ClientException();
    cls = ali.getException(clientException);
    assertEquals(cls, InvalidArgumentException.class);

    cls = ali.getException(new IOException("Channel is closed"));
    assertEquals(cls, UnknownException.class);
  }

  @Test
  void testDoUploadInputStream() {
    doReturn(buildTestV2PutObjectResult())
        .when(mockOssV2Client).putObject(
            any(com.aliyun.sdk.service.oss2.models.PutObjectRequest.class), any());
    verifyUploadTestResults(ali.doUpload(getTestUploadRequest(), mock(InputStream.class)));
  }

  @Test
  void testDoUploadByteArray() {
    doReturn(buildTestV2PutObjectResult())
        .when(mockOssV2Client).putObject(
            any(com.aliyun.sdk.service.oss2.models.PutObjectRequest.class), any());
    verifyUploadTestResults(ali.doUpload(getTestUploadRequest(), new byte[1024]));
  }

  @Test
  void testDoUploadFile() throws IOException {
    doReturn(buildTestV2PutObjectResult())
        .when(mockOssV2Client).putObject(
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
    doReturn(buildTestV2PutObjectResult())
        .when(mockOssV2Client).putObject(
            any(com.aliyun.sdk.service.oss2.models.PutObjectRequest.class), any());
    Path path = Files.createTempFile("tempFile", ".txt");
    try (BufferedWriter writer = Files.newBufferedWriter(path)) {
      writer.write(new char[1024]);
    }
    verifyUploadTestResults(ali.doUpload(getTestUploadRequest(), path));
  }

  void verifyUploadTestResults(UploadResponse uploadResponse) {

    // Verify the parameters passed into the v2 SDK
    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.PutObjectRequest> putObjectRequestCaptor =
        ArgumentCaptor.forClass(com.aliyun.sdk.service.oss2.models.PutObjectRequest.class);
    verify(mockOssV2Client, times(1)).putObject(putObjectRequestCaptor.capture(), any());
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
        .when(mockOssV2Client).getObject(
            any(com.aliyun.sdk.service.oss2.models.GetObjectRequest.class), any());
    verifyDownloadTestResults(
        ali.doDownload(getTestDownloadRequest(), mock(OutputStream.class)), now);
  }

  @Test
  void testDoDownloadInputStream() {
    Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    doReturn(buildTestGetObjectResult(now))
        .when(mockOssV2Client).getObject(
            any(com.aliyun.sdk.service.oss2.models.GetObjectRequest.class), any());
    verifyDownloadTestResults(ali.doDownload(getTestDownloadRequest()), now);
  }

  @Test
  void testDoDownloadByteArrayWrapper() {
    Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    doReturn(buildTestGetObjectResult(now))
        .when(mockOssV2Client).getObject(
            any(com.aliyun.sdk.service.oss2.models.GetObjectRequest.class), any());
    ByteArray byteArray = new ByteArray();
    verifyDownloadTestResults(ali.doDownload(getTestDownloadRequest(), byteArray), now);
    assertEquals("downloadedData", new String(byteArray.getBytes()));
  }

  @Test
  void testDoDownloadFile() {
    Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    doReturn(buildTestGetObjectResult(now))
        .when(mockOssV2Client).getObject(
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
        .when(mockOssV2Client).getObject(
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

    // Verify the parameters passed into the v2 SDK
    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.GetObjectRequest> getObjectRequestCaptor =
        ArgumentCaptor.forClass(com.aliyun.sdk.service.oss2.models.GetObjectRequest.class);
    verify(mockOssV2Client, times(1)).getObject(getObjectRequestCaptor.capture(), any());
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
    verify(mockOssV2Client, times(1)).deleteObject(captor.capture(),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class));
    com.aliyun.sdk.service.oss2.models.DeleteObjectRequest actual = captor.getValue();
    assertEquals("bucket-1", actual.bucket());
    assertEquals("object-1", actual.key());
    assertEquals("version-1", actual.versionId());

    ali.doDelete("object-1", null);
    verify(mockOssV2Client, times(2)).deleteObject(captor.capture(),
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
    verify(mockOssV2Client, times(1)).deleteMultipleObjects(captor.capture(),
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
    verify(mockOssV2Client, times(3)).deleteMultipleObjects(
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
    when(mockOssV2Client.copyObject(
        any(com.aliyun.sdk.service.oss2.models.CopyObjectRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class)))
        .thenReturn(mockCopyResult);

    com.aliyun.sdk.service.oss2.models.HeadObjectResult mockHeadResult =
        mock(com.aliyun.sdk.service.oss2.models.HeadObjectResult.class);
    when(mockHeadResult.lastModified()).thenReturn(lastModifiedRfc);
    when(mockOssV2Client.headObject(
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
    verify(mockOssV2Client, times(1)).copyObject(captor.capture(), any());
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
    when(mockOssV2Client.copyObject(
        any(com.aliyun.sdk.service.oss2.models.CopyObjectRequest.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class)))
        .thenReturn(mockCopyResult);

    com.aliyun.sdk.service.oss2.models.HeadObjectResult mockHeadResult =
        mock(com.aliyun.sdk.service.oss2.models.HeadObjectResult.class);
    when(mockHeadResult.lastModified()).thenReturn(lastModifiedRfc);
    when(mockOssV2Client.headObject(
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
    verify(mockOssV2Client, times(1)).copyObject(captor.capture(), any());
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
    when(mockOssV2Client.headObject(any(HeadObjectRequest.class), any())).thenReturn(mockResult);

    BlobMetadata metadata = ali.doGetMetadata("object-1", "v1");

    ArgumentCaptor<HeadObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(HeadObjectRequest.class);
    verify(mockOssV2Client, times(1)).headObject(requestCaptor.capture(), any());

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
    when(mockOssV2Client.listObjectsV2(
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
    when(mockOssV2Client.listObjectsV2(
        any(com.aliyun.sdk.service.oss2.models.ListObjectsV2Request.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class))).thenReturn(mockResult);
    List<com.aliyun.sdk.service.oss2.models.ObjectSummary> list = getV2List();
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
    }
  }

  private List<com.aliyun.sdk.service.oss2.models.ObjectSummary> getV2List() {
    List<com.aliyun.sdk.service.oss2.models.ObjectSummary> list = new ArrayList<>();
    IntStream.range(1, 100)
        .forEach(
            (i) -> {
              com.aliyun.sdk.service.oss2.models.ObjectSummary summary =
                  com.aliyun.sdk.service.oss2.models.ObjectSummary.newBuilder()
                      .key("key-" + i)
                      .size((long) i)
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
    when(mockOssV2Client.listObjectsV2(
        any(com.aliyun.sdk.service.oss2.models.ListObjectsV2Request.class),
        any(com.aliyun.sdk.service.oss2.OperationOptions.class))).thenReturn(mockResult);
    List<com.aliyun.sdk.service.oss2.models.ObjectSummary> list = getV2List();
    when(mockResult.contents()).thenReturn(list);
    when(mockResult.commonPrefixes()).thenReturn(List.of());
    when(mockResult.isTruncated()).thenReturn(true);
    when(mockResult.nextContinuationToken()).thenReturn("next-page-token");

    ListBlobsPageResponse response = ali.listPage(request);

    // Verify the request is mapped to the SDK
    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.ListObjectsV2Request> requestCaptor =
        ArgumentCaptor.forClass(com.aliyun.sdk.service.oss2.models.ListObjectsV2Request.class);
    verify(mockOssV2Client, times(1)).listObjectsV2(requestCaptor.capture(),
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
    assertEquals("key-99", response.getBlobs().get(98).getKey());
    assertEquals(99, response.getBlobs().get(98).getObjectSize());
  }

  @Test
  void testDoListPageEmpty() {
    ListBlobsPageRequest request = ListBlobsPageRequest.builder().build();
    com.aliyun.sdk.service.oss2.models.ListObjectsV2Result mockResult =
        mock(com.aliyun.sdk.service.oss2.models.ListObjectsV2Result.class);
    when(mockOssV2Client.listObjectsV2(
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
    when(mockOssV2Client.listObjectsV2(
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
    when(mockOssV2Client.listObjectsV2(
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
    when(mockOssV2Client.listObjectsV2(
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
    when(mockOssV2Client.listObjectsV2(
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
    InitiateMultipartUploadResult mockResponse = mock(InitiateMultipartUploadResult.class);
    when(mockOssClient.initiateMultipartUpload((InitiateMultipartUploadRequest) any()))
        .thenReturn(mockResponse);
    Map<String, String> metadata = Map.of("key-1", "value-1");
    MultipartUploadRequest request =
        new MultipartUploadRequest.Builder().withKey("object-1").withMetadata(metadata).build();

    ali.initiateMultipartUpload(request);

    ArgumentCaptor<InitiateMultipartUploadRequest> requestCaptor =
        ArgumentCaptor.forClass(InitiateMultipartUploadRequest.class);
    verify(mockOssClient, times(1)).initiateMultipartUpload(requestCaptor.capture());
    InitiateMultipartUploadRequest actualRequest = requestCaptor.getValue();
    assertEquals("object-1", actualRequest.getKey());
    assertEquals("bucket-1", actualRequest.getBucketName());
    assertEquals(metadata, actualRequest.getObjectMetadata().getUserMetadata());
  }

  @Test
  void testDoInitiateMultipartUploadWithKms() {
    InitiateMultipartUploadResult mockResponse = mock(InitiateMultipartUploadResult.class);
    doReturn("bucket-1").when(mockResponse).getBucketName();
    doReturn("object-1").when(mockResponse).getKey();
    doReturn("mpu-id").when(mockResponse).getUploadId();
    when(mockOssClient.initiateMultipartUpload((InitiateMultipartUploadRequest) any()))
        .thenReturn(mockResponse);
    Map<String, String> metadata = Map.of("key-1", "value-1");
    String kmsKeyId = "test-kms-key-id";
    MultipartUploadRequest request =
        new MultipartUploadRequest.Builder()
            .withKey("object-1")
            .withMetadata(metadata)
            .withKmsKeyId(kmsKeyId)
            .build();

    MultipartUpload response = ali.initiateMultipartUpload(request);

    ArgumentCaptor<InitiateMultipartUploadRequest> requestCaptor =
        ArgumentCaptor.forClass(InitiateMultipartUploadRequest.class);
    verify(mockOssClient, times(1)).initiateMultipartUpload(requestCaptor.capture());
    InitiateMultipartUploadRequest actualRequest = requestCaptor.getValue();
    assertEquals("object-1", actualRequest.getKey());
    assertEquals("bucket-1", actualRequest.getBucketName());
    assertEquals(metadata, actualRequest.getObjectMetadata().getUserMetadata());
    assertEquals(
        ObjectMetadata.KMS_SERVER_SIDE_ENCRYPTION,
        actualRequest.getObjectMetadata().getServerSideEncryption());
    assertEquals(
        kmsKeyId,
        actualRequest
            .getObjectMetadata()
            .getRawMetadata()
            .get(OSSHeaders.OSS_SERVER_SIDE_ENCRYPTION_KEY_ID));

    // Verify the response has KMS key
    assertEquals(kmsKeyId, response.getKmsKeyId());
  }

  @Test
  void testDoUploadMultipartPart() {
    UploadPartResult mockResponse = mock(UploadPartResult.class);
    doReturn(new PartETag(1, "etag")).when(mockResponse).getPartETag();
    when(mockOssClient.uploadPart(any())).thenReturn(mockResponse);
    MultipartUpload multipartUpload =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();
    byte[] content = "This is test data".getBytes(StandardCharsets.UTF_8);
    MultipartPart multipartPart = new MultipartPart(1, content);

    ali.uploadMultipartPart(multipartUpload, multipartPart);

    ArgumentCaptor<UploadPartRequest> requestCaptor =
        ArgumentCaptor.forClass(UploadPartRequest.class);
    verify(mockOssClient, times(1)).uploadPart(requestCaptor.capture());
    UploadPartRequest actualRequest = requestCaptor.getValue();
    assertEquals("object-1", actualRequest.getKey());
    assertEquals("bucket-1", actualRequest.getBucketName());
    assertEquals("mpu-id", actualRequest.getUploadId());
    assertEquals(1, actualRequest.getPartNumber());
  }

  @Test
  void testDoCompleteMultipartUpload() {
    CompleteMultipartUploadResult mockResponse = mock(CompleteMultipartUploadResult.class);
    when(mockOssClient.completeMultipartUpload(any())).thenReturn(mockResponse);
    MultipartUpload multipartUpload =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();
    List<com.salesforce.multicloudj.blob.driver.UploadPartResponse> listOfParts =
        List.of(new com.salesforce.multicloudj.blob.driver.UploadPartResponse(1, "etag", 0));

    ali.completeMultipartUpload(multipartUpload, listOfParts);

    ArgumentCaptor<CompleteMultipartUploadRequest> requestCaptor =
        ArgumentCaptor.forClass(CompleteMultipartUploadRequest.class);
    verify(mockOssClient, times(1)).completeMultipartUpload(requestCaptor.capture());
    CompleteMultipartUploadRequest actualRequest = requestCaptor.getValue();
    assertEquals("object-1", actualRequest.getKey());
    assertEquals("bucket-1", actualRequest.getBucketName());
    assertEquals("mpu-id", actualRequest.getUploadId());
    List<PartETag> parts = actualRequest.getPartETags();
    assertEquals(1, parts.size());
    assertEquals(1, parts.get(0).getPartNumber());
    assertEquals("etag", parts.get(0).getETag());
  }

  @Test
  void testDoListMultipartUpload() {
    PartListing mockResponse = mock(PartListing.class);
    when(mockOssClient.listParts(any())).thenReturn(mockResponse);
    MultipartUpload multipartUpload =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();

    ali.listMultipartUpload(multipartUpload);

    ArgumentCaptor<ListPartsRequest> requestCaptor =
        ArgumentCaptor.forClass(ListPartsRequest.class);
    verify(mockOssClient, times(1)).listParts(requestCaptor.capture());
    ListPartsRequest actualRequest = requestCaptor.getValue();
    assertEquals("object-1", actualRequest.getKey());
    assertEquals("bucket-1", actualRequest.getBucketName());
    assertEquals("mpu-id", actualRequest.getUploadId());
  }

  @Test
  void testDoAbortMultipartUpload() {
    MultipartUpload multipartUpload =
        MultipartUpload.builder().bucket("bucket-1").key("object-1").id("mpu-id").build();

    ali.abortMultipartUpload(multipartUpload);

    ArgumentCaptor<AbortMultipartUploadRequest> requestCaptor =
        ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
    verify(mockOssClient, times(1)).abortMultipartUpload(requestCaptor.capture());
    AbortMultipartUploadRequest actualRequest = requestCaptor.getValue();
    assertEquals("object-1", actualRequest.getKey());
    assertEquals("bucket-1", actualRequest.getBucketName());
    assertEquals("mpu-id", actualRequest.getUploadId());
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
    when(mockOssV2Client.getObjectTagging(
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
    verify(mockOssV2Client, times(1)).putObjectTagging(requestCaptor.capture(), any());

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
    doReturn(mockResult).when(mockOssV2Client).presign(
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

    URL result = ali.doGeneratePresignedUrl(presignedUploadRequest);

    assertNotNull(result);
    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.PutObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(com.aliyun.sdk.service.oss2.models.PutObjectRequest.class);
    verify(mockOssV2Client, times(1)).presign(requestCaptor.capture(),
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
    doReturn(mockResult).when(mockOssV2Client).presign(
        any(com.aliyun.sdk.service.oss2.models.GetObjectRequest.class),
        any(com.aliyun.sdk.service.oss2.PresignOptions.class));

    Duration duration = Duration.ofHours(12);
    PresignedUrlRequest presignedDownloadRequest =
        PresignedUrlRequest.builder()
            .type(PresignedOperation.DOWNLOAD)
            .key("object-1")
            .duration(duration)
            .build();

    URL result = ali.doGeneratePresignedUrl(presignedDownloadRequest);

    assertNotNull(result);
    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.GetObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(com.aliyun.sdk.service.oss2.models.GetObjectRequest.class);
    verify(mockOssV2Client, times(1)).presign(requestCaptor.capture(),
        any(com.aliyun.sdk.service.oss2.PresignOptions.class));
    com.aliyun.sdk.service.oss2.models.GetObjectRequest actualRequest = requestCaptor.getValue();
    assertEquals("bucket-1", actualRequest.bucket());
    assertEquals("object-1", actualRequest.key());
  }

  @Test
  void testDoDoesObjectExist() {
    doReturn(true).when(mockOssV2Client).doesObjectExist(
        any(com.aliyun.sdk.service.oss2.models.GetObjectMetaRequest.class));

    boolean result = ali.doDoesObjectExist("object-1", "version-1");

    ArgumentCaptor<com.aliyun.sdk.service.oss2.models.GetObjectMetaRequest> requestCaptor =
        ArgumentCaptor.forClass(
            com.aliyun.sdk.service.oss2.models.GetObjectMetaRequest.class);
    verify(mockOssV2Client, times(1)).doesObjectExist(requestCaptor.capture());
    com.aliyun.sdk.service.oss2.models.GetObjectMetaRequest actualRequest =
        requestCaptor.getValue();
    assertEquals("bucket-1", actualRequest.bucket());
    assertEquals("object-1", actualRequest.key());
    assertEquals("version-1", actualRequest.versionId());
    assertTrue(result);
  }

  @Test
  void testDoDoesBucketExist() {
    doReturn(true).when(mockOssV2Client).doesBucketExist("bucket-1");

    boolean result = ali.doDoesBucketExist();

    verify(mockOssV2Client, times(1)).doesBucketExist("bucket-1");
    assertTrue(result);
  }

  @Test
  void testDoDoesBucketExist_BucketDoesNotExist() {
    doReturn(false).when(mockOssV2Client).doesBucketExist("bucket-1");

    boolean result = ali.doDoesBucketExist();

    verify(mockOssV2Client, times(1)).doesBucketExist("bucket-1");
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

  private com.aliyun.sdk.service.oss2.models.PutObjectResult buildTestV2PutObjectResult() {
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
  void testGetObjectLock_ThrowsUnsupportedException() {
    // Given
    String key = "test-key";
    String versionId = "version-1";

    // When/Then
    assertThrows(
        UnSupportedOperationException.class,
        () -> {
          ali.getObjectLock(key, versionId);
        });
  }

  @Test
  void testUpdateObjectRetention_ThrowsUnsupportedException() {
    // Given
    String key = "test-key";
    String versionId = "version-1";
    Instant retainUntil = Instant.now().plusSeconds(3600);

    // When/Then
    assertThrows(
        UnSupportedOperationException.class,
        () -> {
          ali.updateObjectRetention(key, versionId, retainUntil);
        });
  }

  @Test
  void testUpdateLegalHold_ThrowsUnsupportedException() {
    // Given
    String key = "test-key";
    String versionId = "version-1";

    // When/Then
    assertThrows(
        UnSupportedOperationException.class,
        () -> {
          ali.updateLegalHold(key, versionId, true);
        });
  }
}
