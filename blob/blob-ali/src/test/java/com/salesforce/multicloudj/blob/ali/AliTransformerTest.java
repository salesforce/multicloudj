package com.salesforce.multicloudj.blob.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ChecksumMethod;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.PresignedOperation;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class AliTransformerTest {

  private static final String BUCKET = "some-bucket";
  private final AliTransformer transformer = new AliTransformer(BUCKET);

  @Test
  void testBucket() {
    assertEquals(BUCKET, transformer.getBucket());
  }

  @Test
  void testToPutObjectRequest() {
    var key = "some-key";
    var metadata = Map.of("some-key", "some-value");
    var tags = Map.of("tag-key", "tag-value");

    var request =
        UploadRequest.builder().withKey(key).withMetadata(metadata).withTags(tags).build();
    com.aliyun.sdk.service.oss2.transport.BinaryData body =
        com.aliyun.sdk.service.oss2.transport.BinaryData.fromBytes("data".getBytes());

    var actual = transformer.toPutObjectRequest(request, body);
    assertEquals(BUCKET, actual.bucket());
    assertEquals(key, actual.key());
    assertEquals("some-value", actual.metadata().get("some-key"));
    assertEquals("tag-key=tag-value", actual.tagging());
  }

  @Test
  void testToPutObjectRequestWithKmsKey() {
    var key = "some-key";
    var metadata = Map.of("some-key", "some-value");
    var kmsKeyId = "alias/my-kms-key";

    var request =
        UploadRequest.builder().withKey(key).withMetadata(metadata).withKmsKeyId(kmsKeyId).build();
    com.aliyun.sdk.service.oss2.transport.BinaryData body =
        com.aliyun.sdk.service.oss2.transport.BinaryData.fromBytes("data".getBytes());

    var actual = transformer.toPutObjectRequest(request, body);
    assertEquals(BUCKET, actual.bucket());
    assertEquals(key, actual.key());
    assertEquals("some-value", actual.metadata().get("some-key"));
    assertEquals("KMS", actual.serverSideEncryption());
    assertEquals(kmsKeyId, actual.serverSideEncryptionKeyId());
  }

  @Test
  void testToPutObjectRequestWithoutKmsKey() {
    var key = "some-key";
    var metadata = Map.of("some-key", "some-value");

    var request = UploadRequest.builder().withKey(key).withMetadata(metadata).build();
    com.aliyun.sdk.service.oss2.transport.BinaryData body =
        com.aliyun.sdk.service.oss2.transport.BinaryData.fromBytes("data".getBytes());

    var actual = transformer.toPutObjectRequest(request, body);
    assertEquals(BUCKET, actual.bucket());
    assertEquals(key, actual.key());
    assertEquals("some-value", actual.metadata().get("some-key"));
    assertNull(actual.serverSideEncryption());
  }

  @Test
  void testToUploadResponse() {
    UploadRequest request =
        UploadRequest.builder()
            .withKey("some-key")
            .withMetadata(Map.of("some-key", "some-value"))
            .build();
    com.aliyun.sdk.service.oss2.models.PutObjectResult result =
        mock(com.aliyun.sdk.service.oss2.models.PutObjectResult.class);
    doReturn("\"etag\"").when(result).eTag();
    doReturn("version-1").when(result).versionId();

    var actual = transformer.toUploadResponse(request, result);

    assertEquals(request.getKey(), actual.getKey());
    assertEquals("version-1", actual.getVersionId());
    assertEquals("etag", actual.getETag());
  }

  @Test
  void testToDeleteObjectRequest() {
    var actual = transformer.toDeleteObjectRequest("key1", "v1");
    assertEquals(BUCKET, actual.bucket());
    assertEquals("key1", actual.key());
    assertEquals("v1", actual.versionId());

    var actualNoVersion = transformer.toDeleteObjectRequest("key2", null);
    assertEquals(BUCKET, actualNoVersion.bucket());
    assertEquals("key2", actualNoVersion.key());
    assertNull(actualNoVersion.versionId());
  }

  @Test
  void testToDeleteMultipleObjectsRequest() {
    Collection<BlobIdentifier> objects =
        List.of(
            new BlobIdentifier("key1", "v1"),
            new BlobIdentifier("key2", null),
            new BlobIdentifier("key3", "v3"));

    var actual = transformer.toDeleteMultipleObjectsRequest(objects);

    assertEquals(BUCKET, actual.bucket());
    var delete = actual.delete();
    assertTrue(delete.quiet());
    var ids = delete.objects();
    assertEquals(3, ids.size());
    assertEquals("key1", ids.get(0).key());
    assertEquals("v1", ids.get(0).versionId());
    assertEquals("key2", ids.get(1).key());
    assertNull(ids.get(1).versionId());
    assertEquals("key3", ids.get(2).key());
    assertEquals("v3", ids.get(2).versionId());
  }

  @Test
  void testToCopyObjectRequest() {
    CopyRequest request =
        CopyRequest.builder()
            .srcKey("key1")
            .srcVersionId("v1")
            .destBucket("bucket2")
            .destKey("key2")
            .build();

    var actual = transformer.toCopyObjectRequest(request);

    assertEquals(BUCKET, actual.sourceBucket());
    assertEquals("key1", actual.sourceKey());
    assertEquals("v1", actual.sourceVersionId());
    assertEquals("bucket2", actual.bucket());
    assertEquals("key2", actual.key());
  }

  @Test
  void testToCopyResponse_withLastModifiedHeader() {
    com.aliyun.sdk.service.oss2.models.CopyObjectResult result =
        mock(com.aliyun.sdk.service.oss2.models.CopyObjectResult.class);
    doReturn("v2").when(result).versionId();
    doReturn("\"etag\"").when(result).eTag();
    doReturn("Fri, 15 May 2026 10:30:00 GMT").when(result).lastModified();

    var actual = transformer.toCopyResponse("key2", result);

    assertEquals("key2", actual.getKey());
    assertEquals("v2", actual.getVersionId());
    assertEquals("etag", actual.getETag());
    assertNotNull(actual.getLastModified());
  }

  @Test
  void testToCopyResponse_withNullLastModifiedHeader() {
    com.aliyun.sdk.service.oss2.models.CopyObjectResult result =
        mock(com.aliyun.sdk.service.oss2.models.CopyObjectResult.class);
    doReturn("v2").when(result).versionId();
    doReturn("\"etag\"").when(result).eTag();
    doReturn(null).when(result).lastModified();

    var actual = transformer.toCopyResponse("key2", result);

    assertEquals("key2", actual.getKey());
    assertEquals("v2", actual.getVersionId());
    assertEquals("etag", actual.getETag());
    assertNull(actual.getLastModified());
  }

  @Test
  void testToInitiateMultipartUploadRequest() {
    Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
    MultipartUploadRequest request =
        new MultipartUploadRequest.Builder().withKey("key").withMetadata(metadata).build();

    var actual = transformer.toInitiateMultipartUploadRequest(request);

    assertEquals(BUCKET, actual.bucket());
    assertEquals("key", actual.key());
    assertEquals(metadata, actual.metadata());
  }

  @Test
  void testToMultipartUpload() {
    com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadResult result =
        mock(com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadResult.class);
    com.aliyun.sdk.service.oss2.models.InitiateMultipartUpload upload =
        mock(com.aliyun.sdk.service.oss2.models.InitiateMultipartUpload.class);
    doReturn(upload).when(result).initiateMultipartUpload();
    doReturn(BUCKET).when(upload).bucket();
    doReturn("key").when(upload).key();
    doReturn("uploadId").when(upload).uploadId();
    Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
    MultipartUploadRequest request = new MultipartUploadRequest.Builder()
        .withKey("key").withMetadata(metadata).withContentType("text/plain").build();

    var actual = transformer.toMultipartUpload(result, request);

    assertEquals(BUCKET, actual.getBucket());
    assertEquals("key", actual.getKey());
    assertEquals("uploadId", actual.getId());
    assertEquals(metadata, actual.getMetadata());
    assertEquals("text/plain", actual.getContentType());
  }

  @Test
  void testToMultipartUploadWithKms() {
    com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadResult result =
        mock(com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadResult.class);
    com.aliyun.sdk.service.oss2.models.InitiateMultipartUpload upload =
        mock(com.aliyun.sdk.service.oss2.models.InitiateMultipartUpload.class);
    doReturn(upload).when(result).initiateMultipartUpload();
    doReturn(BUCKET).when(upload).bucket();
    doReturn("key").when(upload).key();
    doReturn("uploadId").when(upload).uploadId();
    Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
    String kmsKeyId = "test-kms-key-id";
    MultipartUploadRequest request = new MultipartUploadRequest.Builder()
        .withKey("key").withMetadata(metadata).withKmsKeyId(kmsKeyId).build();

    var actual = transformer.toMultipartUpload(result, request);

    assertEquals(BUCKET, actual.getBucket());
    assertEquals("key", actual.getKey());
    assertEquals("uploadId", actual.getId());
    assertEquals(metadata, actual.getMetadata());
    assertEquals(kmsKeyId, actual.getKmsKeyId());
  }

  @Test
  void testToUploadPartRequest() {
    MultipartUpload mpu =
        MultipartUpload.builder()
            .bucket(BUCKET)
            .key("key")
            .id("uploadId")
            .build();
    byte[] content = "Test data".getBytes();
    InputStream inputStream = new ByteArrayInputStream(content);
    MultipartPart mpp = new MultipartPart(1, inputStream, content.length);

    var actual = transformer.toUploadPartRequest(mpu, mpp);

    assertEquals(BUCKET, actual.bucket());
    assertEquals("key", actual.key());
    assertEquals("uploadId", actual.uploadId());
    assertEquals(1L, actual.partNumber());
    assertEquals((long) content.length, actual.contentLength());
  }

  @Test
  void testToUploadPartResponse() {
    byte[] content = "Test data".getBytes();
    InputStream inputStream = new ByteArrayInputStream(content);
    MultipartPart mpp = new MultipartPart(1, inputStream, content.length);
    com.aliyun.sdk.service.oss2.models.UploadPartResult result =
        mock(com.aliyun.sdk.service.oss2.models.UploadPartResult.class);
    doReturn("\"etag\"").when(result).eTag();

    var actual = transformer.toUploadPartResponse(mpp, result);

    assertEquals("etag", actual.getEtag());
    assertEquals(1, actual.getPartNumber());
    assertEquals(content.length, actual.getSizeInBytes());
  }

  @Test
  void testToCompleteMultipartUploadRequest() {
    MultipartUpload mpu =
        MultipartUpload.builder().bucket(BUCKET).key("key").id("uploadId").build();
    List<UploadPartResponse> parts =
        List.of(new UploadPartResponse(1, "etag1", 50), new UploadPartResponse(2, "etag2", 50));

    var actual = transformer.toCompleteMultipartUploadRequest(mpu, parts);

    assertEquals(BUCKET, actual.bucket());
    assertEquals("key", actual.key());
    assertEquals("uploadId", actual.uploadId());
    var actualParts = actual.completeMultipartUpload().parts();
    assertEquals(1L, actualParts.get(0).partNumber());
    assertEquals("etag1", actualParts.get(0).eTag());
    assertEquals(2L, actualParts.get(1).partNumber());
    assertEquals("etag2", actualParts.get(1).eTag());
  }

  @Test
  void testToListPartsRequest() {
    MultipartUpload mpu =
        MultipartUpload.builder().bucket(BUCKET).key("key").id("uploadId").build();

    var actual = transformer.toListPartsRequest(mpu);

    assertEquals(BUCKET, actual.bucket());
    assertEquals("key", actual.key());
    assertEquals("uploadId", actual.uploadId());
  }

  @Test
  void testToListUploadPartResponse() {
    com.aliyun.sdk.service.oss2.models.ListPartsResult result =
        mock(com.aliyun.sdk.service.oss2.models.ListPartsResult.class);
    com.aliyun.sdk.service.oss2.models.Part part2 =
        com.aliyun.sdk.service.oss2.models.Part.newBuilder()
            .partNumber(2L).eTag("\"etag2\"").size(50L).build();
    com.aliyun.sdk.service.oss2.models.Part part1 =
        com.aliyun.sdk.service.oss2.models.Part.newBuilder()
            .partNumber(1L).eTag("\"etag1\"").size(100L).build();
    doReturn(List.of(part2, part1)).when(result).parts();

    var actual = transformer.toListUploadPartResponse(result);

    assertEquals(1, actual.get(0).getPartNumber());
    assertEquals("etag1", actual.get(0).getEtag());
    assertEquals(100L, actual.get(0).getSizeInBytes());
    assertEquals(2, actual.get(1).getPartNumber());
    assertEquals("etag2", actual.get(1).getEtag());
    assertEquals(50L, actual.get(1).getSizeInBytes());
  }

  @Test
  void testToAbortMultipartUploadRequest() {
    MultipartUpload mpu =
        MultipartUpload.builder().bucket(BUCKET).key("key").id("uploadId").build();

    var actual = transformer.toAbortMultipartUploadRequest(mpu);

    assertEquals(BUCKET, actual.bucket());
    assertEquals("key", actual.key());
    assertEquals("uploadId", actual.uploadId());
  }

  @Test
  void testToPresignedUploadRequest() {
    Map<String, String> metadata = Map.of("key-1", "value-1");
    Map<String, String> tags = Map.of("tag-1", "tag-value-1");
    Duration duration = Duration.ofHours(12);
    PresignedUrlRequest presignedUploadRequest =
        PresignedUrlRequest.builder()
            .type(PresignedOperation.UPLOAD)
            .key("object-1")
            .metadata(metadata)
            .tags(tags)
            .duration(duration)
            .build();

    var actual = transformer.toPresignedPutObjectRequest(presignedUploadRequest);

    assertEquals(BUCKET, actual.bucket());
    assertEquals("object-1", actual.key());
    assertEquals("tag-1=tag-value-1", actual.tagging());
    assertEquals("value-1", actual.metadata().get("key-1"));
  }

  @Test
  void testToPresignedUploadRequestWithKmsKey() {
    Map<String, String> metadata = Map.of("key-1", "value-1");
    String kmsKeyId = "alias/my-kms-key";
    Duration duration = Duration.ofHours(12);
    PresignedUrlRequest presignedUploadRequest =
        PresignedUrlRequest.builder()
            .type(PresignedOperation.UPLOAD)
            .key("object-1")
            .metadata(metadata)
            .kmsKeyId(kmsKeyId)
            .duration(duration)
            .build();

    var actual = transformer.toPresignedPutObjectRequest(presignedUploadRequest);

    assertEquals(BUCKET, actual.bucket());
    assertEquals("object-1", actual.key());
    assertEquals("KMS", actual.serverSideEncryption());
    assertEquals(kmsKeyId, actual.serverSideEncryptionKeyId());
    assertEquals("value-1", actual.metadata().get("key-1"));
  }

  @Test
  void testToPresignedUploadRequestWithoutKmsKey() {
    Map<String, String> metadata = Map.of("key-1", "value-1");
    Duration duration = Duration.ofHours(12);
    PresignedUrlRequest presignedUploadRequest =
        PresignedUrlRequest.builder()
            .type(PresignedOperation.UPLOAD)
            .key("object-1")
            .metadata(metadata)
            .duration(duration)
            .build();

    var actual = transformer.toPresignedPutObjectRequest(presignedUploadRequest);

    assertEquals(BUCKET, actual.bucket());
    assertEquals("object-1", actual.key());
    assertNull(actual.serverSideEncryption());
    assertNull(actual.serverSideEncryptionKeyId());
    assertEquals("value-1", actual.metadata().get("key-1"));
  }

  @Test
  void testToPresignedDownloadRequest() {
    Duration duration = Duration.ofHours(12);
    PresignedUrlRequest presignedDownloadRequest =
        PresignedUrlRequest.builder()
            .type(PresignedOperation.DOWNLOAD)
            .key("object-1")
            .duration(duration)
            .build();

    var actual = transformer.toPresignedGetObjectRequest(presignedDownloadRequest);

    assertEquals(BUCKET, actual.bucket());
    assertEquals("object-1", actual.key());
  }

  @Test
  void testToPresignOptions() {
    Duration duration = Duration.ofHours(12);
    PresignedUrlRequest request =
        PresignedUrlRequest.builder()
            .type(PresignedOperation.UPLOAD)
            .key("object-1")
            .duration(duration)
            .build();

    var actual = transformer.toPresignOptions(request);

    assertTrue(actual.expiration().isPresent());
  }

  @Test
  void testToListObjectsRequest_fromPageRequest() {
    ListBlobsPageRequest request =
        ListBlobsPageRequest.builder()
            .withDelimiter(":")
            .withPrefix("some/prefix/path/thingie")
            .withPaginationToken("next-token")
            .withMaxResults(100)
            .build();

    com.aliyun.sdk.service.oss2.models.ListObjectsV2Request actual =
        transformer.toListObjectsRequest(request);
    assertEquals(BUCKET, actual.bucket());
    assertEquals(request.getDelimiter(), actual.delimiter());
    assertEquals(request.getPrefix(), actual.prefix());
    assertEquals(request.getPaginationToken(), actual.continuationToken());
    assertEquals(request.getMaxResults().longValue(), actual.maxKeys());
  }

  @Test
  void testToListObjectsRequest_fromListBlobsRequest() {
    ListBlobsRequest request =
        new ListBlobsRequest.Builder().withPrefix("abc").withDelimiter("/").build();

    com.aliyun.sdk.service.oss2.models.ListObjectsV2Request actual =
        transformer.toListObjectsRequest(request, "cont-token");
    assertEquals(BUCKET, actual.bucket());
    assertEquals("abc", actual.prefix());
    assertEquals("/", actual.delimiter());
    assertEquals("cont-token", actual.continuationToken());
  }

  @Test
  void testToListObjectsRequest_nullContinuationToken() {
    ListBlobsRequest request =
        new ListBlobsRequest.Builder().withPrefix("xyz").build();

    com.aliyun.sdk.service.oss2.models.ListObjectsV2Request actual =
        transformer.toListObjectsRequest(request, null);
    assertEquals(BUCKET, actual.bucket());
    assertEquals("xyz", actual.prefix());
    assertNull(actual.continuationToken());
  }


  @Test
  void testToInitiateMultipartUploadRequestWithContentType() {
    MultipartUploadRequest request =
        new MultipartUploadRequest.Builder()
            .withKey("test-key")
            .withContentType("application/x-directory")
            .build();

    var result = transformer.toInitiateMultipartUploadRequest(request);

    assertEquals("application/x-directory", result.contentType());
  }

  @Test
  void testToPutObjectRequestWithStorageClass() {
    UploadRequest uploadRequest =
        UploadRequest.builder().withKey("test-key").withStorageClass("IA").build();

    com.aliyun.sdk.service.oss2.transport.BinaryData body =
        com.aliyun.sdk.service.oss2.transport.BinaryData.fromBytes("test data".getBytes());
    var result = transformer.toPutObjectRequest(uploadRequest, body);

    assertEquals(BUCKET, result.bucket());
    assertEquals("test-key", result.key());
    assertEquals("IA", result.storageClass());
  }

  @Test
  void testToPutObjectRequest_WithSha256Checksum() {
    UploadRequest uploadRequest =
        UploadRequest.builder()
            .withKey("test-key")
            .withChecksumValue("abc123sha256value")
            .withChecksumAlgorithm(ChecksumMethod.SHA256)
            .build();

    com.aliyun.sdk.service.oss2.transport.BinaryData body =
        com.aliyun.sdk.service.oss2.transport.BinaryData.fromBytes("data".getBytes());
    var result = transformer.toPutObjectRequest(uploadRequest, body);

    assertEquals("abc123sha256value", result.headers().get("x-oss-content-sha256"));
    assertNull(result.headers().get("x-oss-hash-crc64ecma"));
  }

  @Test
  void testToPutObjectRequest_WithCrc64Checksum() {
    UploadRequest uploadRequest =
        UploadRequest.builder()
            .withKey("test-key")
            .withChecksumValue("12345678901234")
            .build();

    com.aliyun.sdk.service.oss2.transport.BinaryData body =
        com.aliyun.sdk.service.oss2.transport.BinaryData.fromBytes("data".getBytes());
    var result = transformer.toPutObjectRequest(uploadRequest, body);

    assertEquals("12345678901234", result.headers().get("x-oss-hash-crc64ecma"));
    assertNull(result.headers().get("x-oss-content-sha256"));
  }

  @Test
  void testToMultipartUpload_WithChecksumAlgorithm() {
    com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadResult result =
        mock(com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadResult.class);
    com.aliyun.sdk.service.oss2.models.InitiateMultipartUpload upload =
        mock(com.aliyun.sdk.service.oss2.models.InitiateMultipartUpload.class);
    doReturn(upload).when(result).initiateMultipartUpload();
    doReturn(BUCKET).when(upload).bucket();
    doReturn("key").when(upload).key();
    doReturn("uploadId").when(upload).uploadId();
    MultipartUploadRequest request = new MultipartUploadRequest.Builder()
        .withKey("key")
        .withChecksumAlgorithm(ChecksumMethod.SHA256)
        .build();

    var actual = transformer.toMultipartUpload(result, request);

    assertEquals(BUCKET, actual.getBucket());
    assertEquals("key", actual.getKey());
    assertEquals("uploadId", actual.getId());
    assertEquals(ChecksumMethod.SHA256, actual.getChecksumAlgorithm());
  }

  @Test
  void testToPutObjectRequestWithContentType() {
    UploadRequest uploadRequest =
        UploadRequest.builder().withKey("test-key").withContentType("text/plain").build();

    com.aliyun.sdk.service.oss2.transport.BinaryData body =
        com.aliyun.sdk.service.oss2.transport.BinaryData.fromBytes("data".getBytes());
    var result = transformer.toPutObjectRequest(uploadRequest, body);

    assertEquals(BUCKET, result.bucket());
    assertEquals("test-key", result.key());
    assertEquals("text/plain", result.contentType());
  }

  @Test
  void testToAliRetryerExponentialMode() {
    RetryConfig config = RetryConfig.builder()
        .mode(RetryConfig.Mode.EXPONENTIAL)
        .maxAttempts(5)
        .initialDelayMillis(200L)
        .maxDelayMillis(10000L)
        .build();

    var retryer = AliTransformer.toAliRetryer(config);

    assertNotNull(retryer);
    assertEquals(5, retryer.maxAttempts());
    // Verify delay is within expected range (EqualJitterBackoff adds jitter)
    Duration delay = retryer.retryDelay(1, new RuntimeException("test"));
    assertNotNull(delay);
    assertTrue(delay.toMillis() >= 0);
    assertTrue(delay.toMillis() <= 10000);
  }

  @Test
  void testToAliRetryerFixedMode() {
    RetryConfig config = RetryConfig.builder()
        .mode(RetryConfig.Mode.FIXED)
        .maxAttempts(3)
        .fixedDelayMillis(500L)
        .build();

    var retryer = AliTransformer.toAliRetryer(config);

    assertNotNull(retryer);
    assertEquals(3, retryer.maxAttempts());
    Duration delay = retryer.retryDelay(1, new RuntimeException("test"));
    assertEquals(500L, delay.toMillis());
  }

  @Test
  void testToAliRetryerNullConfigReturnsNull() {
    assertNull(AliTransformer.toAliRetryer(null));
  }

  @Test
  void testToAliRetryerNoModeUsesDefaults() {
    RetryConfig config = RetryConfig.builder()
        .maxAttempts(4)
        .build();

    var retryer = AliTransformer.toAliRetryer(config);

    assertNotNull(retryer);
    assertEquals(4, retryer.maxAttempts());
  }

  @Test
  void testToAliRetryerInvalidMaxAttempts() {
    RetryConfig config = RetryConfig.builder()
        .mode(RetryConfig.Mode.EXPONENTIAL)
        .maxAttempts(0)
        .initialDelayMillis(100L)
        .maxDelayMillis(5000L)
        .build();

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> AliTransformer.toAliRetryer(config));
    assertEquals("RetryConfig.maxAttempts must be greater than 0, got: 0", ex.getMessage());
  }

  @Test
  void testToAliRetryerNegativeMaxAttempts() {
    RetryConfig config = RetryConfig.builder()
        .mode(RetryConfig.Mode.EXPONENTIAL)
        .maxAttempts(-1)
        .initialDelayMillis(100L)
        .maxDelayMillis(5000L)
        .build();

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> AliTransformer.toAliRetryer(config));
    assertEquals("RetryConfig.maxAttempts must be greater than 0, got: -1", ex.getMessage());
  }

  @Test
  void testToAliRetryerExponentialInvalidInitialDelay() {
    RetryConfig config = RetryConfig.builder()
        .mode(RetryConfig.Mode.EXPONENTIAL)
        .maxAttempts(3)
        .initialDelayMillis(0L)
        .maxDelayMillis(5000L)
        .build();

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> AliTransformer.toAliRetryer(config));
    assertEquals(
        "RetryConfig.initialDelayMillis must be greater than 0 for EXPONENTIAL mode, got: 0",
        ex.getMessage());
  }

  @Test
  void testToAliRetryerExponentialInvalidMaxDelay() {
    RetryConfig config = RetryConfig.builder()
        .mode(RetryConfig.Mode.EXPONENTIAL)
        .maxAttempts(3)
        .initialDelayMillis(100L)
        .maxDelayMillis(0L)
        .build();

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> AliTransformer.toAliRetryer(config));
    assertEquals(
        "RetryConfig.maxDelayMillis must be greater than 0 for EXPONENTIAL mode, got: 0",
        ex.getMessage());
  }

  @Test
  void testToAliRetryerFixedInvalidDelay() {
    RetryConfig config = RetryConfig.builder()
        .mode(RetryConfig.Mode.FIXED)
        .maxAttempts(3)
        .fixedDelayMillis(0L)
        .build();

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> AliTransformer.toAliRetryer(config));
    assertEquals(
        "RetryConfig.fixedDelayMillis must be greater than 0 for FIXED mode, got: 0",
        ex.getMessage());
  }
}
