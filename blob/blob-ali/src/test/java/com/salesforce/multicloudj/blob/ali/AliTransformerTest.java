package com.salesforce.multicloudj.blob.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.aliyun.sdk.service.oss2.models.CopyObjectResult;
import com.aliyun.sdk.service.oss2.models.HeadObjectResult;
import com.aliyun.sdk.service.oss2.models.InitiateMultipartUpload;
import com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadResult;
import com.aliyun.sdk.service.oss2.models.ListObjectsV2Request;
import com.aliyun.sdk.service.oss2.models.ListPartsResult;
import com.aliyun.sdk.service.oss2.models.Part;
import com.aliyun.sdk.service.oss2.models.PutObjectResult;
import com.aliyun.sdk.service.oss2.models.UploadPartResult;
import com.aliyun.sdk.service.oss2.transport.BinaryData;
import com.aliyun.sdk.service.oss2.transport.HttpClientOptions;
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
import com.salesforce.multicloudj.blob.driver.ObjectLockInfo;
import com.salesforce.multicloudj.blob.driver.PresignedOperation;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.RetentionMode;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
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
    BinaryData body =
        BinaryData.fromBytes("data".getBytes());

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
    BinaryData body =
        BinaryData.fromBytes("data".getBytes());

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
    BinaryData body =
        BinaryData.fromBytes("data".getBytes());

    var actual = transformer.toPutObjectRequest(request, body);
    assertEquals(BUCKET, actual.bucket());
    assertEquals(key, actual.key());
    assertEquals("some-value", actual.metadata().get("some-key"));
    assertNull(actual.serverSideEncryption());
  }

  @Test
  void testToPutObjectRequest_useKmsManagedKey_setsKmsWithoutKeyId() {
    var request =
        UploadRequest.builder().withKey("some-key").withUseKmsManagedKey(true).build();
    BinaryData body =
        BinaryData.fromBytes("data".getBytes());

    var actual = transformer.toPutObjectRequest(request, body);

    assertEquals("KMS", actual.serverSideEncryption());
    assertNull(actual.serverSideEncryptionKeyId());
  }

  @Test
  void testToPutObjectRequest_explicitKmsKeyIdTakesPrecedenceOverManagedKey() {
    var kmsKeyId = "alias/my-kms-key";
    var request =
        UploadRequest.builder()
            .withKey("some-key")
            .withKmsKeyId(kmsKeyId)
            .withUseKmsManagedKey(true)
            .build();
    BinaryData body =
        BinaryData.fromBytes("data".getBytes());

    var actual = transformer.toPutObjectRequest(request, body);

    assertEquals("KMS", actual.serverSideEncryption());
    assertEquals(kmsKeyId, actual.serverSideEncryptionKeyId());
  }

  @Test
  void testToUploadResponse() {
    UploadRequest request =
        UploadRequest.builder()
            .withKey("some-key")
            .withMetadata(Map.of("some-key", "some-value"))
            .build();
    PutObjectResult result =
        mock(PutObjectResult.class);
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
    CopyObjectResult result =
        mock(CopyObjectResult.class);
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
    CopyObjectResult result =
        mock(CopyObjectResult.class);
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
  void testToInitiateMultipartUploadRequest_useKmsManagedKey_setsKmsWithoutKeyId() {
    MultipartUploadRequest request =
        new MultipartUploadRequest.Builder().withKey("key").withUseKmsManagedKey(true).build();

    var actual = transformer.toInitiateMultipartUploadRequest(request);

    assertEquals("KMS", actual.serverSideEncryption());
    assertNull(actual.serverSideEncryptionKeyId());
  }

  @Test
  void testToInitiateMultipartUploadRequest_explicitKmsKeyIdTakesPrecedenceOverManagedKey() {
    String kmsKeyId = "alias/my-kms-key";
    MultipartUploadRequest request =
        new MultipartUploadRequest.Builder()
            .withKey("key")
            .withKmsKeyId(kmsKeyId)
            .withUseKmsManagedKey(true)
            .build();

    var actual = transformer.toInitiateMultipartUploadRequest(request);

    assertEquals("KMS", actual.serverSideEncryption());
    assertEquals(kmsKeyId, actual.serverSideEncryptionKeyId());
  }

  @Test
  void testToMultipartUpload() {
    InitiateMultipartUploadResult result =
        mock(InitiateMultipartUploadResult.class);
    InitiateMultipartUpload upload =
        mock(InitiateMultipartUpload.class);
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
    InitiateMultipartUploadResult result =
        mock(InitiateMultipartUploadResult.class);
    InitiateMultipartUpload upload =
        mock(InitiateMultipartUpload.class);
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
  void testToMultipartUpload_checksumEnabledNoAlgorithm_defaultsToCrc64() {
    // OSS's native object checksum is CRC64-ECMA. When checksumming is enabled without an explicit
    // algorithm, the stored MultipartUpload should reflect CRC64 so the value surfaced at
    // completion is honestly labeled.
    InitiateMultipartUploadResult result =
        mock(InitiateMultipartUploadResult.class);
    InitiateMultipartUpload upload =
        mock(InitiateMultipartUpload.class);
    doReturn(upload).when(result).initiateMultipartUpload();
    doReturn(BUCKET).when(upload).bucket();
    doReturn("key").when(upload).key();
    doReturn("uploadId").when(upload).uploadId();
    MultipartUploadRequest request = new MultipartUploadRequest.Builder()
        .withKey("key").withChecksumEnabled(true).build();

    var actual = transformer.toMultipartUpload(result, request);

    assertTrue(actual.isChecksumEnabled());
    assertEquals(ChecksumMethod.CRC64, actual.getChecksumAlgorithm());
  }

  @Test
  void testToMultipartUpload_explicitCrc64_preserved() {
    InitiateMultipartUploadResult result =
        mock(InitiateMultipartUploadResult.class);
    InitiateMultipartUpload upload =
        mock(InitiateMultipartUpload.class);
    doReturn(upload).when(result).initiateMultipartUpload();
    doReturn(BUCKET).when(upload).bucket();
    doReturn("key").when(upload).key();
    doReturn("uploadId").when(upload).uploadId();
    MultipartUploadRequest request = new MultipartUploadRequest.Builder()
        .withKey("key").withChecksumAlgorithm(ChecksumMethod.CRC64).build();

    var actual = transformer.toMultipartUpload(result, request);

    assertTrue(actual.isChecksumEnabled());
    assertEquals(ChecksumMethod.CRC64, actual.getChecksumAlgorithm());
  }

  @Test
  void testToMultipartUpload_checksumDisabled_algorithmStaysNull() {
    InitiateMultipartUploadResult result =
        mock(InitiateMultipartUploadResult.class);
    InitiateMultipartUpload upload =
        mock(InitiateMultipartUpload.class);
    doReturn(upload).when(result).initiateMultipartUpload();
    doReturn(BUCKET).when(upload).bucket();
    doReturn("key").when(upload).key();
    doReturn("uploadId").when(upload).uploadId();
    MultipartUploadRequest request = new MultipartUploadRequest.Builder()
        .withKey("key").build();

    var actual = transformer.toMultipartUpload(result, request);

    assertFalse(actual.isChecksumEnabled());
    assertNull(actual.getChecksumAlgorithm());
  }

  @Test
  void testRejectUnsupportedChecksum_allowsCrc64AndNull() {
    // CRC64 is OSS's native algorithm; null means "use the substrate default" — both allowed.
    AliTransformer.rejectUnsupportedChecksum(ChecksumMethod.CRC64);
    AliTransformer.rejectUnsupportedChecksum(null);
  }

  @Test
  void testRejectUnsupportedChecksum_rejectsCrc32cAndSha256() {
    assertThrows(UnSupportedOperationException.class,
        () -> AliTransformer.rejectUnsupportedChecksum(ChecksumMethod.CRC32C));
    assertThrows(UnSupportedOperationException.class,
        () -> AliTransformer.rejectUnsupportedChecksum(ChecksumMethod.SHA256));
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
    UploadPartResult result =
        mock(UploadPartResult.class);
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
    ListPartsResult result =
        mock(ListPartsResult.class);
    Part part2 =
        Part.newBuilder()
            .partNumber(2L).eTag("\"etag2\"").size(50L).build();
    Part part1 =
        Part.newBuilder()
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
  void testToPresignedPutObjectRequest_withConstraints() {
    PresignedUrlRequest request =
        PresignedUrlRequest.builder()
            .type(PresignedOperation.UPLOAD)
            .key("object-1")
            .duration(Duration.ofHours(1))
            .contentLength(1024)
            .contentType("application/json")
            .build();

    var actual = transformer.toPresignedPutObjectRequest(request);

    assertEquals(BUCKET, actual.bucket());
    assertEquals("object-1", actual.key());
    assertEquals(Integer.valueOf(1024), actual.contentLength());
    assertEquals("application/json", actual.contentType());
  }

  @Test
  void testToPresignedPutObjectRequest_contentLengthOverflow() {
    PresignedUrlRequest request =
        PresignedUrlRequest.builder()
            .type(PresignedOperation.UPLOAD)
            .key("object-1")
            .duration(Duration.ofHours(1))
            .contentLength((long) Integer.MAX_VALUE + 1)
            .build();

    assertThrows(
        InvalidArgumentException.class,
        () -> transformer.toPresignedPutObjectRequest(request));
  }

  @Test
  void testToPresignedPutObjectRequest_withChecksumCrc32c() {
    PresignedUrlRequest request =
        PresignedUrlRequest.builder()
            .type(PresignedOperation.UPLOAD)
            .key("object-1")
            .duration(Duration.ofHours(1))
            .checksumValue("abc123==")
            .checksumAlgorithm(ChecksumMethod.CRC32C)
            .build();

    var actual = transformer.toPresignedPutObjectRequest(request);

    assertEquals(BUCKET, actual.bucket());
    assertEquals("object-1", actual.key());
    assertEquals("abc123==", actual.headers().get("x-oss-hash-crc64ecma"));
  }

  @Test
  void testToPresignedPutObjectRequest_withChecksumSha256() {
    PresignedUrlRequest request =
        PresignedUrlRequest.builder()
            .type(PresignedOperation.UPLOAD)
            .key("object-1")
            .duration(Duration.ofHours(1))
            .checksumValue("sha256val==")
            .checksumAlgorithm(ChecksumMethod.SHA256)
            .build();

    var actual = transformer.toPresignedPutObjectRequest(request);

    assertEquals("sha256val==", actual.headers().get("x-oss-content-sha256"));
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
    assertNull(actual.responseContentDisposition());
  }

  @Test
  void testToPresignedGetObjectRequest_withContentDisposition() {
    PresignedUrlRequest presignedDownloadRequest =
        PresignedUrlRequest.builder()
            .type(PresignedOperation.DOWNLOAD)
            .key("object-1")
            .duration(Duration.ofHours(12))
            .contentDisposition("attachment; filename=\"report.pdf\"")
            .build();

    var actual = transformer.toPresignedGetObjectRequest(presignedDownloadRequest);

    assertEquals(BUCKET, actual.bucket());
    assertEquals("object-1", actual.key());
    assertEquals(
        "attachment; filename=\"report.pdf\"", actual.responseContentDisposition());
  }

  @Test
  void testToPresignedPutObjectRequest_ignoresContentDisposition() {
    PresignedUrlRequest presignedUploadRequest =
        PresignedUrlRequest.builder()
            .type(PresignedOperation.UPLOAD)
            .key("object-1")
            .duration(Duration.ofHours(12))
            .contentDisposition("attachment; filename=\"report.pdf\"")
            .build();

    var actual = transformer.toPresignedPutObjectRequest(presignedUploadRequest);

    assertEquals(BUCKET, actual.bucket());
    assertEquals("object-1", actual.key());
    assertNull(actual.contentDisposition());
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

    ListObjectsV2Request actual =
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

    ListObjectsV2Request actual =
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

    ListObjectsV2Request actual =
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

    BinaryData body =
        BinaryData.fromBytes("test data".getBytes());
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

    BinaryData body =
        BinaryData.fromBytes("data".getBytes());
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

    BinaryData body =
        BinaryData.fromBytes("data".getBytes());
    var result = transformer.toPutObjectRequest(uploadRequest, body);

    assertEquals("12345678901234", result.headers().get("x-oss-hash-crc64ecma"));
    assertNull(result.headers().get("x-oss-content-sha256"));
  }

  @Test
  void testToMultipartUpload_WithChecksumAlgorithm() {
    InitiateMultipartUploadResult result =
        mock(InitiateMultipartUploadResult.class);
    InitiateMultipartUpload upload =
        mock(InitiateMultipartUpload.class);
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

    BinaryData body =
        BinaryData.fromBytes("data".getBytes());
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
    assertTrue(delay.toMillis() >= 100);
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
  void testToAliRetryerNullConfigThrows() {
    InvalidArgumentException ex = assertThrows(InvalidArgumentException.class,
        () -> AliTransformer.toAliRetryer(null));
    assertEquals("RetryConfig cannot be null", ex.getMessage());
  }

  @Test
  void testToAliRetryerNoModeUsesDefaults() {
    RetryConfig config = RetryConfig.builder()
        .maxAttempts(4)
        .build();

    var retryer = AliTransformer.toAliRetryer(config);

    assertNotNull(retryer);
    assertEquals(4, retryer.maxAttempts());
    // Verify SDK default backoff produces a sane delay (not zero or negative)
    Duration delay = retryer.retryDelay(1, new RuntimeException("test"));
    assertNotNull(delay);
    assertTrue(delay.toMillis() > 0,
        "Default backoff should produce a positive delay, got: " + delay.toMillis());
  }

  @Test
  void testToAliRetryerInvalidMaxAttempts() {
    RetryConfig config = RetryConfig.builder()
        .mode(RetryConfig.Mode.EXPONENTIAL)
        .maxAttempts(0)
        .initialDelayMillis(100L)
        .maxDelayMillis(5000L)
        .build();

    InvalidArgumentException ex = assertThrows(InvalidArgumentException.class,
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

    InvalidArgumentException ex = assertThrows(InvalidArgumentException.class,
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

    InvalidArgumentException ex = assertThrows(InvalidArgumentException.class,
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

    InvalidArgumentException ex = assertThrows(InvalidArgumentException.class,
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

    InvalidArgumentException ex = assertThrows(InvalidArgumentException.class,
        () -> AliTransformer.toAliRetryer(config));
    assertEquals(
        "RetryConfig.fixedDelayMillis must be greater than 0 for FIXED mode, got: 0",
        ex.getMessage());
  }

  // ---- toBlobMetadata: objectLockInfo extraction from x-oss-object-worm-* headers ----

  @Test
  void testToBlobMetadata_locked_populatesObjectLockInfoFromHeaders() {
    // OSS surfaces object-lock state on HeadObject as response headers (verified live against
    // a versioned + WORM-enabled bucket). The transformer must read those headers and build an
    // ObjectLockInfo.
    Map<String, String> headers =
        Map.of(
            "x-oss-object-worm-mode", "GOVERNANCE",
            "x-oss-object-worm-retain-until-date", "2026-06-10T23:49:51.000Z",
            "x-oss-object-worm-legal-hold", "ON");
    HeadObjectResult result =
        HeadObjectResult.newBuilder()
            .headers(headers)
            .build();

    BlobMetadata metadata = transformer.toBlobMetadata("k", result);

    ObjectLockInfo info = metadata.getObjectLockInfo();
    assertNotNull(info, "objectLockInfo should be populated when worm headers are present");
    assertEquals(
        RetentionMode.GOVERNANCE, info.getMode());
    assertEquals(Instant.parse("2026-06-10T23:49:51.000Z"), info.getRetainUntilDate());
    assertTrue(info.isLegalHold());
    // OSS has no event-based-hold concept; AWS leaves it null too.
    assertNull(info.getUseEventBasedHold());
  }

  @Test
  void testToBlobMetadata_complianceModeWithoutLegalHold_populatesPartial() {
    // COMPLIANCE retention with no legal hold: legalHold should be false, mode COMPLIANCE.
    Map<String, String> headers =
        Map.of(
            "x-oss-object-worm-mode", "COMPLIANCE",
            "x-oss-object-worm-retain-until-date", "2030-01-01T00:00:00.000Z");
    HeadObjectResult result =
        HeadObjectResult.newBuilder()
            .headers(headers)
            .build();

    ObjectLockInfo info =
        transformer.toBlobMetadata("k", result).getObjectLockInfo();

    assertNotNull(info);
    assertEquals(
        RetentionMode.COMPLIANCE, info.getMode());
    assertEquals(Instant.parse("2030-01-01T00:00:00.000Z"), info.getRetainUntilDate());
    assertFalse(info.isLegalHold());
  }

  @Test
  void testToBlobMetadata_legalHoldOnlyWithoutRetention_populatesObjectLockInfo() {
    // OSS allows legal hold without retention. The transformer must still surface
    // objectLockInfo with mode=null and legalHold=true so callers can act on the hold.
    Map<String, String> headers = Map.of("x-oss-object-worm-legal-hold", "ON");
    HeadObjectResult result =
        HeadObjectResult.newBuilder()
            .headers(headers)
            .build();

    ObjectLockInfo info =
        transformer.toBlobMetadata("k", result).getObjectLockInfo();

    assertNotNull(info,
        "objectLockInfo should be populated even when only legal hold is set");
    assertNull(info.getMode());
    assertNull(info.getRetainUntilDate());
    assertTrue(info.isLegalHold());
  }

  @Test
  void testToBlobMetadata_noWormHeaders_objectLockInfoIsNull() {
    // No worm-related headers on the response — objectLockInfo should remain null, matching
    // the AWS guard ("extract object lock info if present").
    Map<String, String> headers =
        Map.of(
            "Content-Type", "application/octet-stream",
            "ETag", "\"abc\"",
            "x-oss-storage-class", "Standard");
    HeadObjectResult result =
        HeadObjectResult.newBuilder()
            .headers(headers)
            .build();

    BlobMetadata metadata = transformer.toBlobMetadata("k", result);

    assertNull(metadata.getObjectLockInfo(),
        "objectLockInfo should be null when no x-oss-object-worm-* headers are present");
  }

  @Test
  void testToBlobMetadata_caseInsensitiveHeaderLookup() {
    // OSS header casing is conventionally lowercase, but the SDK headers Map should not
    // dictate behavior here. Use mixed casing to verify the lookup is case-insensitive.
    Map<String, String> headers =
        Map.of(
            "X-OSS-Object-Worm-Mode", "GOVERNANCE",
            "X-OSS-Object-Worm-Retain-Until-Date", "2026-06-10T23:49:51.000Z",
            "X-OSS-Object-Worm-Legal-Hold", "on");
    HeadObjectResult result =
        HeadObjectResult.newBuilder()
            .headers(headers)
            .build();

    ObjectLockInfo info =
        transformer.toBlobMetadata("k", result).getObjectLockInfo();

    assertNotNull(info);
    assertEquals(
        RetentionMode.GOVERNANCE, info.getMode());
    assertTrue(info.isLegalHold(),
        "legal-hold value comparison should be case-insensitive (\"on\" -> true)");
  }

  @Test
  void testToBlobMetadata_unknownWormMode_leavesModeNullBestEffort() {
    // OSS returns a worm-mode value the SDK enum does not recognize. The read is best-effort and
    // must not throw: objectLockInfo is still populated (the other worm headers are present),
    // mode falls back to null, and the remaining fields are unaffected.
    Map<String, String> headers =
        Map.of(
            "x-oss-object-worm-mode", "UNKNOWN_MODE",
            "x-oss-object-worm-retain-until-date", "2026-06-10T23:49:51.000Z",
            "x-oss-object-worm-legal-hold", "ON");
    HeadObjectResult result =
        HeadObjectResult.newBuilder()
            .headers(headers)
            .build();

    ObjectLockInfo info =
        transformer.toBlobMetadata("k", result).getObjectLockInfo();

    assertNotNull(info,
        "objectLockInfo should still be populated when other worm headers are present");
    assertNull(info.getMode(),
        "an unrecognized worm-mode must fall back to null rather than throw");
    assertEquals(Instant.parse("2026-06-10T23:49:51.000Z"), info.getRetainUntilDate());
    assertTrue(info.isLegalHold());
  }

  @Test
  void testToBlobMetadata_malformedRetainUntilDate_leavesRetainUntilNullBestEffort() {
    // OSS returns a retain-until-date that is not valid ISO-8601. The read is best-effort and must
    // not throw: objectLockInfo is still populated, retainUntilDate falls back to null, and the
    // mode/legal-hold fields parse normally.
    Map<String, String> headers =
        Map.of(
            "x-oss-object-worm-mode", "GOVERNANCE",
            "x-oss-object-worm-retain-until-date", "not-a-valid-date",
            "x-oss-object-worm-legal-hold", "ON");
    HeadObjectResult result =
        HeadObjectResult.newBuilder()
            .headers(headers)
            .build();

    ObjectLockInfo info =
        transformer.toBlobMetadata("k", result).getObjectLockInfo();

    assertNotNull(info);
    assertEquals(
        RetentionMode.GOVERNANCE, info.getMode());
    assertNull(info.getRetainUntilDate(),
        "an unparseable retain-until-date must fall back to null rather than throw");
    assertTrue(info.isLegalHold());
  }

  @Test
  void testToHttpClientOptionsAllNullKeepsSdkDefaults() {
    // When no inputs are supplied, the produced options must match the OSS SDK defaults so a
    // client built from them behaves identically to the SDK's own default client.
    var defaults = HttpClientOptions.custom().build();

    var options = AliTransformer.toHttpClientOptions(null, null, null, null);

    assertNotNull(options);
    assertEquals(defaults.maxConnections(), options.maxConnections());
    assertEquals(defaults.keepAliveTimeout(), options.keepAliveTimeout());
    assertEquals(defaults.readWriteTimeout(), options.readWriteTimeout());
    assertEquals(defaults.connectTimeout(), options.connectTimeout());
    assertNull(options.proxyHost());
  }

  @Test
  void testToHttpClientOptionsAppliesMaxConnectionsAndIdleTimeout() {
    var options = AliTransformer.toHttpClientOptions(
        null, null, 42, Duration.ofSeconds(75));

    assertEquals(42, options.maxConnections());
    assertEquals(Duration.ofSeconds(75), options.keepAliveTimeout());
  }

  @Test
  void testToHttpClientOptionsPreservesProxyHostAndReadWriteTimeout() {
    // Supplying a custom client bypasses the SDK's own option derivation, so the transport
    // fields the driver otherwise sets (proxyHost, readWriteTimeout) must survive.
    var options = AliTransformer.toHttpClientOptions(
        "proxy.example.com:8080", Duration.ofSeconds(20), 10, Duration.ofSeconds(50));

    assertEquals("proxy.example.com:8080", options.proxyHost());
    assertEquals(Duration.ofSeconds(20), options.readWriteTimeout());
    assertEquals(10, options.maxConnections());
    assertEquals(Duration.ofSeconds(50), options.keepAliveTimeout());
  }

  @Test
  void testToHttpClientOptionsOnlyIdleTimeoutLeavesMaxConnectionsDefault() {
    var defaults = HttpClientOptions.custom().build();

    var options = AliTransformer.toHttpClientOptions(
        null, null, null, Duration.ofSeconds(90));

    assertEquals(Duration.ofSeconds(90), options.keepAliveTimeout());
    assertEquals(defaults.maxConnections(), options.maxConnections(),
        "maxConnections must keep the SDK default when only idle timeout is set");
  }
}
