package com.salesforce.multicloudj.blob.ali;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartListing;
import com.aliyun.oss.model.PartSummary;
import com.aliyun.oss.model.UploadPartResult;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ChecksumMethod;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.PresignedOperation;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

public class AliTransformerTest {

  private static final String BUCKET = "some-bucket";
  private final AliTransformer transformer = new AliTransformer(BUCKET);

  @Test
  void testBucket() {
    assertEquals(BUCKET, transformer.getBucket());
  }

  @Test
  void testToV2PutObjectRequest() {
    var key = "some-key";
    var metadata = Map.of("some-key", "some-value");
    var tags = Map.of("tag-key", "tag-value");

    var request =
        UploadRequest.builder().withKey(key).withMetadata(metadata).withTags(tags).build();
    com.aliyun.sdk.service.oss2.transport.BinaryData body =
        com.aliyun.sdk.service.oss2.transport.BinaryData.fromBytes("data".getBytes());

    var actual = transformer.toV2PutObjectRequest(request, body);
    assertEquals(BUCKET, actual.bucket());
    assertEquals(key, actual.key());
    assertEquals("some-value", actual.metadata().get("some-key"));
    assertEquals("tag-key=tag-value", actual.tagging());
  }

  @Test
  void testToV2PutObjectRequestWithKmsKey() {
    var key = "some-key";
    var metadata = Map.of("some-key", "some-value");
    var kmsKeyId = "alias/my-kms-key";

    var request =
        UploadRequest.builder().withKey(key).withMetadata(metadata).withKmsKeyId(kmsKeyId).build();
    com.aliyun.sdk.service.oss2.transport.BinaryData body =
        com.aliyun.sdk.service.oss2.transport.BinaryData.fromBytes("data".getBytes());

    var actual = transformer.toV2PutObjectRequest(request, body);
    assertEquals(BUCKET, actual.bucket());
    assertEquals(key, actual.key());
    assertEquals("some-value", actual.metadata().get("some-key"));
    assertEquals("KMS", actual.serverSideEncryption());
    assertEquals(kmsKeyId, actual.serverSideEncryptionKeyId());
  }

  @Test
  void testToV2PutObjectRequestWithoutKmsKey() {
    var key = "some-key";
    var metadata = Map.of("some-key", "some-value");

    var request = UploadRequest.builder().withKey(key).withMetadata(metadata).build();
    com.aliyun.sdk.service.oss2.transport.BinaryData body =
        com.aliyun.sdk.service.oss2.transport.BinaryData.fromBytes("data".getBytes());

    var actual = transformer.toV2PutObjectRequest(request, body);
    assertEquals(BUCKET, actual.bucket());
    assertEquals(key, actual.key());
    assertEquals("some-value", actual.metadata().get("some-key"));
    assertNull(actual.serverSideEncryption());
  }

  @Test
  void testToV2UploadResponse() {
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
  void testToGetObjectRequest() {
    var request =
        DownloadRequest.builder()
            .withKey("some/key/path.file")
            .withVersionId("version-1")
            .withRange(0L, 500L)
            .build();

    var actual = transformer.toGetObjectRequest(request);

    assertEquals(BUCKET, actual.getBucketName());
    assertEquals(request.getKey(), actual.getKey());
    assertEquals("version-1", actual.getVersionId());
    assertEquals(0L, actual.getRange()[0]);
    assertEquals(500L, actual.getRange()[1]);
  }

  @Test
  void testComputeRange() {
    Pair<Long, Long> result = transformer.computeRange(0L, 500L);
    assertEquals(result.getLeft(), 0);
    assertEquals(result.getRight(), 500);

    result = transformer.computeRange(100L, 600L);
    assertEquals(result.getLeft(), 100);
    assertEquals(result.getRight(), 600);

    result = transformer.computeRange(null, 500L);
    assertEquals(result.getLeft(), -1);
    assertEquals(result.getRight(), 500);

    result = transformer.computeRange(500L, null);
    assertEquals(result.getLeft(), 500);
    assertEquals(result.getRight(), -1);
  }

  @Test
  void testToDownloadResponse() {
    OSSObject ossObject = mock(OSSObject.class);
    doReturn("key").when(ossObject).getKey();
    ObjectMetadata objectMetadata = mock(ObjectMetadata.class);
    doReturn(objectMetadata).when(ossObject).getObjectMetadata();
    doReturn("version-1").when(objectMetadata).getVersionId();
    doReturn("etag").when(objectMetadata).getETag();
    Date date = Date.from(Instant.now());
    doReturn(date).when(objectMetadata).getLastModified();
    Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
    doReturn(metadata).when(objectMetadata).getUserMetadata();
    doReturn(100L).when(objectMetadata).getContentLength();

    var actual = transformer.toDownloadResponse(ossObject);

    assertEquals("key", actual.getKey());
    BlobMetadata blobMetadata = actual.getMetadata();
    assertEquals("key", blobMetadata.getKey());
    assertEquals("version-1", blobMetadata.getVersionId());
    assertEquals("etag", blobMetadata.getETag());
    assertEquals(metadata, blobMetadata.getMetadata());
    assertEquals(date.toInstant(), blobMetadata.getLastModified());
    assertEquals(100L, blobMetadata.getObjectSize());
  }

  @Test
  void testToDownloadResponseWithInputStream() {
    OSSObject ossObject = mock(OSSObject.class);
    doReturn("key").when(ossObject).getKey();
    ObjectMetadata objectMetadata = mock(ObjectMetadata.class);
    doReturn(objectMetadata).when(ossObject).getObjectMetadata();
    doReturn("version-1").when(objectMetadata).getVersionId();
    doReturn("etag").when(objectMetadata).getETag();
    Date date = Date.from(Instant.now());
    doReturn(date).when(objectMetadata).getLastModified();
    Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
    doReturn(metadata).when(objectMetadata).getUserMetadata();
    doReturn(100L).when(objectMetadata).getContentLength();

    InputStream inputStream = new ByteArrayInputStream("test content".getBytes());

    var actual = transformer.toDownloadResponse(ossObject, inputStream);

    assertEquals("key", actual.getKey());
    assertEquals(inputStream, actual.getInputStream());
    BlobMetadata blobMetadata = actual.getMetadata();
    assertEquals("key", blobMetadata.getKey());
    assertEquals("version-1", blobMetadata.getVersionId());
    assertEquals("etag", blobMetadata.getETag());
    assertEquals(metadata, blobMetadata.getMetadata());
    assertEquals(date.toInstant(), blobMetadata.getLastModified());
    assertEquals(100L, blobMetadata.getObjectSize());
  }

  @Test
  void testToV2DeleteObjectRequest() {
    var actual = transformer.toV2DeleteObjectRequest("key1", "v1");
    assertEquals(BUCKET, actual.bucket());
    assertEquals("key1", actual.key());
    assertEquals("v1", actual.versionId());

    var actualNoVersion = transformer.toV2DeleteObjectRequest("key2", null);
    assertEquals(BUCKET, actualNoVersion.bucket());
    assertEquals("key2", actualNoVersion.key());
    assertNull(actualNoVersion.versionId());
  }

  @Test
  void testToV2DeleteMultipleObjectsRequest() {
    Collection<BlobIdentifier> objects =
        List.of(
            new BlobIdentifier("key1", "v1"),
            new BlobIdentifier("key2", null),
            new BlobIdentifier("key3", "v3"));

    var actual = transformer.toV2DeleteMultipleObjectsRequest(objects);

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
  void testToV2CopyObjectRequest() {
    CopyRequest request =
        CopyRequest.builder()
            .srcKey("key1")
            .srcVersionId("v1")
            .destBucket("bucket2")
            .destKey("key2")
            .build();

    var actual = transformer.toV2CopyObjectRequest(request);

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
  void testToMetadataRequest() {
    var actual = transformer.toMetadataRequest("key", "v1");
    assertEquals(BUCKET, actual.getBucketName());
    assertEquals("key", actual.getKey());
    assertEquals("v1", actual.getVersionId());
  }

  @Test
  void testToBlobMetadata() {
    ObjectMetadata objectMetadata = mock(ObjectMetadata.class);
    doReturn("version-1").when(objectMetadata).getVersionId();
    doReturn("etag").when(objectMetadata).getETag();
    Date date = Date.from(Instant.now());
    doReturn(date).when(objectMetadata).getLastModified();
    Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
    doReturn(metadata).when(objectMetadata).getUserMetadata();
    doReturn(100L).when(objectMetadata).getContentLength();
    doReturn("5d41402abc4b2a76b9719d911017c592").when(objectMetadata).getContentMD5();

    var actual = transformer.toBlobMetadata("key", objectMetadata);

    assertEquals("key", actual.getKey());
    assertEquals("version-1", actual.getVersionId());
    assertEquals("etag", actual.getETag());
    assertEquals(metadata, actual.getMetadata());
    assertEquals(date.toInstant(), actual.getLastModified());
    assertEquals(100L, actual.getObjectSize());

    byte[] expectedMd5 = {93, 65, 64, 42, -68, 75, 42, 118, -71, 113, -99, -111, 16, 23, -59, -110};
    assertArrayEquals(expectedMd5, actual.getMd5());
  }

  @Test
  void testToInitiateMultipartUploadRequest() {
    Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
    MultipartUploadRequest request =
        new MultipartUploadRequest.Builder().withKey("key").withMetadata(metadata).build();

    var actual = transformer.toInitiateMultipartUploadRequest(request);

    assertEquals(BUCKET, actual.getBucketName());
    assertEquals("key", actual.getKey());
    assertEquals(metadata, actual.getObjectMetadata().getUserMetadata());
  }

  @Test
  void testToMultipartUpload() {
    InitiateMultipartUploadResult initiateMultipartUploadResult =
        mock(InitiateMultipartUploadResult.class);
    doReturn(BUCKET).when(initiateMultipartUploadResult).getBucketName();
    doReturn("key").when(initiateMultipartUploadResult).getKey();
    doReturn("uploadId").when(initiateMultipartUploadResult).getUploadId();
    Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
    MultipartUploadRequest request = new MultipartUploadRequest.Builder()
        .withKey("key").withMetadata(metadata).withContentType("text/plain").build();

    var actual = transformer.toMultipartUpload(initiateMultipartUploadResult, request);

    assertEquals(BUCKET, actual.getBucket());
    assertEquals("key", actual.getKey());
    assertEquals("uploadId", actual.getId());
    assertEquals(metadata, actual.getMetadata());
    assertEquals("text/plain", actual.getContentType());
  }

  @Test
  void testToMultipartUploadWithKms() {
    InitiateMultipartUploadResult initiateMultipartUploadResult =
        mock(InitiateMultipartUploadResult.class);
    doReturn(BUCKET).when(initiateMultipartUploadResult).getBucketName();
    doReturn("key").when(initiateMultipartUploadResult).getKey();
    doReturn("uploadId").when(initiateMultipartUploadResult).getUploadId();
    Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
    String kmsKeyId = "test-kms-key-id";
    MultipartUploadRequest request = new MultipartUploadRequest.Builder()
        .withKey("key").withMetadata(metadata).withKmsKeyId(kmsKeyId).build();

    var actual = transformer.toMultipartUpload(initiateMultipartUploadResult, request);

    assertEquals(BUCKET, actual.getBucket());
    assertEquals("key", actual.getKey());
    assertEquals("uploadId", actual.getId());
    assertEquals(metadata, actual.getMetadata());
    assertEquals(kmsKeyId, actual.getKmsKeyId());
  }

  @Test
  void testToUploadPartRequest() {
    Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
    MultipartUpload mpu =
        MultipartUpload.builder()
            .bucket(BUCKET)
            .key("key")
            .id("uploadId")
            .metadata(metadata)
            .build();
    byte[] content = "Test data".getBytes();
    InputStream inputStream = new ByteArrayInputStream(content);
    MultipartPart mpp = new MultipartPart(1, inputStream, content.length);

    var actual = transformer.toUploadPartRequest(mpu, mpp);

    assertEquals(BUCKET, actual.getBucketName());
    assertEquals("key", actual.getKey());
    assertEquals("uploadId", actual.getUploadId());
    assertEquals(1, actual.getPartNumber());
    assertEquals(inputStream, actual.getInputStream());
    assertEquals(content.length, actual.getPartSize());
  }

  @Test
  void testToUploadPartResponse() {
    byte[] content = "Test data".getBytes();
    InputStream inputStream = new ByteArrayInputStream(content);
    MultipartPart mpp = new MultipartPart(1, inputStream, content.length);
    UploadPartResult uploadPartResult = new UploadPartResult();
    uploadPartResult.setETag("etag");
    uploadPartResult.setPartNumber(1);
    uploadPartResult.setPartSize(content.length);

    var actual = transformer.toUploadPartResponse(mpp, uploadPartResult);

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

    assertEquals(BUCKET, actual.getBucketName());
    assertEquals("key", actual.getKey());
    assertEquals("uploadId", actual.getUploadId());
    var actualParts = actual.getPartETags();
    assertEquals(1, actualParts.get(0).getPartNumber());
    assertEquals("etag1", actualParts.get(0).getETag());
    assertEquals(2, actualParts.get(1).getPartNumber());
    assertEquals("etag2", actualParts.get(1).getETag());
  }

  @Test
  void testToListPartsRequest() {
    MultipartUpload mpu =
        MultipartUpload.builder().bucket(BUCKET).key("key").id("uploadId").build();

    var actual = transformer.toListPartsRequest(mpu);

    assertEquals(BUCKET, actual.getBucketName());
    assertEquals("key", actual.getKey());
    assertEquals("uploadId", actual.getUploadId());
  }

  @Test
  void testToListUploadPartResponse() {
    PartListing partListing = new PartListing();
    var part2 = new PartSummary();
    part2.setETag("etag2");
    part2.setPartNumber(2);
    part2.setSize(50L);
    partListing.addPart(part2); // Intentionally out of order, to verify sort
    var part1 = new PartSummary();
    part1.setETag("etag1");
    part1.setPartNumber(1);
    part1.setSize(100L);
    partListing.addPart(part1);

    var actual = transformer.toListUploadPartResponse(partListing);

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

    assertEquals(BUCKET, actual.getBucketName());
    assertEquals("key", actual.getKey());
    assertEquals("uploadId", actual.getUploadId());
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
  void testToListObjectsRequest() {
    ListBlobsPageRequest request =
        ListBlobsPageRequest.builder()
            .withDelimiter(":")
            .withPrefix("some/prefix/path/thingie")
            .withPaginationToken("next-token")
            .withMaxResults(100)
            .build();

    com.aliyun.oss.model.ListObjectsRequest actual = transformer.toListObjectsRequest(request);
    assertEquals(BUCKET, actual.getBucketName());
    assertEquals(request.getDelimiter(), actual.getDelimiter());
    assertEquals(request.getPrefix(), actual.getPrefix());
    assertEquals(request.getPaginationToken(), actual.getMarker());
    assertEquals(request.getMaxResults(), actual.getMaxKeys());
  }


  @Test
  void testToInitiateMultipartUploadRequestWithContentType() {
    MultipartUploadRequest request =
        new MultipartUploadRequest.Builder()
            .withKey("test-key")
            .withContentType("application/x-directory")
            .build();

    com.aliyun.oss.model.InitiateMultipartUploadRequest result =
        transformer.toInitiateMultipartUploadRequest(request);

    assertEquals("application/x-directory", result.getObjectMetadata().getContentType());
  }

  @Test
  void testToV2PutObjectRequestWithStorageClass() {
    UploadRequest uploadRequest =
        UploadRequest.builder().withKey("test-key").withStorageClass("IA").build();

    com.aliyun.sdk.service.oss2.transport.BinaryData body =
        com.aliyun.sdk.service.oss2.transport.BinaryData.fromBytes("test data".getBytes());
    var result = transformer.toV2PutObjectRequest(uploadRequest, body);

    assertEquals(BUCKET, result.bucket());
    assertEquals("test-key", result.key());
    assertEquals("IA", result.storageClass());
  }

  @Test
  void testToV2PutObjectRequest_WithSha256Checksum() {
    UploadRequest uploadRequest =
        UploadRequest.builder()
            .withKey("test-key")
            .withChecksumValue("abc123sha256value")
            .withChecksumAlgorithm(ChecksumMethod.SHA256)
            .build();

    com.aliyun.sdk.service.oss2.transport.BinaryData body =
        com.aliyun.sdk.service.oss2.transport.BinaryData.fromBytes("data".getBytes());
    var result = transformer.toV2PutObjectRequest(uploadRequest, body);

    assertEquals("abc123sha256value", result.headers().get("x-oss-content-sha256"));
    assertNull(result.headers().get("x-oss-hash-crc64ecma"));
  }

  @Test
  void testToV2PutObjectRequest_WithCrc64Checksum() {
    UploadRequest uploadRequest =
        UploadRequest.builder()
            .withKey("test-key")
            .withChecksumValue("12345678901234")
            .build();

    com.aliyun.sdk.service.oss2.transport.BinaryData body =
        com.aliyun.sdk.service.oss2.transport.BinaryData.fromBytes("data".getBytes());
    var result = transformer.toV2PutObjectRequest(uploadRequest, body);

    assertEquals("12345678901234", result.headers().get("x-oss-hash-crc64ecma"));
    assertNull(result.headers().get("x-oss-content-sha256"));
  }

  @Test
  void testToMultipartUpload_WithChecksumAlgorithm() {
    InitiateMultipartUploadResult initiateMultipartUploadResult =
        mock(InitiateMultipartUploadResult.class);
    doReturn(BUCKET).when(initiateMultipartUploadResult).getBucketName();
    doReturn("key").when(initiateMultipartUploadResult).getKey();
    doReturn("uploadId").when(initiateMultipartUploadResult).getUploadId();
    MultipartUploadRequest request = new MultipartUploadRequest.Builder()
        .withKey("key")
        .withChecksumAlgorithm(ChecksumMethod.SHA256)
        .build();

    var actual = transformer.toMultipartUpload(
        initiateMultipartUploadResult, request);

    assertEquals(BUCKET, actual.getBucket());
    assertEquals("key", actual.getKey());
    assertEquals("uploadId", actual.getId());
    assertEquals(ChecksumMethod.SHA256, actual.getChecksumAlgorithm());
  }

  @Test
  void testToV2PutObjectRequestWithContentType() {
    UploadRequest uploadRequest =
        UploadRequest.builder().withKey("test-key").withContentType("text/plain").build();

    com.aliyun.sdk.service.oss2.transport.BinaryData body =
        com.aliyun.sdk.service.oss2.transport.BinaryData.fromBytes("data".getBytes());
    var result = transformer.toV2PutObjectRequest(uploadRequest, body);

    assertEquals(BUCKET, result.bucket());
    assertEquals("test-key", result.key());
    assertEquals("text/plain", result.contentType());
  }
}
