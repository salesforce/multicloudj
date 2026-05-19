package com.salesforce.multicloudj.blob.ali;

import com.aliyun.oss.internal.OSSHeaders;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.GenericRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ListPartsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PartListing;
import com.aliyun.oss.model.PartSummary;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import com.aliyun.sdk.service.oss2.models.HeadObjectRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectResult;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ChecksumMethod;
import com.salesforce.multicloudj.blob.driver.CopyFromRequest;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.util.HexUtil;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

@Getter
public class AliTransformer {

  private final String bucket;

  public AliTransformer(String bucket) {
    this.bucket = bucket;
  }

  public com.aliyun.sdk.service.oss2.models.PutObjectRequest toV2PutObjectRequest(
      UploadRequest uploadRequest,
      com.aliyun.sdk.service.oss2.transport.BinaryData body) {
    com.aliyun.sdk.service.oss2.models.PutObjectRequest.Builder builder =
        com.aliyun.sdk.service.oss2.models.PutObjectRequest.newBuilder()
            .bucket(bucket)
            .key(uploadRequest.getKey())
            .body(body);

    if (uploadRequest.getMetadata() != null && !uploadRequest.getMetadata().isEmpty()) {
      builder.metadata(uploadRequest.getMetadata());
    }

    if (uploadRequest.getTags() != null && !uploadRequest.getTags().isEmpty()) {
      builder.tagging(encodeTags(uploadRequest.getTags()));
    }

    if (uploadRequest.getContentLength() > 0) {
      builder.contentLength((int) uploadRequest.getContentLength());
    }

    if (StringUtils.isNotEmpty(uploadRequest.getStorageClass())) {
      builder.storageClass(uploadRequest.getStorageClass());
    }

    if (StringUtils.isNotEmpty(uploadRequest.getKmsKeyId())) {
      builder.serverSideEncryption("KMS");
      builder.serverSideEncryptionKeyId(uploadRequest.getKmsKeyId());
    }

    if (StringUtils.isNotEmpty(uploadRequest.getChecksumValue())) {
      if (uploadRequest.getChecksumAlgorithm() == ChecksumMethod.SHA256) {
        builder.header("x-oss-content-sha256", uploadRequest.getChecksumValue());
      } else {
        builder.header("x-oss-hash-crc64ecma", uploadRequest.getChecksumValue());
      }
    }

    if (StringUtils.isNotEmpty(uploadRequest.getContentType())) {
      builder.contentType(uploadRequest.getContentType());
    }

    return builder.build();
  }

  public UploadResponse toUploadResponse(
      UploadRequest uploadRequest,
      com.aliyun.sdk.service.oss2.models.PutObjectResult result) {
    UploadResponse.UploadResponseBuilder builder =
        UploadResponse.builder()
            .key(uploadRequest.getKey())
            .versionId(result.versionId())
            .eTag(stripQuotes(result.eTag()));

    if (result.hashCrc64ecma() != null) {
      builder.checksumValue(result.hashCrc64ecma());
    }

    return builder.build();
  }

  private String encodeTags(Map<String, String> tags) {
    return tags.entrySet().stream()
        .map(e -> e.getKey() + "=" + e.getValue())
        .collect(Collectors.joining("&"));
  }


  public GetObjectRequest toGetObjectRequest(DownloadRequest downloadRequest) {
    GetObjectRequest request =
        new GetObjectRequest(bucket, downloadRequest.getKey(), downloadRequest.getVersionId());
    if (downloadRequest.getStart() != null || downloadRequest.getEnd() != null) {
      Pair<Long, Long> range = computeRange(downloadRequest.getStart(), downloadRequest.getEnd());
      request.withRange(range.getLeft(), range.getRight());
    }
    return request;
  }

  public com.aliyun.sdk.service.oss2.models.GetObjectRequest toV2GetObjectRequest(
      DownloadRequest downloadRequest) {
    com.aliyun.sdk.service.oss2.models.GetObjectRequest.Builder builder =
        com.aliyun.sdk.service.oss2.models.GetObjectRequest.newBuilder()
            .bucket(bucket)
            .key(downloadRequest.getKey());
    if (downloadRequest.getVersionId() != null) {
      builder.versionId(downloadRequest.getVersionId());
    }
    if (downloadRequest.getStart() != null || downloadRequest.getEnd() != null) {
      builder.range(toHttpRange(downloadRequest.getStart(), downloadRequest.getEnd()));
    }
    return builder.build();
  }

  private String toHttpRange(Long start, Long end) {
    if (start == null) {
      return "bytes=-" + end;
    } else if (end == null) {
      return "bytes=" + start + "-";
    }
    return "bytes=" + start + "-" + end;
  }

  /**
   * Reading the first 500 bytes - computeRange(0, 500) - (0, 500) Reading a middle 500 bytes -
   * computeRange(123, 623) - (123, 623) Reading the last 500 bytes - computeRange(null, 500) - (-1,
   * 500) Reading everything but first 500 bytes - computeRange(500, null) - (500, -1)
   */
  protected Pair<Long, Long> computeRange(Long start, Long end) {
    return new ImmutablePair<>(start == null ? -1 : start, end == null ? -1 : end);
  }

  // OSS does not expose a separate creation timestamp, lastModified is the best available value
  public DownloadResponse toDownloadResponse(OSSObject ossObject) {
    return DownloadResponse.builder()
        .key(ossObject.getKey())
        .metadata(
            BlobMetadata.builder()
                .key(ossObject.getKey())
                .versionId(ossObject.getObjectMetadata().getVersionId())
                .eTag(ossObject.getObjectMetadata().getETag())
                .lastModified(ossObject.getObjectMetadata().getLastModified().toInstant())
                .createdTime(ossObject.getObjectMetadata().getLastModified().toInstant())
                .metadata(ossObject.getObjectMetadata().getUserMetadata())
                .objectSize(ossObject.getObjectMetadata().getContentLength())
                .contentType(ossObject.getObjectMetadata().getContentType())
                .build())
        .build();
  }

  public DownloadResponse toDownloadResponse(OSSObject ossObject, InputStream inputStream) {
    return DownloadResponse.builder()
        .key(ossObject.getKey())
        .metadata(
            BlobMetadata.builder()
                .key(ossObject.getKey())
                .versionId(ossObject.getObjectMetadata().getVersionId())
                .eTag(ossObject.getObjectMetadata().getETag())
                .lastModified(ossObject.getObjectMetadata().getLastModified().toInstant())
                .createdTime(ossObject.getObjectMetadata().getLastModified().toInstant())
                .metadata(ossObject.getObjectMetadata().getUserMetadata())
                .objectSize(ossObject.getObjectMetadata().getContentLength())
                .contentType(ossObject.getObjectMetadata().getContentType())
                .build())
        .inputStream(inputStream)
        .build();
  }

  public DownloadResponse toDownloadResponse(
      String key, com.aliyun.sdk.service.oss2.models.GetObjectResult result) {
    Instant lastModified = parseLastModified(result.lastModified());
    Long contentLength = result.contentLength();
    long objectSize = contentLength != null ? contentLength : 0L;
    return DownloadResponse.builder()
        .key(key)
        .metadata(
            BlobMetadata.builder()
                .key(key)
                .versionId(result.versionId())
                .eTag(stripQuotes(result.eTag()))
                .lastModified(lastModified)
                .createdTime(lastModified)
                .metadata(result.metadata())
                .objectSize(objectSize)
                .contentType(result.contentType())
                .build())
        .build();
  }

  public DownloadResponse toDownloadResponse(
      String key, com.aliyun.sdk.service.oss2.models.GetObjectResult result,
      InputStream inputStream) {
    Instant lastModified = parseLastModified(result.lastModified());
    Long contentLength = result.contentLength();
    long objectSize = contentLength != null ? contentLength : 0L;
    return DownloadResponse.builder()
        .key(key)
        .metadata(
            BlobMetadata.builder()
                .key(key)
                .versionId(result.versionId())
                .eTag(stripQuotes(result.eTag()))
                .lastModified(lastModified)
                .createdTime(lastModified)
                .metadata(result.metadata())
                .objectSize(objectSize)
                .contentType(result.contentType())
                .build())
        .inputStream(inputStream)
        .build();
  }

  public com.aliyun.sdk.service.oss2.models.DeleteObjectRequest toV2DeleteObjectRequest(
      String key, String versionId) {
    com.aliyun.sdk.service.oss2.models.DeleteObjectRequest.Builder builder =
        com.aliyun.sdk.service.oss2.models.DeleteObjectRequest.newBuilder()
            .bucket(bucket)
            .key(key);
    if (versionId != null) {
      builder.versionId(versionId);
    }
    return builder.build();
  }

  public com.aliyun.sdk.service.oss2.models.DeleteMultipleObjectsRequest
      toV2DeleteMultipleObjectsRequest(Collection<BlobIdentifier> objects) {
    List<com.aliyun.sdk.service.oss2.models.ObjectIdentifier> identifiers = objects.stream()
        .map(obj -> {
          com.aliyun.sdk.service.oss2.models.ObjectIdentifier.Builder idBuilder =
              com.aliyun.sdk.service.oss2.models.ObjectIdentifier.newBuilder()
                  .key(obj.getKey());
          if (obj.getVersionId() != null) {
            idBuilder.versionId(obj.getVersionId());
          }
          return idBuilder.build();
        })
        .collect(Collectors.toList());
    com.aliyun.sdk.service.oss2.models.Delete delete =
        com.aliyun.sdk.service.oss2.models.Delete.newBuilder()
            .quiet(true)
            .objects(identifiers)
            .build();
    return com.aliyun.sdk.service.oss2.models.DeleteMultipleObjectsRequest.newBuilder()
        .bucket(bucket)
        .delete(delete)
        .build();
  }

  public com.aliyun.sdk.service.oss2.models.CopyObjectRequest toV2CopyObjectRequest(
      CopyRequest request) {
    com.aliyun.sdk.service.oss2.models.CopyObjectRequest.Builder builder =
        com.aliyun.sdk.service.oss2.models.CopyObjectRequest.newBuilder()
            .bucket(request.getDestBucket())
            .key(request.getDestKey())
            .sourceBucket(bucket)
            .sourceKey(request.getSrcKey());
    if (request.getSrcVersionId() != null) {
      builder.sourceVersionId(request.getSrcVersionId());
    }
    return builder.build();
  }

  public com.aliyun.sdk.service.oss2.models.CopyObjectRequest toV2CopyObjectRequest(
      CopyFromRequest request) {
    com.aliyun.sdk.service.oss2.models.CopyObjectRequest.Builder builder =
        com.aliyun.sdk.service.oss2.models.CopyObjectRequest.newBuilder()
            .bucket(bucket)
            .key(request.getDestKey())
            .sourceBucket(request.getSrcBucket())
            .sourceKey(request.getSrcKey());
    if (request.getSrcVersionId() != null) {
      builder.sourceVersionId(request.getSrcVersionId());
    }
    return builder.build();
  }

  public CopyResponse toCopyResponse(
      String destKey, com.aliyun.sdk.service.oss2.models.CopyObjectResult result) {
    String eTag = result.eTag();
    if (eTag != null) {
      eTag = eTag.replace("\"", "");
    }
    return CopyResponse.builder()
        .key(destKey)
        .versionId(result.versionId())
        .eTag(eTag)
        .lastModified(parseLastModified(result.lastModified()))
        .build();
  }

  public GenericRequest toMetadataRequest(String key, String versionId) {
    return new GenericRequest().withBucketName(bucket).withKey(key).withVersionId(versionId);
  }

  public BlobMetadata toBlobMetadata(String key, ObjectMetadata metadata) {
    long objectSize = metadata.getContentLength();
    Map<String, String> rawMetadata = metadata.getUserMetadata();
    return BlobMetadata.builder()
        .key(key)
        .versionId(metadata.getVersionId())
        .eTag(metadata.getETag())
        .objectSize(objectSize)
        .metadata(rawMetadata)
        .lastModified(metadata.getLastModified().toInstant())
        .createdTime(metadata.getLastModified().toInstant())
        .md5(HexUtil.convertToBytes(metadata.getContentMD5()))
        .contentType(metadata.getContentType())
        .build();
  }

  public HeadObjectRequest toHeadObjectRequest(String key, String versionId) {
    HeadObjectRequest.Builder builder = HeadObjectRequest.newBuilder()
        .bucket(bucket)
        .key(key);
    if (versionId != null) {
      builder.versionId(versionId);
    }
    return builder.build();
  }

  public BlobMetadata toBlobMetadata(String key, HeadObjectResult result) {
    Long contentLength = result.contentLength();
    long objectSize = contentLength != null ? contentLength : 0L;
    Instant lastModified = parseLastModified(result.lastModified());
    return BlobMetadata.builder()
        .key(key)
        .versionId(result.versionId())
        .eTag(stripQuotes(result.eTag()))
        .objectSize(objectSize)
        .metadata(result.metadata())
        .lastModified(lastModified)
        .createdTime(lastModified)
        .md5(HexUtil.convertToBytes(result.contentMd5()))
        .contentType(result.contentType())
        .build();
  }

  private String stripQuotes(String value) {
    if (value == null) {
      return null;
    }
    if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
      return value.substring(1, value.length() - 1);
    }
    return value;
  }

  Instant parseLastModified(String httpDate) {
    if (httpDate == null) {
      return null;
    }
    return ZonedDateTime.parse(httpDate, DateTimeFormatter.RFC_1123_DATE_TIME)
        .toInstant();
  }

  public Map<String, String> toTagMap(
      com.aliyun.sdk.service.oss2.models.GetObjectTaggingResult result) {
    com.aliyun.sdk.service.oss2.models.Tagging tagging = result.tagging();
    if (tagging == null || tagging.tagSet() == null || tagging.tagSet().tags() == null) {
      return Map.of();
    }
    return tagging.tagSet().tags().stream()
        .collect(Collectors.toMap(
            com.aliyun.sdk.service.oss2.models.Tag::key,
            com.aliyun.sdk.service.oss2.models.Tag::value));
  }

  public com.aliyun.sdk.service.oss2.models.PutObjectTaggingRequest toPutObjectTaggingRequest(
      String key, Map<String, String> tags) {
    List<com.aliyun.sdk.service.oss2.models.Tag> tagList = tags.entrySet().stream()
        .map(e -> com.aliyun.sdk.service.oss2.models.Tag.newBuilder()
            .key(e.getKey())
            .value(e.getValue())
            .build())
        .collect(Collectors.toList());
    com.aliyun.sdk.service.oss2.models.TagSet tagSet =
        com.aliyun.sdk.service.oss2.models.TagSet.newBuilder()
            .tags(tagList)
            .build();
    com.aliyun.sdk.service.oss2.models.Tagging tagging =
        com.aliyun.sdk.service.oss2.models.Tagging.newBuilder()
            .tagSet(tagSet)
            .build();
    return com.aliyun.sdk.service.oss2.models.PutObjectTaggingRequest.newBuilder()
        .bucket(bucket)
        .key(key)
        .tagging(tagging)
        .build();
  }

  public InitiateMultipartUploadRequest toInitiateMultipartUploadRequest(
      MultipartUploadRequest request) {
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setUserMetadata(request.getMetadata());

    if (StringUtils.isNotEmpty(request.getKmsKeyId())) {
      metadata.setServerSideEncryption(ObjectMetadata.KMS_SERVER_SIDE_ENCRYPTION);
      metadata.setHeader(OSSHeaders.OSS_SERVER_SIDE_ENCRYPTION_KEY_ID, request.getKmsKeyId());
    }

    // Set content type if provided
    if (StringUtils.isNotEmpty(request.getContentType())) {
      metadata.setContentType(request.getContentType());
    }

    return new InitiateMultipartUploadRequest(getBucket(), request.getKey(), metadata);
  }

  public MultipartUpload toMultipartUpload(
      InitiateMultipartUploadResult initiateMultipartUploadResult,
      MultipartUploadRequest request) {
    return MultipartUpload.builder()
        .bucket(initiateMultipartUploadResult.getBucketName())
        .key(initiateMultipartUploadResult.getKey())
        .id(initiateMultipartUploadResult.getUploadId())
        .metadata(request.getMetadata())
        .kmsKeyId(request.getKmsKeyId())
        .checksumEnabled(request.isChecksumEnabled())
        .checksumAlgorithm(request.getChecksumAlgorithm())
        .contentType(request.getContentType())
        .build();
  }

  public UploadPartRequest toUploadPartRequest(MultipartUpload mpu, MultipartPart mpp) {
    return new UploadPartRequest(
        getBucket(),
        mpu.getKey(),
        mpu.getId(),
        mpp.getPartNumber(),
        mpp.getInputStream(),
        mpp.getContentLength());
  }

  public UploadPartResponse toUploadPartResponse(
      MultipartPart mpp, UploadPartResult uploadPartResult) {
    return new UploadPartResponse(
        mpp.getPartNumber(), uploadPartResult.getPartETag().getETag(), mpp.getContentLength());
  }

  public CompleteMultipartUploadRequest toCompleteMultipartUploadRequest(
      MultipartUpload mpu, List<UploadPartResponse> parts) {
    List<PartETag> completedParts =
        parts.stream()
            .sorted(Comparator.comparingInt(UploadPartResponse::getPartNumber))
            .map(part -> new PartETag(part.getPartNumber(), part.getEtag()))
            .collect(Collectors.toList());
    return new CompleteMultipartUploadRequest(
        getBucket(), mpu.getKey(), mpu.getId(), completedParts);
  }

  public ListPartsRequest toListPartsRequest(MultipartUpload mpu) {
    return new ListPartsRequest(bucket, mpu.getKey(), mpu.getId());
  }

  public List<UploadPartResponse> toListUploadPartResponse(PartListing partListing) {
    return partListing.getParts().stream()
        .sorted(Comparator.comparingInt(PartSummary::getPartNumber))
        .map((part) -> new UploadPartResponse(part.getPartNumber(), part.getETag(), part.getSize()))
        .collect(Collectors.toList());
  }

  public AbortMultipartUploadRequest toAbortMultipartUploadRequest(MultipartUpload mpu) {
    return new AbortMultipartUploadRequest(bucket, mpu.getKey(), mpu.getId());
  }

  public com.aliyun.sdk.service.oss2.models.PutObjectRequest toPresignedPutObjectRequest(
      PresignedUrlRequest request) {
    com.aliyun.sdk.service.oss2.models.PutObjectRequest.Builder builder =
        com.aliyun.sdk.service.oss2.models.PutObjectRequest.newBuilder()
            .bucket(bucket)
            .key(request.getKey());

    Map<String, String> userMetadata = request.getMetadata();
    if (userMetadata != null && !userMetadata.isEmpty()) {
      builder.metadata(userMetadata);
    }

    if (request.getTags() != null && !request.getTags().isEmpty()) {
      builder.tagging(encodeTags(request.getTags()));
    }

    if (StringUtils.isNotEmpty(request.getKmsKeyId())) {
      builder.serverSideEncryption("KMS");
      builder.serverSideEncryptionKeyId(request.getKmsKeyId());
    }

    return builder.build();
  }

  public com.aliyun.sdk.service.oss2.models.GetObjectRequest toPresignedGetObjectRequest(
      PresignedUrlRequest request) {
    return com.aliyun.sdk.service.oss2.models.GetObjectRequest.newBuilder()
        .bucket(bucket)
        .key(request.getKey())
        .build();
  }

  public com.aliyun.sdk.service.oss2.PresignOptions toPresignOptions(
      PresignedUrlRequest request) {
    return com.aliyun.sdk.service.oss2.PresignOptions.newBuilder()
        .expiration(request.getDuration())
        .build();
  }

  public com.aliyun.sdk.service.oss2.models.ListObjectsV2Request toV2ListObjectsRequest(
      ListBlobsPageRequest request) {
    com.aliyun.sdk.service.oss2.models.ListObjectsV2Request.Builder builder =
        com.aliyun.sdk.service.oss2.models.ListObjectsV2Request.newBuilder()
            .bucket(bucket);

    if (request.getPrefix() != null) {
      builder.prefix(request.getPrefix());
    }

    if (request.getDelimiter() != null) {
      builder.delimiter(request.getDelimiter());
    }

    if (request.getPaginationToken() != null) {
      builder.continuationToken(request.getPaginationToken());
    }

    if (request.getMaxResults() != null) {
      builder.maxKeys(request.getMaxResults().longValue());
    }

    return builder.build();
  }

  public com.aliyun.sdk.service.oss2.models.ListObjectsV2Request toV2ListObjectsRequest(
      ListBlobsRequest request, String continuationToken) {
    com.aliyun.sdk.service.oss2.models.ListObjectsV2Request.Builder builder =
        com.aliyun.sdk.service.oss2.models.ListObjectsV2Request.newBuilder()
            .bucket(bucket);

    if (request != null && request.getPrefix() != null) {
      builder.prefix(request.getPrefix());
    }

    if (request != null && request.getDelimiter() != null) {
      builder.delimiter(request.getDelimiter());
    }

    if (continuationToken != null) {
      builder.continuationToken(continuationToken);
    }

    return builder.build();
  }
}
