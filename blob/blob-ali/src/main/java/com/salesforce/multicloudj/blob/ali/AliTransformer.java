package com.salesforce.multicloudj.blob.ali;

import com.aliyun.sdk.service.oss2.PresignOptions;
import com.aliyun.sdk.service.oss2.models.AbortMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.CompleteMultipartUpload;
import com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.CopyObjectRequest;
import com.aliyun.sdk.service.oss2.models.CopyObjectResult;
import com.aliyun.sdk.service.oss2.models.Delete;
import com.aliyun.sdk.service.oss2.models.DeleteMultipleObjectsRequest;
import com.aliyun.sdk.service.oss2.models.DeleteObjectRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectResult;
import com.aliyun.sdk.service.oss2.models.GetObjectTaggingResult;
import com.aliyun.sdk.service.oss2.models.HeadObjectRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectResult;
import com.aliyun.sdk.service.oss2.models.InitiateMultipartUpload;
import com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadResult;
import com.aliyun.sdk.service.oss2.models.ListObjectsV2Request;
import com.aliyun.sdk.service.oss2.models.ListPartsRequest;
import com.aliyun.sdk.service.oss2.models.ListPartsResult;
import com.aliyun.sdk.service.oss2.models.ObjectIdentifier;
import com.aliyun.sdk.service.oss2.models.Part;
import com.aliyun.sdk.service.oss2.models.PutObjectRequest;
import com.aliyun.sdk.service.oss2.models.PutObjectResult;
import com.aliyun.sdk.service.oss2.models.PutObjectTaggingRequest;
import com.aliyun.sdk.service.oss2.models.Tag;
import com.aliyun.sdk.service.oss2.models.TagSet;
import com.aliyun.sdk.service.oss2.models.Tagging;
import com.aliyun.sdk.service.oss2.models.UploadPartRequest;
import com.aliyun.sdk.service.oss2.models.UploadPartResult;
import com.aliyun.sdk.service.oss2.transport.BinaryData;
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

@Getter
public class AliTransformer {

  private static final String SERVER_SIDE_ENCRYPTION_KMS = "KMS";

  private final String bucket;

  public AliTransformer(String bucket) {
    this.bucket = bucket;
  }

  public PutObjectRequest toPutObjectRequest(
      UploadRequest uploadRequest,
      BinaryData body) {
    PutObjectRequest.Builder builder =
        PutObjectRequest.newBuilder()
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
      builder.serverSideEncryption(SERVER_SIDE_ENCRYPTION_KMS);
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
      PutObjectResult result) {
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


  public GetObjectRequest toGetObjectRequest(
      DownloadRequest downloadRequest) {
    GetObjectRequest.Builder builder =
        GetObjectRequest.newBuilder()
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

  public DownloadResponse toDownloadResponse(
      String key, GetObjectResult result) {
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
      String key, GetObjectResult result,
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

  public DeleteObjectRequest toDeleteObjectRequest(
      String key, String versionId) {
    DeleteObjectRequest.Builder builder =
        DeleteObjectRequest.newBuilder()
            .bucket(bucket)
            .key(key);
    if (versionId != null) {
      builder.versionId(versionId);
    }
    return builder.build();
  }

  public DeleteMultipleObjectsRequest
      toDeleteMultipleObjectsRequest(Collection<BlobIdentifier> objects) {
    List<ObjectIdentifier> identifiers = objects.stream()
        .map(obj -> {
          ObjectIdentifier.Builder idBuilder =
              ObjectIdentifier.newBuilder()
                  .key(obj.getKey());
          if (obj.getVersionId() != null) {
            idBuilder.versionId(obj.getVersionId());
          }
          return idBuilder.build();
        })
        .collect(Collectors.toList());
    Delete delete =
        Delete.newBuilder()
            .quiet(true)
            .objects(identifiers)
            .build();
    return DeleteMultipleObjectsRequest.newBuilder()
        .bucket(bucket)
        .delete(delete)
        .build();
  }

  public CopyObjectRequest toCopyObjectRequest(
      CopyRequest request) {
    CopyObjectRequest.Builder builder =
        CopyObjectRequest.newBuilder()
            .bucket(request.getDestBucket())
            .key(request.getDestKey())
            .sourceBucket(bucket)
            .sourceKey(request.getSrcKey());
    if (request.getSrcVersionId() != null) {
      builder.sourceVersionId(request.getSrcVersionId());
    }
    return builder.build();
  }

  public CopyObjectRequest toCopyObjectRequest(
      CopyFromRequest request) {
    CopyObjectRequest.Builder builder =
        CopyObjectRequest.newBuilder()
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
      String destKey, CopyObjectResult result) {
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
      GetObjectTaggingResult result) {
    Tagging tagging = result.tagging();
    if (tagging == null || tagging.tagSet() == null || tagging.tagSet().tags() == null) {
      return Map.of();
    }
    return tagging.tagSet().tags().stream()
        .collect(Collectors.toMap(
            Tag::key,
            Tag::value));
  }

  public PutObjectTaggingRequest toPutObjectTaggingRequest(
      String key, Map<String, String> tags) {
    List<Tag> tagList = tags.entrySet().stream()
        .map(e -> Tag.newBuilder()
            .key(e.getKey())
            .value(e.getValue())
            .build())
        .collect(Collectors.toList());
    TagSet tagSet =
        TagSet.newBuilder()
            .tags(tagList)
            .build();
    Tagging tagging =
        Tagging.newBuilder()
            .tagSet(tagSet)
            .build();
    return PutObjectTaggingRequest.newBuilder()
        .bucket(bucket)
        .key(key)
        .tagging(tagging)
        .build();
  }

  public InitiateMultipartUploadRequest
      toInitiateMultipartUploadRequest(MultipartUploadRequest request) {
    InitiateMultipartUploadRequest.Builder builder =
        InitiateMultipartUploadRequest.newBuilder()
            .bucket(bucket)
            .key(request.getKey());

    if (request.getMetadata() != null && !request.getMetadata().isEmpty()) {
      builder.metadata(request.getMetadata());
    }

    if (StringUtils.isNotEmpty(request.getKmsKeyId())) {
      builder.serverSideEncryption(SERVER_SIDE_ENCRYPTION_KMS);
      builder.serverSideEncryptionKeyId(request.getKmsKeyId());
    }

    if (StringUtils.isNotEmpty(request.getContentType())) {
      builder.contentType(request.getContentType());
    }

    if (request.getTags() != null && !request.getTags().isEmpty()) {
      builder.tagging(encodeTags(request.getTags()));
    }

    return builder.build();
  }

  public MultipartUpload toMultipartUpload(
      InitiateMultipartUploadResult result,
      MultipartUploadRequest request) {
    InitiateMultipartUpload upload =
        result.initiateMultipartUpload();
    return MultipartUpload.builder()
        .bucket(upload.bucket())
        .key(upload.key())
        .id(upload.uploadId())
        .metadata(request.getMetadata())
        .kmsKeyId(request.getKmsKeyId())
        .checksumEnabled(request.isChecksumEnabled())
        .checksumAlgorithm(request.getChecksumAlgorithm())
        .contentType(request.getContentType())
        .build();
  }

  public UploadPartRequest toUploadPartRequest(
      MultipartUpload mpu, MultipartPart mpp) {
    BinaryData body =
        BinaryData.fromStream(
            mpp.getInputStream(), mpp.getContentLength());
    return UploadPartRequest.newBuilder()
        .bucket(bucket)
        .key(mpu.getKey())
        .uploadId(mpu.getId())
        .partNumber((long) mpp.getPartNumber())
        .body(body)
        .contentLength(mpp.getContentLength())
        .build();
  }

  public UploadPartResponse toUploadPartResponse(
      MultipartPart mpp, UploadPartResult result) {
    return new UploadPartResponse(
        mpp.getPartNumber(), stripQuotes(result.eTag()), mpp.getContentLength());
  }

  public CompleteMultipartUploadRequest
      toCompleteMultipartUploadRequest(
          MultipartUpload mpu, List<UploadPartResponse> parts) {
    List<Part> ossParts =
        parts.stream()
            .sorted(Comparator.comparingInt(UploadPartResponse::getPartNumber))
            .map(part -> Part.newBuilder()
                .partNumber((long) part.getPartNumber())
                .eTag(part.getEtag())
                .build())
            .collect(Collectors.toList());
    CompleteMultipartUpload body =
        CompleteMultipartUpload.newBuilder()
            .parts(ossParts)
            .build();
    return CompleteMultipartUploadRequest.newBuilder()
        .bucket(bucket)
        .key(mpu.getKey())
        .uploadId(mpu.getId())
        .completeMultipartUpload(body)
        .build();
  }

  public ListPartsRequest toListPartsRequest(
      MultipartUpload mpu) {
    return ListPartsRequest.newBuilder()
        .bucket(bucket)
        .key(mpu.getKey())
        .uploadId(mpu.getId())
        .build();
  }

  public List<UploadPartResponse> toListUploadPartResponse(
      ListPartsResult result) {
    return result.parts().stream()
        .sorted(Comparator.comparingLong(Part::partNumber))
        .map(part -> new UploadPartResponse(
            part.partNumber().intValue(), stripQuotes(part.eTag()),
            part.size() != null ? part.size() : 0L))
        .collect(Collectors.toList());
  }

  public AbortMultipartUploadRequest
      toAbortMultipartUploadRequest(MultipartUpload mpu) {
    return AbortMultipartUploadRequest.newBuilder()
        .bucket(bucket)
        .key(mpu.getKey())
        .uploadId(mpu.getId())
        .build();
  }

  public PutObjectRequest toPresignedPutObjectRequest(
      PresignedUrlRequest request) {
    PutObjectRequest.Builder builder =
        PutObjectRequest.newBuilder()
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
      builder.serverSideEncryption(SERVER_SIDE_ENCRYPTION_KMS);
      builder.serverSideEncryptionKeyId(request.getKmsKeyId());
    }

    return builder.build();
  }

  public GetObjectRequest toPresignedGetObjectRequest(
      PresignedUrlRequest request) {
    return GetObjectRequest.newBuilder()
        .bucket(bucket)
        .key(request.getKey())
        .build();
  }

  public PresignOptions toPresignOptions(
      PresignedUrlRequest request) {
    return PresignOptions.newBuilder()
        .expiration(request.getDuration())
        .build();
  }

  public ListObjectsV2Request toListObjectsRequest(
      ListBlobsPageRequest request) {
    ListObjectsV2Request.Builder builder =
        ListObjectsV2Request.newBuilder()
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

  public ListObjectsV2Request toListObjectsRequest(
      ListBlobsRequest request, String continuationToken) {
    ListObjectsV2Request.Builder builder =
        ListObjectsV2Request.newBuilder()
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
