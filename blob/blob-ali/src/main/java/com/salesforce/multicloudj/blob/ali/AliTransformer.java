package com.salesforce.multicloudj.blob.ali;

import com.aliyun.sdk.service.oss2.PresignOptions;
import com.aliyun.sdk.service.oss2.models.AbortMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.CommonPrefix;
import com.aliyun.sdk.service.oss2.models.CompleteMultipartUpload;
import com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.CopyObjectRequest;
import com.aliyun.sdk.service.oss2.models.CopyObjectResult;
import com.aliyun.sdk.service.oss2.models.Delete;
import com.aliyun.sdk.service.oss2.models.DeleteMultipleObjectsRequest;
import com.aliyun.sdk.service.oss2.models.DeleteObjectRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectLegalHoldRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectLegalHoldResult;
import com.aliyun.sdk.service.oss2.models.GetObjectRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectResult;
import com.aliyun.sdk.service.oss2.models.GetObjectRetentionRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectRetentionResult;
import com.aliyun.sdk.service.oss2.models.GetObjectTaggingResult;
import com.aliyun.sdk.service.oss2.models.HeadObjectRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectResult;
import com.aliyun.sdk.service.oss2.models.InitiateMultipartUpload;
import com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadResult;
import com.aliyun.sdk.service.oss2.models.LegalHold;
import com.aliyun.sdk.service.oss2.models.ListObjectsV2Request;
import com.aliyun.sdk.service.oss2.models.ListObjectsV2Result;
import com.aliyun.sdk.service.oss2.models.ListPartsRequest;
import com.aliyun.sdk.service.oss2.models.ListPartsResult;
import com.aliyun.sdk.service.oss2.models.ObjectIdentifier;
import com.aliyun.sdk.service.oss2.models.ObjectLegalHoldStatusType;
import com.aliyun.sdk.service.oss2.models.ObjectRetentionModeType;
import com.aliyun.sdk.service.oss2.models.Part;
import com.aliyun.sdk.service.oss2.models.PutObjectLegalHoldRequest;
import com.aliyun.sdk.service.oss2.models.PutObjectRequest;
import com.aliyun.sdk.service.oss2.models.PutObjectResult;
import com.aliyun.sdk.service.oss2.models.PutObjectRetentionRequest;
import com.aliyun.sdk.service.oss2.models.PutObjectTaggingRequest;
import com.aliyun.sdk.service.oss2.models.Retention;
import com.aliyun.sdk.service.oss2.models.Tag;
import com.aliyun.sdk.service.oss2.models.TagSet;
import com.aliyun.sdk.service.oss2.models.Tagging;
import com.aliyun.sdk.service.oss2.models.UploadPartRequest;
import com.aliyun.sdk.service.oss2.models.UploadPartResult;
import com.aliyun.sdk.service.oss2.retry.BackoffDelayer;
import com.aliyun.sdk.service.oss2.retry.EqualJitterBackoff;
import com.aliyun.sdk.service.oss2.retry.FixedDelayBackoff;
import com.aliyun.sdk.service.oss2.retry.Retryer;
import com.aliyun.sdk.service.oss2.retry.StandardRetryer;
import com.aliyun.sdk.service.oss2.transport.BinaryData;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ChecksumMethod;
import com.salesforce.multicloudj.blob.driver.CopyFromRequest;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsBatch;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.ObjectLockInfo;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.RetentionMode;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.common.util.HexUtil;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
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
        .objectLockInfo(extractObjectLockInfo(result.headers()))
        .build();
  }

  /**
   * OSS exposes per-object WORM (retention + legal hold) state on HeadObject as response headers
   * (verified live against a versioned + WORM-enabled bucket): {@code x-oss-object-worm-mode},
   * {@code x-oss-object-worm-retain-until-date} (ISO-8601), and
   * {@code x-oss-object-worm-legal-hold} (value <code>"ON"</code>/absent). This single-call read
   * avoids the latency of a follow-up {@code GetObjectRetention} + {@code GetObjectLegalHold}
   * round-trip pair.
   *
   * @return an {@link ObjectLockInfo} when any of the three worm headers is present; otherwise
   *     {@code null} so {@code BlobMetadata.objectLockInfo} stays unset on objects without lock
   *     state.
   */
  private static ObjectLockInfo extractObjectLockInfo(Map<String, String> headers) {
    if (headers == null || headers.isEmpty()) {
      return null;
    }
    String wormMode = caseInsensitiveGet(headers, "x-oss-object-worm-mode");
    String wormRetainUntil = caseInsensitiveGet(headers, "x-oss-object-worm-retain-until-date");
    String wormLegalHold = caseInsensitiveGet(headers, "x-oss-object-worm-legal-hold");
    if (wormMode == null && wormRetainUntil == null && wormLegalHold == null) {
      return null;
    }
    RetentionMode mode = null;
    if (wormMode != null) {
      try {
        mode = RetentionMode.valueOf(wormMode.toUpperCase(java.util.Locale.ROOT));
      } catch (IllegalArgumentException ignored) {
        // Unknown OSS worm-mode value — leave mode null rather than fail the metadata read.
        mode = null;
      }
    }
    Instant retainUntil = null;
    if (wormRetainUntil != null) {
      try {
        retainUntil = Instant.parse(wormRetainUntil);
      } catch (java.time.format.DateTimeParseException ignored) {
        retainUntil = null;
      }
    }
    boolean legalHold = "ON".equalsIgnoreCase(wormLegalHold);
    return ObjectLockInfo.builder()
        .mode(mode)
        .retainUntilDate(retainUntil)
        .legalHold(legalHold)
        // useEventBasedHold left null — OSS has no event-based-hold concept.
        .build();
  }

  private static String caseInsensitiveGet(Map<String, String> headers, String name) {
    String exact = headers.get(name);
    if (exact != null) {
      return exact;
    }
    for (Map.Entry<String, String> e : headers.entrySet()) {
      if (e.getKey() != null && e.getKey().equalsIgnoreCase(name)) {
        return e.getValue();
      }
    }
    return null;
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

  /**
   * OSS computes a CRC64-ECMA checksum over uploaded objects and exposes no CRC32C or SHA256
   * object checksum. A {@code null} algorithm means "use the substrate-native default" (CRC64 on
   * OSS) and is allowed; any other explicit algorithm is rejected up front so callers receive a
   * clear failure rather than a checksum value labeled with an algorithm OSS did not produce.
   */
  public static void rejectUnsupportedChecksum(ChecksumMethod algorithm) {
    if (algorithm != null && algorithm != ChecksumMethod.CRC64) {
      throw new UnSupportedOperationException(
          algorithm + " checksum is not supported by Ali OSS. Use CRC64.");
    }
  }

  public MultipartUpload toMultipartUpload(
      InitiateMultipartUploadResult result,
      MultipartUploadRequest request) {
    InitiateMultipartUpload upload =
        result.initiateMultipartUpload();
    // OSS's native object checksum is CRC64-ECMA. When checksumming is enabled without an explicit
    // algorithm, resolve the substrate-native default (CRC64) so the stored algorithm honestly
    // reflects what OSS will return on completion. Unsupported explicit algorithms are rejected
    // upstream at doInitiateMultipartUpload.
    ChecksumMethod algorithm = request.getChecksumAlgorithm();
    if (algorithm == null && request.isChecksumEnabled()) {
      algorithm = ChecksumMethod.CRC64;
    }
    return MultipartUpload.builder()
        .bucket(upload.bucket())
        .key(upload.key())
        .id(upload.uploadId())
        .metadata(request.getMetadata())
        .kmsKeyId(request.getKmsKeyId())
        .checksumEnabled(request.isChecksumEnabled())
        .checksumAlgorithm(algorithm)
        .contentType(request.getContentType())
        .objectLock(request.getObjectLock())
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

    if (request.getContentLength() > 0) {
      if (request.getContentLength() > Integer.MAX_VALUE) {
        throw new InvalidArgumentException(
            "contentLength exceeds maximum supported by Ali OSS ("
                + Integer.MAX_VALUE + " bytes)");
      }
      builder.contentLength((int) request.getContentLength());
    }

    if (StringUtils.isNotEmpty(request.getContentType())) {
      builder.contentType(request.getContentType());
    }

    if (StringUtils.isNotEmpty(request.getChecksumValue())) {
      ChecksumMethod algo = request.getChecksumAlgorithm() != null
          ? request.getChecksumAlgorithm() : ChecksumMethod.CRC32C;
      if (algo == ChecksumMethod.SHA256) {
        builder.header("x-oss-content-sha256", request.getChecksumValue());
      } else {
        builder.header("x-oss-hash-crc64ecma", request.getChecksumValue());
      }
    }

    return builder.build();
  }

  public GetObjectRequest toPresignedGetObjectRequest(
      PresignedUrlRequest request) {
    GetObjectRequest.Builder builder =
        GetObjectRequest.newBuilder()
            .bucket(bucket)
            .key(request.getKey());

    // OSS treats responseContentDisposition as a response-header override that is folded into the
    // signed presigned URL; on download OSS returns it as the Content-Disposition response header.
    if (StringUtils.isNotEmpty(request.getContentDisposition())) {
      builder.responseContentDisposition(request.getContentDisposition());
    }

    return builder.build();
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

  public ListBlobsPageResponse toListBlobsPageResponse(ListObjectsV2Result result) {
    ListBlobsBatch batch = toListBlobsBatch(result);
    return new ListBlobsPageResponse(
        batch.getBlobs(), batch.getCommonPrefixes(),
        Boolean.TRUE.equals(result.isTruncated()),
        result.nextContinuationToken());
  }

  public ListBlobsBatch toListBlobsBatch(ListObjectsV2Result result) {
    List<BlobInfo> blobs = result.contents().stream()
        .map(obj -> new BlobInfo.Builder()
            .withKey(obj.key())
            .withObjectSize(obj.size() != null ? obj.size() : 0L)
            .withLastModified(obj.lastModified())
            .build())
        .collect(Collectors.toList());

    List<String> commonPrefixes = result.commonPrefixes() != null
        ? result.commonPrefixes().stream()
            .map(CommonPrefix::prefix)
            .collect(Collectors.toList())
        : List.of();

    return new ListBlobsBatch(blobs, commonPrefixes);
  }

  /**
   * Converts a cloud-agnostic {@link RetryConfig} to an Ali OSS v2 SDK {@link Retryer}.
   *
   * <p>Mapping:
   * <ul>
   *   <li>EXPONENTIAL mode → {@link EqualJitterBackoff} with baseDelay and maxBackoff</li>
   *   <li>FIXED mode → {@link FixedDelayBackoff} with constant delay</li>
   *   <li>maxAttempts → {@link StandardRetryer.Builder#maxAttempts(Integer)}</li>
   * </ul>
   *
   * <p>Note: The Ali OSS v2 SDK does not support a totalTimeout equivalent (a hard deadline
   * across all retry attempts combined). This field from RetryConfig is not applied. The
   * attemptTimeout is handled separately via readWriteTimeout on the client builder.
   * The multiplier field is not directly configurable — EqualJitterBackoff uses 2x internally.
   */
  public static Retryer toAliRetryer(RetryConfig config) {
    if (config == null) {
      throw new InvalidArgumentException("RetryConfig cannot be null");
    }

    StandardRetryer.Builder builder = StandardRetryer.newBuilder();

    if (config.getMaxAttempts() != null) {
      if (config.getMaxAttempts() <= 0) {
        throw new InvalidArgumentException(
            "RetryConfig.maxAttempts must be greater than 0, got: "
                + config.getMaxAttempts());
      }
      builder.maxAttempts(config.getMaxAttempts());
    }

    if (config.getMode() != null) {
      BackoffDelayer backoff;
      if (config.getMode() == RetryConfig.Mode.EXPONENTIAL) {
        if (config.getInitialDelayMillis() <= 0) {
          throw new InvalidArgumentException(
              "RetryConfig.initialDelayMillis must be greater than 0 for EXPONENTIAL mode, got: "
                  + config.getInitialDelayMillis());
        }
        if (config.getMaxDelayMillis() <= 0) {
          throw new InvalidArgumentException(
              "RetryConfig.maxDelayMillis must be greater than 0 for EXPONENTIAL mode, got: "
                  + config.getMaxDelayMillis());
        }
        backoff = new EqualJitterBackoff(
            Duration.ofMillis(config.getInitialDelayMillis()),
            Duration.ofMillis(config.getMaxDelayMillis()));
      } else {
        if (config.getFixedDelayMillis() <= 0) {
          throw new InvalidArgumentException(
              "RetryConfig.fixedDelayMillis must be greater than 0 for FIXED mode, got: "
                  + config.getFixedDelayMillis());
        }
        backoff = new FixedDelayBackoff(
            Duration.ofMillis(config.getFixedDelayMillis()));
      }
      builder.backoffDelayer(backoff);
    }

    return builder.build();
  }

  public List<List<BlobInfo>> partitionList(List<BlobInfo> blobInfos, int partitionSize) {
    List<List<BlobInfo>> partitionedList = new ArrayList<>();
    int listSize = blobInfos.size();

    for (int i = 0; i < listSize; i += partitionSize) {
      int endIndex = Math.min(i + partitionSize, listSize);
      partitionedList.add(new ArrayList<>(blobInfos.subList(i, endIndex)));
    }
    return partitionedList;
  }

  public List<BlobIdentifier> toBlobIdentifiers(List<BlobInfo> blobList) {
    return blobList.stream()
        .map(blob -> new BlobIdentifier(blob.getKey(), null))
        .collect(Collectors.toList());
  }

  public GetObjectRetentionRequest toGetObjectRetentionRequest(
      String key, String versionId) {
    GetObjectRetentionRequest.Builder builder = GetObjectRetentionRequest.newBuilder()
        .bucket(bucket)
        .key(key);
    if (versionId != null) {
      builder.versionId(versionId);
    }
    return builder.build();
  }

  public GetObjectLegalHoldRequest toGetObjectLegalHoldRequest(
      String key, String versionId) {
    GetObjectLegalHoldRequest.Builder builder = GetObjectLegalHoldRequest.newBuilder()
        .bucket(bucket)
        .key(key);
    if (versionId != null) {
      builder.versionId(versionId);
    }
    return builder.build();
  }

  public ObjectLockInfo toObjectLockInfo(
      GetObjectRetentionResult retentionResult,
      GetObjectLegalHoldResult legalHoldResult) {
    RetentionMode mode = null;
    Instant retainUntilDate = null;

    if (retentionResult != null && retentionResult.retention() != null) {
      Retention retention = retentionResult.retention();
      mode = toRetentionMode(
          ObjectRetentionModeType.fromString(retention.mode()));
      if (retention.retainUntilDate() != null) {
        retainUntilDate = Instant.parse(retention.retainUntilDate());
      }
    }

    boolean legalHold = false;
    if (legalHoldResult != null && legalHoldResult.legalHold() != null) {
      legalHold = ObjectLegalHoldStatusType.ON.toString()
          .equalsIgnoreCase(legalHoldResult.legalHold().status());
    }

    return ObjectLockInfo.builder()
        .mode(mode)
        .retainUntilDate(retainUntilDate)
        .legalHold(legalHold)
        .build();
  }

  public PutObjectRetentionRequest toPutObjectRetentionRequest(
      String key, String versionId, RetentionMode mode,
      Instant retainUntilDate, Boolean bypassGovernance) {
    Retention retention = Retention.newBuilder()
        .mode(toOssRetentionMode(mode))
        .retainUntilDate(
            DateTimeFormatter.ISO_INSTANT.format(retainUntilDate))
        .build();

    PutObjectRetentionRequest.Builder builder =
        PutObjectRetentionRequest.newBuilder()
            .bucket(bucket)
            .key(key)
            .retention(retention);
    if (versionId != null) {
      builder.versionId(versionId);
    }
    if (Boolean.TRUE.equals(bypassGovernance)) {
      builder.bypassGovernanceRetention(true);
    }
    return builder.build();
  }

  public PutObjectLegalHoldRequest toPutObjectLegalHoldRequest(
      String key, String versionId, boolean legalHold) {
    LegalHold hold = LegalHold.newBuilder()
        .status(legalHold
            ? ObjectLegalHoldStatusType.ON
            : ObjectLegalHoldStatusType.OFF)
        .build();

    PutObjectLegalHoldRequest.Builder builder =
        PutObjectLegalHoldRequest.newBuilder()
            .bucket(bucket)
            .key(key)
            .legalHold(hold);
    if (versionId != null) {
      builder.versionId(versionId);
    }
    return builder.build();
  }

  public static RetentionMode toRetentionMode(ObjectRetentionModeType ossMode) {
    switch (ossMode) {
      case GOVERNANCE:
        return RetentionMode.GOVERNANCE;
      case COMPLIANCE:
        return RetentionMode.COMPLIANCE;
      default:
        return null;
    }
  }

  public static ObjectRetentionModeType toOssRetentionMode(RetentionMode mode) {
    switch (mode) {
      case GOVERNANCE:
        return ObjectRetentionModeType.GOVERNANCE;
      case COMPLIANCE:
        return ObjectRetentionModeType.COMPLIANCE;
      default:
        throw new InvalidArgumentException("Unsupported retention mode: " + mode);
    }
  }
}
