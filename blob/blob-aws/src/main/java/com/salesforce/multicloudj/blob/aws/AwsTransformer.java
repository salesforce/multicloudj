package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.blob.aws.async.S3LoggingTransferListener;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ChecksumMethod;
import com.salesforce.multicloudj.blob.driver.CopyFromRequest;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.FailedBlobDownload;
import com.salesforce.multicloudj.blob.driver.FailedBlobUpload;
import com.salesforce.multicloudj.blob.driver.ListBlobsBatch;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.MultipartUploadResponse;
import com.salesforce.multicloudj.blob.driver.ObjectLockConfiguration;
import com.salesforce.multicloudj.blob.driver.ObjectLockInfo;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.PresignedUrlResponse;
import com.salesforce.multicloudj.blob.driver.RetentionMode;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.common.util.HexUtil;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.awscore.presigner.PresignedRequest;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.retries.AdaptiveRetryStrategy;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectLegalHoldRequest;
import software.amazon.awssdk.services.s3.model.GetObjectLegalHoldResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRetentionRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRetentionResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHold;
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHoldStatus;
import software.amazon.awssdk.services.s3.model.ObjectLockMode;
import software.amazon.awssdk.services.s3.model.ObjectLockRetention;
import software.amazon.awssdk.services.s3.model.ObjectLockRetentionMode;
import software.amazon.awssdk.services.s3.model.PutObjectLegalHoldRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRetentionRequest;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.transfer.s3.config.DownloadFilter;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryDownload;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryUpload;
import software.amazon.awssdk.transfer.s3.model.DownloadDirectoryRequest;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.UploadDirectoryRequest;

public class AwsTransformer {

  /**
   * Object-metadata key under which the SDK persists the operation correlation id during upload,
   * so the value is stored on the blob (as {@code x-amz-meta-correlation-id} in S3) and matches
   * the correlation id that appears in the same upload's logs and trace span.
   */
  public static final String CORRELATION_ID_METADATA_KEY = "correlation-id";

  /** Default MIME type used for the request body when the caller does not provide one. */
  private static final String OCTET_STREAM_MIME = "application/octet-stream";

  private final String bucket;

  public AwsTransformer(String bucket) {
    this.bucket = bucket;
  }

  public String getBucket() {
    return bucket;
  }

  private ChecksumAlgorithm toAwsChecksumAlgorithm(
      ChecksumMethod checksumMethod) {
    switch (checksumMethod) {
      case CRC32C:
        return ChecksumAlgorithm.CRC32_C;
      case SHA256:
        return ChecksumAlgorithm.SHA256;
      default:
        throw new InvalidArgumentException("Unsupported checksum algorithm: " + checksumMethod);
    }
  }

  public ListBlobsBatch toBatch(ListObjectsV2Response response) {
    List<BlobInfo> blobs =
        response.contents().stream().map(this::toInfo).collect(Collectors.toList());

    List<String> prefixes =
        response.commonPrefixes().stream().map(CommonPrefix::prefix).collect(Collectors.toList());

    return new ListBlobsBatch(blobs, prefixes);
  }

  public BlobInfo toInfo(S3Object s3) {
    return new BlobInfo.Builder()
        .withKey(s3.key())
        .withObjectSize(s3.size())
        .withLastModified(s3.lastModified())
        .build();
  }

  public ListObjectsV2Request toRequest(ListBlobsRequest request) {
    return ListObjectsV2Request.builder()
        .bucket(getBucket())
        .delimiter(request.getDelimiter())
        .prefix(request.getPrefix())
        .build();
  }

  public ListObjectsV2Request toRequest(ListBlobsPageRequest request) {
    ListObjectsV2Request.Builder builder =
        ListObjectsV2Request.builder()
            .bucket(getBucket())
            .delimiter(request.getDelimiter())
            .prefix(request.getPrefix());

    if (request.getMaxResults() != null) {
      builder.maxKeys(request.getMaxResults());
    }

    if (request.getPaginationToken() != null) {
      builder.continuationToken(request.getPaginationToken());
    }

    return builder.build();
  }

  public AsyncRequestBody toAsyncRequestBody(UploadRequest uploadRequest, InputStream inputStream) {
    Long contentLength =
        uploadRequest.getContentLength() > 0 ? uploadRequest.getContentLength() : null;
    return AsyncRequestBody.fromInputStream(
        inputStream, contentLength, Executors.newSingleThreadExecutor());
  }

  /**
   * Builds a sync {@link RequestBody} for an {@link InputStream} upload, honouring the optional
   * {@code contentLength} on {@link UploadRequest}. When {@code contentLength} is unspecified
   * (i.e. not positive), an unknown-length {@link ContentStreamProvider}-based body is returned;
   * the AWS SDK will buffer chunks internally to support retries.
   */
  public RequestBody toRequestBody(UploadRequest uploadRequest, InputStream inputStream) {
    if (uploadRequest.getContentLength() > 0) {
      return RequestBody.fromInputStream(inputStream, uploadRequest.getContentLength());
    }
    return RequestBody.fromContentProvider(
        ContentStreamProvider.fromInputStream(inputStream), OCTET_STREAM_MIME);
  }

  public PutObjectRequest toRequest(UploadRequest request) {
    List<Tag> tags =
        request.getTags().entrySet().stream()
            .map(entry -> Tag.builder().key(entry.getKey()).value(entry.getValue()).build())
            .collect(Collectors.toList());

    // Copy the application-supplied metadata and stamp the SDK's correlation id onto the
    // stored object so it persists in S3 alongside the user's metadata. Skipped when the
    // request carries no operation context, or when the app has supplied the same key
    // explicitly.
    Map<String, String> metadata = new HashMap<>(request.getMetadata());
    if (request.getOperationContext() != null
        && StringUtils.isNotBlank(request.getOperationContext().getCorrelationId())
        && !metadata.containsKey(CORRELATION_ID_METADATA_KEY)) {
      metadata.put(CORRELATION_ID_METADATA_KEY, request.getOperationContext().getCorrelationId());
    }

    PutObjectRequest.Builder builder =
        PutObjectRequest.builder()
            .bucket(getBucket())
            .key(request.getKey())
            .metadata(metadata)
            .tagging(Tagging.builder().tagSet(tags).build());

    if (StringUtils.isNotEmpty(request.getKmsKeyId())) {
      builder.serverSideEncryption(ServerSideEncryption.AWS_KMS).ssekmsKeyId(request.getKmsKeyId());
    } else if (request.isUseKmsManagedKey()) {
      builder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
    }
    // else: no SSE headers; S3 applies bucket default encryption

    // Set storage class if provided
    if (StringUtils.isNotEmpty(request.getStorageClass())) {
      try {
        StorageClass awsStorageClass = StorageClass.fromValue(request.getStorageClass());
        builder.storageClass(awsStorageClass);
      } catch (IllegalArgumentException e) {
        throw new InvalidArgumentException(
            "Invalid storage class: " + request.getStorageClass(), e);
      }
    }

    // Set content length if provided (required for presigned URL constraint enforcement)
    if (request.getContentLength() > 0) {
      builder.contentLength(request.getContentLength());
    }

    // Set object lock if provided
    if (request.getObjectLock() != null) {
      applyObjectLockToPutObjectBuilder(builder, request.getObjectLock());
    }

    // Set checksum if provided
    if (StringUtils.isNotEmpty(request.getChecksumValue())
        && request.getChecksumAlgorithm() != null) {
      ChecksumMethod algo = request.getChecksumAlgorithm();
      if (algo == ChecksumMethod.MD5) {
        // MD5 is sent as the classic RFC 1864 Content-MD5 header (server-validated; mismatch ->
        // BadDigest), not via the x-amz-checksum-* "additional checksum" path. The two mechanisms
        // are mutually exclusive, so checksumAlgorithm() is intentionally not set here.
        builder.contentMD5(request.getChecksumValue());
      } else if (algo == ChecksumMethod.SHA256) {
        builder.checksumAlgorithm(toAwsChecksumAlgorithm(algo));
        builder.checksumSHA256(request.getChecksumValue());
      } else {
        builder.checksumAlgorithm(toAwsChecksumAlgorithm(algo));
        builder.checksumCRC32C(request.getChecksumValue());
      }
    }

    // Set content type if provided
    if (StringUtils.isNotEmpty(request.getContentType())) {
      builder.contentType(request.getContentType());
    }

    return builder.build();
  }

  /**
   * Converts SDK RetentionMode to provider SDK ObjectLockMode
   */
  private ObjectLockMode toAwsObjectLockMode(RetentionMode mode) {
    switch (mode) {
      case GOVERNANCE:
        return ObjectLockMode.GOVERNANCE;
      case COMPLIANCE:
        return ObjectLockMode.COMPLIANCE;
      default:
        throw new InvalidArgumentException("Unknown retention mode: " + mode);
    }
  }

  private void applyObjectLockToPutObjectBuilder(
      PutObjectRequest.Builder builder, ObjectLockConfiguration lockConfig) {
    if (lockConfig.getMode() != null) {
      builder.objectLockMode(toAwsObjectLockMode(lockConfig.getMode()));
    }
    if (lockConfig.getRetainUntilDate() != null) {
      builder.objectLockRetainUntilDate(lockConfig.getRetainUntilDate());
    }
    if (lockConfig.isLegalHold()) {
      builder.objectLockLegalHoldStatus(ObjectLockLegalHoldStatus.ON);
    }
  }

  /**
   * Converts provider SDK ObjectLockMode to SDK RetentionMode
   */
  private RetentionMode toDriverRetentionMode(ObjectLockMode awsMode) {
    if (awsMode == null) {
      return null;
    }
    switch (awsMode) {
      case GOVERNANCE:
        return RetentionMode.GOVERNANCE;
      case COMPLIANCE:
        return RetentionMode.COMPLIANCE;
      default:
        throw new FailedPreconditionException("Unknown object lock mode: " + awsMode);
    }
  }

  /**
   * Converts provider SDK ObjectLockRetentionMode to SDK RetentionMode
   */
  private RetentionMode toDriverRetentionMode(ObjectLockRetentionMode awsMode) {
    if (awsMode == null) {
      return null;
    }
    switch (awsMode) {
      case GOVERNANCE:
        return RetentionMode.GOVERNANCE;
      case COMPLIANCE:
        return RetentionMode.COMPLIANCE;
      default:
        throw new FailedPreconditionException("Unknown object lock retention mode: " + awsMode);
    }
  }

  public GetObjectRequest toRequest(DownloadRequest request) {
    var builder =
        GetObjectRequest.builder()
            .bucket(getBucket())
            .key(request.getKey())
            .versionId(request.getVersionId());

    if (request.getStart() != null || request.getEnd() != null) {
      builder.range(createRangeString(request.getStart(), request.getEnd()));
    }
    return builder.build();
  }

  /**
   * Builds a {@link DownloadFileRequest} for use with {@code S3TransferManager.downloadFile}.
   */
  public DownloadFileRequest toRequest(DownloadRequest request, Path destinationPath) {
    return DownloadFileRequest.builder()
        .getObjectRequest(toRequest(request))
        .destination(destinationPath)
        .build();
  }

  /**
   * Reading the first 500 bytes - createRangeString(0, 500) - "bytes=0-500" Reading a middle 500
   * bytes - createRangeString(123, 623) - "bytes=123-623" Reading the last 500 bytes -
   * createRangeString(null, 500) - "bytes=-500" Reading everything but first 500 bytes -
   * createRangeString(500, null) - "bytes=500-"
   */
  protected String createRangeString(Long start, Long end) {
    return "bytes=" + (start == null ? "" : start) + "-" + (end == null ? "" : end);
  }

  // S3 does not expose a separate creation timestamp
  // objects are immutable, lastModified is the best available value
  public DownloadResponse toDownloadResponse(
      DownloadRequest downloadRequest, GetObjectResponse response) {
    return DownloadResponse.builder()
        .key(downloadRequest.getKey())
        .metadata(
            BlobMetadata.builder()
                .key(downloadRequest.getKey())
                .versionId(response.versionId())
                .eTag(response.eTag())
                .lastModified(response.lastModified())
                .createdTime(response.lastModified())
                .metadata(response.metadata())
                .objectSize(response.contentLength())
                .contentType(response.contentType())
                .build())
        .build();
  }

  public DownloadResponse toDownloadResponse(
      DownloadRequest downloadRequest,
      GetObjectResponse response,
      ResponseInputStream<GetObjectResponse> responseInputStream) {
    return DownloadResponse.builder()
        .key(downloadRequest.getKey())
        .metadata(
            BlobMetadata.builder()
                .key(downloadRequest.getKey())
                .versionId(response.versionId())
                .eTag(response.eTag())
                .lastModified(response.lastModified())
                .createdTime(response.lastModified())
                .metadata(response.metadata())
                .objectSize(response.contentLength())
                .contentType(response.contentType())
                .build())
        .inputStream(responseInputStream)
        .build();
  }

  public DeleteObjectRequest toDeleteRequest(String key, String versionId) {
    return DeleteObjectRequest.builder().bucket(getBucket()).key(key).versionId(versionId).build();
  }

  public DeleteObjectsRequest toDeleteRequests(Collection<BlobIdentifier> objects) {
    var objectIds =
        objects.stream()
            .map(
                object ->
                    ObjectIdentifier.builder()
                        .key(object.getKey())
                        .versionId(object.getVersionId())
                        .build())
            .collect(Collectors.toList());

    return DeleteObjectsRequest.builder()
        .bucket(getBucket())
        .delete(Delete.builder().objects(objectIds).build())
        .build();
  }

  public CopyObjectRequest toRequest(CopyRequest request) {
    return CopyObjectRequest.builder()
        .sourceBucket(getBucket())
        .sourceKey(request.getSrcKey())
        .sourceVersionId(request.getSrcVersionId())
        .destinationBucket(request.getDestBucket())
        .destinationKey(request.getDestKey())
        .build();
  }

  public CopyObjectRequest toRequest(CopyFromRequest request) {
    return CopyObjectRequest.builder()
        .sourceBucket(request.getSrcBucket())
        .sourceKey(request.getSrcKey())
        .sourceVersionId(request.getSrcVersionId())
        .destinationBucket(getBucket())
        .destinationKey(request.getDestKey())
        .build();
  }

  public HeadObjectRequest toHeadRequest(String key, String versionId) {
    return HeadObjectRequest.builder().bucket(getBucket()).key(key).versionId(versionId).build();
  }

  public BlobMetadata toMetadata(HeadObjectResponse response, String key) {
    Long objectSize = response.contentLength();
    Map<String, String> metadata = response.metadata();
    String eTag = response.eTag();

    // Extract object lock info if present
    ObjectLockInfo objectLockInfo = null;
    if (response.objectLockMode() != null || response.objectLockRetainUntilDate() != null) {
      objectLockInfo =
          ObjectLockInfo.builder()
              .mode(toDriverRetentionMode(response.objectLockMode()))
              .retainUntilDate(response.objectLockRetainUntilDate())
              .legalHold(response.objectLockLegalHoldStatus() == ObjectLockLegalHoldStatus.ON)
              .build();
    }

    return BlobMetadata.builder()
        .key(key)
        .versionId(response.versionId())
        .eTag(eTag)
        .objectSize(objectSize)
        .metadata(metadata)
        .lastModified(response.lastModified())
        .createdTime(response.lastModified())
        .md5(eTagToMD5(eTag))
        .contentType(response.contentType())
        .objectLockInfo(objectLockInfo)
        .build();
  }

  byte[] eTagToMD5(String eTag) {
    if (eTag == null) {
      return new byte[0];
    }

    if (eTag.length() < 2 || eTag.charAt(0) != '"' || eTag.charAt(eTag.length() - 1) != '"') {
      return new byte[0];
    }

    String unquoted = eTag.substring(1, eTag.length() - 1);
    return HexUtil.convertToBytes(unquoted);
  }

  public CreateMultipartUploadRequest toCreateMultipartUploadRequest(
      MultipartUploadRequest request) {
    CreateMultipartUploadRequest.Builder builder =
        CreateMultipartUploadRequest.builder()
            .bucket(getBucket())
            .key(request.getKey())
            .metadata(request.getMetadata());

    if (request.getTags() != null && !request.getTags().isEmpty()) {
      List<Tag> tags =
          request.getTags().entrySet().stream()
              .map(entry -> Tag.builder().key(entry.getKey()).value(entry.getValue()).build())
              .collect(Collectors.toList());
      builder.tagging(Tagging.builder().tagSet(tags).build());
    }

    if (StringUtils.isNotEmpty(request.getKmsKeyId())) {
      builder.serverSideEncryption(ServerSideEncryption.AWS_KMS).ssekmsKeyId(request.getKmsKeyId());
    } else if (request.isUseKmsManagedKey()) {
      builder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
    }
    // else: no SSE headers; S3 uses bucket default encryption

    if (request.isChecksumEnabled()) {
      ChecksumMethod algo = request.getChecksumAlgorithm() != null
          ? request.getChecksumAlgorithm() : ChecksumMethod.CRC32C;
      builder.checksumAlgorithm(toAwsChecksumAlgorithm(algo));
    }

    // Set object lock if provided
    if (request.getObjectLock() != null) {
      ObjectLockConfiguration lockConfig = request.getObjectLock();
      if (lockConfig.getMode() != null) {
        builder.objectLockMode(toAwsObjectLockMode(lockConfig.getMode()));
      }
      if (lockConfig.getRetainUntilDate() != null) {
        builder.objectLockRetainUntilDate(lockConfig.getRetainUntilDate());
      }
      if (lockConfig.isLegalHold()) {
        builder.objectLockLegalHoldStatus(ObjectLockLegalHoldStatus.ON);
      }
    }

    // Set content type if provided
    if (StringUtils.isNotEmpty(request.getContentType())) {
      builder.contentType(request.getContentType());
    }

    return builder.build();
  }

  public UploadPartRequest toUploadPartRequest(MultipartUpload mpu, MultipartPart mpp) {
    UploadPartRequest.Builder builder = UploadPartRequest.builder()
        .bucket(getBucket())
        .key(mpu.getKey())
        .uploadId(mpu.getId())
        .partNumber(mpp.getPartNumber())
        .contentLength(mpp.getContentLength());

    if (!StringUtils.isEmpty(mpp.getChecksumValue())) {
      ChecksumMethod algo = mpu.getChecksumAlgorithm() != null
          ? mpu.getChecksumAlgorithm() : ChecksumMethod.CRC32C;
      builder.checksumAlgorithm(toAwsChecksumAlgorithm(algo));
      if (algo == ChecksumMethod.SHA256) {
        builder.checksumSHA256(mpp.getChecksumValue());
      } else {
        builder.checksumCRC32C(mpp.getChecksumValue());
      }
    }

    if (StringUtils.isNotEmpty(mpu.getContentType())) {
      builder.overrideConfiguration(
          b -> b.putHeader("Content-Type", mpu.getContentType()));
    }

    return builder.build();
  }

  public CompleteMultipartUploadRequest toCompleteMultipartUploadRequest(
      MultipartUpload mpu, List<UploadPartResponse> parts) {

    List<CompletedPart> completedParts = parts.stream()
        .sorted(Comparator.comparingInt(UploadPartResponse::getPartNumber))
        .map(part -> {
          CompletedPart.Builder partBuilder = CompletedPart.builder()
              .partNumber(part.getPartNumber())
              .eTag(part.getEtag());
          if (StringUtils.isNotEmpty(part.getChecksumValue())) {
            if (mpu.getChecksumAlgorithm() == ChecksumMethod.SHA256) {
              partBuilder.checksumSHA256(part.getChecksumValue());
            } else {
              partBuilder.checksumCRC32C(part.getChecksumValue());
            }
          }
          return partBuilder.build();
        })
        .collect(Collectors.toList());

    return CompleteMultipartUploadRequest.builder()
        .bucket(getBucket())
        .key(mpu.getKey())
        .uploadId(mpu.getId())
        .multipartUpload(mp -> mp.parts(completedParts))
        .build();
  }

  public ListPartsRequest toListPartsRequest(MultipartUpload mpu) {
    return ListPartsRequest.builder()
        .bucket(getBucket())
        .key(mpu.getKey())
        .uploadId(mpu.getId())
        .build();
  }

  public AbortMultipartUploadRequest toAbortMultipartUploadRequest(MultipartUpload mpu) {
    return AbortMultipartUploadRequest.builder()
        .bucket(getBucket())
        .key(mpu.getKey())
        .uploadId(mpu.getId())
        .build();
  }

  public GetObjectTaggingRequest toGetObjectTaggingRequest(String key) {
    return GetObjectTaggingRequest.builder().bucket(getBucket()).key(key).build();
  }

  public PutObjectTaggingRequest toPutObjectTaggingRequest(String key, Map<String, String> tags) {
    List<Tag> listOfTags =
        tags.entrySet().stream()
            .map(entry -> Tag.builder().key(entry.getKey()).value(entry.getValue()).build())
            .collect(Collectors.toList());

    return PutObjectTaggingRequest.builder()
        .bucket(getBucket())
        .key(key)
        .tagging(Tagging.builder().tagSet(listOfTags).build())
        .build();
  }

  public PutObjectPresignRequest toPutObjectPresignRequest(PresignedUrlRequest request) {
    UploadRequest.Builder builder = UploadRequest.builder().withKey(request.getKey());
    if (request.getMetadata() != null) {
      builder.withMetadata(request.getMetadata());
    }
    if (request.getTags() != null) {
      builder.withTags(request.getTags());
    }
    if (request.getKmsKeyId() != null) {
      builder.withKmsKeyId(request.getKmsKeyId());
    }
    if (request.getContentLength() > 0) {
      builder.withContentLength(request.getContentLength());
    }
    if (request.getContentType() != null) {
      builder.withContentType(request.getContentType());
    }
    if (request.getChecksumValue() != null) {
      builder.withChecksumValue(request.getChecksumValue());
      builder.withChecksumAlgorithm(
          request.getChecksumAlgorithm() != null
              ? request.getChecksumAlgorithm()
              : ChecksumMethod.CRC32C);
    }
    UploadRequest uploadRequest = builder.build();

    return PutObjectPresignRequest.builder()
        .signatureDuration(request.getDuration())
        .putObjectRequest(toRequest(uploadRequest))
        .build();
  }

  public GetObjectPresignRequest toGetObjectPresignRequest(PresignedUrlRequest request) {
    GetObjectRequest.Builder getObjectBuilder =
        GetObjectRequest.builder().bucket(getBucket()).key(request.getKey());
    if (request.getContentDisposition() != null) {
      getObjectBuilder.responseContentDisposition(request.getContentDisposition());
    }
    return GetObjectPresignRequest.builder()
        .signatureDuration(request.getDuration())
        .getObjectRequest(getObjectBuilder.build())
        .build();
  }

  public PresignedUrlResponse toPresignedUrlResponse(
      PresignedRequest presigned) {
    Map<String, String> flatHeaders = new LinkedHashMap<>();
    presigned.signedHeaders().forEach((k, values) ->
        flatHeaders.put(k, String.join(",", values)));
    return PresignedUrlResponse.builder()
        .url(presigned.url())
        .signedHeaders(flatHeaders)
        .expiration(presigned.expiration())
        .build();
  }

  /**
   * Builds the S3 Transfer Manager download-directory request. When non-null counters are
   * supplied, {@code totalBytesRequested} is summed inside the filter (post-exclusion) and
   * {@code totalBytesTransferred} drives a per-file logging listener — but only if
   * {@code transferStatusLoggingEnabled} is set on the request. The two counters together let
   * the caller report bytes-transferred without paying the listener's heap cost: on success it
   * can fall back to the requested total.
   */
  public DownloadDirectoryRequest toDownloadDirectoryRequest(
      DirectoryDownloadRequest request,
      AtomicLong totalBytesTransferred,
      AtomicLong totalBytesRequested) {
    DownloadDirectoryRequest.Builder builder =
        DownloadDirectoryRequest.builder()
            .bucket(getBucket())
            .destination(Paths.get(request.getLocalDestinationDirectory()));

    // Download every blob that starts with this prefix
    if (StringUtils.isNotEmpty(request.getPrefixToDownload())) {
      builder.listObjectsV2RequestTransformer(
          b -> b.prefix(request.getPrefixToDownload()));
    }

    // Only install a filter when we actually have work for it (exclusion or byte counting).
    // S3 Transfer Manager may take a fast path internally when no filter is set, and there's
    // no point allocating a pass-through lambda that returns true for every object.
    boolean hasExclusions =
        request.getPrefixesToExclude() != null && !request.getPrefixesToExclude().isEmpty();
    if (hasExclusions || totalBytesRequested != null) {
      builder.filter(getPrefixExclusionsFilter(
          request.getPrefixesToExclude(),
          totalBytesRequested));
    }

    // Symmetric to toUploadDirectoryRequest: only attach the listener when the caller
    // supplied a counter to drive — a null counter signals "don't bother counting", and
    // attaching the listener anyway would NPE inside its constructor.
    if (request.isTransferStatusLoggingEnabled() && totalBytesTransferred != null) {
      S3LoggingTransferListener transferListener =
          S3LoggingTransferListener.create(totalBytesTransferred);
      builder.downloadFileRequestTransformer(b -> b.addTransferListener(transferListener));
    }
    return builder.build();
  }

  // Return false if we want to exclude this blob from the download.
  protected DownloadFilter getPrefixExclusionsFilter(List<String> prefixesToExclude) {
    return getPrefixExclusionsFilter(prefixesToExclude, null);
  }

  /**
   * Same filter contract as the single-arg overload — return {@code false} to exclude. When
   * {@code totalBytesRequested} is non-null, each retained object's size is added to it, so
   * the count reflects only objects S3 Transfer Manager will actually download.
   *
   * <p>Counting relies on the SDK calling this filter exactly once per listed object during
   * the listing phase — the {@link DownloadFilter} contract today. A future SDK change that
   * re-invoked the filter (e.g. on internal page retry) would over-count, but the alternative
   * of deduplicating by key would re-introduce the heap pressure this PR exists to avoid.
   */
  protected DownloadFilter getPrefixExclusionsFilter(
      List<String> prefixesToExclude, AtomicLong totalBytesRequested) {
    List<String> excludes = prefixesToExclude != null ? prefixesToExclude : List.of();
    return s3Object -> {
      for (String prefixToExclude : excludes) {
        if (s3Object.key().startsWith(prefixToExclude)) {
          return false;
        }
      }
      if (totalBytesRequested != null) {
        totalBytesRequested.addAndGet(s3Object.size());
      }
      return true;
    };
  }

  public DirectoryDownloadResponse toDirectoryDownloadResponse(
      CompletedDirectoryDownload completedDirectoryDownload, Long totalBytesTransferred) {
    return DirectoryDownloadResponse.builder()
        .failedTransfers(
            completedDirectoryDownload.failedTransfers().stream()
                .map(
                    item ->
                        FailedBlobDownload.builder()
                            .destination(item.request().destination())
                            .exception(item.exception())
                            .build())
                .collect(Collectors.toList()))
        .totalBytesTransferred(totalBytesTransferred)
        .build();
  }

  /**
   * Builds the S3 Transfer Manager upload-directory request. When non-null counters are
   * supplied, the per-file transformer stats each source into {@code totalBytesRequested} and
   * (if {@code transferStatusLoggingEnabled}) attaches a logging listener that drives
   * {@code totalBytesTransferred}. Together they let the caller report bytes-transferred
   * without paying the listener's heap cost: on success it can fall back to the requested
   * total.
   */
  public UploadDirectoryRequest toUploadDirectoryRequest(
      DirectoryUploadRequest request,
      AtomicLong totalBytesTransferred,
      AtomicLong totalBytesRequested) {
    UploadDirectoryRequest.Builder builder =
        UploadDirectoryRequest.builder()
            .bucket(getBucket())
            .source(Paths.get(request.getLocalSourceDirectory()))
            .maxDepth(request.isIncludeSubFolders() ? Integer.MAX_VALUE : 1)
            .followSymbolicLinks(request.isFollowSymbolicLinks())
            .s3Prefix(request.getPrefix());

    boolean hasTags = request.getTags() != null && !request.getTags().isEmpty();
    boolean hasObjectLock = request.getObjectLock() != null;
    boolean transferStatusLoggingEnabled =
        request.isTransferStatusLoggingEnabled() && totalBytesTransferred != null;
    boolean countRequested = totalBytesRequested != null;

    if (!hasTags && !hasObjectLock && !transferStatusLoggingEnabled && !countRequested) {
      return builder.build();
    }

    List<Tag> tagSet =
        hasTags
            ? request.getTags().entrySet().stream()
              .map(e -> Tag.builder().key(e.getKey()).value(e.getValue()).build())
              .collect(Collectors.toList())
            : null;
    ObjectLockConfiguration lockConfig = request.getObjectLock();
    S3LoggingTransferListener transferListener =
        transferStatusLoggingEnabled
            ? S3LoggingTransferListener.create(totalBytesTransferred)
            : null;
    // Merge tags / object lock into the existing PutObjectRequest per file;
    // putObjectRequest(Consumer) would replace it and drop bucket/key.
    // S3 Transfer Manager doesn't expose a directory-listing filter for uploads, so this
    // per-file hook is also where we stat each source's planned size into
    // totalBytesRequested. This relies on the SDK invoking the transformer exactly once per
    // file (its current contract). A future SDK change that re-invoked on retry could
    // over-count; the same trade-off applies as the download filter above.
    builder.uploadFileRequestTransformer(
        fileRequestBuilder -> {
          if (hasTags || hasObjectLock) {
            PutObjectRequest existing = fileRequestBuilder.build().putObjectRequest();
            PutObjectRequest.Builder putBuilder = existing.toBuilder();
            if (hasTags) {
              putBuilder.tagging(Tagging.builder().tagSet(tagSet).build());
            }
            if (hasObjectLock) {
              applyObjectLockToPutObjectBuilder(putBuilder, lockConfig);
            }
            fileRequestBuilder.putObjectRequest(putBuilder.build());
          }
          if (transferListener != null) {
            fileRequestBuilder.addTransferListener(transferListener);
          }
          if (countRequested) {
            // Best-effort stat, evaluated independently per file. A file that throws here is
            // also a file the SDK can't upload, so it'll appear in failedTransfers and the
            // response will already report 0 via resolveDirectoryTotalBytes — the under-counted
            // requested total is never the value handed back to the caller in that case.
            Path source = fileRequestBuilder.build().source();
            if (source != null) {
              try {
                totalBytesRequested.addAndGet(Files.size(source));
              } catch (IOException ignored) {
                // Skip this file's contribution; other files in the same directory op are
                // unaffected and still get counted.
              }
            }
          }
        });
    return builder.build();
  }

  public DirectoryUploadResponse toDirectoryUploadResponse(
      CompletedDirectoryUpload completedDirectoryUpload, Long totalBytesTransferred) {
    return DirectoryUploadResponse.builder()
        .failedTransfers(
            completedDirectoryUpload.failedTransfers().stream()
                .map(
                    item ->
                        FailedBlobUpload.builder()
                            .source(item.request().source())
                            .exception(item.exception())
                            .build())
                .collect(Collectors.toList()))
        .totalBytesTransferred(totalBytesTransferred)
        .build();
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

  public UploadResponse toUploadResponse(String key, PutObjectResponse response) {
    UploadResponse.UploadResponseBuilder builder =
        UploadResponse.builder().key(key).versionId(response.versionId()).eTag(response.eTag());

    if (response.checksumSHA256() != null) {
      builder.checksumValue(response.checksumSHA256());
    } else if (response.checksumCRC32C() != null) {
      builder.checksumValue(response.checksumCRC32C());
    }

    return builder.build();
  }

  public CopyResponse toCopyResponse(String destKey, CopyObjectResponse response) {
    return CopyResponse.builder()
        .key(destKey)
        .versionId(response.versionId())
        .eTag(response.copyObjectResult().eTag())
        .lastModified(response.copyObjectResult().lastModified())
        .build();
  }

  public MultipartUpload toMultipartUpload(MultipartUploadRequest request,
                                           CreateMultipartUploadResponse response) {
    // S3's default object checksum is CRC32C. When checksumming is enabled without an explicit
    // algorithm, resolve the substrate-native default (CRC32C) so the stored algorithm honestly
    // reflects what S3 uses — matching the algorithm forwarded on the create request.
    ChecksumMethod algorithm = request.getChecksumAlgorithm();
    if (algorithm == null && request.isChecksumEnabled()) {
      algorithm = ChecksumMethod.CRC32C;
    }
    return MultipartUpload.builder()
        .bucket(response.bucket())
        .key(response.key())
        .id(response.uploadId())
        .metadata(request.getMetadata())
        .tags(request.getTags())
        .kmsKeyId(request.getKmsKeyId())
        .checksumEnabled(request.isChecksumEnabled())
        .checksumAlgorithm(algorithm)
        .objectLock(request.getObjectLock())
        .contentType(request.getContentType())
        .build();
  }

  public UploadPartResponse toUploadPartResponse(
      MultipartPart part, software.amazon.awssdk.services.s3.model.UploadPartResponse response) {
    String checksumValue = response.checksumSHA256() != null
        ? response.checksumSHA256() : response.checksumCRC32C();
    return new UploadPartResponse(
        part.getPartNumber(), response.eTag(), part.getContentLength(), checksumValue);
  }

  public MultipartUploadResponse toMultipartUploadResponse(
      CompleteMultipartUploadResponse response) {
    String checksumValue = null;
    if (response.checksumSHA256() != null) {
      checksumValue = response.checksumSHA256();
    } else if (response.checksumCRC32C() != null) {
      checksumValue = response.checksumCRC32C();
    }
    return new MultipartUploadResponse(response.eTag(), checksumValue);
  }

  /**
   * Converts MultiCloudJ RetryConfig to AWS SDK RetryStrategy. EXPONENTIAL and FIXED modes produce
   * a {@link StandardRetryStrategy} with the corresponding backoff; ADAPTIVE mode produces an
   * {@link AdaptiveRetryStrategy} that throttles the client request rate under server-side
   * throttling. A null mode uses the SDK default backoff.
   *
   * @param retryConfig The retry configuration to convert
   * @return AWS SDK RetryStrategy
   * @throws InvalidArgumentException if retryConfig is null or has invalid values
   */
  public RetryStrategy toAwsRetryStrategy(RetryConfig retryConfig) {
    if (retryConfig == null) {
      throw new InvalidArgumentException("RetryConfig cannot be null");
    }
    if (retryConfig.getMaxAttempts() != null && retryConfig.getMaxAttempts() <= 0) {
      throw new InvalidArgumentException(
          "RetryConfig.maxAttempts must be greater than 0, got: " + retryConfig.getMaxAttempts());
    }

    StandardRetryStrategy.Builder strategyBuilder = StandardRetryStrategy.builder();

    // Only set maxAttempts if provided, otherwise use AWS SDK default
    if (retryConfig.getMaxAttempts() != null) {
      strategyBuilder.maxAttempts(retryConfig.getMaxAttempts());
    }

    // If mode is not set, use AWS SDK's default backoff strategy
    if (retryConfig.getMode() == null) {
      return strategyBuilder.build();
    }

    if (retryConfig.getMode() == RetryConfig.Mode.ADAPTIVE) {
      AdaptiveRetryStrategy.Builder adaptiveBuilder = AdaptiveRetryStrategy.builder();
      if (retryConfig.getMaxAttempts() != null) {
        adaptiveBuilder.maxAttempts(retryConfig.getMaxAttempts());
      }
      return adaptiveBuilder.build();
    }

    // Configure backoff strategy based on mode
    if (retryConfig.getMode() == RetryConfig.Mode.EXPONENTIAL) {
      if (retryConfig.getInitialDelayMillis() <= 0) {
        throw new InvalidArgumentException(
            "RetryConfig.initialDelayMillis must be greater than 0 for EXPONENTIAL mode, got: "
                + retryConfig.getInitialDelayMillis());
      }
      if (retryConfig.getMaxDelayMillis() <= 0) {
        throw new InvalidArgumentException(
            "RetryConfig.maxDelayMillis must be greater than 0 for EXPONENTIAL mode, got: "
                + retryConfig.getMaxDelayMillis());
      }
      strategyBuilder.backoffStrategy(
          BackoffStrategy.exponentialDelay(
              Duration.ofMillis(retryConfig.getInitialDelayMillis()),
              Duration.ofMillis(retryConfig.getMaxDelayMillis())));
      return strategyBuilder.build();
    }

    // FIXED mode
    if (retryConfig.getFixedDelayMillis() <= 0) {
      throw new InvalidArgumentException(
          "RetryConfig.fixedDelayMillis must be greater than 0 for FIXED mode, got: "
              + retryConfig.getFixedDelayMillis());
    }
    strategyBuilder.backoffStrategy(
        BackoffStrategy.fixedDelay(Duration.ofMillis(retryConfig.getFixedDelayMillis())));
    return strategyBuilder.build();
  }

  /**
   * Creates a GetObjectRetentionRequest for retrieving object retention
   */
  public GetObjectRetentionRequest toGetObjectRetentionRequest(String key, String versionId) {
    return GetObjectRetentionRequest.builder()
        .bucket(getBucket())
        .key(key)
        .versionId(versionId)
        .build();
  }

  /**
   * Creates a GetObjectLegalHoldRequest for retrieving legal hold status
   */
  public GetObjectLegalHoldRequest toGetObjectLegalHoldRequest(String key, String versionId) {
    return GetObjectLegalHoldRequest.builder()
        .bucket(getBucket())
        .key(key)
        .versionId(versionId)
        .build();
  }

  /**
   * Creates a PutObjectRetentionRequest for updating object retention
   */
  public PutObjectRetentionRequest toPutObjectRetentionRequest(
      String key,
      String versionId,
      ObjectLockRetentionMode mode,
      Instant retainUntilDate) {
    return toPutObjectRetentionRequest(key, versionId, mode, retainUntilDate, false);
  }

  /**
   * Creates a {@link PutObjectRetentionRequest} for the new {@code
   * updateObjectRetention(key, versionId, ObjectRetentionConfig)} overload.
   *
   * <p>The {@code bypassGovernanceRetention} flag is set on the request only when {@code true};
   * AWS S3 ignores the flag on COMPLIANCE objects (per design §E.7), but client-side guards in
   * {@link com.salesforce.multicloudj.blob.driver.ObjectRetentionRules} reject the disallowed
   * combinations before reaching this transformer, so the request shape is always valid.
   */
  public PutObjectRetentionRequest toPutObjectRetentionRequest(
      String key,
      String versionId,
      ObjectLockRetentionMode mode,
      Instant retainUntilDate,
      boolean bypassGovernanceRetention) {
    PutObjectRetentionRequest.Builder builder =
        PutObjectRetentionRequest.builder()
            .bucket(getBucket())
            .key(key)
            .versionId(versionId)
            .retention(
                ObjectLockRetention.builder().mode(mode).retainUntilDate(retainUntilDate).build());
    if (bypassGovernanceRetention) {
      builder.bypassGovernanceRetention(true);
    }
    return builder.build();
  }

  /**
   * Creates a PutObjectLegalHoldRequest for updating legal hold status
   */
  public PutObjectLegalHoldRequest toPutObjectLegalHoldRequest(
      String key, String versionId, boolean legalHold) {
    return PutObjectLegalHoldRequest.builder()
        .bucket(getBucket())
        .key(key)
        .versionId(versionId)
        .legalHold(
            ObjectLockLegalHold.builder()
                .status(legalHold ? ObjectLockLegalHoldStatus.ON : ObjectLockLegalHoldStatus.OFF)
                .build())
        .build();
  }

  /**
   * Converts GetObjectRetentionResponse and GetObjectLegalHoldResponse to ObjectLockInfo
   */
  public ObjectLockInfo toObjectLockInfo(
      GetObjectRetentionResponse retentionResponse, GetObjectLegalHoldResponse legalHoldResponse) {
    if (retentionResponse == null || retentionResponse.retention() == null) {
      return null;
    }

    ObjectLockRetentionMode retentionMode = retentionResponse.retention().mode();
    return ObjectLockInfo.builder()
        .mode(toDriverRetentionMode(retentionMode))
        .retainUntilDate(retentionResponse.retention().retainUntilDate())
        .legalHold(
            legalHoldResponse != null
                && legalHoldResponse.legalHold() != null
                && legalHoldResponse.legalHold().status() == ObjectLockLegalHoldStatus.ON)
        .build();
  }
}
