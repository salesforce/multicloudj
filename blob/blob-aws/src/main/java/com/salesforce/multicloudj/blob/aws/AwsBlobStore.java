package com.salesforce.multicloudj.blob.aws;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
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
import com.salesforce.multicloudj.blob.driver.MultipartUploadResponse;
import com.salesforce.multicloudj.blob.driver.ObjectLockInfo;
import com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig;
import com.salesforce.multicloudj.blob.driver.ObjectRetentionRules;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.PresignedUrlResponse;
import com.salesforce.multicloudj.blob.driver.RetentionMode;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.aws.CredentialsProvider;
import com.salesforce.multicloudj.common.exceptions.ArchiveInfo;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectLegalHoldResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRetentionResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ListPartsResponse;
import software.amazon.awssdk.services.s3.model.ObjectLockRetentionMode;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

/** AWS implementation of BlobStore */
@AutoService(AbstractBlobStore.class)
public class AwsBlobStore extends AbstractBlobStore {
  private final S3Client s3Client;
  private final AwsTransformer transformer;

  public AwsBlobStore() {
    this(new Builder(), null);
  }

  public AwsBlobStore(Builder builder, S3Client s3Client) {
    super(builder);
    this.s3Client = s3Client;
    this.transformer = builder.getTransformerSupplier().get(bucket);
  }

  /** Helper function to determine if any of the HttpClient configuration options have been set */
  protected static boolean shouldConfigureHttpClient(Builder builder) {
    return builder.getProxyEndpoint() != null
        || builder.getMaxConnections() != null
        || builder.getSocketTimeout() != null
        || builder.getIdleConnectionTimeout() != null
        || builder.getUseSystemPropertyProxyValues() != null
        || builder.getUseEnvironmentVariableProxyValues() != null;
  }

  @Override
  public Builder builder() {
    return new Builder();
  }

  @Override
  public Class<? extends SubstrateSdkException> getException(Throwable t) {
    if (t instanceof SubstrateSdkException) {
      return (Class<? extends SubstrateSdkException>) t.getClass();
    } else if (t instanceof AwsServiceException) {
      AwsServiceException awsServiceException = (AwsServiceException) t;
      String requestId = awsServiceException.requestId();
      if ((requestId == null || requestId.isEmpty()) && awsServiceException.statusCode() == 403) {
        return UnAuthorizedException.class;
      }
      String errorCode = awsServiceException.awsErrorDetails().errorCode();
      return ErrorCodeMapping.getException(errorCode);
    } else if (t instanceof SdkClientException || t instanceof IllegalArgumentException) {
      return InvalidArgumentException.class;
    }
    return UnknownException.class;
  }

  /**
   * Performs Blob upload Note: Specifying the contentLength in the UploadRequest can dramatically
   * improve upload efficiency because the substrate SDKs do not need to buffer the contents and
   * calculate it themselves.
   *
   * @param uploadRequest Wrapper object containing upload data
   * @param inputStream The input stream that contains the blob content
   * @return Wrapper object containing the upload result data
   */
  @Override
  protected UploadResponse doUpload(UploadRequest uploadRequest, InputStream inputStream) {
    return doUpload(
        uploadRequest, RequestBody.fromInputStream(inputStream, uploadRequest.getContentLength()));
  }

  /**
   * Performs Blob upload
   *
   * @param uploadRequest Wrapper object containing upload data
   * @param content The byte array that contains the blob content
   * @return Wrapper object containing the upload result data
   */
  @Override
  protected UploadResponse doUpload(UploadRequest uploadRequest, byte[] content) {
    return doUpload(uploadRequest, RequestBody.fromBytes(content));
  }

  /**
   * Performs Blob upload
   *
   * @param uploadRequest Wrapper object containing upload data
   * @param file The File that contains the blob content
   * @return Wrapper object containing the upload result data
   */
  @Override
  protected UploadResponse doUpload(UploadRequest uploadRequest, File file) {
    return doUpload(uploadRequest, RequestBody.fromFile(file));
  }

  /**
   * Performs Blob upload
   *
   * @param uploadRequest Wrapper object containing upload data
   * @param path The Path that contains the blob content
   * @return Wrapper object containing the upload result data
   */
  @Override
  protected UploadResponse doUpload(UploadRequest uploadRequest, Path path) {
    return doUpload(uploadRequest, RequestBody.fromFile(path));
  }

  /** Helper function to upload blobs */
  protected UploadResponse doUpload(UploadRequest uploadRequest, RequestBody requestBody) {
    PutObjectRequest request = transformer.toRequest(uploadRequest);
    PutObjectResponse response = s3Client.putObject(request, requestBody);
    return transformer.toUploadResponse(uploadRequest.getKey(), response);
  }

  /**
   * Performs Blob download
   *
   * @param downloadRequest Wrapper object containing download data
   * @param outputStream The output stream that the blob content will be written to
   * @return Returns a DownloadResponse object that contains metadata about the blob
   */
  @Override
  protected DownloadResponse doDownload(
      DownloadRequest downloadRequest, OutputStream outputStream) {
    try {
      GetObjectRequest request = transformer.toRequest(downloadRequest);
      GetObjectResponse response =
          s3Client.getObject(request, ResponseTransformer.toOutputStream(outputStream));
      return transformer.toDownloadResponse(downloadRequest, response);
    } catch (S3Exception e) {
      handleArchivedObjects(downloadRequest, e);
      throw e;
    }
  }

  /**
   * Performs Blob download
   *
   * @param downloadRequest Wrapper object containing download data
   * @param byteArray The byte array that blob content will be written to
   * @return Returns a DownloadResponse object that contains metadata about the blob
   */
  @Override
  protected DownloadResponse doDownload(DownloadRequest downloadRequest, ByteArray byteArray) {
    try {
      GetObjectRequest request = transformer.toRequest(downloadRequest);
      ResponseBytes<GetObjectResponse> responseBytes =
          s3Client.getObject(request, ResponseTransformer.toBytes());
      byteArray.setBytes(responseBytes.asByteArray());
      GetObjectResponse response = responseBytes.response();
      return transformer.toDownloadResponse(downloadRequest, response);
    } catch (S3Exception e) {
      handleArchivedObjects(downloadRequest, e);
      throw e;
    }
  }

  /**
   * Performs Blob download
   *
   * @param downloadRequest Wrapper object containing download data
   * @param file The File the blob content will be written to
   * @return Returns a DownloadResponse object that contains metadata about the blob
   */
  @Override
  protected DownloadResponse doDownload(DownloadRequest downloadRequest, File file) {
    try {
      GetObjectRequest request = transformer.toRequest(downloadRequest);
      Path destinationPath = createDownloadDestinationPath(downloadRequest, file.toPath());
      GetObjectResponse response =
          s3Client.getObject(request, ResponseTransformer.toFile(destinationPath));
      return transformer.toDownloadResponse(downloadRequest, response);
    } catch (S3Exception e) {
      handleArchivedObjects(downloadRequest, e);
      throw e;
    }
  }

  /**
   * Performs Blob download
   *
   * @param downloadRequest Wrapper object containing download data
   * @param path The Path that blob content will be written to
   * @return Returns a DownloadResponse object that contains metadata about the blob
   */
  @Override
  protected DownloadResponse doDownload(DownloadRequest downloadRequest, Path path) {
    try {
      GetObjectRequest request = transformer.toRequest(downloadRequest);
      Path destinationPath = createDownloadDestinationPath(downloadRequest, path);
      GetObjectResponse response =
          s3Client.getObject(request, ResponseTransformer.toFile(destinationPath));
      return transformer.toDownloadResponse(downloadRequest, response);
    } catch (S3Exception e) {
      handleArchivedObjects(downloadRequest, e);
      throw e;
    }
  }

  /**
   * Performs Blob download and returns an InputStream
   *
   * @param downloadRequest Wrapper object containing download data
   * @return Returns a DownloadResponse object that contains metadata about the blob and an
   *     InputStream for reading the content
   */
  @Override
  protected DownloadResponse doDownload(DownloadRequest downloadRequest) {
    try {
      GetObjectRequest request = transformer.toRequest(downloadRequest);
      ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(request);
      return transformer.toDownloadResponse(
          downloadRequest, responseInputStream.response(), responseInputStream);
    } catch (S3Exception e) {
      handleArchivedObjects(downloadRequest, e);
      throw e;
    }
  }

  private void handleArchivedObjects(DownloadRequest downloadRequest, S3Exception e) {
    if (!downloadRequest.isCheckArchived() || e.statusCode() != 404) {
      return;
    }
    boolean isDeleteMarker = e.awsErrorDetails().sdkHttpResponse()
        .firstMatchingHeader("x-amz-delete-marker")
        .map("true"::equals)
        .orElse(false);
    if (!isDeleteMarker) {
      return;
    }
    ListObjectVersionsResponse versionsResponse = s3Client.listObjectVersions(
        ListObjectVersionsRequest.builder()
            .bucket(bucket)
            .prefix(downloadRequest.getKey())
            .maxKeys(2) // one for delete marker + one for actual object in stack
            .build());
    Iterator<ObjectVersion> it = versionsResponse.versions().iterator();
    String versionId = it.hasNext() ? it.next().versionId() : null;
    throw new ResourceNotFoundException(
        "Object is archived (delete marker): " + downloadRequest.getKey(),
        e,
        ArchiveInfo.builder().archived(true).versionId(versionId).build());
  }

  /**
   * Deletes a single Blob
   *
   * @param key The key of the Blob to be deleted
   * @param versionId The versionId of the blob
   */
  @Override
  protected void doDelete(String key, String versionId) {
    s3Client.deleteObject(transformer.toDeleteRequest(key, versionId));
  }

  /**
   * Deletes a collection of Blobs
   *
   * @param objects A collection of blob identifiers to delete
   */
  @Override
  protected void doDelete(Collection<BlobIdentifier> objects) {
    s3Client.deleteObjects(transformer.toDeleteRequests(objects));
  }

  /**
   * Copies a Blob to a different bucket
   *
   * @param request the copy request
   * @return CopyResponse of the copied Blob
   */
  @Override
  protected CopyResponse doCopy(CopyRequest request) {
    CopyObjectRequest copyRequest = transformer.toRequest(request);
    CopyObjectResponse copyResponse = s3Client.copyObject(copyRequest);
    return transformer.toCopyResponse(request.getDestKey(), copyResponse);
  }

  /**
   * Copies a Blob from a source bucket to the current bucket
   *
   * @param request the copyFrom request
   * @return CopyResponse of the copied Blob
   */
  @Override
  protected CopyResponse doCopyFrom(CopyFromRequest request) {
    CopyObjectRequest copyRequest = transformer.toRequest(request);
    CopyObjectResponse copyResponse = s3Client.copyObject(copyRequest);
    return transformer.toCopyResponse(request.getDestKey(), copyResponse);
  }

  /**
   * Retrieves the Blob metadata
   *
   * @param key Key of the Blob whose metadata is to be retrieved
   * @param versionId The versionId of the blob. This field is optional and only used if your bucket
   *     has versioning enabled. This value should be null unless you're targeting a specific
   *     key/version blob.
   * @return Wrapper Blob metadata object
   */
  @Override
  protected BlobMetadata doGetMetadata(String key, String versionId) {
    HeadObjectRequest request = transformer.toHeadRequest(key, versionId);
    HeadObjectResponse response = s3Client.headObject(request);
    return transformer.toMetadata(response, key);
  }

  /**
   * Lists all objects in the bucket
   *
   * @return Iterator of the list
   */
  @Override
  protected Iterator<BlobInfo> doList(ListBlobsRequest request) {
    return new BlobInfoIterator(s3Client, getBucket(), request);
  }

  /**
   * Lists a single page of objects in the bucket with pagination support
   *
   * @param request The list request containing filters and optional pagination token
   * @return ListBlobsPageResponse containing the blobs, truncation status, and next page token
   */
  @Override
  protected ListBlobsPageResponse doListPage(ListBlobsPageRequest request) {
    ListObjectsV2Request awsRequest = transformer.toRequest(request);
    ListObjectsV2Response response = s3Client.listObjectsV2(awsRequest);

    List<BlobInfo> blobs =
        response.contents().stream().map(transformer::toInfo).collect(Collectors.toList());

    List<String> commonPrefixes =
        response.commonPrefixes().stream().map(CommonPrefix::prefix).collect(Collectors.toList());

    return new ListBlobsPageResponse(
        blobs, commonPrefixes, response.isTruncated(), response.nextContinuationToken());
  }

  /**
   * Lists versions for a single key using S3's paginated {@code ListObjectVersions} API.
   *
   * <p>This implementation is streaming/lazy: it walks paginator pages on demand and does not
   * materialize all pages up front. Only versions for the requested key are returned.
   */
  protected Iterator<BlobMetadata> doListBlobVersions(ListBlobVersionsRequest request) {
    return new BlobMetadataIterator(s3Client, getBucket(), request.getKey());
  }

  /**
   * Initiates a multipart upload
   *
   * @param request the multipart request
   * @return An object that acts as an identifier for subsequent related multipart operations
   */
  @Override
  protected MultipartUpload doInitiateMultipartUpload(final MultipartUploadRequest request) {
    CreateMultipartUploadRequest createMultipartUploadRequest =
        transformer.toCreateMultipartUploadRequest(request);
    CreateMultipartUploadResponse createMultipartUploadResponse =
        s3Client.createMultipartUpload(createMultipartUploadRequest);
    return transformer.toMultipartUpload(request, createMultipartUploadResponse);
  }

  /**
   * Uploads a part of the multipartUpload operation
   *
   * @param mpu The multipartUpload identifier
   * @param mpp The part to be uploaded
   * @return Returns an identifier of the uploaded part
   */
  @Override
  protected UploadPartResponse doUploadMultipartPart(
      final MultipartUpload mpu, final MultipartPart mpp) {
    UploadPartRequest uploadPartRequest = transformer.toUploadPartRequest(mpu, mpp);
    var uploadPartResponse =
        s3Client.uploadPart(
            uploadPartRequest,
            RequestBody.fromInputStream(mpp.getInputStream(), mpp.getContentLength()));
    return transformer.toUploadPartResponse(mpp, uploadPartResponse);
  }

  /**
   * Completes a multipartUpload operation
   *
   * @param mpu The multipartUpload identifier
   * @param parts The list of all parts that were uploaded
   * @return Returns a MultipartUploadResponse that contains an etag of the resultant blob
   */
  @Override
  protected MultipartUploadResponse doCompleteMultipartUpload(
      final MultipartUpload mpu, final List<UploadPartResponse> parts) {
    CompleteMultipartUploadRequest completeMultipartUploadRequest =
        transformer.toCompleteMultipartUploadRequest(mpu, parts);
    CompleteMultipartUploadResponse completeMultipartUploadResponse =
        s3Client.completeMultipartUpload(completeMultipartUploadRequest);
    return transformer.toMultipartUploadResponse(completeMultipartUploadResponse);
  }

  /**
   * List all parts that have been uploaded for the multipartUpload so far
   *
   * @param mpu The multipartUpload identifier
   * @return Returns a list of all uploaded parts
   */
  protected List<UploadPartResponse> doListMultipartUpload(final MultipartUpload mpu) {
    ListPartsRequest listPartsRequest = transformer.toListPartsRequest(mpu);
    ListPartsResponse listPartsResponse = s3Client.listParts(listPartsRequest);
    return listPartsResponse.parts().stream()
        .sorted(Comparator.comparingInt(Part::partNumber))
        .map((part) -> new UploadPartResponse(part.partNumber(), part.eTag(), part.size()))
        .collect(Collectors.toList());
  }

  /**
   * Aborts a multipartUpload that's in progress
   *
   * @param mpu The multipartUpload identifier
   */
  protected void doAbortMultipartUpload(final MultipartUpload mpu) {
    s3Client.abortMultipartUpload(transformer.toAbortMultipartUploadRequest(mpu));
  }

  /**
   * Returns a map of all the tags associated with the blob
   *
   * @param key Name of the blob whose tags are to be retrieved
   * @return The blob's tags
   */
  @Override
  protected Map<String, String> doGetTags(String key) {
    GetObjectTaggingResponse response =
        s3Client.getObjectTagging(transformer.toGetObjectTaggingRequest(key));
    return response.tagSet().stream().collect(Collectors.toMap(Tag::key, Tag::value));
  }

  /**
   * Sets tags on a blob
   *
   * @param key Name of the blob to set tags on
   * @param tags The tags to set
   */
  @Override
  protected void doSetTags(String key, Map<String, String> tags) {
    s3Client.putObjectTagging(transformer.toPutObjectTaggingRequest(key, tags));
  }

  @Override
  protected PresignedUrlResponse doPresign(PresignedUrlRequest request) {
    try (S3Presigner presigner = getPresigner()) {
      software.amazon.awssdk.awscore.presigner.PresignedRequest presigned;
      switch (request.getType()) {
        case UPLOAD:
          presigned = presigner.presignPutObject(transformer.toPutObjectPresignRequest(request));
          break;
        case DOWNLOAD:
          presigned = presigner.presignGetObject(transformer.toGetObjectPresignRequest(request));
          break;
        default:
          throw new InvalidArgumentException(
              "Unsupported PresignedOperation. type=" + request.getType());
      }
      Map<String, String> flatHeaders = new java.util.LinkedHashMap<>();
      presigned.signedHeaders().forEach((k, values) ->
          flatHeaders.put(k, String.join(", ", values)));
      return PresignedUrlResponse.builder()
          .url(presigned.url())
          .signedHeaders(flatHeaders)
          .expiration(presigned.expiration())
          .build();
    }
  }

  /**
   * Returns an S3Presigner for the current credentials
   *
   * @return Returns an S3Presigner for the current credentials
   */
  protected S3Presigner getPresigner() {
    return S3Presigner.builder()
        .credentialsProvider(s3Client.serviceClientConfiguration().credentialsProvider())
        .region(Region.of(getRegion()))
        .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
        .s3Client(s3Client)
        .build();
  }

  /**
   * Determines if an object exists for a given key/versionId
   *
   * @param key Name of the blob to check
   * @param versionId The version of the blob to check
   * @return Returns true if the object exists. Returns false if it doesn't exist.
   */
  @Override
  protected boolean doDoesObjectExist(String key, String versionId) {
    try {
      s3Client.headObject(transformer.toHeadRequest(key, versionId));
      return true;
    } catch (S3Exception e) {
      if (e.statusCode() == 404) {
        return false;
      }
      throw e;
    }
  }

  @Override
  protected boolean doDoesBucketExist() {
    try {
      s3Client.headBucket(builder -> builder.bucket(bucket));
      return true;
    } catch (S3Exception e) {
      if (e.statusCode() == 404) {
        return false;
      }
      throw e;
    }
  }

  /** Gets object lock configuration for a blob. */
  @Override
  public ObjectLockInfo getObjectLock(String key, String versionId) {
    GetObjectRetentionResponse retentionResponse =
        s3Client.getObjectRetention(transformer.toGetObjectRetentionRequest(key, versionId));
    GetObjectLegalHoldResponse legalHoldResponse =
        s3Client.getObjectLegalHold(transformer.toGetObjectLegalHoldRequest(key, versionId));
    return transformer.toObjectLockInfo(retentionResponse, legalHoldResponse);
  }

  /**
   * Updates object retention date. Only works if object is in GOVERNANCE mode. COMPLIANCE mode
   * objects cannot be updated.
   */
  @Override
  public void updateObjectRetention(String key, String versionId, Instant retainUntilDate) {
    // First get current retention to check mode
    GetObjectRetentionResponse currentRetention =
        s3Client.getObjectRetention(transformer.toGetObjectRetentionRequest(key, versionId));

    if (currentRetention == null || currentRetention.retention() == null) {
      throw new FailedPreconditionException(
          "Object does not have retention configured. Cannot update retention.");
    }

    ObjectLockRetentionMode currentMode = currentRetention.retention().mode();

    if (currentMode == ObjectLockRetentionMode.COMPLIANCE) {
      throw new FailedPreconditionException(
          "Cannot update retention for objects in COMPLIANCE mode. "
              + "Only GOVERNANCE mode objects can have their retention updated.");
    }

    s3Client.putObjectRetention(
        transformer.toPutObjectRetentionRequest(key, versionId, currentMode, retainUntilDate));
  }

  /**
   * Provider hook for {@link
   * com.salesforce.multicloudj.blob.driver.BlobStore#updateObjectRetention(String, String,
   * ObjectRetentionConfig)}.
   *
   * <p>Stateless validation has already run; this method enforces the state-dependent rules from
   * {@link ObjectRetentionRules} (no-current-retention, mode-downgrade, shorten-with-bypass) so
   * AWS surfaces the same {@code FailedPreconditionException} types and messages as GCP and the
   * in-memory provider.
   */
  @Override
  protected void doUpdateObjectRetention(
      String key, String versionId, ObjectRetentionConfig config) {
    RetentionMode currentMode = null;
    Instant currentRetainUntil = null;
    try {
      GetObjectRetentionResponse currentRetentionResponse =
          s3Client.getObjectRetention(transformer.toGetObjectRetentionRequest(key, versionId));
      if (currentRetentionResponse != null && currentRetentionResponse.retention() != null) {
        currentMode = toMulticloudMode(currentRetentionResponse.retention().mode());
        currentRetainUntil = currentRetentionResponse.retention().retainUntilDate();
      }
    } catch (S3Exception e) {
      if (e.statusCode() != 404) {
        throw e;
      }
      // 404 means no retention configured — fall through with nulls so ObjectRetentionRules
      // rejects the request with FailedPreconditionException.
    }

    RetentionMode resolvedMode =
        ObjectRetentionRules.resolveAndValidate(currentMode, currentRetainUntil, config);

    boolean bypass = Boolean.TRUE.equals(config.getBypassGovernanceRetention());
    s3Client.putObjectRetention(
        transformer.toPutObjectRetentionRequest(
            key, versionId, toAwsRetentionMode(resolvedMode), config.getRetainUntilDate(), bypass));
  }

  /**
   * Converts a MultiCloudJ {@link RetentionMode} to an AWS {@link ObjectLockRetentionMode}.
   * Mapping: GOVERNANCE↔GOVERNANCE, COMPLIANCE↔COMPLIANCE.
   */
  private static ObjectLockRetentionMode toAwsRetentionMode(RetentionMode mode) {
    return mode == RetentionMode.COMPLIANCE
        ? ObjectLockRetentionMode.COMPLIANCE
        : ObjectLockRetentionMode.GOVERNANCE;
  }

  /** Inverse of {@link #toAwsRetentionMode(RetentionMode)}. */
  private static RetentionMode toMulticloudMode(ObjectLockRetentionMode mode) {
    if (mode == null) {
      return null;
    }
    return mode == ObjectLockRetentionMode.COMPLIANCE
        ? RetentionMode.COMPLIANCE
        : RetentionMode.GOVERNANCE;
  }

  /** Updates legal hold status on an object. */
  @Override
  public void updateLegalHold(String key, String versionId, boolean legalHold) {
    s3Client.putObjectLegalHold(transformer.toPutObjectLegalHoldRequest(key, versionId, legalHold));
  }

  /** Closes the underlying S3 client and releases any resources. */
  @Override
  public void close() {
    if (s3Client != null) {
      s3Client.close();
    }
  }

  @Getter
  public static class Builder extends AbstractBlobStore.Builder<AwsBlobStore, Builder> {

    private S3Client s3Client;
    private AwsTransformerSupplier transformerSupplier = new AwsTransformerSupplier();

    public Builder() {
      providerId(AwsConstants.PROVIDER_ID);
    }

    @Override
    public Builder self() {
      return this;
    }

    /** Helper function to generate the client */
    private static S3Client buildS3Client(Builder builder) {
      Region regionObj = Region.of(builder.getRegion());
      S3ClientBuilder b = S3Client.builder();
      b.region(regionObj);

      AwsCredentialsProvider credentialsProvider =
          CredentialsProvider.getCredentialsProvider(builder.getCredentialsOverrider(), regionObj);
      if (credentialsProvider != null) {
        b.credentialsProvider(credentialsProvider);
      }
      if (builder.getEndpoint() != null) {
        b.endpointOverride(builder.getEndpoint());
      }
      if (shouldConfigureHttpClient(builder)) {
        b.httpClient(generateHttpClient(builder));
      }
      if (builder.getRetryConfig() != null) {
        // Create a temporary transformer instance for retry strategy conversion
        AwsTransformer transformer = builder.getTransformerSupplier().get(builder.getBucket());
        b.overrideConfiguration(
            config -> {
              config.retryStrategy(transformer.toAwsRetryStrategy(builder.getRetryConfig()));
              // Set API call timeouts if provided
              if (builder.getRetryConfig().getAttemptTimeout() != null) {
                config.apiCallAttemptTimeout(
                    Duration.ofMillis(builder.getRetryConfig().getAttemptTimeout()));
              }
              if (builder.getRetryConfig().getTotalTimeout() != null) {
                config.apiCallTimeout(
                    Duration.ofMillis(builder.getRetryConfig().getTotalTimeout()));
              }
            });
      }

      return b.build();
    }

    /** Helper function to generate the HttpClient */
    private static SdkHttpClient generateHttpClient(Builder builder) {
      ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder();
      if (builder.getProxyEndpoint() != null
          || builder.getUseSystemPropertyProxyValues() != null
          || builder.getUseEnvironmentVariableProxyValues() != null) {
        ProxyConfiguration.Builder proxyConfigBuilder = ProxyConfiguration.builder();
        if (builder.getProxyEndpoint() != null) {
          proxyConfigBuilder.endpoint(builder.getProxyEndpoint());
        }
        if (builder.getUseSystemPropertyProxyValues() != null) {
          proxyConfigBuilder.useSystemPropertyValues(builder.getUseSystemPropertyProxyValues());
        }
        if (builder.getUseEnvironmentVariableProxyValues() != null) {
          proxyConfigBuilder.useEnvironmentVariableValues(
              builder.getUseEnvironmentVariableProxyValues());
        }
        httpClientBuilder.proxyConfiguration(proxyConfigBuilder.build());
      }
      if (builder.getMaxConnections() != null) {
        httpClientBuilder.maxConnections(builder.getMaxConnections());
      }
      if (builder.getSocketTimeout() != null) {
        httpClientBuilder.socketTimeout(builder.getSocketTimeout());
      }
      if (builder.getIdleConnectionTimeout() != null) {
        httpClientBuilder.connectionMaxIdleTime(builder.getIdleConnectionTimeout());
      }
      return httpClientBuilder.build();
    }

    public Builder withS3Client(S3Client s3Client) {
      this.s3Client = s3Client;
      return this;
    }

    public Builder withTransformerSupplier(AwsTransformerSupplier transformerSupplier) {
      this.transformerSupplier = transformerSupplier;
      return this;
    }

    @Override
    public AwsBlobStore build() {
      if (s3Client == null) {
        s3Client = buildS3Client(this);
      }

      return new AwsBlobStore(this, s3Client);
    }
  }
}
