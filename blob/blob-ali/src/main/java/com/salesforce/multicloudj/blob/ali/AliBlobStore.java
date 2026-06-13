package com.salesforce.multicloudj.blob.ali;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.OperationOptions;
import com.aliyun.sdk.service.oss2.PresignOptions;
import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.exceptions.OperationException;
import com.aliyun.sdk.service.oss2.exceptions.ServiceException;
import com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadResult;
import com.aliyun.sdk.service.oss2.models.CopyObjectRequest;
import com.aliyun.sdk.service.oss2.models.CopyObjectResult;
import com.aliyun.sdk.service.oss2.models.DeleteMultipleObjectsRequest;
import com.aliyun.sdk.service.oss2.models.DeleteObjectRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectLegalHoldResult;
import com.aliyun.sdk.service.oss2.models.GetObjectMetaRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectResult;
import com.aliyun.sdk.service.oss2.models.GetObjectRetentionResult;
import com.aliyun.sdk.service.oss2.models.GetObjectTaggingRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectTaggingResult;
import com.aliyun.sdk.service.oss2.models.HeadObjectRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectResult;
import com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadResult;
import com.aliyun.sdk.service.oss2.models.ListObjectVersionsRequest;
import com.aliyun.sdk.service.oss2.models.ListObjectVersionsResult;
import com.aliyun.sdk.service.oss2.models.ListObjectsV2Request;
import com.aliyun.sdk.service.oss2.models.ListObjectsV2Result;
import com.aliyun.sdk.service.oss2.models.ListPartsRequest;
import com.aliyun.sdk.service.oss2.models.ListPartsResult;
import com.aliyun.sdk.service.oss2.models.ObjectVersion;
import com.aliyun.sdk.service.oss2.models.PresignResult;
import com.aliyun.sdk.service.oss2.models.PutObjectRequest;
import com.aliyun.sdk.service.oss2.models.PutObjectResult;
import com.aliyun.sdk.service.oss2.models.PutObjectTaggingRequest;
import com.aliyun.sdk.service.oss2.models.UploadPartRequest;
import com.aliyun.sdk.service.oss2.models.UploadPartResult;
import com.aliyun.sdk.service.oss2.retry.Retryer;
import com.aliyun.sdk.service.oss2.transport.BinaryData;
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
import com.salesforce.multicloudj.blob.driver.ObjectLockConfiguration;
import com.salesforce.multicloudj.blob.driver.ObjectLockInfo;
import com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig;
import com.salesforce.multicloudj.blob.driver.ObjectRetentionRules;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.PresignedUrlResponse;
import com.salesforce.multicloudj.blob.driver.RetentionMode;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.ali.AliConstants;
import com.salesforce.multicloudj.common.exceptions.ArchiveInfo;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.provider.Provider;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.Getter;

/** Alibaba implementation of BlobStore */
@AutoService(AbstractBlobStore.class)
public class AliBlobStore extends AbstractBlobStore {

  private static final int COPY_BUFFER_SIZE = 16 * 1024;

  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  private final OSSClient ossClient;
  private final AliTransformer transformer;

  public AliBlobStore() {
    this(new Builder(), null);
  }

  public AliBlobStore(Builder builder, OSSClient ossClient) {
    super(builder);
    this.ossClient = ossClient;
    this.transformer = builder.getTransformerSupplier().get(bucket);
  }

  @Override
  public Provider.Builder builder() {
    return new Builder();
  }

  @Override
  public Class<? extends SubstrateSdkException> getException(Throwable t) {
    if (t instanceof SubstrateSdkException) {
      return (Class<? extends SubstrateSdkException>) t.getClass();
    } else if (t instanceof OperationException) {
      Throwable cause = t.getCause();
      if (cause instanceof ServiceException) {
        String errorCode =
            ((ServiceException) cause).errorCode();
        return ErrorCodeMapping.getException(errorCode);
      }
      return UnknownException.class;
    } else if (t instanceof ServiceException) {
      String errorCode =
          ((ServiceException) t).errorCode();
      return ErrorCodeMapping.getException(errorCode);
    } else if (t instanceof IllegalArgumentException) {
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
    long contentLength = uploadRequest.getContentLength();
    BinaryData body =
        BinaryData.fromStream(
            inputStream, contentLength > 0 ? contentLength : null);
    return doUploadInternal(uploadRequest, body);
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
    BinaryData body =
        BinaryData.fromBytes(content);
    return doUploadInternal(uploadRequest, body);
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
    try {
      BinaryData body =
          BinaryData.fromStream(
              Files.newInputStream(file.toPath()), file.length());
      return doUploadInternal(uploadRequest, body);
    } catch (IOException e) {
      throw new RuntimeException("Failed to read file for upload: " + file.getPath(), e);
    }
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
    return doUpload(uploadRequest, path.toFile());
  }

  /** Helper function to upload blobs */
  protected UploadResponse doUploadInternal(
      UploadRequest uploadRequest,
      BinaryData body) {
    PutObjectRequest request =
        transformer.toPutObjectRequest(uploadRequest, body);
    PutObjectResult result =
        ossClient.putObject(request,
            OperationOptions.defaults());
    UploadResponse response =
        transformer.toUploadResponse(uploadRequest, result);
    applyObjectLockAfterUpload(
        uploadRequest.getKey(), response.getVersionId(),
        uploadRequest.getObjectLock());
    return response;
  }

  private void applyObjectLockAfterUpload(
      String key, String versionId,
      ObjectLockConfiguration lockConfig) {
    if (lockConfig == null) {
      return;
    }
    if (lockConfig.getMode() != null && lockConfig.getRetainUntilDate() != null) {
      ossClient.putObjectRetention(
          transformer.toPutObjectRetentionRequest(
              key, versionId, lockConfig.getMode(),
              lockConfig.getRetainUntilDate(), false),
          OperationOptions.defaults());
    }
    if (lockConfig.isLegalHold()) {
      ossClient.putObjectLegalHold(
          transformer.toPutObjectLegalHoldRequest(key, versionId, true),
          OperationOptions.defaults());
    }
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
    GetObjectRequest request =
        transformer.toGetObjectRequest(downloadRequest);
    try (GetObjectResult result =
        ossClient.getObject(request,
            OperationOptions.defaults())) {
      validateRangeResponse(downloadRequest, result);
      copyStream(result.body(), outputStream);
      return transformer.toDownloadResponse(downloadRequest.getKey(), result);
    } catch (IOException e) {
      throw new RuntimeException("Failed to download Blob: " + downloadRequest.getKey(), e);
    } catch (Exception e) {
      handleArchivedObjects(downloadRequest, e);
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException("Failed to download Blob: " + downloadRequest.getKey(), e);
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
    GetObjectRequest request = transformer.toGetObjectRequest(downloadRequest);
    try (GetObjectResult result = ossClient.getObject(request)) {
      long contentLength = result.contentLength() != null ? result.contentLength() : 0L;
      byteArray.setBytes(readAllBytes(result.body(), contentLength));
      return transformer.toDownloadResponse(downloadRequest.getKey(), result);
    } catch (IOException e) {
      throw new RuntimeException("Failed to download Blob: " + downloadRequest.getKey(), e);
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException("Failed to download Blob: " + downloadRequest.getKey(), e);
    }
  }

  private byte[] readAllBytes(InputStream in, long contentLength) throws IOException {
    if (contentLength > 0 && contentLength <= MAX_ARRAY_SIZE) {
      byte[] bytes = new byte[(int) contentLength];
      int offset = 0;
      while (offset < bytes.length) {
        int read = in.read(bytes, offset, bytes.length - offset);
        if (read == -1) {
          break;
        }
        offset += read;
      }
      return offset == bytes.length ? bytes : Arrays.copyOf(bytes, offset);
    }
    // Fallback: Drain the entire stream when the content length isn't specified.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    copyStream(in, outputStream);
    return outputStream.toByteArray();
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
    return doDownload(downloadRequest, file.toPath());
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
    Path destinationPath =
        createDownloadDestinationPath(downloadRequest, path);
    GetObjectRequest request =
        transformer.toGetObjectRequest(downloadRequest);
    try (GetObjectResult result =
        ossClient.getObject(request,
            OperationOptions.defaults())) {
      validateRangeResponse(downloadRequest, result);
      Files.copy(result.body(), destinationPath);
      return transformer.toDownloadResponse(downloadRequest.getKey(), result);
    } catch (IOException e) {
      throw new RuntimeException("Failed to download Blob: " + downloadRequest.getKey(), e);
    } catch (Exception e) {
      handleArchivedObjects(downloadRequest, e);
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException("Failed to download Blob: " + downloadRequest.getKey(), e);
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
  public DownloadResponse doDownload(DownloadRequest downloadRequest) {
    GetObjectRequest request =
        transformer.toGetObjectRequest(downloadRequest);
    GetObjectResult result;
    try {
      result = ossClient.getObject(request, OperationOptions.defaults());
    } catch (Exception e) {
      handleArchivedObjects(downloadRequest, e);
      throw e;
    }
    try {
      validateRangeResponse(downloadRequest, result);
    } catch (RuntimeException e) {
      try {
        result.close();
      } catch (Exception ignored) {
        // best-effort cleanup
      }
      throw e;
    }
    return transformer.toDownloadResponse(downloadRequest.getKey(), result, result.body());
  }

  /**
   * If the caller asked for archive detection and the OSS GET failed with HTTP 404 carrying
   * the {@code x-oss-delete-marker} header, list the prior versions of the key and throw
   * {@link ResourceNotFoundException} populated with {@link ArchiveInfo} so the caller can
   * recover the archived data via the prior {@code versionId}. Mirrors the on-the-wire
   * delete-marker semantics observed against a real versioned + WORM-enabled bucket
   * (404 NoSuchKey + {@code x-oss-delete-marker: true} + {@code x-oss-version-id} of the
   * marker).
   *
   * <p>The prior version is resolved deterministically by paging through {@code ListObjectVersions}
   * (via the SDK paginator) and stopping at the first entry whose key matches exactly. Because OSS
   * delete markers share the listing page-size budget with real versions, a single bounded page can
   * return markers only; pagination guarantees the real version is found regardless of how many
   * delete markers precede it (e.g. after repeated PUT/DELETE cycles), without relying on a
   * guessed page size.
   *
   * <p>Archive detection is strictly best-effort: this method returns silently — allowing the
   * original exception to propagate unchanged — if (a) {@code checkArchived} is off, (b) the
   * underlying OSS exception is not 404, (c) the 404 response did not carry a delete-marker
   * header, (d) the {@code ListObjectVersions} paging itself fails (network error, permission
   * denied, versioning disabled, throttling), or (e) no prior version of the key could be
   * resolved from the listing. It only throws {@link ResourceNotFoundException} with
   * {@link ArchiveInfo} when it has positively identified an archived prior version
   * ({@code versionId} non-null). This guarantees the guard never masks the caller's original
   * download failure with a secondary error, and never reports {@code archived=true} without a
   * usable {@code versionId}.
   */
  private void handleArchivedObjects(
      DownloadRequest downloadRequest, Throwable failure) {
    if (!downloadRequest.isCheckArchived()) {
      return;
    }
    ServiceException service = unwrapServiceException(failure);
    if (service == null || service.statusCode() != 404) {
      return;
    }
    Map<String, String> headers = service.headers();
    if (headers == null) {
      return;
    }
    boolean isDeleteMarker = false;
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      if (entry.getKey() != null
          && entry.getKey().equalsIgnoreCase("x-oss-delete-marker")
          && "true".equalsIgnoreCase(entry.getValue())) {
        isDeleteMarker = true;
        break;
      }
    }
    if (!isDeleteMarker) {
      return;
    }
    // Resolve the prior (non-marker) version id by listing versions for the key. A simple
    // single-page listing is not deterministic: repeated PUT/DELETE cycles stack multiple delete
    // markers ahead of the first real version, and delete markers share the page-size budget with
    // versions, so a bounded page can come back with markers only. Instead, page through the
    // results (the SDK paginator follows the key/version markers transparently) and stop at the
    // first entry whose key matches exactly — the prefix listing can also return sibling keys.
    // This yields a correct result regardless of how many delete markers precede the version.
    String versionId = null;
    try {
      outer:
      for (ListObjectVersionsResult page :
          ossClient.listObjectVersionsPaginator(
              ListObjectVersionsRequest.newBuilder()
                  .bucket(bucket)
                  .prefix(downloadRequest.getKey())
                  .build())) {
        if (page == null || page.versions() == null) {
          continue;
        }
        for (ObjectVersion v : page.versions()) {
          if (downloadRequest.getKey().equals(v.key())) {
            versionId = v.versionId();
            break outer;
          }
        }
      }
    } catch (Exception listFailure) {
      // Best-effort: if version listing fails (network error, permission denied, versioning
      // disabled, throttling), do not mask the caller's original download failure — let the
      // original exception propagate unchanged.
      return;
    }
    if (versionId == null) {
      // No prior version of the key could be resolved (e.g. only delete markers exist). Rather
      // than report archived=true with a null versionId the caller cannot act on, fall through
      // and let the original 404 propagate unchanged.
      return;
    }
    throw new ResourceNotFoundException(
        "Object is archived (delete marker): " + downloadRequest.getKey(),
        failure,
        ArchiveInfo.builder().archived(true).versionId(versionId).build());
  }

  /**
   * Walks the cause chain of a thrown OSS exception to find the underlying
   * {@link ServiceException}.
   */
  private static ServiceException unwrapServiceException(Throwable t) {
    Throwable cur = t;
    while (cur != null) {
      if (cur instanceof ServiceException) {
        return (ServiceException) cur;
      }
      cur = cur.getCause();
    }
    return null;
  }

  // OSS returns the full object with HTTP 200 when range start exceeds object size,
  // unlike S3/GCS which return HTTP 416. Detect via contentRange absence and throw
  // to make behavior consistent with other substrates.
  private void validateRangeResponse(
      DownloadRequest downloadRequest,
      GetObjectResult result) {
    if (downloadRequest.getStart() == null) {
      return;
    }
    if (result.contentRange() == null) {
      Long contentLength = result.contentLength();
      long objectSize = contentLength != null ? contentLength : 0L;
      if (downloadRequest.getStart() >= objectSize) {
        throw new InvalidArgumentException(
            "The requested range start ("
                + downloadRequest.getStart()
                + ") is not satisfiable for object of size "
                + objectSize);
      }
    }
  }

  private void copyStream(InputStream in, OutputStream out) {
    try {
      byte[] buffer = new byte[COPY_BUFFER_SIZE];
      int bytesRead;
      while ((bytesRead = in.read(buffer)) != -1) {
        out.write(buffer, 0, bytesRead);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deletes a single blob
   *
   * @param key Object name of the Blob
   * @param versionId The versionId of the blob
   */
  @Override
  protected void doDelete(String key, String versionId) {
    DeleteObjectRequest request =
        transformer.toDeleteObjectRequest(key, versionId);
    ossClient.deleteObject(request,
        OperationOptions.defaults());
  }

  /**
   * Deletes a collection of Blobs
   *
   * @param objects A collection of blob identifiers to delete
   */
  @Override
  protected void doDelete(Collection<BlobIdentifier> objects) {
    if (objects.isEmpty()) {
      return;
    }
    DeleteMultipleObjectsRequest request =
        transformer.toDeleteMultipleObjectsRequest(objects);
    ossClient.deleteMultipleObjects(request,
        OperationOptions.defaults());
  }

  /**
   * Copies a Blob to a different bucket
   *
   * @param request the copy request
   * @return CopyResponse of the copied Blob
   */
  @Override
  protected CopyResponse doCopy(CopyRequest request) {
    CopyObjectRequest copyRequest =
        transformer.toCopyObjectRequest(request);
    CopyObjectResult result =
        ossClient.copyObject(copyRequest,
            OperationOptions.defaults());
    return buildCopyResponse(request.getDestKey(), result);
  }

  /**
   * Copies a Blob from a source bucket to the current bucket
   *
   * @param request the copyFrom request
   * @return CopyResponse of the copied Blob
   */
  @Override
  protected CopyResponse doCopyFrom(CopyFromRequest request) {
    CopyObjectRequest copyRequest =
        transformer.toCopyObjectRequest(request);
    CopyObjectResult result =
        ossClient.copyObject(copyRequest,
            OperationOptions.defaults());
    return buildCopyResponse(request.getDestKey(), result);
  }

  private CopyResponse buildCopyResponse(
      String destKey, CopyObjectResult result) {
    CopyResponse response = transformer.toCopyResponse(destKey, result);
    if (response.getLastModified() == null) {
      HeadObjectRequest headRequest =
          transformer.toHeadObjectRequest(destKey, response.getVersionId());
      HeadObjectResult headResult =
          ossClient.headObject(headRequest,
              OperationOptions.defaults());
      return CopyResponse.builder()
          .key(response.getKey())
          .versionId(response.getVersionId())
          .eTag(response.getETag())
          .lastModified(transformer.parseLastModified(headResult.lastModified()))
          .build();
    }
    return response;
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
    HeadObjectRequest request =
        transformer.toHeadObjectRequest(key, versionId);
    HeadObjectResult result =
        ossClient.headObject(request,
            OperationOptions.defaults());
    return transformer.toBlobMetadata(key, result);
  }

  /**
   * Lists all objects in the bucket
   *
   * @return Iterator of the list
   */
  @Override
  protected Iterator<BlobInfo> doList(ListBlobsRequest request) {
    return new BlobInfoIterator(ossClient, transformer, request);
  }

  /**
   * Lists a single page of objects in the bucket with pagination support
   *
   * @param request The list request containing filters and optional pagination token
   * @return ListBlobsPageResult containing the blobs, truncation status, and next page token
   */
  @Override
  protected ListBlobsPageResponse doListPage(ListBlobsPageRequest request) {
    ListObjectsV2Request listRequest =
        transformer.toListObjectsRequest(request);
    ListObjectsV2Result response =
        ossClient.listObjectsV2(listRequest,
            OperationOptions.defaults());
    return transformer.toListBlobsPageResponse(response);
  }

  @Override
  protected Iterator<BlobMetadata> doListBlobVersions(ListBlobVersionsRequest request) {
    return new BlobMetadataIterator(ossClient, getBucket(), request.getKey());
  }

  /**
   * Initiates a multipart upload
   *
   * @param request the multipart request
   * @return An object that acts as an identifier for subsequent related multipart operations
   */
  @Override
  protected MultipartUpload doInitiateMultipartUpload(final MultipartUploadRequest request) {
    AliTransformer.rejectUnsupportedChecksum(request.getChecksumAlgorithm());
    InitiateMultipartUploadRequest ossRequest =
        transformer.toInitiateMultipartUploadRequest(request);
    InitiateMultipartUploadResult result =
        ossClient.initiateMultipartUpload(ossRequest,
            OperationOptions.defaults());
    return transformer.toMultipartUpload(result, request);
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
    UploadPartRequest request =
        transformer.toUploadPartRequest(mpu, mpp);
    UploadPartResult result =
        ossClient.uploadPart(request,
            OperationOptions.defaults());
    return transformer.toUploadPartResponse(mpp, result);
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
    CompleteMultipartUploadRequest request =
        transformer.toCompleteMultipartUploadRequest(mpu, parts);
    CompleteMultipartUploadResult result =
        ossClient.completeMultipartUpload(request,
            OperationOptions.defaults());
    // OSS does not support object lock headers on multipart initiation, so apply retention
    // and legal hold after the object is assembled.
    applyObjectLockAfterUpload(mpu.getKey(), result.versionId(), mpu.getObjectLock());
    // OSS computes a CRC64 over the assembled object and returns it on the result; surface it
    // as the cross-cloud composite checksum on MultipartUploadResponse.
    return new MultipartUploadResponse(
        stripQuotes(result.completeMultipartUpload().eTag()),
        result.hashCRC64());
  }

  /**
   * List all parts that have been uploaded for the multipartUpload so far
   *
   * @param mpu The multipartUpload identifier
   * @return Returns a list of all uploaded parts
   */
  protected List<UploadPartResponse> doListMultipartUpload(final MultipartUpload mpu) {
    ListPartsRequest request =
        transformer.toListPartsRequest(mpu);
    ListPartsResult result =
        ossClient.listParts(request,
            OperationOptions.defaults());
    return transformer.toListUploadPartResponse(result);
  }

  /**
   * Aborts a multipartUpload that's in progress
   *
   * @param mpu The multipartUpload identifier
   */
  protected void doAbortMultipartUpload(final MultipartUpload mpu) {
    ossClient.abortMultipartUpload(transformer.toAbortMultipartUploadRequest(mpu),
        OperationOptions.defaults());
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

  /**
   * Returns a map of all the tags associated with the blob
   *
   * @param key Name of the blob whose tags are to be retrieved
   * @return The blob's tags
   */
  @Override
  protected Map<String, String> doGetTags(String key) {
    GetObjectTaggingRequest request =
        GetObjectTaggingRequest.newBuilder()
            .bucket(bucket)
            .key(key)
            .build();
    GetObjectTaggingResult result =
        ossClient.getObjectTagging(request,
            OperationOptions.defaults());
    return transformer.toTagMap(result);
  }

  /**
   * Sets tags on a blob
   *
   * @param key Name of the blob to set tags on
   * @param tags The tags to set
   */
  @Override
  protected void doSetTags(String key, Map<String, String> tags) {
    PutObjectTaggingRequest request =
        transformer.toPutObjectTaggingRequest(key, tags);
    ossClient.putObjectTagging(request,
        OperationOptions.defaults());
  }

  @Override
  public ObjectLockInfo getObjectLock(String key, String versionId) {
    // OSS exposes retention and legal hold as two separate calls, and returns 404
    // (NoSuchObjectRetention / NoSuchObjectLegalHoldConfiguration) when that specific
    // configuration was never set on the object. An object may have retention but no
    // legal hold (or vice versa), so each absence is treated as "not configured"
    // rather than an error.
    GetObjectRetentionResult retentionResult = null;
    try {
      retentionResult = ossClient.getObjectRetention(
          transformer.toGetObjectRetentionRequest(key, versionId),
          OperationOptions.defaults());
    } catch (Exception e) {
      if (!isNoSuchConfiguration(e)) {
        throw e;
      }
    }

    GetObjectLegalHoldResult legalHoldResult = null;
    try {
      legalHoldResult = ossClient.getObjectLegalHold(
          transformer.toGetObjectLegalHoldRequest(key, versionId),
          OperationOptions.defaults());
    } catch (Exception e) {
      if (!isNoSuchConfiguration(e)) {
        throw e;
      }
    }

    return transformer.toObjectLockInfo(retentionResult, legalHoldResult);
  }

  /**
   * Returns true when the throwable represents an OSS "configuration not found" response
   * for retention or legal hold — i.e. the object exists but simply does not have that
   * lock configuration set. This is NOT an error for {@link #getObjectLock}.
   *
   * <p>Matches 404 errors with codes like {@code NoSuchObjectRetentionConfiguration} or
   * {@code NoSuchObjectLegalHoldConfiguration}, but explicitly excludes {@code NoSuchKey}
   * (object does not exist) which should propagate as an error.
   */
  private static boolean isNoSuchConfiguration(Throwable t) {
    ServiceException se = extractServiceException(t);
    if (se == null) {
      return false;
    }
    String errorCode = se.errorCode();
    return se.statusCode() == 404
        && errorCode != null
        && errorCode.startsWith("NoSuch")
        && !"NoSuchKey".equals(errorCode);
  }

  private static ServiceException extractServiceException(Throwable t) {
    if (t instanceof ServiceException) {
      return (ServiceException) t;
    } else if (t instanceof OperationException
        && t.getCause() instanceof ServiceException) {
      return (ServiceException) t.getCause();
    }
    return null;
  }

  // TODO(objectlock): OSS stores/returns retention timestamps at coarser (second/millisecond)
  // precision than the Instant we send (Instant.now() carries micro/nanoseconds), so a
  // round-tripped retainUntilDate is truncated. The shorten check below
  // (ObjectRetentionRules.resolveAndValidate -> config.getRetainUntilDate().isBefore(
  // currentRetainUntil)) compares the caller's full-precision value against the truncated
  // stored value; at sub-second boundaries this could misclassify an extend vs. shorten.
  // Negligible under whole-second usage (conformance tests use whole-second constants), but
  // revisit if sub-second retain dates are ever supported — likely normalize/truncate to
  // seconds on both the inbound config and the parsed current value before comparing.
  @Override
  protected void doUpdateObjectRetention(
      String key, String versionId, ObjectRetentionConfig config) {
    // Fetch current retention state. OSS returns 404 NoSuchObjectRetentionConfiguration
    // when the object has no retention set — treat that as "no current retention" so
    // ObjectRetentionRules.resolveAndValidate can reject with FailedPreconditionException.
    GetObjectRetentionResult currentResult = null;
    try {
      currentResult = ossClient.getObjectRetention(
          transformer.toGetObjectRetentionRequest(key, versionId),
          OperationOptions.defaults());
    } catch (Exception e) {
      if (!isNoSuchConfiguration(e)) {
        throw e;
      }
      // currentResult stays null → no current retention
    }

    RetentionMode currentMode = null;
    java.time.Instant currentRetainUntil = null;
    if (currentResult != null && currentResult.retention() != null) {
      currentMode = AliTransformer.toRetentionMode(
          com.aliyun.sdk.service.oss2.models.ObjectRetentionModeType
              .fromString(currentResult.retention().mode()));
      if (currentResult.retention().retainUntilDate() != null) {
        currentRetainUntil = java.time.Instant.parse(
            currentResult.retention().retainUntilDate());
      }
    }

    RetentionMode resolvedMode = ObjectRetentionRules.resolveAndValidate(
        currentMode, currentRetainUntil, config);

    // OSS does not support changing an object's retention mode once set. A
    // GOVERNANCE -> COMPLIANCE upgrade (which AWS/GCP allow with a bypass) is rejected
    // server-side with HTTP 409 FileImmutable. Detect it here and fail with a clear,
    // typed exception rather than leaking the provider-specific HTTP error. The shared
    // ObjectRetentionRules has already allowed this transition for bypass-capable
    // providers; this is the OSS-specific platform limitation.
    if (currentMode == RetentionMode.GOVERNANCE
        && resolvedMode == RetentionMode.COMPLIANCE) {
      throw new UnSupportedOperationException(
          "Alibaba OSS does not support upgrading an object's retention mode from "
              + "GOVERNANCE to COMPLIANCE; the mode is immutable once set.");
    }

    ossClient.putObjectRetention(
        transformer.toPutObjectRetentionRequest(
            key, versionId, resolvedMode,
            config.getRetainUntilDate(),
            config.getBypassGovernanceRetention()),
        OperationOptions.defaults());
  }

  @Override
  public void updateLegalHold(String key, String versionId, boolean legalHold) {
    ossClient.putObjectLegalHold(
        transformer.toPutObjectLegalHoldRequest(key, versionId, legalHold),
        OperationOptions.defaults());
  }

  @Override
  protected PresignedUrlResponse doPresign(PresignedUrlRequest request) {
    PresignOptions options = transformer.toPresignOptions(request);
    PresignResult result;
    switch (request.getType()) {
      case UPLOAD:
        result = ossClient.presign(transformer.toPresignedPutObjectRequest(request), options);
        break;
      case DOWNLOAD:
        result = ossClient.presign(transformer.toPresignedGetObjectRequest(request), options);
        break;
      default:
        throw new InvalidArgumentException(
            "Unsupported PresignedOperation. type=" + request.getType());
    }
    try {
      return PresignedUrlResponse.builder()
          .url(new URL(result.url()))
          .signedHeaders(result.signedHeaders().orElse(Map.of()))
          .expiration(result.expiration().orElse(null))
          .build();
    } catch (java.net.MalformedURLException e) {
      throw new RuntimeException("Invalid presigned URL: " + result.url(), e);
    }
  }

  /** {@inheritdoc} */
  @Override
  protected boolean doDoesObjectExist(String key, String versionId) {
    GetObjectMetaRequest.Builder reqBuilder =
        GetObjectMetaRequest.newBuilder()
            .bucket(bucket)
            .key(key);
    if (versionId != null) {
      reqBuilder.versionId(versionId);
    }
    return ossClient.doesObjectExist(reqBuilder.build());
  }

  /**
   * Determines if the bucket exists
   *
   * @return Returns true if the bucket exists. Returns false if it doesn't exist.
   */
  @Override
  protected boolean doDoesBucketExist() {
    return ossClient.doesBucketExist(bucket);
  }

  @Override
  public void close() {
    if (ossClient != null) {
      try {
        ossClient.close();
      } catch (Exception e) {
        throw new SubstrateSdkException("Failed to close Ali OSS client", e);
      }
    }
  }

  @Getter
  public static class Builder extends AbstractBlobStore.Builder<AliBlobStore, Builder> {

    private OSSClient client;
    private AliTransformerSupplier transformerSupplier = new AliTransformerSupplier();

    public Builder() {
      providerId(AliConstants.PROVIDER_ID);
    }

    @Override
    public Builder self() {
      return this;
    }

    public Builder withClient(OSSClient client) {
      this.client = client;
      return this;
    }

    public Builder withTransformerSupplier(AliTransformerSupplier transformerSupplier) {
      this.transformerSupplier = transformerSupplier;
      return this;
    }

    private static OSSClient buildOSSClient(Builder builder) {
      CredentialsProvider creds =
          OSSCredentialsProvider.getCredentialsProvider(
              builder.getCredentialsOverrider(), builder.getRegion());
      if (creds == null) {
        return null;
      }

      var clientBuilder = OSSClient.newBuilder()
          .region(builder.getRegion())
          .credentialsProvider(creds);

      if (builder.getEndpoint() != null) {
        clientBuilder.endpoint(builder.getEndpoint().toString());
      }
      if (builder.getProxyEndpoint() != null) {
        clientBuilder.proxyHost(
            builder.getProxyEndpoint().getHost()
                + ":" + builder.getProxyEndpoint().getPort());
      }
      if (builder.getRetryConfig() != null) {
        Retryer retryer = AliTransformer.toAliRetryer(builder.getRetryConfig());
        if (retryer != null) {
          clientBuilder.retryer(retryer);
        }
        if (builder.getRetryConfig().getAttemptTimeout() != null) {
          clientBuilder.readWriteTimeout(
              Duration.ofMillis(builder.getRetryConfig().getAttemptTimeout()));
        }
      }

      return clientBuilder.build();
    }

    @Override
    public AliBlobStore build() {
      OSSClient ossClient = this.client;
      if (ossClient == null) {
        ossClient = buildOSSClient(this);
      }
      return new AliBlobStore(this, ossClient);
    }
  }
}
