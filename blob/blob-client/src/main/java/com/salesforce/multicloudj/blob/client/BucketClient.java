package com.salesforce.multicloudj.blob.client;

import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.BlobSpanNames;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyFromRequest;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.ListObjectVersionsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.MultipartUploadResponse;
import com.salesforce.multicloudj.blob.driver.ObjectLockInfo;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.observability.MultiCloudJLogger;
import com.salesforce.multicloudj.common.observability.OperationContext;
import com.salesforce.multicloudj.common.observability.TracingPolicy;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Entry point for Client code to interact with the Blob storage. */
public class BucketClient implements AutoCloseable {

  private static final String SDK_SERVICE = "blob";

  protected AbstractBlobStore blobStore;
  protected final MultiCloudJLogger multiCloudJLogger;

  protected BucketClient(AbstractBlobStore blobStore) {
    this(blobStore, null);
  }

  protected BucketClient(AbstractBlobStore blobStore, TracingPolicy tracingPolicy) {
    this.blobStore = blobStore;
    this.multiCloudJLogger =
        new MultiCloudJLogger(
            tracingPolicy, SDK_SERVICE, blobStore != null ? blobStore.getProviderId() : null);
  }

  public static BlobBuilder builder(String providerId) {
    return new BlobBuilder(providerId);
  }

  public String getBucket() {
    return blobStore.getBucket();
  }

  /**
   * Uploads the Blob content to substrate-specific Blob storage. Note: Specifying the contentLength
   * in the UploadRequest can dramatically improve upload efficiency because the substrate SDKs do
   * not need to buffer the contents and calculate it themselves.
   *
   * @param uploadRequest Wrapper, containing upload data
   * @param inputStream The input stream that contains the blob content
   * @return Returns an UploadResponse object that contains metadata about the blob
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public UploadResponse upload(UploadRequest uploadRequest, InputStream inputStream) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.UPLOAD,
        bucketAttrs(),
        uploadRequest.getOperationContext(),
        ctx -> {
          UploadRequest enriched = withResolvedContext(uploadRequest, ctx);
          try {
            return withCorrelationId(blobStore.upload(enriched, inputStream), ctx);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Uploads the Blob content to substrate-specific Blob storage
   *
   * @param uploadRequest Wrapper, containing upload data
   * @param content The byte array that contains the blob content
   * @return Returns an UploadResponse object that contains metadata about the blob
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public UploadResponse upload(UploadRequest uploadRequest, byte[] content) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.UPLOAD,
        bucketAttrs(),
        uploadRequest.getOperationContext(),
        ctx -> {
          UploadRequest enriched = withResolvedContext(uploadRequest, ctx);
          try {
            return withCorrelationId(blobStore.upload(enriched, content), ctx);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Uploads the Blob content to substrate-specific Blob storage
   *
   * @param uploadRequest Wrapper, containing upload data
   * @param file The File that contains the blob content
   * @return Returns an UploadResponse object that contains metadata about the blob
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public UploadResponse upload(UploadRequest uploadRequest, File file) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.UPLOAD,
        bucketAttrs(),
        uploadRequest.getOperationContext(),
        ctx -> {
          UploadRequest enriched = withResolvedContext(uploadRequest, ctx);
          try {
            return withCorrelationId(blobStore.upload(enriched, file), ctx);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Uploads the Blob content to substrate-specific Blob storage
   *
   * @param uploadRequest Wrapper, containing upload data
   * @param path The Path that contains the blob content
   * @return Returns an UploadResponse object that contains metadata about the blob
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public UploadResponse upload(UploadRequest uploadRequest, Path path) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.UPLOAD,
        bucketAttrs(),
        uploadRequest.getOperationContext(),
        ctx -> {
          UploadRequest enriched = withResolvedContext(uploadRequest, ctx);
          try {
            return withCorrelationId(blobStore.upload(enriched, path), ctx);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Downloads the Blob content from substrate-specific Blob storage
   *
   * @param downloadRequest downloadRequest Wrapper, containing download data
   * @param outputStream The output stream that the blob content will be written to
   * @return Returns a DownloadResponse object that contains metadata about the blob
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public DownloadResponse download(DownloadRequest downloadRequest, OutputStream outputStream) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.DOWNLOAD,
        bucketAttrs(),
        downloadRequest.getOperationContext(),
        ctx -> {
          try {
            return withCorrelationId(blobStore.download(downloadRequest, outputStream), ctx);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Downloads the Blob content from substrate-specific Blob storage
   *
   * @param downloadRequest downloadRequest Wrapper, containing download data
   * @param byteArray The byte array that blob content will be written to
   * @return Returns a DownloadResponse object that contains metadata about the blob
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public DownloadResponse download(DownloadRequest downloadRequest, ByteArray byteArray) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.DOWNLOAD,
        bucketAttrs(),
        downloadRequest.getOperationContext(),
        ctx -> {
          try {
            return withCorrelationId(blobStore.download(downloadRequest, byteArray), ctx);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Downloads the Blob content from substrate-specific Blob storage.
   *
   * @param downloadRequest downloadRequest Wrapper, containing download data
   * @param file The File the blob content will be written to
   * @return Returns a DownloadResponse object that contains metadata about the blob
   * @throws SubstrateSdkException Thrown if the operation fails. Throws an exception if the file
   *     already exists.
   */
  public DownloadResponse download(DownloadRequest downloadRequest, File file) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.DOWNLOAD,
        bucketAttrs(),
        downloadRequest.getOperationContext(),
        ctx -> {
          try {
            return withCorrelationId(blobStore.download(downloadRequest, file), ctx);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Downloads the Blob content from substrate-specific Blob storage.
   *
   * @param downloadRequest downloadRequest Wrapper, containing download data
   * @param path The Path that blob content will be written to
   * @return Returns a DownloadResponse object that contains metadata about the blob
   * @throws SubstrateSdkException Thrown if the operation fails. Throws an exception if a file
   *     already exists at the path location.
   */
  public DownloadResponse download(DownloadRequest downloadRequest, Path path) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.DOWNLOAD,
        bucketAttrs(),
        downloadRequest.getOperationContext(),
        ctx -> {
          try {
            return withCorrelationId(blobStore.download(downloadRequest, path), ctx);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Downloads the Blob content and returns an InputStream for reading the content
   *
   * @param downloadRequest downloadRequest Wrapper, containing download data
   * @return Returns a DownloadResponse object that contains metadata about the blob and an
   *     InputStream for reading the content
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public DownloadResponse download(DownloadRequest downloadRequest) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.DOWNLOAD,
        bucketAttrs(),
        downloadRequest.getOperationContext(),
        ctx -> {
          try {
            return withCorrelationId(blobStore.download(downloadRequest), ctx);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Deletes a single blob from substrate-specific Blob storage.
   *
   * @param key Object name of the Blob
   * @param versionId The versionId of the blob. This field is optional and should be null unless
   *     you're targeting the deletion of a specific key/version blob.
   * @throws SubstrateSdkException Thrown if the operation fails. Will not throw an exception if the
   *     blob does not exist.
   */
  public void delete(String key, String versionId) {
    multiCloudJLogger.traceVoidOperation(
        BlobSpanNames.DELETE,
        bucketAttrs(),
        null,
        ctx -> {
          try {
            blobStore.delete(key, versionId);
          } catch (Throwable t) {
            propagate(t);
          }
        });
  }

  /**
   * Deletes a collection of Blobs from a substrate-specific Blob storage.
   *
   * @param objects A collection of blob identifiers to delete
   * @throws SubstrateSdkException Thrown if the operation fails. Will not throw an exception if a
   *     blob in the list does not exist.
   */
  public void delete(Collection<BlobIdentifier> objects) {
    multiCloudJLogger.traceVoidOperation(
        BlobSpanNames.DELETE,
        bucketAttrs(),
        null,
        ctx -> {
          try {
            blobStore.delete(objects);
          } catch (Throwable t) {
            propagate(t);
          }
        });
  }

  /**
   * Copies the Blob to other bucket
   *
   * @param request copy request wrapper. Contains the information necessary to perform a copy
   * @return CopyResponse of the copied Blob
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public CopyResponse copy(CopyRequest request) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.COPY,
        bucketAttrs(),
        request.getOperationContext(),
        ctx -> {
          try {
            return withCorrelationId(blobStore.copy(request), ctx);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Copies the Blob from other bucket to the current bucket
   *
   * @param request copyFrom request wrapper. Contains the information necessary to perform a copy
   *     from a source bucket
   * @return CopyResponse of the copied Blob
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public CopyResponse copyFrom(CopyFromRequest request) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.COPY_FROM,
        bucketAttrs(),
        request.getOperationContext(),
        ctx -> {
          try {
            return withCorrelationId(blobStore.copyFrom(request), ctx);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Retrieves the metadata of the Blob
   *
   * @param key Name of the Blob, whose metadata is to be retrieved
   * @param versionId The versionId of the blob. This field is optional and only used if your bucket
   *     has versioning enabled. This value should be null unless you're targeting a specific
   *     key/version blob.
   * @return Metadata of the Blob
   * @throws SubstrateSdkException Thrown if the operation fails. Throws an exception if the blob
   *     does not exist.
   */
  public BlobMetadata getMetadata(String key, String versionId) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.GET_METADATA,
        bucketAttrs(),
        null,
        ctx -> {
          try {
            return withCorrelationId(blobStore.getMetadata(key, versionId), ctx);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Retrieves the list of Blob in the bucket
   *
   * @return Iterator object of the Blobs
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public Iterator<BlobInfo> list(ListBlobsRequest request) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.LIST,
        bucketAttrs(),
        null,
        ctx -> {
          try {
            return blobStore.list(request);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Retrieves a single page of blobs from the bucket with pagination support
   *
   * @param request The pagination request containing filters, pagination token, and max results
   * @return ListBlobsPageResponse containing the blobs, truncation status, and next page token
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public ListBlobsPageResponse listPage(ListBlobsPageRequest request) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.LIST_PAGE,
        bucketAttrs(),
        null,
        ctx -> {
          try {
            return blobStore.listPage(request);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Lists all available versions for a given object key.
   *
   * @param request The list versions request containing the target key and optional limits
   * @return Iterator object of BlobMetadata for each version
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public Iterator<BlobMetadata> listObjectVersions(ListObjectVersionsRequest request) {
    try {
      return blobStore.listObjectVersions(request);
    } catch (Throwable t) {
      Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
      ExceptionHandler.handleAndPropagate(exception, t);
      return null;
    }
  }

  /**
   * Initiates a multipartUpload for a Blob
   *
   * @param request Contains information about the blob to upload
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public MultipartUpload initiateMultipartUpload(MultipartUploadRequest request) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.INITIATE_MULTIPART_UPLOAD,
        bucketAttrs(),
        null,
        ctx -> {
          try {
            return blobStore.initiateMultipartUpload(request);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Uploads a part of the multipartUpload
   *
   * @param mpu The multipartUpload to use
   * @param mpp The multipartPart data
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public UploadPartResponse uploadMultipartPart(MultipartUpload mpu, MultipartPart mpp) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.UPLOAD_MULTIPART_PART,
        bucketAttrs(),
        null,
        ctx -> {
          try {
            return blobStore.uploadMultipartPart(mpu, mpp);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Completes a multipartUpload
   *
   * @param mpu The multipartUpload to use
   * @param parts A list of the parts contained in the multipartUpload
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public MultipartUploadResponse completeMultipartUpload(
      MultipartUpload mpu, List<UploadPartResponse> parts) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.COMPLETE_MULTIPART_UPLOAD,
        bucketAttrs(),
        null,
        ctx -> {
          try {
            return blobStore.completeMultipartUpload(mpu, parts);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Returns a list of all uploaded parts for the given MultipartUpload
   *
   * @param mpu The multipartUpload to query against
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public List<UploadPartResponse> listMultipartUpload(MultipartUpload mpu) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.LIST_MULTIPART_UPLOAD,
        bucketAttrs(),
        null,
        ctx -> {
          try {
            return blobStore.listMultipartUpload(mpu);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Aborts a multipartUpload
   *
   * @param mpu The multipartUpload to abort
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public void abortMultipartUpload(MultipartUpload mpu) {
    multiCloudJLogger.traceVoidOperation(
        BlobSpanNames.ABORT_MULTIPART_UPLOAD,
        bucketAttrs(),
        null,
        ctx -> {
          try {
            blobStore.abortMultipartUpload(mpu);
          } catch (Throwable t) {
            propagate(t);
          }
        });
  }

  /**
   * Returns a map of all the tags associated with the blob.
   *
   * @param key Name of the blob whose tags are to be retrieved
   * @return The blob's tags
   * @throws SubstrateSdkException Thrown if the operation fails. Throws an exception if the blob
   *     does not exist.
   */
  public Map<String, String> getTags(String key) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.GET_TAGS,
        bucketAttrs(),
        null,
        ctx -> {
          try {
            return blobStore.getTags(key);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Sets tags on a blob.
   *
   * @param key Name of the blob to set tags on
   * @param tags The tags to set
   * @throws SubstrateSdkException Thrown if the operation fails. Throws an exception if the blob
   *     does not exist.
   */
  public void setTags(String key, Map<String, String> tags) {
    multiCloudJLogger.traceVoidOperation(
        BlobSpanNames.SET_TAGS,
        bucketAttrs(),
        null,
        ctx -> {
          try {
            blobStore.setTags(key, tags);
          } catch (Throwable t) {
            propagate(t);
          }
        });
  }

  /**
   * Generates a presigned URL for uploading/downloading blobs
   *
   * @param request The presigned request
   * @return Returns the presigned URL
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public URL generatePresignedUrl(PresignedUrlRequest request) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.GENERATE_PRESIGNED_URL,
        bucketAttrs(),
        request.getOperationContext(),
        ctx -> {
          try {
            return blobStore.generatePresignedUrl(request);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Determines if an object exists for a given key/versionId
   *
   * @param key Name of the blob to check
   * @param versionId The version of the blob to check. This field is optional and should be null
   *     unless you're checking for the existence of a specific key/version blob.
   * @return Returns true if the object exists. Returns false if it doesn't exist.
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public boolean doesObjectExist(String key, String versionId) {
    Boolean result =
        multiCloudJLogger.traceOperation(
            BlobSpanNames.DOES_OBJECT_EXIST,
            bucketAttrs(),
            null,
            ctx -> {
              try {
                return blobStore.doesObjectExist(key, versionId);
              } catch (Throwable t) {
                propagate(t);
                return false;
              }
            });
    return Boolean.TRUE.equals(result);
  }

  /**
   * Determines if the bucket exists
   *
   * @return Returns true if the bucket exists. Returns false if it doesn't exist.
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public boolean doesBucketExist() {
    Boolean result =
        multiCloudJLogger.traceOperation(
            BlobSpanNames.DOES_BUCKET_EXIST,
            bucketAttrs(),
            null,
            ctx -> {
              try {
                return blobStore.doesBucketExist();
              } catch (Throwable t) {
                propagate(t);
                return false;
              }
            });
    return Boolean.TRUE.equals(result);
  }

  /**
   * Gets object lock configuration for a blob.
   *
   * @param key Object key
   * @param versionId Optional version ID. For versioned buckets, null means latest version.
   * @return ObjectLockInfo containing lock configuration, or null if object lock is not configured
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public ObjectLockInfo getObjectLock(String key, String versionId) {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.GET_OBJECT_LOCK,
        bucketAttrs(),
        null,
        ctx -> {
          try {
            return blobStore.getObjectLock(key, versionId);
          } catch (Throwable t) {
            propagate(t);
            return null;
          }
        });
  }

  /**
   * Updates object retention date, preserving the object's current retention mode.
   *
   * <p><strong>Deprecated.</strong> Prefer {@link #updateObjectRetention(String, String,
   * com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig)} which accepts an explicit mode
   * and bypass flag. This method is retained for backward compatibility.
   *
   * <p><strong>Existing non-uniform behavior:</strong> The AWS provider has historically thrown
   * {@code FailedPreconditionException} for any update on a COMPLIANCE-mode object — even
   * extension. GCP allows extending LOCKED. The new overload follows uniform rules across
   * providers; this deprecated method preserves each provider's existing semantics.
   *
   * @param key Object key
   * @param versionId Optional version ID. For versioned buckets, null means latest version.
   * @param retainUntilDate New retention expiration date
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  @Deprecated
  public void updateObjectRetention(String key, String versionId, Instant retainUntilDate) {
    multiCloudJLogger.traceVoidOperation(
        BlobSpanNames.UPDATE_OBJECT_RETENTION,
        bucketAttrs(),
        null,
        ctx -> {
          try {
            blobStore.updateObjectRetention(key, versionId, retainUntilDate);
          } catch (Throwable t) {
            propagate(t);
          }
        });
  }

  /**
   * Updates per-object retention with explicit mode and bypass control.
   *
   * <p>See {@link com.salesforce.multicloudj.blob.driver.BlobStore#updateObjectRetention(String,
   * String, com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig)} for the full rules
   * table governing every {@code (config.mode, config.bypassGovernanceRetention, current state,
   * new date vs current)} combination.
   *
   * @param key Object key
   * @param versionId Optional version ID. For versioned buckets, null means latest version.
   * @param config retention configuration (mode, retain-until date, bypass flag)
   * @throws SubstrateSdkException Thrown if the operation fails
   * @throws IllegalArgumentException if {@code config} or {@code config.retainUntilDate} is null
   */
  public void updateObjectRetention(
      String key,
      String versionId,
      com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig config) {
    try {
      blobStore.updateObjectRetention(key, versionId, config);
    } catch (Throwable t) {
      Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
      ExceptionHandler.handleAndPropagate(exception, t);
    }
  }

  /**
   * Updates legal hold status on an object.
   *
   * @param key Object key
   * @param versionId Optional version ID. For versioned buckets, null means latest version.
   * @param legalHold true to apply hold, false to release hold
   * @throws SubstrateSdkException Thrown if the operation fails
   */
  public void updateLegalHold(String key, String versionId, boolean legalHold) {
    multiCloudJLogger.traceVoidOperation(
        BlobSpanNames.UPDATE_LEGAL_HOLD,
        bucketAttrs(),
        null,
        ctx -> {
          try {
            blobStore.updateLegalHold(key, versionId, legalHold);
          } catch (Throwable t) {
            propagate(t);
          }
        });
  }

  /** Closes the underlying blob store and releases any resources. */
  @Override
  public void close() throws Exception {
    if (blobStore != null) {
      blobStore.close();
    }
  }

  // ---- helpers ------------------------------------------------------------

  private Map<String, String> bucketAttrs() {
    String b = blobStore.getBucket();
    return b != null ? Map.of("bucket", b) : null;
  }

  private void propagate(Throwable t) {
    Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
    ExceptionHandler.handleAndPropagate(exception, t);
  }

  private static UploadResponse withCorrelationId(UploadResponse r, OperationContext ctx) {
    if (r == null) {
      return null;
    }
    return UploadResponse.builder()
        .key(r.getKey())
        .versionId(r.getVersionId())
        .eTag(r.getETag())
        .checksumValue(r.getChecksumValue())
        .correlationId(ctx.getCorrelationId())
        .build();
  }

  private static DownloadResponse withCorrelationId(DownloadResponse r, OperationContext ctx) {
    if (r == null) {
      return null;
    }
    return DownloadResponse.builder()
        .key(r.getKey())
        .metadata(withCorrelationId(r.getMetadata(), ctx))
        .inputStream(r.getInputStream())
        .correlationId(ctx.getCorrelationId())
        .build();
  }

  private static BlobMetadata withCorrelationId(BlobMetadata m, OperationContext ctx) {
    if (m == null) {
      return null;
    }
    return BlobMetadata.builder()
        .key(m.getKey())
        .versionId(m.getVersionId())
        .eTag(m.getETag())
        .objectSize(m.getObjectSize())
        .metadata(m.getMetadata())
        .lastModified(m.getLastModified())
        .createdTime(m.getCreatedTime())
        .md5(m.getMd5())
        .contentType(m.getContentType())
        .objectLockInfo(m.getObjectLockInfo())
        .correlationId(ctx.getCorrelationId())
        .build();
  }

  private static CopyResponse withCorrelationId(CopyResponse r, OperationContext ctx) {
    if (r == null) {
      return null;
    }
    return CopyResponse.builder()
        .key(r.getKey())
        .versionId(r.getVersionId())
        .eTag(r.getETag())
        .lastModified(r.getLastModified())
        .correlationId(ctx.getCorrelationId())
        .build();
  }

  /**
   * Returns a copy of {@code req} with the resolved {@link OperationContext} attached so the
   * provider's transformer can read the same correlation id that this call's trace/log/MDC
   * emit (auto-generating a UUID via {@code MultiCloudJLogger} when the caller didn't supply
   * one). The transformer then persists that id onto the stored object metadata under
   * {@link com.salesforce.multicloudj.blob.driver.BlobMetadataKeys#CORRELATION_ID}.
   */
  static UploadRequest withResolvedContext(UploadRequest req, OperationContext ctx) {
    if (ctx == req.getOperationContext()) {
      return req;
    }
    return UploadRequest.builder()
        .withKey(req.getKey())
        .withContentLength(req.getContentLength())
        .withMetadata(req.getMetadata())
        .withTags(req.getTags())
        .withStorageClass(req.getStorageClass())
        .withKmsKeyId(req.getKmsKeyId())
        .withUseKmsManagedKey(req.isUseKmsManagedKey())
        .withObjectLock(req.getObjectLock())
        .withChecksumValue(req.getChecksumValue())
        .withChecksumAlgorithm(req.getChecksumAlgorithm())
        .withContentType(req.getContentType())
        .withOperationContext(ctx)
        .build();
  }

  public static class BlobBuilder {

    private final AbstractBlobStore.Builder<?, ?> blobStoreBuilder;

    public BlobBuilder(String providerId) {
      this.blobStoreBuilder = ProviderSupplier.findProviderBuilder(providerId);
    }

    /**
     * Method to supply bucket
     *
     * @param bucket Bucket
     * @return An instance of self
     */
    public BlobBuilder withBucket(String bucket) {
      this.blobStoreBuilder.withBucket(bucket);
      return this;
    }

    /**
     * Method to supply region
     *
     * @param region Region
     * @return An instance of self
     */
    public BlobBuilder withRegion(String region) {
      this.blobStoreBuilder.withRegion(region);
      return this;
    }

    /**
     * Method to supply an endpoint override
     *
     * @param endpoint The endpoint override
     * @return An instance of self
     */
    public BlobBuilder withEndpoint(URI endpoint) {
      this.blobStoreBuilder.withEndpoint(endpoint);
      return this;
    }

    /**
     * Method to supply a proxy endpoint override
     *
     * @param proxyEndpoint The proxy endpoint override
     * @return An instance of self
     */
    public BlobBuilder withProxyEndpoint(URI proxyEndpoint) {
      this.blobStoreBuilder.withProxyEndpoint(proxyEndpoint);
      return this;
    }

    /**
     * Method to supply a maximum connection count. Value must be a positive integer if specified.
     *
     * @param maxConnections The maximum number of connections allowed in the connection pool
     * @return An instance of self
     */
    public BlobBuilder withMaxConnections(Integer maxConnections) {
      this.blobStoreBuilder.withMaxConnections(maxConnections);
      return this;
    }

    /**
     * Method to supply a socket timeout
     *
     * @param socketTimeout The amount of time to wait for data to be transferred over an
     *     established, open connection before the connection is timed out. A duration of 0 means
     *     infinity, and is not recommended.
     * @return An instance of self
     */
    public BlobBuilder withSocketTimeout(Duration socketTimeout) {
      this.blobStoreBuilder.withSocketTimeout(socketTimeout);
      return this;
    }

    /**
     * Method to supply an idle connection timeout
     *
     * @param idleConnectionTimeout The maximum amount of time that a connection should be allowed
     *     to remain open while idle. Value must be a positive duration.
     * @return An instance of self
     */
    public BlobBuilder withIdleConnectionTimeout(Duration idleConnectionTimeout) {
      this.blobStoreBuilder.withIdleConnectionTimeout(idleConnectionTimeout);
      return this;
    }

    /**
     * Method to supply credentialsOverrider
     *
     * @param credentialsOverrider CredentialsOverrider
     * @return An instance of self
     */
    public BlobBuilder withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
      this.blobStoreBuilder.withCredentialsOverrider(credentialsOverrider);
      return this;
    }

    /**
     * Method to supply retry configuration
     *
     * @param retryConfig The retry configuration to use for retrying failed requests
     * @return An instance of self
     */
    public BlobBuilder withRetryConfig(RetryConfig retryConfig) {
      this.blobStoreBuilder.withRetryConfig(retryConfig);
      return this;
    }

    /**
     * Method to control whether system property values should be used for proxy configuration.
     *
     * @param useSystemPropertyProxyValues Whether to use system property values for proxy
     *     configuration
     * @return An instance of self
     */
    public BlobBuilder withUseSystemPropertyProxyValues(Boolean useSystemPropertyProxyValues) {
      this.blobStoreBuilder.withUseSystemPropertyProxyValues(useSystemPropertyProxyValues);
      return this;
    }

    /**
     * Method to control whether environment variable values should be used for proxy configuration.
     *
     * @param useEnvironmentVariableProxyValues Whether to use environment variable values for proxy
     *     configuration
     * @return An instance of self
     */
    public BlobBuilder withUseEnvironmentVariableProxyValues(
        Boolean useEnvironmentVariableProxyValues) {
      this.blobStoreBuilder.withUseEnvironmentVariableProxyValues(
          useEnvironmentVariableProxyValues);
      return this;
    }

    /**
     * Method to supply a quota project ID. This method is not applicable
     * to all providers
     *
     * @param quotaProjectId The project ID to use for quota and billing
     * @return An instance of self
     */
    public BlobBuilder withQuotaProjectId(String quotaProjectId) {
      this.blobStoreBuilder.withQuotaProjectId(quotaProjectId);
      return this;
    }

    /**
     * Method to supply the per-client tracing policy. Default is {@link TracingPolicy#DISABLED}.
     *
     * @param tracingPolicy the tracing policy
     * @return An instance of self
     */
    public BlobBuilder withTracingPolicy(TracingPolicy tracingPolicy) {
      this.blobStoreBuilder.withTracingPolicy(tracingPolicy);
      return this;
    }

    /**
     * Builds and returns an instance of BucketClient.
     *
     * @return An instance of BucketClient.
     */
    public BucketClient build() {
      return new BucketClient(blobStoreBuilder.build(), blobStoreBuilder.getTracingPolicy());
    }
  }
}
