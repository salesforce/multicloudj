package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.observability.MultiCloudJLogger;
import com.salesforce.multicloudj.common.observability.OperationContext;
import com.salesforce.multicloudj.common.observability.TracingPolicy;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.Getter;

/**
 * Base class for substrate-specific implementations.AbstractBlobStore This class serves the purpose
 * of providing common (i.e. substrate-agnostic) functionality
 */
public abstract class AbstractBlobStore implements BlobStore, AutoCloseable {

  private static final String SDK_SERVICE = "blob";

  @Getter private final String providerId;
  @Getter protected final String bucket;
  @Getter protected final String region;
  protected final CredentialsOverrider credentialsOverrider;
  protected final BlobStoreValidator validator;
  protected final MultiCloudJLogger multiCloudJLogger;

  protected AbstractBlobStore(Builder<?, ?> builder) {
    this(
        builder.getProviderId(),
        builder.getBucket(),
        builder.getRegion(),
        builder.getCredentialsOverrider(),
        builder.getValidator(),
        builder.getTracingPolicy());
  }

  public AbstractBlobStore(
      String providerId,
      String bucket,
      String region,
      CredentialsOverrider credentials,
      BlobStoreValidator validator) {
    this(providerId, bucket, region, credentials, validator, null);
  }

  public AbstractBlobStore(
      String providerId,
      String bucket,
      String region,
      CredentialsOverrider credentials,
      BlobStoreValidator validator,
      TracingPolicy tracingPolicy) {
    this.providerId = providerId;
    this.bucket = bucket;
    this.region = region;
    this.credentialsOverrider = credentials;
    this.validator = validator;
    this.multiCloudJLogger = new MultiCloudJLogger(tracingPolicy, SDK_SERVICE, providerId);
  }

  /** {@inheritDoc} */
  @Override
  public UploadResponse upload(UploadRequest uploadRequest, InputStream inputStream) {
    return multiCloudJLogger.traceOperation(
        "blob.upload",
        Map.of("bucket", bucket),
        uploadRequest.getOperationContext(),
        ctx -> {
          validator.validate(uploadRequest);
          return withCorrelationId(doUpload(uploadRequest, inputStream), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public UploadResponse upload(UploadRequest uploadRequest, byte[] content) {
    return multiCloudJLogger.traceOperation(
        "blob.upload",
        Map.of("bucket", bucket),
        uploadRequest.getOperationContext(),
        ctx -> {
          validator.validate(uploadRequest);
          return withCorrelationId(doUpload(uploadRequest, content), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public UploadResponse upload(UploadRequest uploadRequest, File file) {
    return multiCloudJLogger.traceOperation(
        "blob.upload",
        Map.of("bucket", bucket),
        uploadRequest.getOperationContext(),
        ctx -> {
          validator.validate(uploadRequest);
          return withCorrelationId(doUpload(uploadRequest, file), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public UploadResponse upload(UploadRequest uploadRequest, Path path) {
    return multiCloudJLogger.traceOperation(
        "blob.upload",
        Map.of("bucket", bucket),
        uploadRequest.getOperationContext(),
        ctx -> {
          validator.validate(uploadRequest);
          return withCorrelationId(doUpload(uploadRequest, path), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public DownloadResponse download(DownloadRequest downloadRequest, OutputStream outputStream) {
    return multiCloudJLogger.traceOperation(
        "blob.download",
        Map.of("bucket", bucket),
        downloadRequest.getOperationContext(),
        ctx -> {
          validator.validate(downloadRequest);
          return withCorrelationId(doDownload(downloadRequest, outputStream), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public DownloadResponse download(DownloadRequest downloadRequest, ByteArray byteArray) {
    return multiCloudJLogger.traceOperation(
        "blob.download",
        Map.of("bucket", bucket),
        downloadRequest.getOperationContext(),
        ctx -> {
          validator.validate(downloadRequest);
          return withCorrelationId(doDownload(downloadRequest, byteArray), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public DownloadResponse download(DownloadRequest downloadRequest, File file) {
    return multiCloudJLogger.traceOperation(
        "blob.download",
        Map.of("bucket", bucket),
        downloadRequest.getOperationContext(),
        ctx -> {
          validator.validate(downloadRequest);
          return withCorrelationId(doDownload(downloadRequest, file), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public DownloadResponse download(DownloadRequest downloadRequest, Path path) {
    return multiCloudJLogger.traceOperation(
        "blob.download",
        Map.of("bucket", bucket),
        downloadRequest.getOperationContext(),
        ctx -> {
          validator.validate(downloadRequest);
          return withCorrelationId(doDownload(downloadRequest, path), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public DownloadResponse download(DownloadRequest downloadRequest) {
    return multiCloudJLogger.traceOperation(
        "blob.download",
        Map.of("bucket", bucket),
        downloadRequest.getOperationContext(),
        ctx -> {
          validator.validate(downloadRequest);
          return withCorrelationId(doDownload(downloadRequest), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public void delete(String key, String versionId) {
    multiCloudJLogger.traceVoidOperation(
        "blob.delete",
        Map.of("bucket", bucket),
        null,
        ctx -> {
          validator.validateDelete(key);
          doDelete(key, versionId);
        });
  }

  /** {@inheritDoc} */
  @Override
  public void delete(Collection<BlobIdentifier> objects) {
    multiCloudJLogger.traceVoidOperation(
        "blob.delete",
        Map.of("bucket", bucket),
        null,
        ctx -> {
          validator.validateBlobIdentifiers(objects);
          doDelete(objects);
        });
  }

  /** {@inheritDoc} */
  @Override
  public CopyResponse copy(CopyRequest request) {
    return multiCloudJLogger.traceOperation(
        "blob.copy",
        Map.of("bucket", bucket),
        request.getOperationContext(),
        ctx -> {
          validator.validate(request);
          return withCorrelationId(doCopy(request), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public CopyResponse copyFrom(CopyFromRequest request) {
    return multiCloudJLogger.traceOperation(
        "blob.copyFrom",
        Map.of("bucket", bucket),
        request.getOperationContext(),
        ctx -> {
          validator.validate(request);
          return withCorrelationId(doCopyFrom(request), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public BlobMetadata getMetadata(String key, String versionId) {
    return multiCloudJLogger.traceOperation(
        "blob.getMetadata",
        Map.of("bucket", bucket),
        null,
        ctx -> {
          validator.validateKey(key);
          return withCorrelationId(doGetMetadata(key, versionId), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public Iterator<BlobInfo> list(ListBlobsRequest request) {
    return multiCloudJLogger.traceOperation(
        "blob.list", Map.of("bucket", bucket), null, ctx -> doList(request));
  }

  /** {@inheritDoc} */
  @Override
  public ListBlobsPageResponse listPage(ListBlobsPageRequest request) {
    return multiCloudJLogger.traceOperation(
        "blob.listPage", Map.of("bucket", bucket), null, ctx -> doListPage(request));
  }

  /** {@inheritDoc} */
  @Override
  public MultipartUpload initiateMultipartUpload(MultipartUploadRequest request) {
    return multiCloudJLogger.traceOperation(
        "blob.initiateMultipartUpload",
        Map.of("bucket", bucket),
        null,
        ctx -> doInitiateMultipartUpload(request));
  }

  /** {@inheritDoc} */
  @Override
  public UploadPartResponse uploadMultipartPart(MultipartUpload mpu, MultipartPart mpp) {
    return multiCloudJLogger.traceOperation(
        "blob.uploadMultipartPart",
        Map.of("bucket", bucket),
        null,
        ctx -> {
          validator.validate(mpu, getBucket());
          return doUploadMultipartPart(mpu, mpp);
        });
  }

  /** {@inheritDoc} */
  @Override
  public MultipartUploadResponse completeMultipartUpload(
      MultipartUpload mpu, List<UploadPartResponse> parts) {
    return multiCloudJLogger.traceOperation(
        "blob.completeMultipartUpload",
        Map.of("bucket", bucket),
        null,
        ctx -> {
          validator.validate(mpu, getBucket());
          return doCompleteMultipartUpload(mpu, parts);
        });
  }

  /** {@inheritDoc} */
  @Override
  public List<UploadPartResponse> listMultipartUpload(MultipartUpload mpu) {
    return multiCloudJLogger.traceOperation(
        "blob.listMultipartUpload",
        Map.of("bucket", bucket),
        null,
        ctx -> {
          validator.validate(mpu, getBucket());
          return doListMultipartUpload(mpu);
        });
  }

  /** {@inheritDoc} */
  @Override
  public void abortMultipartUpload(MultipartUpload mpu) {
    multiCloudJLogger.traceVoidOperation(
        "blob.abortMultipartUpload",
        Map.of("bucket", bucket),
        null,
        ctx -> {
          validator.validate(mpu, getBucket());
          doAbortMultipartUpload(mpu);
        });
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> getTags(String key) {
    return multiCloudJLogger.traceOperation(
        "blob.getTags",
        Map.of("bucket", bucket),
        null,
        ctx -> {
          validator.validateKey(key);
          return doGetTags(key);
        });
  }

  /** {@inheritDoc} */
  @Override
  public void setTags(String key, Map<String, String> tags) {
    multiCloudJLogger.traceVoidOperation(
        "blob.setTags",
        Map.of("bucket", bucket),
        null,
        ctx -> {
          validator.validateKey(key);
          validator.validateTags(tags);
          doSetTags(key, tags);
        });
  }

  /** {@inheritDoc} */
  @Override
  public URL generatePresignedUrl(PresignedUrlRequest request) {
    return multiCloudJLogger.traceOperation(
        "blob.generatePresignedUrl",
        Map.of("bucket", bucket),
        request.getOperationContext(),
        ctx -> {
          validator.validate(request);
          return doGeneratePresignedUrl(request);
        });
  }

  /** {@inheritDoc} */
  @Override
  public boolean doesObjectExist(String key, String versionId) {
    return multiCloudJLogger.traceOperation(
        "blob.doesObjectExist",
        Map.of("bucket", bucket),
        null,
        ctx -> {
          validator.validateKey(key);
          return doDoesObjectExist(key, versionId);
        });
  }

  /** {@inheritDoc} */
  @Override
  public boolean doesBucketExist() {
    return multiCloudJLogger.traceOperation(
        "blob.doesBucketExist", Map.of("bucket", bucket), null, ctx -> doDoesBucketExist());
  }

  /** {@inheritDoc} */
  @Override
  public DirectoryDownloadResponse downloadDirectory(
      DirectoryDownloadRequest directoryDownloadRequest) {
    return multiCloudJLogger.traceOperation(
        "blob.downloadDirectory",
        Map.of("bucket", bucket),
        null,
        ctx -> {
          validator.validate(directoryDownloadRequest);
          return doDownloadDirectory(directoryDownloadRequest);
        });
  }

  /** {@inheritDoc} */
  @Override
  public DirectoryUploadResponse uploadDirectory(DirectoryUploadRequest directoryUploadRequest) {
    return multiCloudJLogger.traceOperation(
        "blob.uploadDirectory",
        Map.of("bucket", bucket),
        null,
        ctx -> {
          validator.validate(directoryUploadRequest);
          return doUploadDirectory(directoryUploadRequest);
        });
  }

  /** {@inheritDoc} Allow null/empty prefix for deleting all objects in bucket */
  @Override
  public void deleteDirectory(String prefix) {
    multiCloudJLogger.traceVoidOperation(
        "blob.deleteDirectory",
        Map.of("bucket", bucket),
        null,
        ctx -> doDeleteDirectory(prefix));
  }

  protected abstract UploadResponse doUpload(UploadRequest uploadRequest, InputStream inputStream);

  protected abstract UploadResponse doUpload(UploadRequest uploadRequest, byte[] content);

  protected abstract UploadResponse doUpload(UploadRequest uploadRequest, File file);

  protected abstract UploadResponse doUpload(UploadRequest uploadRequest, Path path);

  protected abstract DownloadResponse doDownload(
      DownloadRequest downloadRequest, OutputStream outputStream);

  protected abstract DownloadResponse doDownload(
      DownloadRequest downloadRequest, ByteArray byteArray);

  protected abstract DownloadResponse doDownload(DownloadRequest downloadRequest, File file);

  protected abstract DownloadResponse doDownload(DownloadRequest downloadRequest, Path path);

  protected abstract DownloadResponse doDownload(DownloadRequest downloadRequest);

  protected abstract void doDelete(String key, String versionId);

  protected abstract void doDelete(Collection<BlobIdentifier> objects);

  protected abstract CopyResponse doCopy(CopyRequest request);

  protected abstract CopyResponse doCopyFrom(CopyFromRequest request);

  protected abstract BlobMetadata doGetMetadata(String key, String versionId);

  protected abstract Iterator<BlobInfo> doList(ListBlobsRequest request);

  protected abstract ListBlobsPageResponse doListPage(ListBlobsPageRequest request);

  protected abstract MultipartUpload doInitiateMultipartUpload(MultipartUploadRequest request);

  protected abstract UploadPartResponse doUploadMultipartPart(
      MultipartUpload mpu, MultipartPart mpp);

  protected abstract MultipartUploadResponse doCompleteMultipartUpload(
      MultipartUpload mpu, List<UploadPartResponse> parts);

  protected abstract List<UploadPartResponse> doListMultipartUpload(MultipartUpload mpu);

  protected abstract void doAbortMultipartUpload(MultipartUpload mpu);

  protected abstract Map<String, String> doGetTags(String key);

  protected abstract void doSetTags(String key, Map<String, String> tags);

  protected abstract URL doGeneratePresignedUrl(PresignedUrlRequest request);

  protected abstract boolean doDoesObjectExist(String key, String versionId);

  protected abstract boolean doDoesBucketExist();

  protected DirectoryDownloadResponse doDownloadDirectory(
      DirectoryDownloadRequest directoryDownloadRequest) {
    throw new UnsupportedOperationException(
        "Directory download is not supported by this substrate implementation");
  }

  protected DirectoryUploadResponse doUploadDirectory(
      DirectoryUploadRequest directoryUploadRequest) {
    throw new UnsupportedOperationException(
        "Directory upload is not supported by this substrate implementation");
  }

  protected void doDeleteDirectory(String prefix) {
    throw new UnsupportedOperationException(
        "Directory delete is not supported by this substrate implementation");
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
   * Resolves the local download destination; when {@link DownloadRequest#isCreateParentPath()} is
   * true, appends the object key and creates any missing parent directories. Subclasses may
   * override to change the exception type thrown on directory-creation failure.
   */
  protected Path createDownloadDestinationPath(DownloadRequest request, Path destination) {
    if (!request.isCreateParentPath()) {
      return destination;
    }
    Path resolved = destination.resolve(request.getKey()).normalize();
    Path parent = resolved.getParent();
    if (parent != null) {
      try {
        Files.createDirectories(parent);
      } catch (IOException e) {
        throw new SubstrateSdkException("Failed to create destination directories", e);
      }
    }
    return resolved;
  }

  public abstract static class Builder<A extends AbstractBlobStore, T extends Builder<A, T>>
      extends BlobStoreBuilder<A> implements Provider.Builder {

    @Override
    public T providerId(String providerId) {
      super.providerId(providerId);
      return self();
    }

    public abstract T self();

    /**
     * Builds and returns an instance of AbstractBlobStore.
     *
     * @return An instance of AbstractBlobStore.
     */
    public abstract A build();
  }
}
