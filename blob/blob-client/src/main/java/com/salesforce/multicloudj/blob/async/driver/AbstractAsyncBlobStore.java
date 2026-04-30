package com.salesforce.multicloudj.blob.async.driver;

import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.BlobStoreValidator;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsBatch;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.MultipartUploadResponse;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.observability.MultiCloudJLogger;
import com.salesforce.multicloudj.common.observability.OperationContext;
import com.salesforce.multicloudj.common.observability.TracingPolicy;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import lombok.Getter;

/** Baseline blob store for async api calls. */
public abstract class AbstractAsyncBlobStore implements AsyncBlobStore {

  private static final String SDK_SERVICE = "blob";

  @Getter private final String providerId;
  @Getter protected final String bucket;
  @Getter protected final String region;
  protected final CredentialsOverrider credentialsOverrider;
  protected final BlobStoreValidator validator;
  protected final MultiCloudJLogger multiCloudJLogger;

  protected AbstractAsyncBlobStore(
      String providerId,
      String bucket,
      String region,
      CredentialsOverrider credentialsOverrider,
      BlobStoreValidator validator) {
    this(providerId, bucket, region, credentialsOverrider, validator, null);
  }

  protected AbstractAsyncBlobStore(
      String providerId,
      String bucket,
      String region,
      CredentialsOverrider credentialsOverrider,
      BlobStoreValidator validator,
      TracingPolicy tracingPolicy) {
    this.providerId = providerId;
    this.bucket = bucket;
    this.region = region;
    this.credentialsOverrider = credentialsOverrider;
    this.validator = validator;
    this.multiCloudJLogger = new MultiCloudJLogger(tracingPolicy, SDK_SERVICE, providerId);
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<UploadResponse> upload(
      UploadRequest uploadRequest, InputStream inputStream) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.upload",
        Map.of("bucket", bucket),
        uploadRequest.getOperationContext(),
        ctx -> {
          validator.validate(uploadRequest);
          return uploadResponseWithCorrelationId(doUpload(uploadRequest, inputStream), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, byte[] content) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.upload",
        Map.of("bucket", bucket),
        uploadRequest.getOperationContext(),
        ctx -> {
          validator.validate(uploadRequest);
          return uploadResponseWithCorrelationId(doUpload(uploadRequest, content), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, File file) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.upload",
        Map.of("bucket", bucket),
        uploadRequest.getOperationContext(),
        ctx -> {
          validator.validate(uploadRequest);
          return uploadResponseWithCorrelationId(doUpload(uploadRequest, file), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, Path path) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.upload",
        Map.of("bucket", bucket),
        uploadRequest.getOperationContext(),
        ctx -> {
          validator.validate(uploadRequest);
          return uploadResponseWithCorrelationId(doUpload(uploadRequest, path), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<DownloadResponse> download(
      DownloadRequest downloadRequest, OutputStream outputStream) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.download",
        Map.of("bucket", bucket),
        downloadRequest.getOperationContext(),
        ctx -> {
          validator.validate(downloadRequest);
          return downloadResponseWithCorrelationId(
              doDownload(downloadRequest, outputStream), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<DownloadResponse> download(
      DownloadRequest downloadRequest, ByteArray byteArray) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.download",
        Map.of("bucket", bucket),
        downloadRequest.getOperationContext(),
        ctx -> {
          validator.validate(downloadRequest);
          return downloadResponseWithCorrelationId(doDownload(downloadRequest, byteArray), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, File file) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.download",
        Map.of("bucket", bucket),
        downloadRequest.getOperationContext(),
        ctx -> {
          validator.validate(downloadRequest);
          return downloadResponseWithCorrelationId(doDownload(downloadRequest, file), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, Path path) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.download",
        Map.of("bucket", bucket),
        downloadRequest.getOperationContext(),
        ctx -> {
          validator.validate(downloadRequest);
          return downloadResponseWithCorrelationId(doDownload(downloadRequest, path), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.download",
        Map.of("bucket", bucket),
        downloadRequest.getOperationContext(),
        ctx -> {
          validator.validate(downloadRequest);
          return downloadResponseWithCorrelationId(doDownload(downloadRequest), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Void> delete(String key, String versionId) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.delete",
        Map.of("bucket", bucket),
        null,
        ctx -> {
          validator.validateDelete(key);
          return doDelete(key, versionId);
        });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Void> delete(Collection<BlobIdentifier> objects) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.delete",
        Map.of("bucket", bucket),
        null,
        ctx -> {
          validator.validateBlobIdentifiers(objects);
          return doDelete(objects);
        });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<CopyResponse> copy(CopyRequest request) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.copy",
        Map.of("bucket", bucket),
        request.getOperationContext(),
        ctx -> {
          validator.validate(request);
          return copyResponseWithCorrelationId(doCopy(request), ctx);
        });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<BlobMetadata> getMetadata(String key, String versionId) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.getMetadata",
        Map.of("bucket", bucket),
        null,
        ctx -> {
          validator.validateKey(key);
          return doGetMetadata(key, versionId);
        });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Void> list(ListBlobsRequest request, Consumer<ListBlobsBatch> consumer) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.list", Map.of("bucket", bucket), null, ctx -> doList(request, consumer));
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<ListBlobsPageResponse> listPage(ListBlobsPageRequest request) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.listPage", Map.of("bucket", bucket), null, ctx -> doListPage(request));
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<MultipartUpload> initiateMultipartUpload(
      MultipartUploadRequest request) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.initiateMultipartUpload",
        Map.of("bucket", bucket),
        null,
        ctx -> doInitiateMultipartUpload(request));
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<UploadPartResponse> uploadMultipartPart(
      MultipartUpload mpu, MultipartPart mpp) {
    return multiCloudJLogger.traceAsyncOperation(
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
  public CompletableFuture<MultipartUploadResponse> completeMultipartUpload(
      MultipartUpload mpu, List<UploadPartResponse> parts) {
    return multiCloudJLogger.traceAsyncOperation(
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
  public CompletableFuture<List<UploadPartResponse>> listMultipartUpload(MultipartUpload mpu) {
    return multiCloudJLogger.traceAsyncOperation(
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
  public CompletableFuture<Void> abortMultipartUpload(MultipartUpload mpu) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.abortMultipartUpload",
        Map.of("bucket", bucket),
        null,
        ctx -> {
          validator.validate(mpu, getBucket());
          return doAbortMultipartUpload(mpu);
        });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Map<String, String>> getTags(String key) {
    return multiCloudJLogger.traceAsyncOperation(
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
  public CompletableFuture<Void> setTags(String key, Map<String, String> tags) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.setTags",
        Map.of("bucket", bucket),
        null,
        ctx -> {
          validator.validateKey(key);
          validator.validateTags(tags);
          return doSetTags(key, tags);
        });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<URL> generatePresignedUrl(PresignedUrlRequest request) {
    return multiCloudJLogger.traceAsyncOperation(
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
  public CompletableFuture<Boolean> doesObjectExist(String key, String versionId) {
    return multiCloudJLogger.traceAsyncOperation(
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
  public CompletableFuture<Boolean> doesBucketExist() {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.doesBucketExist", Map.of("bucket", bucket), null, ctx -> doDoesBucketExist());
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<DirectoryDownloadResponse> downloadDirectory(
      DirectoryDownloadRequest directoryDownloadRequest) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.downloadDirectory",
        Map.of("bucket", bucket),
        null,
        ctx -> doDownloadDirectory(directoryDownloadRequest));
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<DirectoryUploadResponse> uploadDirectory(
      DirectoryUploadRequest directoryUploadRequest) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.uploadDirectory",
        Map.of("bucket", bucket),
        null,
        ctx -> doUploadDirectory(directoryUploadRequest));
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Void> deleteDirectory(String prefix) {
    return multiCloudJLogger.traceAsyncOperation(
        "blob.deleteDirectory",
        Map.of("bucket", bucket),
        null,
        ctx -> doDeleteDirectory(prefix));
  }

  protected abstract CompletableFuture<UploadResponse> doUpload(
      UploadRequest uploadRequest, InputStream inputStream);

  protected abstract CompletableFuture<UploadResponse> doUpload(
      UploadRequest uploadRequest, byte[] content);

  protected abstract CompletableFuture<UploadResponse> doUpload(
      UploadRequest uploadRequest, File file);

  protected abstract CompletableFuture<UploadResponse> doUpload(
      UploadRequest uploadRequest, Path path);

  protected abstract CompletableFuture<DownloadResponse> doDownload(
      DownloadRequest request, OutputStream outputStream);

  protected abstract CompletableFuture<DownloadResponse> doDownload(
      DownloadRequest request, ByteArray byteArray);

  protected abstract CompletableFuture<DownloadResponse> doDownload(
      DownloadRequest request, File file);

  protected abstract CompletableFuture<DownloadResponse> doDownload(
      DownloadRequest request, Path path);

  protected abstract CompletableFuture<DownloadResponse> doDownload(DownloadRequest request);

  protected abstract CompletableFuture<Void> doDelete(String key, String versionId);

  protected abstract CompletableFuture<Void> doDelete(Collection<BlobIdentifier> objects);

  protected abstract CompletableFuture<CopyResponse> doCopy(CopyRequest request);

  protected abstract CompletableFuture<BlobMetadata> doGetMetadata(String key, String versionId);

  protected abstract CompletableFuture<Void> doList(
      ListBlobsRequest request, Consumer<ListBlobsBatch> consumer);

  protected abstract CompletableFuture<ListBlobsPageResponse> doListPage(
      ListBlobsPageRequest request);

  protected abstract CompletableFuture<MultipartUpload> doInitiateMultipartUpload(
      MultipartUploadRequest request);

  protected abstract CompletableFuture<UploadPartResponse> doUploadMultipartPart(
      MultipartUpload mpu, MultipartPart mpp);

  protected abstract CompletableFuture<MultipartUploadResponse> doCompleteMultipartUpload(
      MultipartUpload mpu, List<UploadPartResponse> parts);

  protected abstract CompletableFuture<List<UploadPartResponse>> doListMultipartUpload(
      MultipartUpload mpu);

  protected abstract CompletableFuture<Void> doAbortMultipartUpload(MultipartUpload mpu);

  protected abstract CompletableFuture<Map<String, String>> doGetTags(String key);

  protected abstract CompletableFuture<Void> doSetTags(String key, Map<String, String> tags);

  protected abstract CompletableFuture<URL> doGeneratePresignedUrl(PresignedUrlRequest request);

  protected abstract CompletableFuture<Boolean> doDoesObjectExist(String key, String versionId);

  protected abstract CompletableFuture<Boolean> doDoesBucketExist();

  protected abstract CompletableFuture<DirectoryDownloadResponse> doDownloadDirectory(
      DirectoryDownloadRequest directoryDownloadRequest);

  protected abstract CompletableFuture<DirectoryUploadResponse> doUploadDirectory(
      DirectoryUploadRequest directoryUploadRequest);

  protected abstract CompletableFuture<Void> doDeleteDirectory(String prefix);

  private static CompletableFuture<UploadResponse> uploadResponseWithCorrelationId(
      CompletableFuture<UploadResponse> future, OperationContext ctx) {
    if (future == null) {
      return null;
    }
    return future.thenApply(r -> withCorrelationId(r, ctx));
  }

  private static CompletableFuture<DownloadResponse> downloadResponseWithCorrelationId(
      CompletableFuture<DownloadResponse> future, OperationContext ctx) {
    if (future == null) {
      return null;
    }
    return future.thenApply(r -> withCorrelationId(r, ctx));
  }

  private static CompletableFuture<CopyResponse> copyResponseWithCorrelationId(
      CompletableFuture<CopyResponse> future, OperationContext ctx) {
    if (future == null) {
      return null;
    }
    return future.thenApply(r -> withCorrelationId(r, ctx));
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
        .metadata(r.getMetadata())
        .inputStream(r.getInputStream())
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
}
