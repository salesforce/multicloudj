package com.salesforce.multicloudj.blob.async.client;

import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStore;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStoreProvider;
import com.salesforce.multicloudj.blob.driver.BlobClientBuilder;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.BlobSpanNames;
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
import com.salesforce.multicloudj.blob.driver.PresignedUrlResponse;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/** Entry point for async Client code to interact with the Blob storage. */
public class AsyncBucketClient implements AutoCloseable {

  private static final String SDK_SERVICE = "blob";

  protected AsyncBlobStore blobStore;
  protected final MultiCloudJLogger multiCloudJLogger;

  protected AsyncBucketClient(AsyncBlobStore blobStore) {
    this(blobStore, null);
  }

  protected AsyncBucketClient(AsyncBlobStore blobStore, TracingPolicy tracingPolicy) {
    this.blobStore = blobStore;
    this.multiCloudJLogger =
        new MultiCloudJLogger(
            tracingPolicy, SDK_SERVICE, blobStore != null ? blobStore.getProviderId() : null);
  }

  public static Builder builder(String providerId) {
    return new Builder(providerId);
  }

  protected <T> T handleException(Throwable ex) {
    throw blobStore.mapException(ex);
  }

  /**
   * Uploads the Blob content to substrate-specific Blob storage Note: Specifying the contentLength
   * in the UploadRequest can dramatically improve upload efficiency because the substrate SDKs do
   * not need to buffer the contents and calculate it themselves.
   */
  public CompletableFuture<UploadResponse> upload(
      UploadRequest uploadRequest, InputStream inputStream) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.UPLOAD,
        bucketAttrs(),
        uploadRequest.getOperationContext(),
        ctx ->
            uploadResponseWithCorrelationId(
                    blobStore.upload(withResolvedContext(uploadRequest, ctx), inputStream), ctx)
                .exceptionally(this::handleException));
  }

  /** Uploads the Blob content to substrate-specific Blob storage */
  public CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, byte[] content) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.UPLOAD,
        bucketAttrs(),
        uploadRequest.getOperationContext(),
        ctx ->
            uploadResponseWithCorrelationId(
                    blobStore.upload(withResolvedContext(uploadRequest, ctx), content), ctx)
                .exceptionally(this::handleException));
  }

  /** Uploads the Blob content to substrate-specific Blob storage */
  public CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, File file) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.UPLOAD,
        bucketAttrs(),
        uploadRequest.getOperationContext(),
        ctx ->
            uploadResponseWithCorrelationId(
                    blobStore.upload(withResolvedContext(uploadRequest, ctx), file), ctx)
                .exceptionally(this::handleException));
  }

  /** Uploads the Blob content to substrate-specific Blob storage */
  public CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, Path path) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.UPLOAD,
        bucketAttrs(),
        uploadRequest.getOperationContext(),
        ctx ->
            uploadResponseWithCorrelationId(
                    blobStore.upload(withResolvedContext(uploadRequest, ctx), path), ctx)
                .exceptionally(this::handleException));
  }

  /** Downloads the Blob content from substrate-specific Blob storage */
  public CompletableFuture<DownloadResponse> download(
      DownloadRequest downloadRequest, OutputStream outputStream) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.DOWNLOAD,
        bucketAttrs(),
        downloadRequest.getOperationContext(),
        ctx ->
            downloadResponseWithCorrelationId(
                    blobStore.download(downloadRequest, outputStream), ctx)
                .exceptionally(this::handleException));
  }

  /** Downloads the Blob content from substrate-specific Blob storage */
  public CompletableFuture<DownloadResponse> download(
      DownloadRequest downloadRequest, ByteArray byteArray) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.DOWNLOAD,
        bucketAttrs(),
        downloadRequest.getOperationContext(),
        ctx ->
            downloadResponseWithCorrelationId(blobStore.download(downloadRequest, byteArray), ctx)
                .exceptionally(this::handleException));
  }

  /** Downloads the Blob content from substrate-specific Blob storage. */
  public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, File file) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.DOWNLOAD,
        bucketAttrs(),
        downloadRequest.getOperationContext(),
        ctx ->
            downloadResponseWithCorrelationId(blobStore.download(downloadRequest, file), ctx)
                .exceptionally(this::handleException));
  }

  /** Downloads the Blob content from substrate-specific Blob storage. */
  public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, Path path) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.DOWNLOAD,
        bucketAttrs(),
        downloadRequest.getOperationContext(),
        ctx ->
            downloadResponseWithCorrelationId(blobStore.download(downloadRequest, path), ctx)
                .exceptionally(this::handleException));
  }

  /** Downloads the Blob content and returns an InputStream for reading the content */
  public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.DOWNLOAD,
        bucketAttrs(),
        downloadRequest.getOperationContext(),
        ctx ->
            downloadResponseWithCorrelationId(blobStore.download(downloadRequest), ctx)
                .exceptionally(this::handleException));
  }

  /** Deletes a single Blob from substrate-specific Blob storage */
  public CompletableFuture<Void> delete(String key, String versionId) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.DELETE,
        bucketAttrs(),
        null,
        ctx -> blobStore.delete(key, versionId).exceptionally(this::handleException));
  }

  /** Deletes a collection of Blobs from a substrate-specific Blob storage. */
  public CompletableFuture<Void> delete(Collection<BlobIdentifier> objects) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.DELETE,
        bucketAttrs(),
        null,
        ctx -> blobStore.delete(objects).exceptionally(this::handleException));
  }

  /** Copies the Blob to other bucket */
  public CompletableFuture<CopyResponse> copy(CopyRequest request) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.COPY,
        bucketAttrs(),
        request.getOperationContext(),
        ctx ->
            copyResponseWithCorrelationId(blobStore.copy(request), ctx)
                .exceptionally(this::handleException));
  }

  /** Retrieves the metadata of the Blob */
  public CompletableFuture<BlobMetadata> getMetadata(String key, String versionId) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.GET_METADATA,
        bucketAttrs(),
        null,
        ctx ->
            blobMetadataWithCorrelationId(blobStore.getMetadata(key, versionId), ctx)
                .exceptionally(this::handleException));
  }

  /** Retrieves the list of Blob in the bucket */
  public CompletableFuture<Void> list(ListBlobsRequest request, Consumer<ListBlobsBatch> consumer) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.LIST,
        bucketAttrs(),
        null,
        ctx -> blobStore.list(request, consumer).exceptionally(this::handleException));
  }

  /** Retrieves a single page of blobs from the bucket with pagination support */
  public CompletableFuture<ListBlobsPageResponse> listPage(ListBlobsPageRequest request) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.LIST_PAGE,
        bucketAttrs(),
        null,
        ctx -> blobStore.listPage(request).exceptionally(this::handleException));
  }

  /** Initiates a multipartUpload for a Blob */
  public CompletableFuture<MultipartUpload> initiateMultipartUpload(
      MultipartUploadRequest request) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.INITIATE_MULTIPART_UPLOAD,
        bucketAttrs(),
        null,
        ctx -> blobStore.initiateMultipartUpload(request).exceptionally(this::handleException));
  }

  /** Uploads a part of the multipartUpload */
  public CompletableFuture<UploadPartResponse> uploadMultipartPart(
      MultipartUpload mpu, MultipartPart mpp) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.UPLOAD_MULTIPART_PART,
        bucketAttrs(),
        null,
        ctx -> blobStore.uploadMultipartPart(mpu, mpp).exceptionally(this::handleException));
  }

  /** Completes a multipartUpload */
  public CompletableFuture<MultipartUploadResponse> completeMultipartUpload(
      MultipartUpload mpu, List<UploadPartResponse> parts) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.COMPLETE_MULTIPART_UPLOAD,
        bucketAttrs(),
        null,
        ctx -> blobStore.completeMultipartUpload(mpu, parts).exceptionally(this::handleException));
  }

  /** Returns a list of all uploaded parts for the given MultipartUpload */
  public CompletableFuture<List<UploadPartResponse>> listMultipartUpload(MultipartUpload mpu) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.LIST_MULTIPART_UPLOAD,
        bucketAttrs(),
        null,
        ctx -> blobStore.listMultipartUpload(mpu).exceptionally(this::handleException));
  }

  /** Aborts a multipartUpload */
  public CompletableFuture<Void> abortMultipartUpload(MultipartUpload mpu) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.ABORT_MULTIPART_UPLOAD,
        bucketAttrs(),
        null,
        ctx -> blobStore.abortMultipartUpload(mpu).exceptionally(this::handleException));
  }

  /** Returns a map of all the tags associated with the blob */
  public CompletableFuture<Map<String, String>> getTags(String key) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.GET_TAGS,
        bucketAttrs(),
        null,
        ctx -> blobStore.getTags(key).exceptionally(this::handleException));
  }

  /** Sets tags on a blob */
  public CompletableFuture<Void> setTags(String key, Map<String, String> tags) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.SET_TAGS,
        bucketAttrs(),
        null,
        ctx -> blobStore.setTags(key, tags).exceptionally(this::handleException));
  }

  /** Generates a presigned URL for uploading/downloading blobs */
  public CompletableFuture<URL> generatePresignedUrl(PresignedUrlRequest request) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.GENERATE_PRESIGNED_URL,
        bucketAttrs(),
        request.getOperationContext(),
        ctx -> blobStore.generatePresignedUrl(request).exceptionally(this::handleException));
  }

  /** Generates a presigned URL with full response including signed headers and expiration. */
  public CompletableFuture<PresignedUrlResponse> presign(PresignedUrlRequest request) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.GENERATE_PRESIGNED_URL,
        bucketAttrs(),
        request.getOperationContext(),
        ctx -> blobStore.presign(request).exceptionally(this::handleException));
  }

  /** Determines if an object exists for a given key/versionId */
  public CompletableFuture<Boolean> doesObjectExist(String key, String versionId) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.DOES_OBJECT_EXIST,
        bucketAttrs(),
        null,
        ctx -> blobStore.doesObjectExist(key, versionId).exceptionally(this::handleException));
  }

  /** Determines if the bucket exists */
  public CompletableFuture<Boolean> doesBucketExist() {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.DOES_BUCKET_EXIST,
        bucketAttrs(),
        null,
        ctx -> blobStore.doesBucketExist().exceptionally(this::handleException));
  }

  /** Uploads the directory content to substrate-specific Blob storage */
  public CompletableFuture<DirectoryUploadResponse> uploadDirectory(
      DirectoryUploadRequest directoryUploadRequest) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.UPLOAD_DIRECTORY,
        bucketAttrs(),
        null,
        ctx ->
            blobStore
                .uploadDirectory(directoryUploadRequest)
                .exceptionally(this::handleException));
  }

  /** Downloads the directory content from substrate-specific Blob storage. */
  public CompletableFuture<DirectoryDownloadResponse> downloadDirectory(
      DirectoryDownloadRequest directoryDownloadRequest) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.DOWNLOAD_DIRECTORY,
        bucketAttrs(),
        null,
        ctx ->
            blobStore
                .downloadDirectory(directoryDownloadRequest)
                .exceptionally(this::handleException));
  }

  /** Deletes all blobs in the bucket which have keys that start with the given prefix. */
  public CompletableFuture<Void> deleteDirectory(String prefix) {
    return multiCloudJLogger.traceAsyncOperation(
        BlobSpanNames.DELETE_DIRECTORY,
        bucketAttrs(),
        null,
        ctx -> blobStore.deleteDirectory(prefix).exceptionally(this::handleException));
  }

  /** Closes the underlying async blob store and releases any resources. */
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

  private static CompletableFuture<UploadResponse> uploadResponseWithCorrelationId(
      CompletableFuture<UploadResponse> future, OperationContext ctx) {
    if (future == null) {
      return CompletableFuture.completedFuture(null);
    }
    return future.thenApply(r -> withCorrelationId(r, ctx));
  }

  private static CompletableFuture<DownloadResponse> downloadResponseWithCorrelationId(
      CompletableFuture<DownloadResponse> future, OperationContext ctx) {
    if (future == null) {
      return CompletableFuture.completedFuture(null);
    }
    return future.thenApply(r -> withCorrelationId(r, ctx));
  }

  private static CompletableFuture<BlobMetadata> blobMetadataWithCorrelationId(
      CompletableFuture<BlobMetadata> future, OperationContext ctx) {
    if (future == null) {
      return CompletableFuture.completedFuture(null);
    }
    return future.thenApply(m -> withCorrelationId(m, ctx));
  }

  private static CompletableFuture<CopyResponse> copyResponseWithCorrelationId(
      CompletableFuture<CopyResponse> future, OperationContext ctx) {
    if (future == null) {
      return CompletableFuture.completedFuture(null);
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
   * See {@link com.salesforce.multicloudj.blob.client.BucketClient#withResolvedContext}.
   */
  static UploadRequest withResolvedContext(UploadRequest req, OperationContext ctx) {
    if (ctx == req.getOperationContext()) {
      return req;
    }
    if (req.getOperationContext() == null && isEmptyContext(ctx)) {
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

  private static boolean isEmptyContext(OperationContext ctx) {
    return ctx == null
        || (ctx.getCorrelationId() == null
            && ctx.getTenantId() == null);
  }

  public static class Builder extends BlobClientBuilder<AsyncBucketClient, AsyncBlobStore> {

    public Builder(String providerId) {
      super(ProviderSupplier.findAsyncBuilder(providerId));
    }

    public Builder(AsyncBlobStoreProvider.Builder storeBuilder) {
      super(storeBuilder);
    }

    @Override
    public Builder withProperties(Properties properties) {
      super.withProperties(properties);
      return this;
    }

    @Override
    public Builder withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
      super.withCredentialsOverrider(credentialsOverrider);
      return this;
    }

    @Override
    public Builder withEndpoint(URI endpoint) {
      super.withEndpoint(endpoint);
      return this;
    }

    @Override
    public Builder withProxyEndpoint(URI proxyEndpoint) {
      super.withProxyEndpoint(proxyEndpoint);
      return this;
    }

    @Override
    public Builder withRegion(String region) {
      super.withRegion(region);
      return this;
    }

    @Override
    public Builder withBucket(String bucket) {
      super.withBucket(bucket);
      return this;
    }

    @Override
    public Builder withMaxConnections(Integer maxConnections) {
      super.withMaxConnections(maxConnections);
      return this;
    }

    @Override
    public Builder withSocketTimeout(Duration socketTimeout) {
      super.withSocketTimeout(socketTimeout);
      return this;
    }

    @Override
    public Builder withIdleConnectionTimeout(Duration idleConnectionTimeout) {
      super.withIdleConnectionTimeout(idleConnectionTimeout);
      return this;
    }

    @Override
    public Builder withExecutorService(ExecutorService executorService) {
      super.withExecutorService(executorService);
      return this;
    }

    @Override
    public Builder withThresholdBytes(Long thresholdBytes) {
      super.withThresholdBytes(thresholdBytes);
      return this;
    }

    @Override
    public Builder withPartBufferSize(Long partBufferSize) {
      super.withPartBufferSize(partBufferSize);
      return this;
    }

    /**
     * Method to enable/disable parallel uploads. Enabling this may incur additional
     * per-part request charges depending on the provider.
     *
     * @param parallelUploadsEnabled Whether to enable parallel uploads
     * @return An instance of self
     */
    @Override
    public Builder withParallelUploadsEnabled(Boolean parallelUploadsEnabled) {
      super.withParallelUploadsEnabled(parallelUploadsEnabled);
      return this;
    }

    @Override
    public Builder withParallelDownloadsEnabled(Boolean parallelDownloadsEnabled) {
      super.withParallelDownloadsEnabled(parallelDownloadsEnabled);
      return this;
    }

    @Override
    public Builder withTargetThroughputInGbps(Double targetThroughputInGbps) {
      super.withTargetThroughputInGbps(targetThroughputInGbps);
      return this;
    }

    @Override
    public Builder withMaxNativeMemoryLimitInBytes(Long maxNativeMemoryLimitInBytes) {
      super.withMaxNativeMemoryLimitInBytes(maxNativeMemoryLimitInBytes);
      return this;
    }

    @Override
    public Builder withRetryConfig(RetryConfig retryConfig) {
      super.withRetryConfig(retryConfig);
      return this;
    }

    @Override
    public Builder withUseSystemPropertyProxyValues(Boolean useSystemPropertyProxyValues) {
      super.withUseSystemPropertyProxyValues(useSystemPropertyProxyValues);
      return this;
    }

    @Override
    public Builder withUseEnvironmentVariableProxyValues(
        Boolean useEnvironmentVariableProxyValues) {
      super.withUseEnvironmentVariableProxyValues(useEnvironmentVariableProxyValues);
      return this;
    }

    /**
     * Method to supply the per-client tracing policy. Default is {@link TracingPolicy#DISABLED}.
     */
    @Override
    public Builder withTracingPolicy(TracingPolicy tracingPolicy) {
      super.withTracingPolicy(tracingPolicy);
      return this;
    }

    public Builder withUseTransferListener(Boolean useTransferListener) {
      ((AsyncBlobStoreProvider.Builder) storeBuilder).withUseTransferListener(useTransferListener);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AsyncBucketClient build() {
      return new AsyncBucketClient(storeBuilder.build(), storeBuilder.getTracingPolicy());
    }
  }
}
