package com.salesforce.multicloudj.blob.ali.async;

import com.aliyun.sdk.service.oss2.OSSAsyncClient;
import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.OperationOptions;
import com.aliyun.sdk.service.oss2.PresignOptions;
import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.models.DeleteMultipleObjectsRequest;
import com.aliyun.sdk.service.oss2.models.DeleteObjectRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectMetaRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectResult;
import com.aliyun.sdk.service.oss2.models.GetObjectTaggingRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectResult;
import com.aliyun.sdk.service.oss2.models.ListObjectsV2Request;
import com.aliyun.sdk.service.oss2.models.PresignResult;
import com.aliyun.sdk.service.oss2.models.PutObjectRequest;
import com.aliyun.sdk.service.oss2.models.PutObjectTaggingRequest;
import com.aliyun.sdk.service.oss2.retry.Retryer;
import com.aliyun.sdk.service.oss2.transfermanager.DownloadError;
import com.aliyun.sdk.service.oss2.transfermanager.Downloader;
import com.aliyun.sdk.service.oss2.transport.BinaryData;
import com.salesforce.multicloudj.blob.ali.AliSdkService;
import com.salesforce.multicloudj.blob.ali.AliTransformer;
import com.salesforce.multicloudj.blob.ali.AliTransformerSupplier;
import com.salesforce.multicloudj.blob.ali.OSSCredentialsProvider;
import com.salesforce.multicloudj.blob.async.driver.AbstractAsyncBlobStore;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStore;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStoreProvider;
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
import com.salesforce.multicloudj.common.ali.AliConstants;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import lombok.Getter;

/** Alibaba Cloud OSS native async implementation of AsyncBlobStore. */
public class AliAsyncBlobStore extends AbstractAsyncBlobStore implements AliSdkService {

  private final OSSAsyncClient asyncClient;
  // syncClient is required for operations not available on OSSAsyncClient:
  // 1. Presigned URLs — only OSSClient implements Presignable
  // 2. Parallel downloads — the SDK's Downloader (transfer manager) only accepts OSSClient
  // These sync operations are wrapped in supplyAsync/thenApplyAsync to remain non-blocking.
  private final OSSClient syncClient;
  private final AliTransformer transformer;
  private final ExecutorService executorService;
  private final Downloader downloader;

  public AliAsyncBlobStore(
      String bucket,
      String region,
      CredentialsOverrider credentialsOverrider,
      BlobStoreValidator validator,
      OSSAsyncClient asyncClient,
      OSSClient syncClient,
      AliTransformerSupplier transformerSupplier,
      ExecutorService executorService) {
    this(bucket, region, credentialsOverrider, validator, asyncClient,
        syncClient, transformerSupplier, executorService,
        syncClient != null ? new Downloader(syncClient) : null);
  }

  AliAsyncBlobStore(
      String bucket,
      String region,
      CredentialsOverrider credentialsOverrider,
      BlobStoreValidator validator,
      OSSAsyncClient asyncClient,
      OSSClient syncClient,
      AliTransformerSupplier transformerSupplier,
      ExecutorService executorService,
      Downloader downloader) {
    super(AliConstants.PROVIDER_ID, bucket, region, credentialsOverrider, validator);
    this.asyncClient = asyncClient;
    this.syncClient = syncClient;
    this.transformer = transformerSupplier.get(bucket);
    this.executorService =
        executorService != null ? executorService : ForkJoinPool.commonPool();
    this.downloader = downloader;
  }

  @Override
  public void close() {
    if (asyncClient != null) {
      try {
        asyncClient.close();
      } catch (Exception e) {
        throw new SubstrateSdkException("Failed to close Ali OSS async client", e);
      }
    }
    if (syncClient != null) {
      try {
        syncClient.close();
      } catch (Exception e) {
        throw new SubstrateSdkException("Failed to close Ali OSS sync client", e);
      }
    }
  }

  @Override
  protected CompletableFuture<UploadResponse> doUpload(
      UploadRequest uploadRequest, InputStream inputStream) {
    long contentLength = uploadRequest.getContentLength();
    BinaryData body = BinaryData.fromStream(
        inputStream, contentLength > 0 ? contentLength : null);
    return doUploadInternal(uploadRequest, body);
  }

  @Override
  protected CompletableFuture<UploadResponse> doUpload(
      UploadRequest uploadRequest, byte[] content) {
    BinaryData body = BinaryData.fromBytes(content);
    return doUploadInternal(uploadRequest, body);
  }

  @Override
  protected CompletableFuture<UploadResponse> doUpload(
      UploadRequest uploadRequest, File file) {
    try {
      BinaryData body = BinaryData.fromStream(
          Files.newInputStream(file.toPath()),
          file.length());
      return doUploadInternal(uploadRequest, body);
    } catch (IOException e) {
      return CompletableFuture.failedFuture(
          new SubstrateSdkException(
              "Failed to read file for upload: "
                  + file.getPath(), e));
    }
  }

  @Override
  protected CompletableFuture<UploadResponse> doUpload(
      UploadRequest uploadRequest, Path path) {
    return doUpload(uploadRequest, path.toFile());
  }

  private CompletableFuture<UploadResponse> doUploadInternal(
      UploadRequest uploadRequest, BinaryData body) {
    PutObjectRequest request =
        transformer.toPutObjectRequest(uploadRequest, body);
    return asyncClient
        .putObjectAsync(request, OperationOptions.defaults())
        .thenApply(
            result ->
                transformer.toUploadResponse(uploadRequest, result));
  }

  @Override
  protected CompletableFuture<DownloadResponse> doDownload(
      DownloadRequest request, OutputStream outputStream) {
    return doDownloadInternal(request).thenApply(result -> {
      try (InputStream body = result.body()) {
        copyStream(body, outputStream);
        return transformer.toDownloadResponse(request.getKey(), result);
      } catch (IOException e) {
        throw new SubstrateSdkException(
            "Failed to download blob: " + request.getKey(), e);
      }
    });
  }

  @Override
  protected CompletableFuture<DownloadResponse> doDownload(
      DownloadRequest request, ByteArray byteArray) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    return doDownload(request, outputStream).thenApply(response -> {
      byteArray.setBytes(outputStream.toByteArray());
      return response;
    });
  }

  @Override
  protected CompletableFuture<DownloadResponse> doDownload(
      DownloadRequest request, File file) {
    return doDownload(request, file.toPath());
  }

  @Override
  protected CompletableFuture<DownloadResponse> doDownload(
      DownloadRequest request, Path path) {
    Path destinationPath = createDownloadDestinationPath(request, path);
    if (request.isParallelDownload()
        && request.getStart() == null
        && request.getEnd() == null) {
      return doParallelDownload(request, destinationPath);
    }
    return doDownloadInternal(request).thenApply(result -> {
      try (InputStream body = result.body()) {
        Files.copy(body, destinationPath);
        return transformer.toDownloadResponse(request.getKey(), result);
      } catch (IOException e) {
        throw new SubstrateSdkException(
            "Failed to download blob: " + request.getKey(), e);
      }
    });
  }

  /**
   * Parallel download using the OSS SDK's Downloader (transfer manager). The Downloader internally
   * splits the object into chunks based on partSize, issues concurrent Range GET requests using its
   * own thread pool, and writes each chunk at the correct offset via positional FileChannel writes.
   * It also supports checkpointing for resumable downloads and CRC64 integrity verification.
   *
   * <p>Because the Downloader is sync-only (requires OSSClient), the blocking call is offloaded to
   * the executorService via thenApplyAsync. A preceding async HEAD request fetches object metadata
   * for the response, since the Downloader's DownloadResult only reports bytes written.
   */
  private CompletableFuture<DownloadResponse> doParallelDownload(
      DownloadRequest request, Path destinationPath) {
    HeadObjectRequest headRequest =
        transformer.toHeadObjectRequest(request.getKey(), null);
    return asyncClient
        .headObjectAsync(headRequest, OperationOptions.defaults())
        .thenApplyAsync(headResult -> {
          GetObjectRequest getRequest =
              transformer.toGetObjectRequest(request);
          try {
            downloader.downloadFile(getRequest,
                destinationPath.toString());
          } catch (DownloadError e) {
            throw new SubstrateSdkException(
                "Parallel download failed: " + request.getKey(), e);
          }
          return DownloadResponse.builder()
              .key(request.getKey())
              .metadata(transformer.toBlobMetadata(
                  request.getKey(), headResult))
              .build();
        }, executorService);
  }

  @Override
  protected CompletableFuture<DownloadResponse> doDownload(
      DownloadRequest request) {
    return doDownloadInternal(request).thenApply(result ->
        transformer.toDownloadResponse(
            request.getKey(), result, result.body()));
  }

  private CompletableFuture<GetObjectResult> doDownloadInternal(
      DownloadRequest request) {
    GetObjectRequest getRequest =
        transformer.toGetObjectRequest(request);
    return asyncClient
        .getObjectAsync(getRequest, OperationOptions.defaults());
  }

  private void copyStream(InputStream in, OutputStream out) {
    try {
      byte[] buffer = new byte[8192];
      int bytesRead;
      while ((bytesRead = in.read(buffer)) != -1) {
        out.write(buffer, 0, bytesRead);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected CompletableFuture<Void> doDelete(String key, String versionId) {
    DeleteObjectRequest request =
        transformer.toDeleteObjectRequest(key, versionId);
    return asyncClient
        .deleteObjectAsync(request, OperationOptions.defaults())
        .thenAccept(result -> {});
  }

  @Override
  protected CompletableFuture<Void> doDelete(
      Collection<BlobIdentifier> objects) {
    DeleteMultipleObjectsRequest request =
        transformer.toDeleteMultipleObjectsRequest(objects);
    return asyncClient
        .deleteMultipleObjectsAsync(request, OperationOptions.defaults())
        .thenAccept(result -> {});
  }

  @Override
  protected CompletableFuture<CopyResponse> doCopy(CopyRequest request) {
    return asyncClient
        .copyObjectAsync(
            transformer.toCopyObjectRequest(request),
            OperationOptions.defaults())
        .thenApply(result ->
            transformer.toCopyResponse(request.getDestKey(), result));
  }

  @Override
  protected CompletableFuture<BlobMetadata> doGetMetadata(
      String key, String versionId) {
    return asyncClient
        .headObjectAsync(
            transformer.toHeadObjectRequest(key, versionId),
            OperationOptions.defaults())
        .thenApply(result -> transformer.toBlobMetadata(key, result));
  }

  @Override
  protected CompletableFuture<Void> doList(
      ListBlobsRequest request, Consumer<ListBlobsBatch> consumer) {
    return doListRecursive(request, null, consumer);
  }

  /**
   * Recursively fetches all pages of object listings by manually chaining continuation tokens.
   * Unlike the sync OSSClient which provides a built-in {@code listObjectsV2Paginator()}, the
   * async OSSAsyncClient only offers single-page {@code listObjectsV2Async()} with no async
   * paginator or Publisher-style API. This method fills that gap by composing futures recursively
   * until all pages are consumed.
   */
  private CompletableFuture<Void> doListRecursive(
      ListBlobsRequest request, String continuationToken,
      Consumer<ListBlobsBatch> consumer) {
    ListObjectsV2Request listRequest =
        transformer.toListObjectsRequest(request, continuationToken);
    return asyncClient
        .listObjectsV2Async(listRequest, OperationOptions.defaults())
        .thenCompose(result -> {
          consumer.accept(transformer.toListBlobsBatch(result));
          if (Boolean.TRUE.equals(result.isTruncated())
              && result.nextContinuationToken() != null) {
            return doListRecursive(
                request, result.nextContinuationToken(), consumer);
          }
          return CompletableFuture.completedFuture(null);
        });
  }

  @Override
  protected CompletableFuture<ListBlobsPageResponse> doListPage(
      ListBlobsPageRequest request) {
    ListObjectsV2Request listRequest =
        transformer.toListObjectsRequest(request);
    return asyncClient
        .listObjectsV2Async(listRequest, OperationOptions.defaults())
        .thenApply(transformer::toListBlobsPageResponse);
  }


  @Override
  protected CompletableFuture<MultipartUpload> doInitiateMultipartUpload(
      MultipartUploadRequest request) {
    return asyncClient
        .initiateMultipartUploadAsync(
            transformer.toInitiateMultipartUploadRequest(request),
            OperationOptions.defaults())
        .thenApply(result -> transformer.toMultipartUpload(result, request));
  }

  @Override
  protected CompletableFuture<UploadPartResponse> doUploadMultipartPart(
      MultipartUpload mpu, MultipartPart mpp) {
    return asyncClient
        .uploadPartAsync(
            transformer.toUploadPartRequest(mpu, mpp),
            OperationOptions.defaults())
        .thenApply(result -> transformer.toUploadPartResponse(mpp, result));
  }

  @Override
  protected CompletableFuture<MultipartUploadResponse> doCompleteMultipartUpload(
      MultipartUpload mpu, List<UploadPartResponse> parts) {
    return asyncClient
        .completeMultipartUploadAsync(
            transformer.toCompleteMultipartUploadRequest(mpu, parts),
            OperationOptions.defaults())
        .thenApply(result -> new MultipartUploadResponse(
            stripQuotes(result.completeMultipartUpload().eTag())));
  }

  @Override
  protected CompletableFuture<List<UploadPartResponse>> doListMultipartUpload(
      MultipartUpload mpu) {
    return asyncClient
        .listPartsAsync(
            transformer.toListPartsRequest(mpu),
            OperationOptions.defaults())
        .thenApply(result -> transformer.toListUploadPartResponse(result));
  }

  @Override
  protected CompletableFuture<Void> doAbortMultipartUpload(MultipartUpload mpu) {
    return asyncClient
        .abortMultipartUploadAsync(
            transformer.toAbortMultipartUploadRequest(mpu),
            OperationOptions.defaults())
        .thenAccept(result -> {});
  }

  private String stripQuotes(String value) {
    if (value == null) {
      return null;
    }
    if (value.length() >= 2
        && value.startsWith("\"") && value.endsWith("\"")) {
      return value.substring(1, value.length() - 1);
    }
    return value;
  }

  @Override
  protected CompletableFuture<Map<String, String>> doGetTags(String key) {
    GetObjectTaggingRequest request =
        GetObjectTaggingRequest.newBuilder()
            .bucket(getBucket())
            .key(key)
            .build();
    return asyncClient
        .getObjectTaggingAsync(request, OperationOptions.defaults())
        .thenApply(result -> transformer.toTagMap(result));
  }

  @Override
  protected CompletableFuture<Void> doSetTags(
      String key, Map<String, String> tags) {
    PutObjectTaggingRequest request =
        transformer.toPutObjectTaggingRequest(key, tags);
    return asyncClient
        .putObjectTaggingAsync(request, OperationOptions.defaults())
        .thenAccept(result -> {});
  }

  @Override
  protected CompletableFuture<URL> doGeneratePresignedUrl(
      PresignedUrlRequest request) {
    return CompletableFuture.supplyAsync(() -> {
      PresignOptions options = transformer.toPresignOptions(request);
      PresignResult result;
      switch (request.getType()) {
        case UPLOAD:
          result = syncClient.presign(
              transformer.toPresignedPutObjectRequest(request), options);
          break;
        case DOWNLOAD:
          result = syncClient.presign(
              transformer.toPresignedGetObjectRequest(request), options);
          break;
        default:
          throw new InvalidArgumentException(
              "Unsupported PresignedOperation. type=" + request.getType());
      }
      try {
        return new URL(result.url());
      } catch (java.net.MalformedURLException e) {
        throw new SubstrateSdkException(
            "Invalid presigned URL: " + result.url(), e);
      }
    }, executorService);
  }

  @Override
  protected CompletableFuture<Boolean> doDoesObjectExist(
      String key, String versionId) {
    GetObjectMetaRequest.Builder reqBuilder =
        GetObjectMetaRequest.newBuilder()
            .bucket(getBucket())
            .key(key);
    if (versionId != null) {
      reqBuilder.versionId(versionId);
    }
    return asyncClient.doesObjectExistAsync(reqBuilder.build());
  }

  @Override
  protected CompletableFuture<Boolean> doDoesBucketExist() {
    return asyncClient.doesBucketExistAsync(getBucket());
  }

  @Override
  protected CompletableFuture<DirectoryDownloadResponse> doDownloadDirectory(
      DirectoryDownloadRequest directoryDownloadRequest) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<DirectoryUploadResponse> doUploadDirectory(
      DirectoryUploadRequest directoryUploadRequest) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<Void> doDeleteDirectory(String prefix) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public static Builder builder() {
    return new Builder();
  }

  @Getter
  public static class Builder extends AsyncBlobStoreProvider.Builder {

    private OSSAsyncClient asyncClient;
    private OSSClient syncClient;
    private AliTransformerSupplier transformerSupplier = new AliTransformerSupplier();

    public Builder() {
      providerId(AliConstants.PROVIDER_ID);
    }

    public Builder withAsyncClient(OSSAsyncClient asyncClient) {
      this.asyncClient = asyncClient;
      return this;
    }

    public Builder withSyncClient(OSSClient syncClient) {
      this.syncClient = syncClient;
      return this;
    }

    public Builder withTransformerSupplier(AliTransformerSupplier transformerSupplier) {
      this.transformerSupplier = transformerSupplier;
      return this;
    }

    @Override
    public AsyncBlobStore build() {
      CredentialsProvider creds =
          OSSCredentialsProvider.getCredentialsProvider(
              getCredentialsOverrider(), getRegion());

      Retryer retryer = AliTransformer.toAliRetryer(getRetryConfig());

      OSSAsyncClient async = this.asyncClient;
      if (async == null && creds != null) {
        var asyncBuilder = OSSAsyncClient.newBuilder()
            .region(getRegion())
            .credentialsProvider(creds);
        if (getEndpoint() != null) {
          asyncBuilder.endpoint(getEndpoint().toString());
        }
        if (getProxyEndpoint() != null) {
          asyncBuilder.proxyHost(
              getProxyEndpoint().getHost()
                  + ":" + getProxyEndpoint().getPort());
        }
        if (retryer != null) {
          asyncBuilder.retryer(retryer);
        }
        // socketTimeout and RetryConfig.attemptTimeout both map to the Ali SDK's
        // single readWriteTimeout setting. When both are set, attemptTimeout (the
        // more specific per-attempt deadline) takes precedence over the
        // transport-level socketTimeout.
        if (getRetryConfig() != null
            && getRetryConfig().getAttemptTimeout() != null) {
          asyncBuilder.readWriteTimeout(
              java.time.Duration.ofMillis(getRetryConfig().getAttemptTimeout()));
        } else if (getSocketTimeout() != null) {
          asyncBuilder.readWriteTimeout(getSocketTimeout());
        }
        async = asyncBuilder.build();
      }

      OSSClient sync = this.syncClient;
      if (sync == null && creds != null) {
        var syncBuilder = OSSClient.newBuilder()
            .region(getRegion())
            .credentialsProvider(creds);
        if (getEndpoint() != null) {
          syncBuilder.endpoint(getEndpoint().toString());
        }
        if (getProxyEndpoint() != null) {
          syncBuilder.proxyHost(
              getProxyEndpoint().getHost()
                  + ":" + getProxyEndpoint().getPort());
        }
        if (retryer != null) {
          syncBuilder.retryer(retryer);
        }
        // socketTimeout and RetryConfig.attemptTimeout both map to the Ali SDK's
        // single readWriteTimeout setting. When both are set, attemptTimeout (the
        // more specific per-attempt deadline) takes precedence over the
        // transport-level socketTimeout.
        if (getRetryConfig() != null
            && getRetryConfig().getAttemptTimeout() != null) {
          syncBuilder.readWriteTimeout(
              java.time.Duration.ofMillis(getRetryConfig().getAttemptTimeout()));
        } else if (getSocketTimeout() != null) {
          syncBuilder.readWriteTimeout(getSocketTimeout());
        }
        sync = syncBuilder.build();
      }

      return new AliAsyncBlobStore(
          getBucket(),
          getRegion(),
          getCredentialsOverrider(),
          getValidator(),
          async,
          sync,
          getTransformerSupplier(),
          getExecutorService());
    }
  }
}
