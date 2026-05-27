package com.salesforce.multicloudj.blob.ali.async;

import com.aliyun.sdk.service.oss2.OSSAsyncClient;
import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
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
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import lombok.Getter;

/** Alibaba Cloud OSS native async implementation of AsyncBlobStore. */
public class AliAsyncBlobStore extends AbstractAsyncBlobStore implements AliSdkService {

  private final OSSAsyncClient asyncClient;
  // syncClient is needed for presigned URLs — OSSAsyncClient does not implement Presignable
  private final OSSClient syncClient;
  private final AliTransformer transformer;
  private final ExecutorService executorService;

  public AliAsyncBlobStore(
      String bucket,
      String region,
      CredentialsOverrider credentialsOverrider,
      BlobStoreValidator validator,
      OSSAsyncClient asyncClient,
      OSSClient syncClient,
      AliTransformerSupplier transformerSupplier,
      ExecutorService executorService) {
    super(AliConstants.PROVIDER_ID, bucket, region, credentialsOverrider, validator);
    this.asyncClient = asyncClient;
    this.syncClient = syncClient;
    this.transformer = transformerSupplier.get(bucket);
    this.executorService =
        executorService != null ? executorService : ForkJoinPool.commonPool();
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
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<UploadResponse> doUpload(
      UploadRequest uploadRequest, byte[] content) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<UploadResponse> doUpload(
      UploadRequest uploadRequest, File file) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<UploadResponse> doUpload(
      UploadRequest uploadRequest, Path path) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<DownloadResponse> doDownload(
      DownloadRequest request, OutputStream outputStream) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<DownloadResponse> doDownload(
      DownloadRequest request, ByteArray byteArray) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<DownloadResponse> doDownload(
      DownloadRequest request, File file) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<DownloadResponse> doDownload(
      DownloadRequest request, Path path) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<DownloadResponse> doDownload(DownloadRequest request) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<Void> doDelete(String key, String versionId) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<Void> doDelete(Collection<BlobIdentifier> objects) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<CopyResponse> doCopy(CopyRequest request) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<BlobMetadata> doGetMetadata(
      String key, String versionId) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<Void> doList(
      ListBlobsRequest request, Consumer<ListBlobsBatch> consumer) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<ListBlobsPageResponse> doListPage(
      ListBlobsPageRequest request) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<MultipartUpload> doInitiateMultipartUpload(
      MultipartUploadRequest request) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<UploadPartResponse> doUploadMultipartPart(
      MultipartUpload mpu, MultipartPart mpp) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<MultipartUploadResponse> doCompleteMultipartUpload(
      MultipartUpload mpu, List<UploadPartResponse> parts) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<List<UploadPartResponse>> doListMultipartUpload(
      MultipartUpload mpu) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<Void> doAbortMultipartUpload(MultipartUpload mpu) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<Map<String, String>> doGetTags(String key) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<Void> doSetTags(
      String key, Map<String, String> tags) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<URL> doGeneratePresignedUrl(
      PresignedUrlRequest request) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<Boolean> doDoesObjectExist(
      String key, String versionId) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected CompletableFuture<Boolean> doDoesBucketExist() {
    throw new UnsupportedOperationException("Not yet implemented");
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
