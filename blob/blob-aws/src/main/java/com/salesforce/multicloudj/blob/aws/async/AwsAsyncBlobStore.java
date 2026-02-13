package com.salesforce.multicloudj.blob.aws.async;

import com.salesforce.multicloudj.blob.async.driver.AbstractAsyncBlobStore;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStore;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStoreProvider;
import com.salesforce.multicloudj.blob.aws.AwsSdkService;
import com.salesforce.multicloudj.blob.aws.AwsTransformer;
import com.salesforce.multicloudj.blob.aws.AwsTransformerSupplier;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
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
import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.aws.CredentialsProvider;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import lombok.Getter;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.multipart.MultipartConfiguration;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * AWS implementation of AsyncBlobStore
 */
public class AwsAsyncBlobStore extends AbstractAsyncBlobStore implements AwsSdkService {

    private static final int MAX_OBJECTS_PER_DELETE = 1000;

    private final S3AsyncClient client;
    private final S3TransferManager transferManager;
    private final AwsTransformer transformer;

    public AwsAsyncBlobStore(
            String bucket,
            String region,
            CredentialsOverrider credentialsOverrider,
            BlobStoreValidator validator,
            S3AsyncClient client,
            S3TransferManager transferManager,
            AwsTransformerSupplier transformerSupplier) {
        super(AwsConstants.PROVIDER_ID, bucket, region, credentialsOverrider, validator);
        this.client = client;
        this.transferManager = transferManager;
        this.transformer = transformerSupplier.get(bucket);
    }

    @Override
    protected CompletableFuture<UploadResponse> doUpload(UploadRequest uploadRequest, InputStream inputStream) {
        return doUpload(uploadRequest, transformer.toAsyncRequestBody(uploadRequest, inputStream));
    }

    @Override
    public CompletableFuture<UploadResponse> doUpload(UploadRequest uploadRequest, byte[] content) {
        return doUpload(uploadRequest, AsyncRequestBody.fromBytes(content));
    }

    @Override
    public CompletableFuture<UploadResponse> doUpload(UploadRequest uploadRequest, File file) {
        return doUpload(uploadRequest, AsyncRequestBody.fromFile(file));
    }

    @Override
    public CompletableFuture<UploadResponse> doUpload(UploadRequest uploadRequest, Path path) {
        return doUpload(uploadRequest, AsyncRequestBody.fromFile(path));
    }

    /**
     * Helper function to upload blobs
     */
    private CompletableFuture<UploadResponse> doUpload(UploadRequest uploadRequest, AsyncRequestBody asyncRequestBody) {
        return client
                .putObject(transformer.toRequest(uploadRequest), asyncRequestBody)
                .thenApply(response -> UploadResponse.builder()
                        .key(uploadRequest.getKey())
                        .versionId(response.versionId())
                        .eTag(response.eTag())
                        .build());
    }

    @Override
    protected CompletableFuture<DownloadResponse> doDownload(DownloadRequest request, OutputStream outputStream) {
        return client.getObject(transformer.toRequest(request), AsyncResponseTransformer.toBlockingInputStream())
                .thenApply(response -> {
                    try (response) {
                        response.transferTo(outputStream);
                    } catch (IOException e) {
                        throw new SubstrateSdkException("Request failed while transforming to output stream", e);
                    }
                    return transformer.toDownloadResponse(request, response.response());
                });
    }

    @Override
    protected CompletableFuture<DownloadResponse> doDownload(DownloadRequest request, ByteArray byteArray) {
        return client.getObject(transformer.toRequest(request), AsyncResponseTransformer.toBytes())
                .thenApply(responseBytes -> {
                    byteArray.setBytes(responseBytes.asByteArray());
                    return transformer.toDownloadResponse(request, responseBytes.response());
                });
    }

    @Override
    protected CompletableFuture<DownloadResponse> doDownload(DownloadRequest request, File file) {
        return client.getObject(transformer.toRequest(request), AsyncResponseTransformer.toFile(file))
                .thenApply(response -> transformer.toDownloadResponse(request, response));
    }

    /**
     * Performs Blob download
     *
     * @param request the download request
     * @param path The Path that blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     */
    @Override
    protected CompletableFuture<DownloadResponse> doDownload(DownloadRequest request, Path path) {
        return client.getObject(transformer.toRequest(request), path)
                .thenApply(response -> transformer.toDownloadResponse(request, response));
    }

    /**
     * Performs Blob download and returns an InputStream
     *
     * @param request the download request
     * @return Returns a DownloadResponse object that contains metadata about the blob and an InputStream for reading the content
     */
    @Override
    protected CompletableFuture<DownloadResponse> doDownload(DownloadRequest request) {
        GetObjectRequest getObjectRequest = transformer.toRequest(request);
        return client.getObject(getObjectRequest, AsyncResponseTransformer.toBlockingInputStream())
                .thenApply(responseInputStream -> transformer.toDownloadResponse(request, responseInputStream.response(), responseInputStream));
    }

    @Override
    protected CompletableFuture<Void> doDelete(String key, String versionId) {
        var aws = transformer.toDeleteRequest(key, versionId);
        return client
                .deleteObject(aws)
                .thenAccept(response -> {
                });
    }

    @Override
    protected CompletableFuture<Void> doDelete(Collection<BlobIdentifier> objects) {
        var request = transformer.toDeleteRequests(objects);
        return client
                .deleteObjects(request)
                .thenAccept(response -> {
                });
    }

    @Override
    protected CompletableFuture<CopyResponse> doCopy(CopyRequest request) {
        var aws = transformer.toRequest(request);
        return client
                .copyObject(aws)
                .thenApply(response -> CopyResponse.builder()
                        .key(request.getDestKey())
                        .versionId(response.versionId())
                        .eTag(response.copyObjectResult().eTag())
                        .lastModified(response.copyObjectResult().lastModified())
                        .build());
    }

    @Override
    protected CompletableFuture<BlobMetadata> doGetMetadata(String key, String versionId) {
        var request = transformer.toHeadRequest(key, versionId);
        return client
                .headObject(request)
                .thenApply(response -> transformer.toMetadata(response, key));
    }

    @Override
    protected CompletableFuture<Void> doList(ListBlobsRequest request, Consumer<ListBlobsBatch> consumer) {
        ListObjectsV2Request aws = transformer.toRequest(request);
        // the publisher lets us subscribe to pagination events automatically, so we don't need to invoke
        // each pages fetch operation
        ListObjectsV2Publisher publisher = client.listObjectsV2Paginator(aws);
        // here we wrap our consumer with a translation layer, so we can convert to the appropriate type.
        ConsumerWrapper<ListObjectsV2Response, ListBlobsBatch> wrapper = new ConsumerWrapper<>(
                consumer,
                transformer::toBatch
        );

        return publisher.subscribe(wrapper);
    }

    @Override
    protected CompletableFuture<ListBlobsPageResponse> doListPage(ListBlobsPageRequest request) {
        ListObjectsV2Request awsRequest = transformer.toRequest(request);
        return client.listObjectsV2(awsRequest)
                .thenApply(response -> {
                    List<BlobInfo> blobs = response.contents().stream()
                            .map(transformer::toInfo)
                            .collect(Collectors.toList());

                    return new ListBlobsPageResponse(
                            blobs,
                            response.isTruncated(),
                            response.nextContinuationToken()
                    );
                });
    }

    @Override
    protected CompletableFuture<MultipartUpload> doInitiateMultipartUpload(MultipartUploadRequest request) {
        return client.createMultipartUpload(transformer.toCreateMultipartUploadRequest(request))
                .thenApply(response -> transformer.toMultipartUpload(request, response));
    }

    @Override
    protected CompletableFuture<UploadPartResponse> doUploadMultipartPart(MultipartUpload mpu, MultipartPart mpp) {

        UploadPartRequest uploadPartRequest = transformer.toUploadPartRequest(mpu, mpp);
        AsyncRequestBody asyncRequestBody = AsyncRequestBody.fromInputStream(
                mpp.getInputStream(),
                mpp.getContentLength(),
                Executors.newSingleThreadExecutor());

        return client.uploadPart(uploadPartRequest, asyncRequestBody)
                .thenApply(response -> new UploadPartResponse(
                        mpp.getPartNumber(),
                        response.eTag(),
                        mpp.getContentLength()));
    }

    @Override
    protected CompletableFuture<MultipartUploadResponse> doCompleteMultipartUpload(MultipartUpload mpu, List<UploadPartResponse> parts) {
        return client.completeMultipartUpload(transformer.toCompleteMultipartUploadRequest(mpu, parts))
                .thenApply(response -> new MultipartUploadResponse(response.eTag()));
    }

    @Override
    protected CompletableFuture<List<UploadPartResponse>> doListMultipartUpload(MultipartUpload mpu) {
        return client.listParts(transformer.toListPartsRequest(mpu))
                .thenApply(response -> response.parts().stream()
                        .sorted(Comparator.comparingInt(Part::partNumber))
                        .map((part) -> new UploadPartResponse(part.partNumber(), part.eTag(), part.size()))
                        .collect(Collectors.toList()));
    }

    @Override
    protected CompletableFuture<Void> doAbortMultipartUpload(MultipartUpload mpu) {
        return client.abortMultipartUpload(transformer.toAbortMultipartUploadRequest(mpu))
                .thenAccept(response -> {
                });
    }

    @Override
    protected CompletableFuture<Map<String, String>> doGetTags(String key) {
        return client.getObjectTagging(transformer.toGetObjectTaggingRequest(key))
                .thenApply(response -> response
                        .tagSet()
                        .stream()
                        .collect(Collectors.toMap(Tag::key, Tag::value)));
    }

    @Override
    protected CompletableFuture<Void> doSetTags(String key, Map<String, String> tags) {
        return client.putObjectTagging(transformer.toPutObjectTaggingRequest(key, tags))
                .thenAccept(response -> {
                });
    }

    @Override
    protected CompletableFuture<URL> doGeneratePresignedUrl(PresignedUrlRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            try (S3Presigner presigner = getPresigner()) {
                switch (request.getType()) {
                    case UPLOAD:
                        return presigner.presignPutObject(transformer.toPutObjectPresignRequest(request)).url();
                    case DOWNLOAD:
                        return presigner.presignGetObject(transformer.toGetObjectPresignRequest(request)).url();
                }
                throw new InvalidArgumentException("Unsupported PresignedOperation. type=" + request.getType());
            }
        });
    }

    @Override
    protected CompletableFuture<DirectoryDownloadResponse> doDownloadDirectory(DirectoryDownloadRequest directoryDownloadRequest) {
        return transferManager.downloadDirectory(transformer.toDownloadDirectoryRequest(directoryDownloadRequest))
                .completionFuture()
                .thenApply(transformer::toDirectoryDownloadResponse);
    }

    @Override
    protected CompletableFuture<DirectoryUploadResponse> doUploadDirectory(DirectoryUploadRequest directoryUploadRequest) {
        return transferManager.uploadDirectory(transformer.toUploadDirectoryRequest(directoryUploadRequest))
                .completionFuture()
                .thenApply(transformer::toDirectoryUploadResponse);
    }

    @Override
    protected CompletableFuture<Void> doDeleteDirectory(String prefix) {
        List<CompletableFuture> futures = new ArrayList<>();

        // When listed batches of blobs come in, partition them into groups, then delete them
        Consumer<ListBlobsBatch> consumer = batch -> {
            List<List<BlobInfo>> partitionedBlobLists = transformer.partitionList(batch.getBlobs(), MAX_OBJECTS_PER_DELETE);
            for(List<BlobInfo> blobList : partitionedBlobLists) {
                futures.add(doDelete(transformer.toBlobIdentifiers(blobList)));
            }
        };
        CompletableFuture<Void> listFuture = doList(ListBlobsRequest.builder()
                .withPrefix(prefix)
                .build(), consumer);
        futures.add(listFuture);
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    /**
     * Returns an S3Presigner for the current credentials
     *
     * @return Returns an S3Presigner for the current credentials
     */
    protected S3Presigner getPresigner() {
        return S3Presigner.builder()
                .credentialsProvider(client.serviceClientConfiguration().credentialsProvider())
                .region(Region.of(getRegion()))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .build();
    }

    @Override
    protected CompletableFuture<Boolean> doDoesObjectExist(String key, String versionId) {
        return client
                .headObject(transformer.toHeadRequest(key, versionId))
                .thenApply(response -> true)
                .exceptionally(e -> {
                    if (e.getCause() instanceof S3Exception && ((S3Exception) e.getCause()).statusCode() == 404) {
                        return false;
                    } else {
                        throw new SubstrateSdkException("Request failed. Reason=" + e.getMessage(), e);
                    }
                });
    }

    @Override
    protected CompletableFuture<Boolean> doDoesBucketExist() {
        return client
                .headBucket(builder -> builder.bucket(bucket))
                .thenApply(response -> true)
                .exceptionally(e -> {
                    if (e.getCause() instanceof S3Exception && ((S3Exception) e.getCause()).statusCode() == 404) {
                        return false;
                    } else {
                        throw new SubstrateSdkException("Request failed. Reason=" + e.getMessage(), e);
                    }
                });
    }

    /**
     * Closes the underlying S3 async client and transfer manager, releasing any resources.
     */
    @Override
    public void close() {
        if (transferManager != null) {
            transferManager.close();
        }
        if (client != null) {
            client.close();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    @Getter
    public static class Builder extends AsyncBlobStoreProvider.Builder {

        private S3AsyncClient s3Client;
        private S3TransferManager transferManager;
        private AwsTransformerSupplier transformerSupplier = new AwsTransformerSupplier();

        public Builder() {
            providerId(AwsConstants.PROVIDER_ID);
        }

        private static S3AsyncClient buildS3Client(Builder builder) {
            Region regionObj = Region.of(builder.getRegion());

            // Use CRT-based client for parallel downloads, standard client otherwise
            if (Boolean.TRUE.equals(builder.getParallelDownloadsEnabled())) {
                return buildCrtS3Client(builder, regionObj);
            } else {
                return buildStandardS3Client(builder, regionObj);
            }
        }

        private static S3AsyncClient buildCrtS3Client(Builder builder, Region regionObj) {
            // Use AWS CRT-based S3 client for optimal parallel download performance
            var crtBuilder = S3AsyncClient.crtBuilder();

            // Configure CRT-specific settings only
            if (builder.getTargetThroughputInGbps() != null) {
                crtBuilder.targetThroughputInGbps(builder.getTargetThroughputInGbps());
            }
            if (builder.getMaxNativeMemoryLimitInBytes() != null) {
                crtBuilder.maxNativeMemoryLimitInBytes(builder.getMaxNativeMemoryLimitInBytes());
            }
            if (builder.getInitialReadBufferSizeInBytes() != null) {
                crtBuilder.initialReadBufferSizeInBytes(builder.getInitialReadBufferSizeInBytes());
            }
            if (builder.getMaxConcurrency() != null) {
                crtBuilder.maxConcurrency(builder.getMaxConcurrency());
            }

            // Apply common configuration (credentials, endpoint, proxy, part buffer size)
            applyCommonConfig(crtBuilder, builder, regionObj);

            return crtBuilder.build();
        }

        private static S3AsyncClient buildStandardS3Client(Builder builder, Region regionObj) {
            S3AsyncClientBuilder b = S3AsyncClient.builder();

            // Configure standard client specific settings only
            if (builder.getParallelUploadsEnabled() != null) {
                b.multipartEnabled(builder.getParallelUploadsEnabled());
            }

            // Apply common configuration (credentials, endpoint, proxy, multipart config, executor)
            applyCommonConfig(b, builder, regionObj);

            return b.build();
        }

        private static void applyCommonConfig(S3AsyncClientBuilder builder, Builder config, Region regionObj) {
            // Configure credentials
            AwsCredentialsProvider credentialsProvider = CredentialsProvider.getCredentialsProvider(
                    config.getCredentialsOverrider(),
                    regionObj
            );
            builder.region(regionObj);
            if (credentialsProvider != null) {
                builder.credentialsProvider(credentialsProvider);
            }

            // Configure endpoint override if specified
            if (config.getEndpoint() != null) {
                builder.endpointOverride(config.getEndpoint());
            }

            // Configure HTTP client if any settings are specified
            if (config.getProxyEndpoint() != null || config.getMaxConnections() != null ||
                config.getSocketTimeout() != null || config.getIdleConnectionTimeout() != null) {

                NettyNioAsyncHttpClient.Builder httpClientBuilder = NettyNioAsyncHttpClient.builder();

                // Configure proxy if specified
                if (config.getProxyEndpoint() != null) {
                    ProxyConfiguration.Builder proxyConfigBuilder = ProxyConfiguration.builder()
                            .scheme(config.getProxyEndpoint().getScheme())
                            .host(config.getProxyEndpoint().getHost())
                            .port(config.getProxyEndpoint().getPort());
                    if (config.getProxyUsername() != null) {
                        proxyConfigBuilder.username(config.getProxyUsername());
                    }
                    if (config.getProxyPassword() != null) {
                        proxyConfigBuilder.password(config.getProxyPassword());
                    }
                    if (config.getNonProxyHosts() != null) {
                        proxyConfigBuilder.nonProxyHosts(config.getNonProxyHosts());
                    }
                    httpClientBuilder.proxyConfiguration(proxyConfigBuilder.build());
                }

                // Configure max connections if specified
                if (config.getMaxConnections() != null) {
                    httpClientBuilder.maxConcurrency(config.getMaxConnections());
                }

                // Configure socket timeout if specified
                if (config.getSocketTimeout() != null) {
                    httpClientBuilder.writeTimeout(config.getSocketTimeout());
                    httpClientBuilder.readTimeout(config.getSocketTimeout());
                }

                // Configure idle connection timeout if specified
                if (config.getIdleConnectionTimeout() != null) {
                    httpClientBuilder.connectionMaxIdleTime(config.getIdleConnectionTimeout());
                }

                builder.httpClient(httpClientBuilder.build());
            }

            // Configure multipart configuration (common for both clients)
            MultipartConfiguration.Builder configBuilder = MultipartConfiguration.builder();
            if (config.getThresholdBytes() != null) {
                configBuilder.thresholdInBytes(config.getThresholdBytes());
            }
            if (config.getPartBufferSize() != null) {
                configBuilder.minimumPartSizeInBytes(config.getPartBufferSize());
            }
            builder.multipartConfiguration(configBuilder.build());

            // Configure retry strategy if specified
            if (config.getRetryConfig() != null) {
                // Create a temporary transformer instance for retry strategy conversion
                AwsTransformer transformer = config.getTransformerSupplier().get(config.getBucket());
                builder.overrideConfiguration(overrideConfig -> {
                    overrideConfig.retryStrategy(transformer.toAwsRetryStrategy(config.getRetryConfig()));
                    // Set API call timeouts if provided
                    if (config.getRetryConfig().getAttemptTimeout() != null) {
                        overrideConfig.apiCallAttemptTimeout(Duration.ofMillis(config.getRetryConfig().getAttemptTimeout()));
                    }
                    if (config.getRetryConfig().getTotalTimeout() != null) {
                        overrideConfig.apiCallTimeout(Duration.ofMillis(config.getRetryConfig().getTotalTimeout()));
                    }
                });
            }

            // Configure async configuration if executor service is specified
            if (config.getExecutorService() != null) {
                builder.asyncConfiguration(ClientAsyncConfiguration.builder()
                        .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, config.getExecutorService())
                        .build());
            }
        }

        private static void applyCommonConfig(S3CrtAsyncClientBuilder builder, Builder config, Region regionObj) {
            // Configure region
            builder.region(regionObj);

            // Configure credentials
            AwsCredentialsProvider credentialsProvider = CredentialsProvider.getCredentialsProvider(
                    config.getCredentialsOverrider(),
                    regionObj
            );
            if (credentialsProvider != null) {
                builder.credentialsProvider(credentialsProvider);
            }

            // Configure endpoint override if specified
            if (config.getEndpoint() != null) {
                builder.endpointOverride(config.getEndpoint());
            }

            // Configure proxy if specified
            if (config.getProxyEndpoint() != null) {
                S3CrtHttpConfiguration httpConfig = S3CrtHttpConfiguration.builder()
                        .proxyConfiguration(proxyBuilder -> {
                            proxyBuilder
                                .scheme(config.getProxyEndpoint().getScheme())
                                .host(config.getProxyEndpoint().getHost())
                                .port(config.getProxyEndpoint().getPort());
                            if (config.getProxyUsername() != null) {
                                proxyBuilder.username(config.getProxyUsername());
                            }
                            if (config.getProxyPassword() != null) {
                                proxyBuilder.password(config.getProxyPassword());
                            }
                        })
                        .build();
                builder.httpConfiguration(httpConfig);
            }

            // Configure part buffer size (common for both clients)
            if (config.getPartBufferSize() != null) {
                builder.minimumPartSizeInBytes(config.getPartBufferSize());
            }

            // Configure retry policy if specified
            if (config.getRetryConfig() != null) {
                builder.retryConfiguration(retryConfig -> retryConfig.numRetries(config.getRetryConfig().getMaxAttempts() - 1));
            }

            // Configure executor service if specified
            if (config.getExecutorService() != null) {
               builder.futureCompletionExecutor(config.getExecutorService());
            }
        }

        public Builder withS3Client(S3AsyncClient s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public Builder withTransferManager(S3TransferManager transferManager) {
            this.transferManager = transferManager;
            return this;
        }

        public Builder withTransformerSupplier(AwsTransformerSupplier transformerSupplier) {
            this.transformerSupplier = transformerSupplier;
            return this;
        }

        @Override
        public AsyncBlobStore build() {
            S3AsyncClient client = getS3Client();
            if (client == null) {
                client = buildS3Client(this);
            }
            S3TransferManager tm = getTransferManager();
            if (tm == null) {
                var transferManagerBuilder = S3TransferManager.builder()
                        .s3Client(client);

                // Configure executor service for transfer manager
                if (getExecutorService() != null) {
                    // Use custom executor service if provided
                    transferManagerBuilder.executor(getExecutorService());
                } else {
                    // Determine thread pool size for transfer manager
                    Integer poolSize = getTransferManagerThreadPoolSize();

                    if (poolSize != null) {
                        transferManagerBuilder.executor(Executors.newFixedThreadPool(poolSize));
                    }
                }

                // Configure transferDirectoryMaxConcurrency if specified
                if (getTransferDirectoryMaxConcurrency() != null) {
                    transferManagerBuilder.transferDirectoryMaxConcurrency(getTransferDirectoryMaxConcurrency());
                }

                tm = transferManagerBuilder.build();
            }

            return new AwsAsyncBlobStore(
                    getBucket(),
                    getRegion(),
                    getCredentialsOverrider(),
                    getValidator(),
                    client,
                    tm,
                    getTransformerSupplier()
            );
        }
    }


}
