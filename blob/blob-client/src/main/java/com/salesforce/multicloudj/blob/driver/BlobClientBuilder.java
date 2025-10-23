package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.service.SdkService;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;

import java.net.URI;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

/**
 * Helper class for combining the configuration inputs for BlobClient or AsyncBlobClient instances.
 * @param <C> the type of client built by this builder
 * @param <S> the type of blob store required by this builder
 */
public abstract class BlobClientBuilder<C, S extends SdkService> {

    protected final BlobStoreBuilder<S> storeBuilder;
    protected Properties properties = new Properties();

    protected BlobClientBuilder(BlobStoreBuilder<S> storeBuilder) {
        this.storeBuilder = storeBuilder;
    }

    public BlobClientBuilder<C, S> withProperties(Properties properties) {
        this.properties = properties;
        this.storeBuilder.withProperties(properties);
        return this;
    }

    public Properties getProperties() {
        return properties;
    }

    /**
     * Method to supply bucket
     * @param bucket Bucket
     * @return An instance of self
     */
    public BlobClientBuilder<C, S> withBucket(String bucket) {
        this.storeBuilder.withBucket(bucket);
        return this;
    }

    /**
     * Method to supply region
     * @param region Region
     * @return An instance of self
     */
    public BlobClientBuilder<C, S> withRegion(String region) {
        this.storeBuilder.withRegion(region);
        return this;
    }

    /**
     * Method to supply an endpoint override
     *
     * @param endpoint The endpoint override
     * @return An instance of self
     */
    public BlobClientBuilder<C, S> withEndpoint(URI endpoint) {
        this.storeBuilder.withEndpoint(endpoint);
        return this;
    }

    /**
     * Method to supply a proxy endpoint override
     * @param proxyEndpoint The proxy endpoint override
     * @return An instance of self
     */
    public BlobClientBuilder<C, S> withProxyEndpoint(URI proxyEndpoint) {
        this.storeBuilder.withProxyEndpoint(proxyEndpoint);
        return this;
    }

    /**
     * Method to supply a maximum connection count. Value must be a positive integer if specified.
     * @param maxConnections The maximum number of connections allowed in the connection pool
     * @return An instance of self
     */
    public BlobClientBuilder<C, S> withMaxConnections(Integer maxConnections) {
        this.storeBuilder.withMaxConnections(maxConnections);
        return this;
    }

    /**
     * Method to supply a socket timeout
     * @param socketTimeout The amount of time to wait for data to be transferred over an established, open connection
     *                      before the connection is timed out. A duration of 0 means infinity, and is not recommended.
     * @return An instance of self
     */
    public BlobClientBuilder<C, S> withSocketTimeout(Duration socketTimeout) {
        this.storeBuilder.withSocketTimeout(socketTimeout);
        return this;
    }

    /**
     * Method to supply an idle connection timeout
     * @param idleConnectionTimeout The maximum amount of time that a connection should be allowed to remain open while idle.
     *                              Value must be a positive duration.
     * @return An instance of self
     */
    public BlobClientBuilder<C, S> withIdleConnectionTimeout(Duration idleConnectionTimeout) {
        this.storeBuilder.withIdleConnectionTimeout(idleConnectionTimeout);
        return this;
    }

    /**
     * Method to supply credentialsOverrider
     *
     * @param credentialsOverrider CredentialsOverrider
     * @return An instance of self
     */
    public BlobClientBuilder<C, S> withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
        this.storeBuilder.withCredentialsOverrider(credentialsOverrider);
        return this;
    }

    /**
     * Method to supply a custom ExecutorService for async operations.
     * @param executorService The ExecutorService to use
     * @return An instance of self
     */
    public BlobClientBuilder<C, S> withExecutorService(ExecutorService executorService) {
        this.storeBuilder.withExecutorService(executorService);
        return this;
    }

    /**
     * Method to supply multipart threshold in bytes
     * @param thresholdBytes The threshold in bytes above which multipart upload will be used
     * @return An instance of self
     */
    public BlobClientBuilder<C, S> withThresholdBytes(Long thresholdBytes) {
        this.storeBuilder.withThresholdBytes(thresholdBytes);
        return this;
    }

    /**
     * Method to supply multipart part buffer size in bytes
     * @param partBufferSize The buffer size in bytes for each part in a multipart upload
     * @return An instance of self
     */
    public BlobClientBuilder<C, S> withPartBufferSize(Long partBufferSize) {
        this.storeBuilder.withPartBufferSize(partBufferSize);
        return this;
    }

    /**
     * Method to enable/disable parallel uploads
     * @param parallelUploadsEnabled Whether to enable parallel uploads
     * @return An instance of self
     */
    public BlobClientBuilder<C, S> withParallelUploadsEnabled(Boolean parallelUploadsEnabled) {
        this.storeBuilder.withParallelUploadsEnabled(parallelUploadsEnabled);
        return this;
    }

    /**
     * Method to enable/disable parallel downloads
     * @param parallelDownloadsEnabled Whether to enable parallel downloads
     * @return An instance of self
     */
    public BlobClientBuilder<C, S> withParallelDownloadsEnabled(Boolean parallelDownloadsEnabled) {
        this.storeBuilder.withParallelDownloadsEnabled(parallelDownloadsEnabled);
        return this;
    }

    /**
     * Method to set target throughput in Gbps
     * @param targetThroughputInGbps The target throughput in Gbps
     * @return An instance of self
     */
    public BlobClientBuilder<C, S> withTargetThroughputInGbps(Double targetThroughputInGbps) {
        this.storeBuilder.withTargetThroughputInGbps(targetThroughputInGbps);
        return this;
    }

    /**
     * Method to set maximum native memory limit in bytes
     * @param maxNativeMemoryLimitInBytes The maximum native memory limit in bytes
     * @return An instance of self
     */
    public BlobClientBuilder<C, S> withMaxNativeMemoryLimitInBytes(Long maxNativeMemoryLimitInBytes) {
        this.storeBuilder.withMaxNativeMemoryLimitInBytes(maxNativeMemoryLimitInBytes);
        return this;
    }

    /**
     * Method to set initial read buffer size in bytes
     * @param initialReadBufferSizeInBytes The initial read buffer size in bytes
     * @return An instance of self
     */
    public BlobClientBuilder<C, S> withInitialReadBufferSizeInBytes(Long initialReadBufferSizeInBytes) {
        this.storeBuilder.withInitialReadBufferSizeInBytes(initialReadBufferSizeInBytes);
        return this;
    }

    /**
     * Method to set maximum concurrency
     * @param maxConcurrency The maximum number of concurrent operations
     * @return An instance of self
     */
    public BlobClientBuilder<C, S> withMaxConcurrency(Integer maxConcurrency) {
        this.storeBuilder.withMaxConcurrency(maxConcurrency);
        return this;
    }

    /**
     * Method to set transfer manager thread pool size
     * @param transferManagerThreadPoolSize The number of threads in the transfer manager thread pool
     * @return An instance of self
     */
    public BlobClientBuilder<C, S> withTransferManagerThreadPoolSize(Integer transferManagerThreadPoolSize) {
        this.storeBuilder.withTransferManagerThreadPoolSize(transferManagerThreadPoolSize);
        return this;
    }

    /**
     * Method to set maximum concurrency for directory transfers in S3 Transfer Manager
     * @param transferDirectoryMaxConcurrency The maximum number of concurrent file transfers during directory operations
     * @return An instance of self
     */
    public BlobClientBuilder<C, S> withTransferDirectoryMaxConcurrency(Integer transferDirectoryMaxConcurrency) {
        this.storeBuilder.withTransferDirectoryMaxConcurrency(transferDirectoryMaxConcurrency);
        return this;
    }

    /**
     * Builds and returns an instance of the target client implementation.
     * @return A fully constructed client implementation.
     */
    public abstract C build();

}
