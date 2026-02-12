package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.provider.SdkProvider;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.common.service.SdkService;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import lombok.Getter;

import java.net.URI;
import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;

@Getter
public abstract class BlobStoreBuilder<T extends SdkService> implements SdkProvider.Builder<T> {

    private String providerId;
    private String bucket;
    private String region;
    private URI endpoint;
    private URI proxyEndpoint;
    private String proxyUsername;
    private String proxyPassword;
    private Set<String> nonProxyHosts;
    private Integer maxConnections;
    private Duration socketTimeout;
    private Duration idleConnectionTimeout;
    private CredentialsOverrider credentialsOverrider;
    private ExecutorService executorService;
    private Properties properties = new Properties();
    private BlobStoreValidator validator = new BlobStoreValidator();
    private Long thresholdBytes;
    private Long partBufferSize;
    private Boolean parallelUploadsEnabled;
    private Boolean parallelDownloadsEnabled;
    private Double targetThroughputInGbps;
    private Long maxNativeMemoryLimitInBytes;
    private Long initialReadBufferSizeInBytes;
    private Integer maxConcurrency;
    private Integer transferManagerThreadPoolSize;
    private Integer transferDirectoryMaxConcurrency;
    private RetryConfig retryConfig;

    public BlobStoreBuilder<T> providerId(String providerId) {
        this.providerId = providerId;
        return this;
    }

    /**
     * Method to supply bucket
     * @param bucket Bucket
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withBucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    /**
     * Method to supply region
     * @param region Region
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withRegion(String region) {
        this.region = region;
        return this;
    }

    /**
     * Method to supply an endpoint override
     * @param endpoint The endpoint to set.
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withEndpoint(URI endpoint) {
        validator.validateEndpoint(endpoint, false);
        this.endpoint = endpoint;
        return this;
    }

    /**
     * Method to supply a proxy endpoint override
     * @param proxyEndpoint The proxy endpoint to set.
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withProxyEndpoint(URI proxyEndpoint) {
        validator.validateEndpoint(proxyEndpoint, true);
        this.proxyEndpoint = proxyEndpoint;
        return this;
    }

    /**
     * Method to supply a proxy username for authentication
     * @param proxyUsername The username for proxy authentication
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withProxyUsername(String proxyUsername) {
        this.proxyUsername = proxyUsername;
        return this;
    }

    /**
     * Method to supply a proxy password for authentication
     * @param proxyPassword The password for proxy authentication
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
        return this;
    }

    /**
     * Method to supply a set of hosts that should bypass the proxy
     * @param nonProxyHosts The set of hosts that should not use the proxy
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withNonProxyHosts(Set<String> nonProxyHosts) {
        this.nonProxyHosts = nonProxyHosts;
        return this;
    }

    /**
     * Method to supply a maximum connection count. Value must be a positive integer if specified.
     * @param maxConnections The maximum number of connections allowed in the connection pool
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withMaxConnections(Integer maxConnections) {
        validator.validateMaxConnections(maxConnections);
        this.maxConnections = maxConnections;
        return this;
    }

    /**
     * Method to supply a socket timeout
     * @param socketTimeout The amount of time to wait for data to be transferred over an established, open connection
     *                      before the connection is timed out. A duration of 0 means infinity, and is not recommended.
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withSocketTimeout(Duration socketTimeout) {
        validator.validateSocketTimeout(socketTimeout);
        this.socketTimeout = socketTimeout;
        return this;
    }

    /**
     * Method to supply an idle connection timeout
     * @param idleConnectionTimeout The maximum amount of time that a connection should be allowed to remain open while idle.
     *                              Value must be a positive duration.
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withIdleConnectionTimeout(Duration idleConnectionTimeout) {
        validator.validateDuration(idleConnectionTimeout);
        this.idleConnectionTimeout = idleConnectionTimeout;
        return this;
    }

    /**
     * Method to supply credentialsOverrider
     * @param credentialsOverrider CredentialsOverrider
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
        this.credentialsOverrider = credentialsOverrider;
        return this;
    }

    /**
     * Method to supply a custom ExecutorService for async operations.
     * @param executorService The ExecutorService to use
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    /**
     * Method to supply a custom validator
     * @param validator the validator to use for input validation
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withValidator(BlobStoreValidator validator) {
        this.validator = validator;
        return this;
    }

    public BlobStoreBuilder<T> withProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    /**
     * Method to supply multipart threshold in bytes
     * @param thresholdBytes The threshold in bytes above which multipart upload will be used
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withThresholdBytes(Long thresholdBytes) {
        this.thresholdBytes = thresholdBytes;
        return this;
    }

    /**
     * Method to supply multipart part buffer size in bytes
     * @param partBufferSize The buffer size in bytes for each part in a multipart upload
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withPartBufferSize(Long partBufferSize) {
        this.partBufferSize = partBufferSize;
        return this;
    }

    /**
     * Method to enable/disable parallel uploads
     * @param parallelUploadsEnabled Whether to enable parallel uploads
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withParallelUploadsEnabled(Boolean parallelUploadsEnabled) {
        this.parallelUploadsEnabled = parallelUploadsEnabled;
        return this;
    }

    /**
     * Method to enable/disable parallel downloads
     * @param parallelDownloadsEnabled Whether to enable parallel downloads
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withParallelDownloadsEnabled(Boolean parallelDownloadsEnabled) {
        this.parallelDownloadsEnabled = parallelDownloadsEnabled;
        return this;
    }

    /**
     * Method to set target throughput in Gbps
     * @param targetThroughputInGbps The target throughput in Gbps
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withTargetThroughputInGbps(Double targetThroughputInGbps) {
        this.targetThroughputInGbps = targetThroughputInGbps;
        return this;
    }

    /**
     * Method to set maximum native memory limit in bytes
     * @param maxNativeMemoryLimitInBytes The maximum native memory limit in bytes
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withMaxNativeMemoryLimitInBytes(Long maxNativeMemoryLimitInBytes) {
        this.maxNativeMemoryLimitInBytes = maxNativeMemoryLimitInBytes;
        return this;
    }

    /**
     * Method to set initial read buffer size in bytes
     * @param initialReadBufferSizeInBytes The initial read buffer size in bytes
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withInitialReadBufferSizeInBytes(Long initialReadBufferSizeInBytes) {
        this.initialReadBufferSizeInBytes = initialReadBufferSizeInBytes;
        return this;
    }

    /**
     * Method to set maximum concurrency
     * @param maxConcurrency The maximum number of concurrent operations
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withMaxConcurrency(Integer maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
        return this;
    }

    /**
     * Method to set transfer manager thread pool size
     * @param transferManagerThreadPoolSize The number of threads in the transfer manager thread pool
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withTransferManagerThreadPoolSize(Integer transferManagerThreadPoolSize) {
        this.transferManagerThreadPoolSize = transferManagerThreadPoolSize;
        return this;
    }

    /**
     * Method to set maximum concurrency for directory transfers in S3 Transfer Manager
     * @param transferDirectoryMaxConcurrency The maximum number of concurrent file transfers during directory operations
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withTransferDirectoryMaxConcurrency(Integer transferDirectoryMaxConcurrency) {
        this.transferDirectoryMaxConcurrency = transferDirectoryMaxConcurrency;
        return this;
    }

    /**
     * Method to supply retry configuration
     * @param retryConfig The retry configuration to use for retrying failed requests
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withRetryConfig(RetryConfig retryConfig) {
        this.retryConfig = retryConfig;
        return this;
    }

}
