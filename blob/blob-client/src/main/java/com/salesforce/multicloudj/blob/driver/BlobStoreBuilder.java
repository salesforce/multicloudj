package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.provider.SdkProvider;
import com.salesforce.multicloudj.common.service.SdkService;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import lombok.Getter;

import java.net.URI;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

@Getter
public abstract class BlobStoreBuilder<T extends SdkService> implements SdkProvider.Builder<T> {

    private String providerId;
    private String bucket;
    private String region;
    private URI endpoint;
    private URI proxyEndpoint;
    private Integer maxConnections;
    private Duration socketTimeout;
    private Duration idleConnectionTimeout;
    private CredentialsOverrider credentialsOverrider;
    private ExecutorService executorService;
    private Properties properties = new Properties();
    private BlobStoreValidator validator = new BlobStoreValidator();

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

}
