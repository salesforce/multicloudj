package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.provider.SdkProvider;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.common.service.SdkService;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import lombok.Getter;

import java.net.URI;
import java.util.Properties;

/**
 * Class used to build BlobClient service instances.
 * <p>
 * This class is not intended to be instantiated directly. Instead, use one of the subclasses that are provided
 * by the library.
 * <p>
 *
 * @param <T>
 */
@Getter
public abstract class BlobBuilder<T extends SdkService> implements SdkProvider.Builder<T> {
    private String providerId;
    private String region;
    private URI endpoint;
    private URI proxyEndpoint;
    private CredentialsOverrider credentialsOverrider;
    private Properties properties = new Properties();
    private RetryConfig retryConfig;
    private Boolean useSystemPropertyProxyValues;
    private Boolean useEnvironmentVariableProxyValues;

    public BlobBuilder<T> providerId(String providerId) {
        this.providerId = providerId;
        return this;
    }

    /**
     * Method to supply region
     * @param region Region
     * @return An instance of self
     */
    public BlobBuilder<T> withRegion(String region) {
        this.region = region;
        return this;
    }

    /**
     * Method to supply an endpoint override
     * @param endpoint The endpoint to set.
     * @return An instance of self
     */
    public BlobBuilder<T> withEndpoint(URI endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    /**
     * Method to supply a proxy endpoint override
     * @param proxyEndpoint The proxy endpoint to set.
     * @return An instance of self
     */
    public BlobBuilder<T> withProxyEndpoint(URI proxyEndpoint) {
        this.proxyEndpoint = proxyEndpoint;
        return this;
    }

    /**
     * Method to supply credentialsOverrider
     * @param credentialsOverrider CredentialsOverrider
     * @return An instance of self
     */
    public BlobBuilder<T> withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
        this.credentialsOverrider = credentialsOverrider;
        return this;
    }

    public BlobBuilder<T> withProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    /**
     * Method to supply retry configuration
     * @param retryConfig The retry configuration to use for retrying failed requests
     * @return An instance of self
     */
    public BlobBuilder<T> withRetryConfig(RetryConfig retryConfig) {
        this.retryConfig = retryConfig;
        return this;
    }

    /**
     * Method to control whether system property values (e.g., http.proxyHost, http.proxyPort,
     * https.proxyHost, https.proxyPort) should be used for proxy configuration.
     * When set to false, these system properties will be ignored.
     *
     * @param useSystemPropertyProxyValues Whether to use system property values for proxy configuration
     * @return An instance of self
     */
    public BlobBuilder<T> withUseSystemPropertyProxyValues(Boolean useSystemPropertyProxyValues) {
        this.useSystemPropertyProxyValues = useSystemPropertyProxyValues;
        return this;
    }

    /**
     * Method to control whether environment variable values (e.g., HTTP_PROXY, HTTPS_PROXY,
     * NO_PROXY) should be used for proxy configuration.
     * When set to false, these environment variables will be ignored.
     *
     * @param useEnvironmentVariableProxyValues Whether to use environment variable values for proxy configuration
     * @return An instance of self
     */
    public BlobBuilder<T> withUseEnvironmentVariableProxyValues(Boolean useEnvironmentVariableProxyValues) {
        this.useEnvironmentVariableProxyValues = useEnvironmentVariableProxyValues;
        return this;
    }

}
