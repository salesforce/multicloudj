package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.provider.SdkProvider;
import com.salesforce.multicloudj.common.service.SdkService;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;

import java.net.URI;
import java.util.Properties;

/**
 * Class used to build BlobClient service instances.
 * <p>
 * This class is not intended to be instantiated directly. Instead, use one of the subclasses that are provided
 * by the library.
 * <p>
 * Example: {@link com.salesforce.multicloudj.blob.ali.AliBlobClient}
 *
 * @param <T>
 */
public abstract class BlobBuilder<T extends SdkService> implements SdkProvider.Builder<T> {
    private String providerId;
    private String region;
    private URI endpoint;
    private URI proxyEndpoint;
    private CredentialsOverrider credentialsOverrider;
    private Properties properties = new Properties();

    /**
     * Gets the providerId that this builder was built with.
     * @return the id of the provider
     */
    public String getProviderId() {
        return this.providerId;
    }

    /**
     * Gets the region.
     * @return The region.
     */
    public String getRegion(){
        return this.region;
    }

    /**
     * Gets the endpoint.
     * @return The endpoint.
     */
    public URI getEndpoint(){
        return this.endpoint;
    }

    /**
     * Gets the proxy endpoint.
     * @return The proxy endpoint.
     */
    public URI getProxyEndpoint(){
        return this.proxyEndpoint;
    }

    /**
     * Gets the CredentialsOverrider.
     * @return The CredentialsOverrider.
     */
    public CredentialsOverrider getCredentialsOverrider() {
        return this.credentialsOverrider;
    }

    public Properties getProperties() {
        return this.properties;
    }

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

}
