package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.provider.SdkProvider;
import com.salesforce.multicloudj.common.service.SdkService;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;

import java.net.URI;
import java.util.Properties;

public abstract class BlobStoreBuilder<T extends SdkService> implements SdkProvider.Builder<T> {
    private String providerId;
    private String bucket;
    private String region;
    private URI endpoint;
    private URI proxyEndpoint;
    private CredentialsOverrider credentialsOverrider;
    private Properties properties = new Properties();
    private BlobStoreValidator validator = new BlobStoreValidator();

    /**
     * Gets the providerId that this builder was built with.
     * @return the id of the provider
     */
    public String getProviderId() {
        return this.providerId;
    }

    /**
     * Gets the bucket name
     * @return The bucket name
     */
    public String getBucket(){
        return this.bucket;
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

    /**
     * Gets the BlobStoreValidator used for input validation.
     * @return the validator used for input validation.
     */
    public BlobStoreValidator getValidator() {
        return this.validator;
    }

    public Properties getProperties() {
        return this.properties;
    }

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
     * Method to supply credentialsOverrider
     * @param credentialsOverrider CredentialsOverrider
     * @return An instance of self
     */
    public BlobStoreBuilder<T> withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
        this.credentialsOverrider = credentialsOverrider;
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
