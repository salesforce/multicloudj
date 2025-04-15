package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.service.SdkService;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;

import java.net.URI;
import java.util.Properties;

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
     * Builds and returns an instance of the target client implementation.
     * @return A fully constructed client implementation.
     */
    public abstract C build();

}
