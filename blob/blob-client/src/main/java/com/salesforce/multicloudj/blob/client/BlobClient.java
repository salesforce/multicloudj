package com.salesforce.multicloudj.blob.client;

import com.salesforce.multicloudj.blob.driver.AbstractBlobClient;
import com.salesforce.multicloudj.blob.driver.ListBucketsResponse;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;

import java.net.URI;


/**
 * Entry point for Client code to interact with the Blob service.
 * This is a Service client, which can be used to interact with the blob service, unlike {@link BucketClient} that interacts with buckets.
 *
 * <p>This class serves the purpose of providing common (i.e. substrate-agnostic) service functionality.
 *
 */
public class BlobClient {

    protected AbstractBlobClient<?> blobClient;

    protected BlobClient(AbstractBlobClient<?> blobClient) {
        this.blobClient = blobClient;
    }

    public static BlobClientBuilder builder(String providerId) {
        return new BlobClientBuilder(providerId);
    }


    /**
     * Lists the buckets associated with the authenticated account in the substrate.
     *
     */
    public ListBucketsResponse listBuckets() {
        try {
            return blobClient.listBuckets();
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobClient.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    public static class BlobClientBuilder {

        private final AbstractBlobClient.Builder<?> blobClientBuilder;

        public BlobClientBuilder(String providerId) {
            this.blobClientBuilder = ProviderSupplier.findBlobClientProviderBuilder(providerId);
        }

        /**
         * Method to supply region
         *
         * @param region Region
         * @return An instance of self
         */
        public BlobClientBuilder withRegion(String region) {
            this.blobClientBuilder.withRegion(region);
            return this;
        }

        /**
         * Method to supply an endpoint override
         *
         * @param endpoint The endpoint override
         * @return An instance of self
         */
        public BlobClientBuilder withEndpoint(URI endpoint) {
            this.blobClientBuilder.withEndpoint(endpoint);
            return this;
        }

        /**
         * Method to supply a proxy endpoint override
         *
         * @param proxyEndpoint The proxy endpoint override
         * @return An instance of self
         */
        public BlobClientBuilder withProxyEndpoint(URI proxyEndpoint) {
            this.blobClientBuilder.withProxyEndpoint(proxyEndpoint);
            return this;
        }

        /**
         * Method to supply credentialsOverrider
         *
         * @param credentialsOverrider CredentialsOverrider
         * @return An instance of self
         */
        public BlobClientBuilder withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
            this.blobClientBuilder.withCredentialsOverrider(credentialsOverrider);
            return this;
        }

        /**
         * Builds and returns an instance of BlobClient.
         *
         * @return An instance of BlobClient.
         */
        public BlobClient build() {
            return new BlobClient(blobClientBuilder.build());
        }
    }
}
