package com.salesforce.multicloudj.registry.client;

import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import com.salesforce.multicloudj.registry.driver.PullResult;

import java.nio.file.Path;

/**
 * Entry point for Client code to interact with a specific repository in the registry.
 * This is a resource-level client for operations on a specific repository.
 *
 * <p>This class serves the purpose of providing common (i.e. substrate-agnostic) repository functionality.
 * Client layer only exposes pullImage method - all Docker authentication is handled internally in driver layer.
 */
public class RepositoryClient implements AutoCloseable {

    protected AbstractRegistry registry;

    protected RepositoryClient(AbstractRegistry registry) {
        this.registry = registry;
    }

    public static RepositoryClientBuilder builder(String providerId) {
        return new RepositoryClientBuilder(providerId);
    }

    /**
     * Pulls a Docker image from the registry and saves it as a tar file.
     *
     * @param imageRef Image reference in the format "name:tag" or "name@digest"
     *                 Examples: "my-image:latest", "my-image@sha256:abc123..."
     * @param destination Path where the image tar file will be saved
     * @return Result with image metadata
     */
    public PullResult pullImage(String imageRef, Path destination) {
        try {
            return registry.pullImage(imageRef, destination);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = registry.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Pulls a Docker image from the registry and writes it to an OutputStream as a tar file.
     *
     * @param imageRef Image reference in the format "name:tag" or "name@digest"
     * @param outputStream OutputStream where the image tar file will be written
     * @return Result with image metadata
     */
    public PullResult pullImage(String imageRef, java.io.OutputStream outputStream) {
        try {
            return registry.pullImage(imageRef, outputStream);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = registry.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Pulls a Docker image from the registry and saves it to a File as a tar file.
     *
     * @param imageRef Image reference in the format "name:tag" or "name@digest"
     * @param file File where the image tar file will be saved
     * @return Result with image metadata
     */
    public PullResult pullImage(String imageRef, java.io.File file) {
        try {
            return registry.pullImage(imageRef, file);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = registry.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Pulls a Docker image from the registry and returns an InputStream for reading the tar file.
     * This provides lazy loading similar to go-containerregistry - data is streamed on-demand.
     * The caller is responsible for closing the InputStream.
     *
     * @param imageRef Image reference in the format "name:tag" or "name@digest"
     * @return Result with image metadata and InputStream for reading the tar file
     */
    public PullResult pullImage(String imageRef) {
        try {
            return registry.pullImage(imageRef);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = registry.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Closes the underlying registry and releases any resources.
     */
    @Override
    public void close() throws Exception {
        if (registry != null) {
            registry.close();
        }
    }

    public static class RepositoryClientBuilder {

        private final AbstractRegistry.Builder<?, ?> registryBuilder;

        public RepositoryClientBuilder(String providerId) {
            this.registryBuilder = ProviderSupplier.findRegistryProviderBuilder(providerId);
        }

        /**
         * Method to supply repository
         *
         * @param repository Repository name
         * @return An instance of self
         */
        public RepositoryClientBuilder withRepository(String repository) {
            this.registryBuilder.withRepository(repository);
            return this;
        }

        /**
         * Method to supply region
         *
         * @param region Region
         * @return An instance of self
         */
        public RepositoryClientBuilder withRegion(String region) {
            this.registryBuilder.withRegion(region);
            return this;
        }

        /**
         * Method to supply an endpoint override
         *
         * @param endpoint The endpoint override
         * @return An instance of self
         */
        public RepositoryClientBuilder withEndpoint(java.net.URI endpoint) {
            this.registryBuilder.withEndpoint(endpoint);
            return this;
        }

        /**
         * Method to supply a proxy endpoint override
         *
         * @param proxyEndpoint The proxy endpoint override
         * @return An instance of self
         */
        public RepositoryClientBuilder withProxyEndpoint(java.net.URI proxyEndpoint) {
            this.registryBuilder.withProxyEndpoint(proxyEndpoint);
            return this;
        }

        /**
         * Method to supply credentialsOverrider
         *
         * @param credentialsOverrider CredentialsOverrider
         * @return An instance of self
         */
        public RepositoryClientBuilder withCredentialsOverrider(
                com.salesforce.multicloudj.sts.model.CredentialsOverrider credentialsOverrider) {
            this.registryBuilder.withCredentialsOverrider(credentialsOverrider);
            return this;
        }

        /**
         * Method to supply retry configuration
         * @param retryConfig The retry configuration to use for retrying failed requests
         * @return An instance of self
         */
        public RepositoryClientBuilder withRetryConfig(com.salesforce.multicloudj.common.retries.RetryConfig retryConfig) {
            this.registryBuilder.withRetryConfig(retryConfig);
            return this;
        }

        /**
         * Builds and returns an instance of RepositoryClient.
         *
         * @return An instance of RepositoryClient.
         */
        public RepositoryClient build() {
            return new RepositoryClient(registryBuilder.build());
        }
    }
}
