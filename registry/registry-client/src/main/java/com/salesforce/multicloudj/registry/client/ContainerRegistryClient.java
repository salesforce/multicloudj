package com.salesforce.multicloudj.registry.client;

import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import com.salesforce.multicloudj.registry.model.Image;
import com.salesforce.multicloudj.registry.model.Platform;

import java.io.InputStream;

/**
 * Entry point for users to interact with a container registry.
 * 
 * <p>ContainerRegistryClient provides a high-level API for pulling images
 * and extracting filesystems in a substrate-agnostic way.
 */
public class ContainerRegistryClient implements AutoCloseable {
    protected AbstractRegistry registry;

    protected ContainerRegistryClient(AbstractRegistry registry) {
        this.registry = registry;
    }

    public static ContainerRegistryClientBuilder builder(String providerId) {
        return new ContainerRegistryClientBuilder(providerId);
    }

    /**
     * Pulls a Docker image from the registry and returns an Image object.
     * Uses the platform specified in the builder (or default linux/amd64).
     * 
     * <p>Platform selection happens during pull, as per OCI specification.
     * To pull a different platform, create a new client with the desired platform:
     * <pre>
     * // Pull with default platform (linux/amd64)
     * Image img1 = client.pull("my-image:tag");
     *
     * // Pull with different platform - create new client
     * ContainerRegistryClient arm64Client = ContainerRegistryClient.builder("aws")
     *     .withRegistryEndpoint("https://123456789.dkr.ecr.us-east-1.amazonaws.com")
     *     .withPlatform(Platform.builder()
     *         .os("linux")
     *         .architecture("arm64")
     *         .build())
     *     .build();
     * Image img2 = arm64Client.pull("my-image:tag");
     * </pre>
     * 
     * @param imageRef Image reference (e.g., "my-image:tag" or "my-image@sha256:...")
     * @return Image object representing the pulled image
     */
    public Image pull(String imageRef) {
        try {
            return registry.pull(imageRef);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = registry.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Extracts the image filesystem as a tar stream.
     *
     * <p>This returns an InputStream containing the image's flattened filesystem.
     *
     * <p>The returned stream can be used in various ways:
     * <ul>
     *   <li>Parse with TarArchiveInputStream to extract specific files (e.g., CSV)</li>
     *   <li>Copy to any OutputStream using IOUtils.copy()</li>
     *   <li>Read directly for custom processing</li>
     * </ul>
     *
     * @param image Image object (from {@link #pull(String)})
     * @return InputStream containing the filesystem tar stream
     * @throws SubstrateSdkException if extraction fails
     */
    public InputStream extract(Image image) {
        try {
            return registry.extract(image);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = registry.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        if (registry != null) {
            registry.close();
        }
    }

    public static class ContainerRegistryClientBuilder {
        private final AbstractRegistry.Builder<?, ?> registryBuilder;

        ContainerRegistryClientBuilder(String providerId) {
            this.registryBuilder = ProviderSupplier.findProviderBuilder(providerId);
        }

        /**
         * Sets the registry endpoint (e.g. "https://123456789.dkr.ecr.us-east-1.amazonaws.com"). Required.
         */
        public ContainerRegistryClientBuilder withRegistryEndpoint(String registryEndpoint) {
            this.registryBuilder.withRegistryEndpoint(registryEndpoint);
            return this;
        }

        /**
         * Sets a proxy endpoint override for HTTP requests.
         */
        public ContainerRegistryClientBuilder withProxyEndpoint(java.net.URI proxyEndpoint) {
            this.registryBuilder.withProxyEndpoint(proxyEndpoint);
            return this;
        }

        /**
         * Sets the target platform for multi-arch image selection.
         * If not specified, defaults to linux/amd64.
         * 
         * @param platform Platform object (e.g., Platform.builder().os("linux").architecture("arm64").build())
         * @return this builder
         */
        public ContainerRegistryClientBuilder withPlatform(Platform platform) {
            this.registryBuilder.withPlatform(platform);
            return this;
        }

        /**
         * Sets credentials overrider for authentication.
         * 
         * @param credentialsOverrider CredentialsOverrider
         * @return this builder
         */
        public ContainerRegistryClientBuilder withCredentialsOverrider(
                com.salesforce.multicloudj.sts.model.CredentialsOverrider credentialsOverrider) {
            this.registryBuilder.withCredentialsOverrider(credentialsOverrider);
            return this;
        }

        public ContainerRegistryClient build() {
            AbstractRegistry registry = registryBuilder.build();
            return new ContainerRegistryClient(registry);
        }
    }
}
