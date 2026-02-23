package com.salesforce.multicloudj.registry.client;

import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import com.salesforce.multicloudj.registry.model.Image;
import com.salesforce.multicloudj.registry.model.Platform;

import java.io.InputStream;

/** Entry point for container registry; high-level API for pulling images and extracting filesystems. */
public class ContainerRegistryClient implements AutoCloseable {
    protected AbstractRegistry registry;

    protected ContainerRegistryClient(AbstractRegistry registry) {
        this.registry = registry;
    }

    public static ContainerRegistryClientBuilder builder(String providerId) {
        return new ContainerRegistryClientBuilder(providerId);
    }

    /**
     * Pulls an image from the registry. Uses the builder platform (default linux/amd64).
     * @param imageRef image reference (e.g. my-image:tag or my-image@sha256:...)
     * @return the pulled image
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
     * @param image image from a previous pull
     * @return InputStream of the flattened filesystem tar
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

        /** Sets the registry endpoint. Required. */
        public ContainerRegistryClientBuilder withRegistryEndpoint(String registryEndpoint) {
            this.registryBuilder.withRegistryEndpoint(registryEndpoint);
            return this;
        }

        /** Sets a proxy endpoint override for HTTP requests. */
        public ContainerRegistryClientBuilder withProxyEndpoint(java.net.URI proxyEndpoint) {
            this.registryBuilder.withProxyEndpoint(proxyEndpoint);
            return this;
        }

        /** Sets the target platform for multi-arch selection. Null for default (linux/amd64). */
        public ContainerRegistryClientBuilder withPlatform(Platform platform) {
            this.registryBuilder.withPlatform(platform);
            return this;
        }

        /** Sets credentials overrider for authentication. */
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
