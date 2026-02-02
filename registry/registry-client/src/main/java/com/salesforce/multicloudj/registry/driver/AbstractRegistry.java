package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.common.service.SdkService;
import com.salesforce.multicloudj.registry.model.Image;
import com.salesforce.multicloudj.registry.model.Platform;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import lombok.Getter;

import java.io.InputStream;
import java.net.URI;

public abstract class AbstractRegistry implements SdkService, Provider, AutoCloseable {
    protected final String providerId;
    protected final String repository;
    protected final String region;
    protected final String registryEndpoint;
    protected final URI proxyEndpoint;
    protected final CredentialsOverrider credentialsOverrider;
    @Getter
    protected final Platform targetPlatform;

    protected AbstractRegistry(Builder<?, ?> builder) {
        this.providerId = builder.getProviderId();
        this.repository = builder.getRepository();
        this.region = builder.getRegion();
        this.registryEndpoint = builder.getRegistryEndpoint();
        this.proxyEndpoint = builder.getProxyEndpoint();
        this.credentialsOverrider = builder.getCredentialsOverrider();
        this.targetPlatform = builder.getPlatform() != null ? builder.getPlatform() : Platform.DEFAULT;

        if (registryEndpoint == null || registryEndpoint.isEmpty()) {
            throw new IllegalStateException("Registry endpoint must be provided via builder.withRegistryEndpoint()");
        }
    }

    @Override
    public String getProviderId() {
        return providerId;
    }

    /**
     * Returns a builder instance for this registry type.
     */
    public abstract Builder<?, ?> builder();

    /**
     * Pulls an image from the registry and returns an Image object.
     */
    public abstract Image pull(String imageRef) throws Exception;

    /**
     * Extracts the image filesystem as a tar stream.
     */
    public abstract InputStream extract(Image image) throws Exception;

    /**
     * Maps a throwable to a SubstrateSdkException type.
     */
    public abstract Class<? extends SubstrateSdkException> getException(Throwable t);

    @Override
    public void close() throws Exception {
        // Override in implementations to close resources
    }

    /**
     * Abstract builder for registry implementations.
     * Provider implementations extend this and implement build().
     */
    @Getter
    public abstract static class Builder<A extends AbstractRegistry, T extends Builder<A, T>> implements Provider.Builder {
        protected String providerId;
        protected String repository;
        protected String region;
        protected String registryEndpoint;
        protected URI proxyEndpoint;
        protected CredentialsOverrider credentialsOverrider;
        protected Platform platform;

        public T withRegion(String region) {
            this.region = region;
            return self();
        }

        public T withRegistryEndpoint(String registryEndpoint) {
            this.registryEndpoint = registryEndpoint;
            return self();
        }

        public T withProxyEndpoint(URI proxyEndpoint) {
            this.proxyEndpoint = proxyEndpoint;
            return self();
        }

        public T withPlatform(Platform platform) {
            this.platform = platform;
            return self();
        }

        public T withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
            this.credentialsOverrider = credentialsOverrider;
            return self();
        }

        @Override
        public T providerId(String providerId) {
            this.providerId = providerId;
            return self();
        }

        public T withRepository(String repository) {
            this.repository = repository;
            return self();
        }

        public abstract T self();

        /**
         * Builds and returns an instance of AbstractRegistry.
         */
        public abstract A build();
    }
}
