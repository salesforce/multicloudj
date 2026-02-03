package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.registry.model.Image;
import com.salesforce.multicloudj.registry.model.Platform;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import lombok.Getter;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Abstract registry driver. Each cloud implements auth (getAuthUsername, getAuthToken)
 * and getOciClient(); pull and extract are unified here.
 */
public abstract class AbstractRegistry implements Provider, AutoCloseable, AuthProvider {
    protected final String providerId;
    protected final String registryEndpoint;
    protected final URI proxyEndpoint;
    protected final CredentialsOverrider credentialsOverrider;
    @Getter
    protected final Platform targetPlatform;

    protected AbstractRegistry(Builder<?, ?> builder) {
        this.providerId = builder.getProviderId();
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

    // --- AuthProvider: each cloud implements getAuthUsername() and getAuthToken() ---

    @Override
    public AuthCredentials getAuthCredentials() throws IOException {
        return new AuthCredentials(getAuthUsername(), getAuthToken());
    }

    /** Implemented by each substrate */
    @Override
    public abstract String getAuthUsername() throws IOException;

    /** Implemented by each substrate */
    @Override
    public abstract String getAuthToken() throws IOException;

    /**
     * Returns the OCI client for this registry. Each provider creates and holds it 
     */
    protected abstract OciRegistryClient getOciClient();

    /**
     * Pulls an image from the registry (unified OCI flow). Uses getOciClient() and auth from provider.
     *
     * @param imageRef image reference (e.g. {@code repo:tag} or digest)
     * @return Image metadata and layer descriptors
     */
    public Image pull(String imageRef) throws Exception {
        throw new UnsupportedOperationException("pull() not yet implemented");
    }

    /**
     * Extracts the image filesystem as a tar stream (OCI layer flattening, reverse order, whiteout handling).
     *
     * @param image Image from pull(String)
     * @return InputStream of the flattened filesystem tar
     */
    public InputStream extract(Image image) throws Exception {
        // TODO: implement OCI layer flattening (reverse order, whiteout handling)
        throw new UnsupportedOperationException("extract() not yet implemented");
    }


    public abstract Class<? extends SubstrateSdkException> getException(Throwable t);

    @Override
    public void close() throws Exception {
        // Override in implementations to close OciRegistryClient and other resources
    }

    /**
     * Abstract builder for registry implementations. Provider implementations extend this and implement build().
     */
    @Getter
    public abstract static class Builder<A extends AbstractRegistry, T extends Builder<A, T>> implements Provider.Builder {
        protected String providerId;
        protected String registryEndpoint;
        protected URI proxyEndpoint;
        protected CredentialsOverrider credentialsOverrider;
        protected Platform platform;

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

        public abstract T self();

        public abstract A build();
    }
}
