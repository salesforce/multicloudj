package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.registry.model.Image;
import com.salesforce.multicloudj.registry.model.Platform;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Abstract registry driver. Each cloud implements authentication and OCI client.
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

        if (StringUtils.isBlank(registryEndpoint)) {
            throw new IllegalStateException("Registry endpoint is not configured.");
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

    @Override
    public abstract String getAuthUsername() throws IOException;

    @Override
    public abstract String getAuthToken() throws IOException;

    /** Returns the OCI client for this registry. */
    protected abstract OciRegistryClient getOciClient();

    /**
     * Pulls an image from the registry (unified OCI flow).
     *
     * @param imageRef image reference (e.g. repo:tag or digest)
     * @return Image metadata and layer descriptors
     */
    public Image pull(String imageRef) throws Exception {
        // TODO: need to be implemented
        throw new UnsupportedOperationException("pull() not yet implemented");
    }

    /**
     * Extracts the image filesystem as a tar stream (OCI layer flattening, reverse order, whiteout handling).
     *
     * @param image image from a previous pull
     * @return InputStream of the flattened filesystem tar
     */
    public InputStream extract(Image image) throws Exception {
        // TODO: implement OCI layer flattening (reverse order, whiteout handling)
        throw new UnsupportedOperationException("extract() not yet implemented");
    }


    public abstract Class<? extends SubstrateSdkException> getException(Throwable t);

    @Override
    public abstract void close() throws Exception;

    /** Abstract builder for registry implementations. */
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
