package com.salesforce.multicloudj.registry.gcp;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import com.salesforce.multicloudj.registry.driver.OciRegistryClient;

import java.io.IOException;

/**
 * GCP Artifact Registry implementation.
 */
@AutoService(AbstractRegistry.class)
public class GcpRegistry extends AbstractRegistry {

    public static final String PROVIDER_ID = "gcp";
    private static final String GCP_AUTH_USERNAME = "oauth2accesstoken";

    private final OciRegistryClient ociClient;

    public GcpRegistry(Builder builder) {
        super(builder);
        this.ociClient = new OciRegistryClient(registryEndpoint, this);
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    @Override
    protected OciRegistryClient getOciClient() {
        return ociClient;
    }

    @Override
    public String getAuthUsername() throws IOException {
        return GCP_AUTH_USERNAME;
    }

    @Override
    public String getAuthToken() throws IOException {
        // TODO: need to implement this
        throw new UnsupportedOperationException("getAuthToken() not yet implemented");
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        if (t instanceof SubstrateSdkException) {
            return (Class<? extends SubstrateSdkException>) t.getClass();
        }
        // TODO: map GCP/HTTP errors via CommonErrorCodeMapping
        return UnknownException.class;
    }

    @Override
    public void close() throws Exception {
        if (ociClient != null) {
            ociClient.close();
        }
    }

    public static final class Builder extends AbstractRegistry.Builder<GcpRegistry, Builder> {

        @Override
        public Builder self() {
            return this;
        }

        @Override
        public GcpRegistry build() {
            providerId(PROVIDER_ID);
            return new GcpRegistry(this);
        }
    }
}
