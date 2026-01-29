package com.salesforce.multicloudj.registry.provider;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import com.salesforce.multicloudj.registry.driver.OciRegistryClient;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;

/**
 * GCP Artifact Registry (GAR) implementation.
 * 
 * Registry Endpoint: https://{location}-docker.pkg.dev/{project}/{repository}
 * Auth: Bearer Token (OAuth2 access token) via Token Exchange
 */
@AutoService(AbstractRegistry.class)
public class GcpRegistry extends AbstractRegistry {

    public GcpRegistry() {
        this(new Builder(), null);
    }

    public GcpRegistry(Builder builder, OciRegistryClient ociClient) {
        super(builder);
        this.ociClient = ociClient;
    }

    @Override
    public String getAuthToken() throws IOException {
        // Get OAuth2 Identity Token from Google credentials
        // This token is used for Bearer Token Exchange with GCP Artifact Registry
        // 
        // Example return: "ya29.a0AfH6SMBx..." (OAuth2 access token)
        // 
        // TODO: Implement actual Google OAuth2 token retrieval
        // Should use:
        // - Google Cloud SDK: GoogleCredentials.getAccessToken()
        // - Application Default Credentials (ADC)
        // - Service Account JSON key
        throw new UnsupportedOperationException("Google OAuth2 token retrieval not yet implemented. " +
            "Need to integrate with Google Cloud SDK to get access token.");
    }

    @Override
    public String getAuthUsername() {
        // GCP uses "oauth2accesstoken" as username for Basic Auth (if used)
        // But typically uses Bearer Token instead
        return "oauth2accesstoken";
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        // TODO: Map GCP-specific exceptions to MultiCloudJ exceptions
        // e.g., com.salesforce.multicloudj.gcp.exceptions.GcpSdkException
        throw new UnsupportedOperationException("Exception mapping not yet implemented");
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    /**
     * Builder for GcpRegistry.
     * Creates OciRegistryClient in build() method, following MultiCloudJ pattern.
     */
    public static class Builder extends AbstractRegistry.Builder<GcpRegistry, Builder> {

        private OciRegistryClient ociClient;

        public Builder() {
            providerId("gcp");
        }

        @Override
        public Builder self() {
            return this;
        }

        @Override
        public GcpRegistry build() {
            if (ociClient == null) {
                // Create GcpRegistry instance first (ociClient will be null initially)
                GcpRegistry registry = new GcpRegistry(this, null);
                
                // Now create OciRegistryClient using the registry instance as AuthProvider
                CloseableHttpClient httpClient = registry.getHttpClient();
                ociClient = new OciRegistryClient(getRegistryEndpoint(), registry, httpClient);
                
                // Set the ociClient on the registry instance
                registry.ociClient = ociClient;
                
                return registry;
            }
            return new GcpRegistry(this, ociClient);
        }
    }
}
