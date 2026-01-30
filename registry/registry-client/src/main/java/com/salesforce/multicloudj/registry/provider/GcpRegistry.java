package com.salesforce.multicloudj.registry.provider;

import com.google.auto.service.AutoService;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.gcp.GcpCredentialsProvider;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import com.salesforce.multicloudj.registry.driver.OciRegistryClient;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.Collections;

/**
 * GCP Artifact Registry (GAR) implementation.
 * 
 * Registry Endpoint: https://{location}-docker.pkg.dev/{project}/{repository}
 * Auth: Bearer Token (OAuth2 access token) via Token Exchange
 */
@AutoService(AbstractRegistry.class)
public class GcpRegistry extends AbstractRegistry {
    private final GoogleCredentials credentials;

    public GcpRegistry() {
        this(new Builder(), null, null);
    }

    public GcpRegistry(Builder builder, OciRegistryClient ociClient, GoogleCredentials credentials) {
        super(builder);
        this.ociClient = ociClient;
        this.credentials = credentials;
    }

    @Override
    public String getAuthToken() throws IOException {
        // Use the pre-initialized GoogleCredentials (created in Builder.build())
        credentials.refreshIfExpired();
        AccessToken accessToken = credentials.getAccessToken();
        return accessToken.getTokenValue();
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
     * Creates GoogleCredentials and OciRegistryClient in build() method, following MultiCloudJ pattern.
     */
    public static class Builder extends AbstractRegistry.Builder<GcpRegistry, Builder> {

        private OciRegistryClient ociClient;
        private GoogleCredentials credentials;

        public Builder() {
            providerId("gcp");
        }

        @Override
        public Builder self() {
            return this;
        }

        @Override
        public GcpRegistry build() {
            if (credentials == null) {
                try {
                    GoogleCredentials creds;
                    
                    // Use credentialsOverrider if provided, otherwise use Application Default Credentials
                    if (getCredentialsOverrider() != null) {
                        com.google.auth.Credentials providedCreds = GcpCredentialsProvider.getCredentials(
                            getCredentialsOverrider());
                        if (providedCreds instanceof GoogleCredentials) {
                            creds = (GoogleCredentials) providedCreds;
                        } else {
                            throw new IllegalStateException("GCP credentials must be GoogleCredentials");
                        }
                    } else {
                        // Use Application Default Credentials (ADC)
                        creds = GoogleCredentials.getApplicationDefault();
                    }
                    
                    // Ensure credentials have necessary scopes for Artifact Registry
                    if (creds.getScopes() == null || creds.getScopes().isEmpty()) {
                        creds = creds.createScoped(
                            Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
                    }
                    
                    credentials = creds;
                } catch (IOException e) {
                    throw new RuntimeException("Failed to initialize Google credentials", e);
                }
            }

            if (ociClient == null) {
                // Create GcpRegistry instance first (ociClient will be null initially)
                GcpRegistry registry = new GcpRegistry(this, null, credentials);
                
                // Now create OciRegistryClient using the registry instance as AuthProvider
                CloseableHttpClient httpClient = registry.getHttpClient();
                ociClient = new OciRegistryClient(getRegistryEndpoint(), registry, httpClient);
                
                // Set the ociClient on the registry instance
                registry.ociClient = ociClient;
                
                return registry;
            }
            return new GcpRegistry(this, ociClient, credentials);
        }
    }
}
