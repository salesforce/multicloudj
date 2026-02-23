package com.salesforce.multicloudj.registry.gcp;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.gcp.CommonErrorCodeMapping;
import com.salesforce.multicloudj.common.gcp.GcpCredentialsProvider;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import com.salesforce.multicloudj.registry.driver.OciRegistryClient;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Collections;

/**
 * GCP Artifact Registry implementation.
 *
 * <p>Authentication uses OAuth2 access tokens obtained from Google credentials.
 * The token is automatically refreshed when expired.
 *
 * <p>Registry endpoint format: https://{location}-docker.pkg.dev
 * Example: <a href="https://us-central1-docker.pkg.dev">...</a>
 */
@AutoService(AbstractRegistry.class)
public class GcpRegistry extends AbstractRegistry {

    public static final String PROVIDER_ID = "gcp";
    private static final String GCP_AUTH_USERNAME = "oauth2accesstoken";
    private static final String CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform";

    /** Lock for thread-safe lazy initialization of credentials. */
    private final Object credentialsLock = new Object();

    private final OciRegistryClient ociClient;

    /** Lazily initialized credentials with double-checked locking. */
    private volatile GoogleCredentials credentials;

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
    public String getAuthUsername() {
        return GCP_AUTH_USERNAME;
    }

    @Override
    public String getAuthToken() throws IOException {
        GoogleCredentials creds = getOrCreateCredentials();

        AccessToken accessToken = creds.getAccessToken();
        if (accessToken == null) {
            throw new IOException("Failed to obtain GCP access token: access token is null");
        }
        
        if (accessToken.getTokenValue() == null) {
            throw new IOException("Failed to obtain GCP access token: token value is null");
        }

        return accessToken.getTokenValue();
    }

    /**
     * Returns valid credentials, initializing lazily with double-checked locking
     * and refreshing if expired.
     */
    private GoogleCredentials getOrCreateCredentials() throws IOException {
        if (credentials == null) {
            synchronized (credentialsLock) {
                if (credentials == null) {
                    credentials = createGoogleCredentials();
                }
            }
        }
        credentials.refreshIfExpired();
        return credentials;
    }

    private GoogleCredentials createGoogleCredentials() throws IOException {
        GoogleCredentials creds;

        if (credentialsOverrider != null) {
            creds = (GoogleCredentials) GcpCredentialsProvider.getCredentials(credentialsOverrider);
            if (creds == null) {
                throw new IOException("Failed to obtain credentials from CredentialsOverrider");
            }
            return creds.createScoped(Collections.singletonList(CLOUD_PLATFORM_SCOPE));
        }

        GoogleCredentials defaultCreds = GoogleCredentials.getApplicationDefault();
        if (defaultCreds == null) {
            throw new IOException("Failed to load GCP credentials: application default credentials not available");
        }
        return defaultCreds.createScoped(Collections.singletonList(CLOUD_PLATFORM_SCOPE));
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        if (t instanceof SubstrateSdkException) {
            return (Class<? extends SubstrateSdkException>) t.getClass();
        } else if (t instanceof ApiException) {
            ApiException exception = (ApiException) t;
            StatusCode statusCode = exception.getStatusCode();
            return CommonErrorCodeMapping.getException(statusCode.getCode());
        } else if (t instanceof IllegalArgumentException) {
            return InvalidArgumentException.class;
        }
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
            
            // Validate registry endpoint is set (fail fast)
            if (StringUtils.isBlank(registryEndpoint)) {
                throw new IllegalArgumentException("Registry endpoint is required for GCP Artifact Registry");
            }
            
            return new GcpRegistry(this);
        }
    }
}
