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
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.gcp.GcpCredentialsProvider;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import com.salesforce.multicloudj.registry.driver.OciRegistryClient;
import java.io.IOException;
import java.util.Collections;
import org.apache.commons.lang3.StringUtils;

/**
 * GCP Artifact Registry implementation.
 *
 * <p>Authentication uses OAuth2 access tokens obtained from Google credentials. The token is
 * automatically refreshed when expired.
 *
 * <p>Registry endpoint format: https://{location}-docker.pkg.dev Example: <a
 * href="https://us-central1-docker.pkg.dev">...</a>
 */
@AutoService(AbstractRegistry.class)
public class GcpRegistry extends AbstractRegistry {

  private static final String GCP_AUTH_USERNAME = "oauth2accesstoken";
  private static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";

  /** Lock for thread-safe lazy initialization of credentials. */
  private final Object credentialsLock = new Object();

  private final OciRegistryClient ociClient;

  /** Lazily initialized credentials with double-checked locking. */
  private volatile GoogleCredentials credentials;

  public GcpRegistry() {
    this(new Builder());
  }

  public GcpRegistry(Builder builder) {
    super(builder);
    this.ociClient =
        registryEndpoint != null ? new OciRegistryClient(registryEndpoint, this) : null;
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
  public String getAuthToken() {
    GoogleCredentials creds = getOrCreateCredentials();

    AccessToken accessToken = creds.getAccessToken();
    if (accessToken == null) {
      throw new UnknownException("Failed to obtain GCP access token: access token is null");
    }

    if (accessToken.getTokenValue() == null) {
      throw new UnknownException("Failed to obtain GCP access token: token value is null");
    }

    return accessToken.getTokenValue();
  }

  /**
   * Returns valid credentials, initializing lazily with double-checked locking and refreshing if
   * expired.
   */
  private GoogleCredentials getOrCreateCredentials() {
    if (credentials == null) {
      synchronized (credentialsLock) {
        if (credentials == null) {
          credentials = createGoogleCredentials();
        }
      }
    }
    try {
      credentials.refreshIfExpired();
    } catch (IOException e) {
      throw new UnknownException("Failed to refresh GCP credentials", e);
    }
    return credentials;
  }

  private GoogleCredentials createGoogleCredentials() {
    try {
      GoogleCredentials creds;

      if (credentialsOverrider != null) {
        creds = (GoogleCredentials) GcpCredentialsProvider.getCredentials(credentialsOverrider);
        if (creds == null) {
          throw new UnknownException("Failed to obtain credentials from CredentialsOverrider");
        }
        return creds.createScoped(Collections.singletonList(CLOUD_PLATFORM_SCOPE));
      }

      GoogleCredentials defaultCreds = GoogleCredentials.getApplicationDefault();
      if (defaultCreds == null) {
        throw new UnknownException(
            "Failed to load GCP credentials: application default credentials not available");
      }
      return defaultCreds.createScoped(Collections.singletonList(CLOUD_PLATFORM_SCOPE));
    } catch (IOException e) {
      throw new UnknownException("Failed to load GCP credentials", e);
    }
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

  public static class Builder extends AbstractRegistry.Builder<GcpRegistry, Builder> {

    public Builder() {
      providerId(GcpConstants.PROVIDER_ID);
    }

    @Override
    public Builder self() {
      return this;
    }

    @Override
    public GcpRegistry build() {
      if (StringUtils.isBlank(registryEndpoint)) {
        throw new InvalidArgumentException(
            "Registry endpoint is required for GCP Artifact Registry");
      }

      return new GcpRegistry(this);
    }
  }
}
