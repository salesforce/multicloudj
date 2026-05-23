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
import com.salesforce.multicloudj.registry.driver.AuthChallenge;
import com.salesforce.multicloudj.registry.driver.BearerTokenExchange;
import java.io.IOException;
import java.util.Collections;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

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

  private static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";

  /** Lock for thread-safe lazy initialization of credentials. */
  private final Object credentialsLock = new Object();

  private final CloseableHttpClient tokenHttpClient;
  private final BearerTokenExchange tokenExchange;

  /** Lazily initialized credentials with double-checked locking. */
  private volatile GoogleCredentials credentials;

  public GcpRegistry() {
    this(new Builder());
  }

  public GcpRegistry(Builder builder) {
    this(builder, null, null, null);
  }

  /**
   * Creates GcpRegistry with specified HTTP client and credentials. Used by conformance tests to
   * inject a WireMock proxy client and mock/real credentials.
   *
   * @param builder the builder with configuration
   * @param ociHttpClient the HTTP client for OCI transport (null to create default); also reused
   *     for bearer token exchange when provided
   * @param credentials pre-loaded credentials (null to load from application default)
   */
  public GcpRegistry(
      Builder builder, CloseableHttpClient ociHttpClient, GoogleCredentials credentials) {
    this(builder, ociHttpClient, credentials, null);
  }

  /**
   * Full injection constructor for unit testing. Allows injecting a mock BearerTokenExchange to
   * avoid real HTTP calls during tests.
   */
  GcpRegistry(
      Builder builder,
      CloseableHttpClient ociHttpClient,
      GoogleCredentials credentials,
      BearerTokenExchange tokenExchange) {
    super(builder.withHttpClient(ociHttpClient));
    this.credentials = credentials;
    // In production (no injected client or exchange), create a dedicated client for token exchange
    CloseableHttpClient tokenClient =
        (registryEndpoint != null && tokenExchange == null && ociHttpClient == null)
            ? HttpClients.createDefault()
            : null;
    this.tokenHttpClient = tokenClient;
    this.tokenExchange =
        registryEndpoint == null ? null
        : tokenExchange != null  ? tokenExchange
        : ociHttpClient != null  ? new BearerTokenExchange(ociHttpClient)
                                 : new BearerTokenExchange(tokenClient);
  }

  @Override
  public Builder builder() {
    return new Builder();
  }

  @Override
  public String getAuthorizationHeader(AuthChallenge challenge, String repository) {
    String identityToken = getAuthToken();
    String bearerToken =
        tokenExchange.getBearerToken(challenge, identityToken, repository, "pull");
    return "Bearer " + bearerToken;
  }

  private String getAuthToken() {
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
    closeOciTransport();
    if (tokenHttpClient != null) {
      tokenHttpClient.close();
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
