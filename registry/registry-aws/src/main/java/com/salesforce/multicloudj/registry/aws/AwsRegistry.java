package com.salesforce.multicloudj.registry.aws;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.aws.CommonErrorCodeMapping;
import com.salesforce.multicloudj.common.aws.CredentialsProvider;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import com.salesforce.multicloudj.registry.driver.AuthChallenge;
import com.salesforce.multicloudj.registry.driver.OciHttpTransport;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.impl.client.CloseableHttpClient;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.GetAuthorizationTokenRequest;
import software.amazon.awssdk.services.ecr.model.GetAuthorizationTokenResponse;

/**
 * AWS Elastic Container Registry (ECR) implementation.
 *
 * <p>Authentication uses ECR's GetAuthorizationToken API which returns a Base64-encoded {@code
 * AWS:<password>} pair. The token is valid for 12 hours and is cached in memory. It is proactively
 * refreshed at the halfway point of its validity window (6 hours). If a refresh fails and a cached
 * tokenis available, the cached token is reused as a fallback rather than failing the request.
 */
@AutoService(AbstractRegistry.class)
public class AwsRegistry extends AbstractRegistry {

  private static final String AWS_AUTH_USERNAME = "AWS";

  /** Lock for thread-safe token refresh. */
  private final Object tokenLock = new Object();

  private final OciHttpTransport ociClient;
  private final EcrClient ecrClient;

  private volatile String cachedAuthToken;
  private volatile long tokenRequestedAt;
  private volatile long tokenExpirationTime;

  public AwsRegistry() {
    this(new Builder());
  }

  public AwsRegistry(Builder builder) {
    this(builder, null);
  }

  /**
   * Creates AwsRegistry with specified EcrClient.
   *
   * @param builder the builder with configuration
   * @param ecrClient the ECR client to use (null to create default)
   */
  public AwsRegistry(Builder builder, EcrClient ecrClient) {
    this(builder, ecrClient, null);
  }

  /**
   * Creates AwsRegistry with specified EcrClient and HttpClient.
   *
   * @param builder the builder with configuration
   * @param ecrClient the ECR client to use (null to create default)
   * @param httpClient the HTTP client for OCI transport (null to create default)
   */
  public AwsRegistry(Builder builder, EcrClient ecrClient, CloseableHttpClient httpClient) {
    super(builder);
    this.ecrClient = ecrClient != null ? ecrClient : createEcrClient();
    this.ociClient =
        registryEndpoint != null
            ? new OciHttpTransport(registryEndpoint, this, httpClient)
            : null;
  }

  @Override
  public Builder builder() {
    return new Builder();
  }

  @Override
  protected OciHttpTransport getOciTransport() {
    return ociClient;
  }

  @Override
  public String getAuthorizationHeader(AuthChallenge challenge, String repository) {
    String credentials = AWS_AUTH_USERNAME + ":" + getAuthToken();
    String encoded =
        Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    return "Basic " + encoded;
  }

  private String getAuthToken() {
    if (cachedAuthToken == null || isPastRefreshPoint()) {
      synchronized (tokenLock) {
        if (cachedAuthToken == null || isPastRefreshPoint()) {
          refreshAuthToken();
        }
      }
    }
    return cachedAuthToken;
  }

  @Override
  protected List<HttpRequestInterceptor> getInterceptors() {
    return List.of(new AuthStrippingInterceptor(registryEndpoint));
  }

  /**
   * Returns true if the current time is past the halfway point of the token's validity window, at
   * which point a proactive refresh is triggered i.e. treats tokens as invalid after 50% of their
   * lifetime.
   */
  private boolean isPastRefreshPoint() {
    long halfwayPoint = tokenRequestedAt + (tokenExpirationTime - tokenRequestedAt) / 2;
    return System.currentTimeMillis() >= halfwayPoint;
  }

  private EcrClient createEcrClient() {
    if (StringUtils.isBlank(region)) {
      return null;
    }
    Region awsRegion = Region.of(region);
    AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
    if (credentialsOverrider != null) {
      AwsCredentialsProvider overrideProvider =
          CredentialsProvider.getCredentialsProvider(credentialsOverrider, awsRegion);
      if (overrideProvider != null) {
        credentialsProvider = overrideProvider;
      }
    }
    return EcrClient.builder().region(awsRegion).credentialsProvider(credentialsProvider).build();
  }

  /**
   * Fetches a fresh ECR authorization token and updates the cache. On {@link AwsServiceException},
   * falls back to the existing cached token if one is available.
   */
  private void refreshAuthToken() {
    try {
      GetAuthorizationTokenResponse response =
          ecrClient.getAuthorizationToken(GetAuthorizationTokenRequest.builder().build());

      if (response.authorizationData().isEmpty()) {
        throw new UnknownException("ECR returned empty authorization data");
      }

      // ECR token is Base64-encoded "AWS:<password>"; extract the password portion
      String encodedToken = response.authorizationData().get(0).authorizationToken();
      String decodedToken =
          new String(Base64.getDecoder().decode(encodedToken), StandardCharsets.UTF_8);
      String[] parts = decodedToken.split(":", 2);
      if (parts.length != 2) {
        throw new UnknownException("Invalid ECR authorization token format");
      }

      cachedAuthToken = parts[1];
      tokenRequestedAt = System.currentTimeMillis();
      tokenExpirationTime = response.authorizationData().get(0).expiresAt().toEpochMilli();
    } catch (AwsServiceException e) {
      if (cachedAuthToken != null) {
        return;
      }
      throw new UnknownException("Failed to get ECR authorization token", e);
    }
  }

  @Override
  public Class<? extends SubstrateSdkException> getException(Throwable t) {
    if (t instanceof SubstrateSdkException) {
      return (Class<? extends SubstrateSdkException>) t.getClass();
    } else if (t instanceof AwsServiceException) {
      AwsServiceException awsException = (AwsServiceException) t;
      String errorCode = awsException.awsErrorDetails().errorCode();
      Class<? extends SubstrateSdkException> mappedException =
          CommonErrorCodeMapping.get().get(errorCode);
      return mappedException != null ? mappedException : UnknownException.class;
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
    if (ecrClient != null) {
      ecrClient.close();
    }
  }

  public static final class Builder extends AbstractRegistry.Builder<AwsRegistry, Builder> {

    public Builder() {
      providerId(AwsConstants.PROVIDER_ID);
    }

    @Override
    public Builder self() {
      return this;
    }

    @Override
    public AwsRegistry build() {
      if (StringUtils.isBlank(registryEndpoint)) {
        throw new InvalidArgumentException("Registry endpoint is required for AWS ECR");
      }
      if (StringUtils.isBlank(region)) {
        throw new InvalidArgumentException("AWS region is required");
      }
      return new AwsRegistry(this);
    }
  }
}
