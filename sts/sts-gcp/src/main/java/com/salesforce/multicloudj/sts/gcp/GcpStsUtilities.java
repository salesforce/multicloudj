package com.salesforce.multicloudj.sts.gcp;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auto.service.AutoService;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.iam.credentials.v1.IamCredentialsSettings;
import com.google.cloud.iam.credentials.v1.SignJwtResponse;
import com.salesforce.multicloudj.common.exceptions.DeadlineExceededException;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.gcp.GcpRetryClassifier;
import com.salesforce.multicloudj.sts.driver.AbstractStsUtilities;
import com.salesforce.multicloudj.sts.model.SignOptions;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.io.IOException;
import java.net.http.HttpRequest;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * GCP implementation of the STS signer. It produces a signed identity that is a JWT signed by a
 * service account through the IAM {@code SignJwt} API. The JWT carries the service account email in
 * its {@code iss}/{@code sub} claims, {@code iat}/{@code exp} time bounds, and any caller supplied
 * custom headers as additional claims.
 *
 * <p>Because the JWT is signed with the service account's Google-managed key, a receiver can verify
 * it offline against the service account's public keys published at {@code
 * https://www.googleapis.com/service_accounts/v1/metadata/jwk/{serviceAccountEmail}} — no local key
 * material or shared secret is required.
 *
 * <p>The service account whose key signs the JWT is taken from the {@code role} on the supplied
 * {@link com.salesforce.multicloudj.sts.model.CredentialsOverrider}; when it is set the IAM client
 * impersonates that service account so it can sign as it. When it is absent the signer
 * authenticates with Application Default Credentials and signs as the account those credentials
 * represent.
 */
@AutoService(AbstractStsUtilities.class)
public class GcpStsUtilities extends AbstractStsUtilities<GcpStsUtilities> {
  private static final long JWT_EXPIRY_SECONDS = 300L;
  private static final String SCOPE = "https://www.googleapis.com/auth/cloud-platform";
  private static final String SA_NAME_FORMAT = "projects/-/serviceAccounts/%s";
  private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

  /**
   * Seam over the IAM {@code SignJwt} call so the signing step can be exercised in isolation. The
   * default implementation calls the IAM Credentials API; tests supply their own.
   */
  @FunctionalInterface
  interface JwtSigner {
    /**
     * Signs the JSON claims payload as the given service account and returns the compact JWT.
     *
     * @param serviceAccountName the fully qualified service account resource name
     * @param payload the JSON-encoded JWT claims
     * @return the signed compact JWT
     * @throws IOException if the signing call fails
     */
    String sign(String serviceAccountName, String payload) throws IOException;
  }

  // Non-null only when injected for tests; otherwise the default IAM-backed signer is used lazily.
  private final JwtSigner jwtSigner;
  // Non-null only when injected for tests; skips service account auto-detection.
  private final String serviceAccountEmailOverride;
  // Lazily created IAM client reused across sign calls, guarded by clientLock.
  private volatile IamCredentialsClient iamCredentialsClient;
  private final Object clientLock = new Object();

  public GcpStsUtilities(Builder builder) {
    super(builder);
    this.jwtSigner = null;
    this.serviceAccountEmailOverride = null;
  }

  public GcpStsUtilities() {
    this(new Builder());
  }

  /** Constructor used by tests to inject the signing seam and a fixed service account email. */
  GcpStsUtilities(Builder builder, JwtSigner jwtSigner, String serviceAccountEmail) {
    super(builder);
    this.jwtSigner = jwtSigner;
    this.serviceAccountEmailOverride = serviceAccountEmail;
  }

  @Override
  public Builder builder() {
    return new Builder();
  }

  @Override
  protected SignedAuthRequest newCloudNativeAuthSignedRequest(HttpRequest request) {
    return newCloudNativeAuthSignedRequest(request, null);
  }

  @Override
  protected SignedAuthRequest newCloudNativeAuthSignedRequest(
      HttpRequest request, SignOptions options) {
    if (options == null) {
      options = SignOptions.builder().build();
    }

    String serviceAccountEmail = resolveServiceAccountEmail();

    // Assemble the JWT claims. iss/sub identify the signing service account and iat/exp bound the
    // token's validity; caller supplied custom headers ride along as additional claims that the
    // verifier can assert.
    long now = Instant.now().getEpochSecond();
    GenericJson claims = new GenericJson();
    claims.setFactory(JSON_FACTORY);
    claims.set("iss", serviceAccountEmail);
    claims.set("sub", serviceAccountEmail);
    claims.set("iat", now);
    claims.set("exp", now + JWT_EXPIRY_SECONDS);
    for (Map.Entry<String, String> header : options.getCustomHeaders().entrySet()) {
      claims.set(header.getKey(), header.getValue());
    }

    String payload;
    try {
      payload = JSON_FACTORY.toString(claims);
    } catch (IOException e) {
      throw new UnknownException("failed to serialize JWT claims", e);
    }

    String signedJwt;
    try {
      signedJwt = signer().sign(String.format(SA_NAME_FORMAT, serviceAccountEmail), payload);
    } catch (IOException e) {
      throw new UnknownException("failed to sign JWT for " + serviceAccountEmail, e);
    }

    // The signed JWT is self-contained and offline-verifiable, so it is both the replayable signed
    // identity and the security token carried by the credentials. There is no HTTP request to
    // replay for JWT-based identity.
    StsCredentials credentials =
        new StsCredentials(StringUtils.EMPTY, StringUtils.EMPTY, signedJwt);
    return new SignedAuthRequest(null, credentials, signedJwt);
  }

  private JwtSigner signer() {
    return jwtSigner != null ? jwtSigner : this::signWithIam;
  }

  private String signWithIam(String serviceAccountName, String payload) throws IOException {
    SignJwtResponse response =
        iamCredentialsClient().signJwt(serviceAccountName, Collections.emptyList(), payload);
    return response.getSignedJwt();
  }

  private IamCredentialsClient iamCredentialsClient() throws IOException {
    IamCredentialsClient client = iamCredentialsClient;
    if (client == null) {
      synchronized (clientLock) {
        client = iamCredentialsClient;
        if (client == null) {
          client = buildIamCredentialsClient();
          iamCredentialsClient = client;
        }
      }
    }
    return client;
  }

  private IamCredentialsClient buildIamCredentialsClient() throws IOException {
    GoogleCredentials sourceCredentials = GoogleCredentials.getApplicationDefault();
    if (sourceCredentials.createScopedRequired()) {
      sourceCredentials = sourceCredentials.createScoped(List.of(SCOPE));
    }

    // When the caller names a target service account, impersonate it so the IAM client can sign as
    // that account. This supports environments where the default credentials cannot sign directly
    // but are permitted to impersonate the target service account.
    GoogleCredentials signingCredentials = sourceCredentials;
    String role = credentialsOverrider == null ? null : credentialsOverrider.getRole();
    if (StringUtils.isNotBlank(role)) {
      signingCredentials =
          ImpersonatedCredentials.newBuilder()
              .setSourceCredentials(sourceCredentials)
              .setTargetPrincipal(role)
              .setScopes(List.of(SCOPE))
              .build();
    }

    IamCredentialsSettings settings =
        IamCredentialsSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(signingCredentials))
            .build();
    return IamCredentialsClient.create(settings);
  }

  /**
   * Resolves the service account whose key signs the JWT. A {@code role} on the credentials
   * overrider names it explicitly; otherwise it is derived from the account behind Application
   * Default Credentials.
   */
  private String resolveServiceAccountEmail() {
    if (StringUtils.isNotBlank(serviceAccountEmailOverride)) {
      return serviceAccountEmailOverride;
    }
    String role = credentialsOverrider == null ? null : credentialsOverrider.getRole();
    if (StringUtils.isNotBlank(role)) {
      return role;
    }
    String email = serviceAccountEmailFromCredentials();
    if (StringUtils.isBlank(email)) {
      throw new InvalidArgumentException(
          "could not determine the signing service account email; supply it as the "
              + "credentialsOverrider role");
    }
    return email;
  }

  private static String serviceAccountEmailFromCredentials() {
    try {
      GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
      if (credentials instanceof ServiceAccountCredentials) {
        return ((ServiceAccountCredentials) credentials).getClientEmail();
      }
      if (credentials instanceof ComputeEngineCredentials) {
        return ((ComputeEngineCredentials) credentials).getAccount();
      }
      return null;
    } catch (IOException e) {
      throw new UnknownException("could not load application default credentials", e);
    }
  }

  @Override
  public SubstrateSdkException mapException(Throwable t) {
    Class<? extends SubstrateSdkException> exceptionClass = UnknownException.class;
    if (t instanceof ApiException) {
      StatusCode statusCode = ((ApiException) t).getStatusCode();
      if (statusCode != null) {
        exceptionClass = ERROR_MAPPING.getOrDefault(statusCode.getCode(), UnknownException.class);
      }
    }
    return ExceptionHandler.build(exceptionClass, t, GcpRetryClassifier.classify(t));
  }

  private static final Map<StatusCode.Code, Class<? extends SubstrateSdkException>> ERROR_MAPPING =
      new HashMap<>();

  static {
    ERROR_MAPPING.put(StatusCode.Code.CANCELLED, UnknownException.class);
    ERROR_MAPPING.put(StatusCode.Code.UNKNOWN, UnknownException.class);
    ERROR_MAPPING.put(StatusCode.Code.INVALID_ARGUMENT, InvalidArgumentException.class);
    ERROR_MAPPING.put(StatusCode.Code.DEADLINE_EXCEEDED, DeadlineExceededException.class);
    ERROR_MAPPING.put(StatusCode.Code.NOT_FOUND, ResourceNotFoundException.class);
    ERROR_MAPPING.put(StatusCode.Code.ALREADY_EXISTS, ResourceAlreadyExistsException.class);
    ERROR_MAPPING.put(StatusCode.Code.PERMISSION_DENIED, UnAuthorizedException.class);
    ERROR_MAPPING.put(StatusCode.Code.RESOURCE_EXHAUSTED, ResourceExhaustedException.class);
    ERROR_MAPPING.put(StatusCode.Code.FAILED_PRECONDITION, FailedPreconditionException.class);
    ERROR_MAPPING.put(StatusCode.Code.ABORTED, DeadlineExceededException.class);
    ERROR_MAPPING.put(StatusCode.Code.OUT_OF_RANGE, InvalidArgumentException.class);
    ERROR_MAPPING.put(StatusCode.Code.UNIMPLEMENTED, UnSupportedOperationException.class);
    ERROR_MAPPING.put(StatusCode.Code.INTERNAL, UnknownException.class);
    ERROR_MAPPING.put(StatusCode.Code.UNAVAILABLE, UnknownException.class);
    ERROR_MAPPING.put(StatusCode.Code.DATA_LOSS, UnknownException.class);
    ERROR_MAPPING.put(StatusCode.Code.UNAUTHENTICATED, UnAuthorizedException.class);
  }

  public static class Builder extends AbstractStsUtilities.Builder<GcpStsUtilities> {
    protected Builder() {
      providerId(GcpConstants.PROVIDER_ID);
    }

    /** Builds a signer backed by the supplied signing seam and service account, used by tests. */
    GcpStsUtilities build(JwtSigner jwtSigner, String serviceAccountEmail) {
      return new GcpStsUtilities(this, jwtSigner, serviceAccountEmail);
    }

    @Override
    public GcpStsUtilities build() {
      return new GcpStsUtilities(this);
    }
  }
}
