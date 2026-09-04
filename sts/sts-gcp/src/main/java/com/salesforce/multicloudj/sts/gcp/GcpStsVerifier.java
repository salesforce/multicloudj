package com.salesforce.multicloudj.sts.gcp;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.json.webtoken.JsonWebSignature;
import com.google.auth.http.HttpTransportFactory;
import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.sts.driver.AbstractStsVerifier;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.ValidateOptions;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.RSAPublicKeySpec;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * GCP implementation of the STS verifier. It validates a signed identity that is a JWT produced by
 * a service account (via the IAM {@code SignJwt} API) by verifying the JWT's RSA signature against
 * the service account's Google-managed public keys.
 *
 * <p>The public keys are published at {@code
 * https://www.googleapis.com/service_accounts/v1/metadata/jwk/{serviceAccountEmail}} as a JWKS
 * document. The verifier selects the key by the JWT {@code kid} header, verifies the signature,
 * enforces the {@code exp}/{@code iat} time bounds with a small clock-skew tolerance, and returns
 * the identity carried in the token's {@code iss}/{@code sub} claims.
 */
@AutoService(AbstractStsVerifier.class)
public class GcpStsVerifier extends AbstractStsVerifier {
  private static final String DEFAULT_JWKS_BASE_URL = "https://www.googleapis.com";
  private static final String JWKS_PATH = "/service_accounts/v1/metadata/jwk/";
  private static final String SERVICE_ACCOUNT_DOMAIN_SUFFIX = ".iam.gserviceaccount.com";
  private static final long CLOCK_SKEW_TOLERANCE_SECONDS = 60L;
  private static final long DEFAULT_CACHE_TTL_SECONDS = 3600L;

  private final HttpTransportFactory httpTransportFactory;
  private final String jwksBaseUrl;
  private final JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
  private final Map<String, CachedKeys> keyCache = new ConcurrentHashMap<>();

  public GcpStsVerifier() {
    this(new Builder());
  }

  public GcpStsVerifier(Builder builder) {
    super(builder);
    this.httpTransportFactory = buildHttpTransportFactory(builder);
    this.jwksBaseUrl = resolveJwksBaseUrl(builder);
  }

  /** Constructor that accepts a preconfigured transport factory, used by tests. */
  public GcpStsVerifier(Builder builder, HttpTransportFactory httpTransportFactory) {
    super(builder);
    this.httpTransportFactory = httpTransportFactory;
    this.jwksBaseUrl = resolveJwksBaseUrl(builder);
  }

  @Override
  public Builder builder() {
    return new Builder();
  }

  @Override
  protected CallerIdentity validateSignedAuthRequest(
      String signedIdentity, ValidateOptions options) {
    if (signedIdentity == null || signedIdentity.isEmpty()) {
      throw new InvalidArgumentException("signedIdentity cannot be empty");
    }

    JsonWebSignature jws;
    try {
      jws = JsonWebSignature.parse(jsonFactory, signedIdentity);
    } catch (IOException e) {
      throw new InvalidArgumentException("failed to parse JWT", e);
    }

    String algorithm = jws.getHeader().getAlgorithm();
    if (algorithm == null || !algorithm.startsWith("RS")) {
      throw new InvalidArgumentException("unsupported signing method: " + algorithm);
    }

    JsonWebSignature.Payload payload = jws.getPayload();
    String issuer = payload.getIssuer();
    if (StringUtils.isBlank(issuer)) {
      throw new InvalidArgumentException("JWT missing iss claim");
    }

    Map<String, PublicKey> keys = getPublicKeys(issuer);
    String kid = jws.getHeader().getKeyId();
    PublicKey publicKey = keys.get(kid);
    if (publicKey == null) {
      throw new ResourceNotFoundException(
          "key ID \"" + kid + "\" not found in public keys for " + issuer);
    }

    boolean verified;
    try {
      verified = jws.verifySignature(publicKey);
    } catch (Exception e) {
      throw new UnAuthorizedException("JWT signature verification failed", e);
    }
    if (!verified) {
      throw new UnAuthorizedException("JWT signature verification failed");
    }

    verifyTimeBounds(payload);
    verifyExpectedCustomHeaders(payload, options);

    String subject = payload.getSubject();
    String projectId = projectIdFromIssuer(issuer);
    return new CallerIdentity(subject, issuer, projectId);
  }

  private void verifyTimeBounds(JsonWebSignature.Payload payload) {
    long now = Instant.now().getEpochSecond();
    Long exp = payload.getExpirationTimeSeconds();
    if (exp != null && now > exp + CLOCK_SKEW_TOLERANCE_SECONDS) {
      throw new UnAuthorizedException("JWT is expired");
    }
    Long iat = payload.getIssuedAtTimeSeconds();
    if (iat != null && now + CLOCK_SKEW_TOLERANCE_SECONDS < iat) {
      throw new UnAuthorizedException("JWT used before issued");
    }
  }

  private static void verifyExpectedCustomHeaders(
      JsonWebSignature.Payload payload, ValidateOptions options) {
    if (options == null) {
      return;
    }
    for (Map.Entry<String, String> expected : options.getExpectedCustomHeaders().entrySet()) {
      Object actual = payload.get(expected.getKey());
      if (actual == null) {
        throw new InvalidArgumentException(
            "expected custom header \"" + expected.getKey() + "\" not found in JWT claims");
      }
      if (!expected.getValue().equals(String.valueOf(actual))) {
        throw new InvalidArgumentException(
            "custom header \"" + expected.getKey() + "\" has an unexpected value");
      }
    }
  }

  private static String projectIdFromIssuer(String issuer) {
    int at = issuer.indexOf('@');
    if (at < 0 || at == issuer.length() - 1) {
      return StringUtils.EMPTY;
    }
    String domain = issuer.substring(at + 1);
    if (domain.endsWith(SERVICE_ACCOUNT_DOMAIN_SUFFIX)) {
      return domain.substring(0, domain.length() - SERVICE_ACCOUNT_DOMAIN_SUFFIX.length());
    }
    return StringUtils.EMPTY;
  }

  private Map<String, PublicKey> getPublicKeys(String serviceAccountEmail) {
    CachedKeys cached = keyCache.get(serviceAccountEmail);
    if (cached != null && Instant.now().getEpochSecond() < cached.expiresAt) {
      return cached.keys;
    }

    String url =
        jwksBaseUrl
            + JWKS_PATH
            + URLEncoder.encode(serviceAccountEmail, StandardCharsets.UTF_8);
    GenericJson response;
    try {
      HttpTransport transport =
          httpTransportFactory != null ? httpTransportFactory.create() : new NetHttpTransport();
      HttpResponse httpResponse =
          transport.createRequestFactory().buildGetRequest(new GenericUrl(url)).execute();
      response =
          jsonFactory.fromInputStream(
              httpResponse.getContent(), httpResponse.getContentCharset(), GenericJson.class);
    } catch (IOException e) {
      throw new UnknownException("failed to fetch public keys for " + serviceAccountEmail, e);
    }

    Map<String, PublicKey> keys = parseJwks(response);
    keyCache.put(
        serviceAccountEmail,
        new CachedKeys(keys, Instant.now().getEpochSecond() + DEFAULT_CACHE_TTL_SECONDS));
    return keys;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, PublicKey> parseJwks(GenericJson response) {
    Map<String, PublicKey> keys = new HashMap<>();
    Object keysObject = response.get("keys");
    if (!(keysObject instanceof List)) {
      return keys;
    }
    Base64.Decoder decoder = Base64.getUrlDecoder();
    KeyFactory keyFactory;
    try {
      keyFactory = KeyFactory.getInstance("RSA");
    } catch (Exception e) {
      throw new UnknownException("RSA KeyFactory unavailable", e);
    }
    for (Object element : (List<Object>) keysObject) {
      if (!(element instanceof Map)) {
        continue;
      }
      Map<String, Object> key = (Map<String, Object>) element;
      String kty = asString(key.get("kty"));
      if (!"RSA".equals(kty)) {
        continue;
      }
      String kid = asString(key.get("kid"));
      String modulus = asString(key.get("n"));
      String exponent = asString(key.get("e"));
      if (modulus == null || exponent == null) {
        continue;
      }
      try {
        BigInteger n = new BigInteger(1, decoder.decode(modulus));
        BigInteger e = new BigInteger(1, decoder.decode(exponent));
        RSAPublicKey publicKey =
            (RSAPublicKey) keyFactory.generatePublic(new RSAPublicKeySpec(n, e));
        keys.put(kid, publicKey);
      } catch (Exception ex) {
        throw new UnknownException("failed to decode public key " + kid, ex);
      }
    }
    return keys;
  }

  private static String asString(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  private static String resolveJwksBaseUrl(Builder builder) {
    if (builder.getEndpoint() != null) {
      String endpoint = builder.getEndpoint().toString();
      return endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
    }
    return DEFAULT_JWKS_BASE_URL;
  }

  private static HttpTransportFactory buildHttpTransportFactory(Builder builder) {
    if (builder.getProxyEndpoint() == null
        && builder.getUseSystemPropertyProxyValues() == null
        && builder.getUseEnvironmentVariableProxyValues() == null) {
      return null;
    }
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    if (Boolean.FALSE.equals(builder.getUseSystemPropertyProxyValues())) {
      httpClientBuilder.setRoutePlanner(
          new org.apache.http.impl.conn.DefaultRoutePlanner(
              org.apache.http.impl.conn.DefaultSchemePortResolver.INSTANCE) {
            @Override
            protected org.apache.http.HttpHost determineProxy(
                org.apache.http.HttpHost target,
                org.apache.http.HttpRequest request,
                org.apache.http.protocol.HttpContext context) {
              return null;
            }
          });
    } else {
      httpClientBuilder.useSystemProperties();
    }
    httpClientBuilder.setDefaultRequestConfig(buildRequestConfig(builder));
    CloseableHttpClient httpClient = httpClientBuilder.build();
    ApacheHttpTransport transport = new ApacheHttpTransport(httpClient);
    return () -> transport;
  }

  private static RequestConfig buildRequestConfig(Builder builder) {
    RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
    if (builder.getProxyEndpoint() != null) {
      URI endpoint = builder.getProxyEndpoint();
      requestConfigBuilder.setProxy(
          new org.apache.http.HttpHost(
              endpoint.getHost(), endpoint.getPort(), endpoint.getScheme()));
    }
    return requestConfigBuilder.build();
  }

  @Override
  public SubstrateSdkException mapException(Throwable t) {
    return ExceptionHandler.build(UnknownException.class, t);
  }

  private static final class CachedKeys {
    private final Map<String, PublicKey> keys;
    private final long expiresAt;

    CachedKeys(Map<String, PublicKey> keys, long expiresAt) {
      this.keys = keys;
      this.expiresAt = expiresAt;
    }
  }

  public static class Builder extends AbstractStsVerifier.Builder<GcpStsVerifier, Builder> {
    protected Builder() {
      providerId(GcpConstants.PROVIDER_ID);
    }

    @Override
    public Builder self() {
      return this;
    }

    /** Builds a verifier backed by the supplied transport factory, used by tests. */
    public GcpStsVerifier build(HttpTransportFactory httpTransportFactory) {
      return new GcpStsVerifier(this, httpTransportFactory);
    }

    @Override
    public GcpStsVerifier build() {
      return new GcpStsVerifier(this);
    }
  }
}
