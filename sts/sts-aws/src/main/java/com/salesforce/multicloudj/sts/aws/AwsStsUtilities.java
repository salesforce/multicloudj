package com.salesforce.multicloudj.sts.aws;

import static java.util.concurrent.CompletableFuture.completedFuture;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.aws.AwsRetryClassifier;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.driver.AbstractStsUtilities;
import com.salesforce.multicloudj.sts.driver.FlowCollector;
import com.salesforce.multicloudj.sts.model.SignOptions;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

@AutoService(AbstractStsUtilities.class)
public class AwsStsUtilities extends AbstractStsUtilities<AwsStsUtilities> {
  private static final String SIGN_STS_ENDPOINT = "https://sts.%s.amazonaws.com/";
  private static final String DEFAULT_API_ACTION_NAME = "GetCallerIdentity";
  private static final String DEFAULT_API_VERSION = "2011-06-15";
  private static final String SERVICE_SIGNING_NAME = "sts";
  private static final String AWS4_HMAC_SHA256 = "AWS4-HMAC-SHA256";
  private static final String AWS4_REQUEST = "aws4_request";
  private static final DateTimeFormatter AMZ_DATE_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withLocale(Locale.US);
  private static final DateTimeFormatter AMZ_DATESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd").withLocale(Locale.US);
  private static final String REQUEST_HASH_HEADER = "x-request-hash";
  private static final String CONTENT_TYPE_HEADER = "content-type";
  private static final String CONTENT_TYPE_VALUE =
      "application/x-www-form-urlencoded; charset=utf-8";

  // Resolves credentials from the AWS default chain when the caller supplies none. Built lazily and
  // reused across sign calls so the chain's cached, auto-refreshing credentials are retained rather
  // than rebuilt every request.
  private volatile DefaultCredentialsProvider defaultCredentialsProvider;

  public AwsStsUtilities(Builder builder) {
    super(builder);
  }

  public AwsStsUtilities() {
    super(new Builder());
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
    StsCredentials signingCredentials = getSigningCredentials();
    if (options == null) {
      options = SignOptions.builder().build();
    }

    String stsEndpoint = String.format(SIGN_STS_ENDPOINT, region);
    URI uri = URI.create(stsEndpoint);

    // The request is optional. When one is supplied we sign its body; otherwise there is no service
    // payload and we sign a bare GetCallerIdentity.
    byte[] body = extractBody(request);

    // Action=GetCallerIdentity&Version=2011-06-15 travels either in the URL query string or in the
    // request body (the AWS Query form). Signing it into the query string keeps the body empty, so
    // the request can be replayed from its URL and headers alone.
    // See - https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html
    String canonicalQuery = "";
    if (options.isActionInQueryString()) {
      canonicalQuery = "Action=" + DEFAULT_API_ACTION_NAME + "&Version=" + DEFAULT_API_VERSION;
    } else if (body == null) {
      body =
          ("Action=" + DEFAULT_API_ACTION_NAME + "&Version=" + DEFAULT_API_VERSION)
              .getBytes(StandardCharsets.UTF_8);
    }
    byte[] payload = body == null ? new byte[0] : body;
    boolean hasBody = payload.length > 0;

    // Assemble the canonical header set. host, the signing date, and the session token (when the
    // credentials are temporary) are always signed. Header names must be lowercase per the SigV4
    // canonical rules; the TreeMap keeps them sorted.
    Instant now = Instant.now();
    String amzDate = AMZ_DATE_FORMAT.format(now.atOffset(ZoneOffset.UTC));
    String dateStamp = AMZ_DATESTAMP_FORMAT.format(now.atOffset(ZoneOffset.UTC));

    SortedMap<String, String> headers = new TreeMap<>();
    headers.put("host", uri.getHost());
    headers.put("x-amz-date", amzDate);
    if (signingCredentials.getSecurityToken() != null
        && !signingCredentials.getSecurityToken().isEmpty()) {
      headers.put("x-amz-security-token", signingCredentials.getSecurityToken());
    }
    // Content-Type describes the request body, so it is only relevant for the body form.
    if (hasBody && !options.isExcludeContentTypeHeader()) {
      headers.put(CONTENT_TYPE_HEADER, CONTENT_TYPE_VALUE);
    }
    // The request hash header is meaningful only when a caller service request was supplied.
    if (request != null && !options.isExcludeRequestHashHeader()) {
      headers.put(REQUEST_HASH_HEADER, getSha256Hash(payload));
    }
    // Caller supplied custom headers (e.g. a federation target resource) are always signed. Names
    // must already be lowercase to satisfy the SigV4 canonical rules.
    for (Map.Entry<String, String> header : options.getCustomHeaders().entrySet()) {
      headers.put(header.getKey(), header.getValue());
    }

    // Authorization is not itself a signed header, so it is added after signing. host was signed
    // but is dropped now: the JDK HTTP client rejects host as a restricted header, and the STS
    // endpoint URL already carries it.
    String authorization =
        signSigV4(signingCredentials, canonicalQuery, headers, payload, amzDate, dateStamp);
    headers.put("authorization", authorization);
    headers.remove("host");

    URI signedUri = canonicalQuery.isEmpty() ? uri : URI.create(stsEndpoint + "?" + canonicalQuery);
    HttpRequest.BodyPublisher bodyPublisher =
        hasBody
            ? HttpRequest.BodyPublishers.ofByteArray(payload)
            : HttpRequest.BodyPublishers.noBody();
    HttpRequest.Builder builder = HttpRequest.newBuilder(signedUri).method("POST", bodyPublisher);
    headers.forEach(builder::setHeader);

    // The signed identity encodes the STS endpoint, action/version, and every request header as
    // query parameters, so it can be parsed back into a POST and replayed against AWS STS.
    String signedIdentity = buildSignedIdentity(uri, headers);

    return new SignedAuthRequest(builder.build(), signingCredentials, signedIdentity);
  }

  /**
   * Collects the body of an optional unsigned request into a byte array. Returns null when no
   * request or no body is present.
   *
   * <p>A {@link FlowCollector} is used because the body contents are not known in advance. See -
   * https://stackoverflow.com/a/77705720
   */
  private static byte[] extractBody(HttpRequest request) {
    if (request == null) {
      return null;
    }
    return request
        .bodyPublisher()
        .map(
            publisher -> {
              FlowCollector<ByteBuffer> collector = new FlowCollector<>();
              publisher.subscribe(collector);
              return collector
                  .items()
                  .thenApply(items -> items.isEmpty() ? null : items.get(0).array());
            })
        .orElseGet(() -> completedFuture(null))
        .join();
  }

  /**
   * Computes the AWS Signature Version 4 over the given canonical query, headers, and payload, and
   * returns the Authorization header value. The signature covers exactly the supplied headers and
   * no others.
   */
  private String signSigV4(
      StsCredentials signingCredentials,
      String canonicalQuery,
      SortedMap<String, String> headers,
      byte[] payload,
      String amzDate,
      String dateStamp) {
    String signedHeaders = String.join(";", headers.keySet());

    StringBuilder headerBlock = new StringBuilder();
    headers.forEach(
        (name, value) -> headerBlock.append(name).append(':').append(value).append('\n'));

    String canonicalRequest =
        "POST\n"
            + "/\n"
            + canonicalQuery
            + "\n"
            + headerBlock
            + "\n"
            + signedHeaders
            + "\n"
            + getSha256Hash(payload);

    String credentialScope =
        dateStamp + "/" + region + "/" + SERVICE_SIGNING_NAME + "/" + AWS4_REQUEST;
    String stringToSign =
        AWS4_HMAC_SHA256
            + "\n"
            + amzDate
            + "\n"
            + credentialScope
            + "\n"
            + getSha256Hash(canonicalRequest);

    byte[] signingKey =
        deriveSigningKey(
            signingCredentials.getAccessKeySecret(), dateStamp, region, SERVICE_SIGNING_NAME);
    String signature = bytesToHex(hmacSha256(signingKey, stringToSign));

    return AWS4_HMAC_SHA256
        + " Credential="
        + signingCredentials.getAccessKeyId()
        + "/"
        + credentialScope
        + ", SignedHeaders="
        + signedHeaders
        + ", Signature="
        + signature;
  }

  /** Derives the AWS Signature Version 4 signing key for the given scope. */
  private static byte[] deriveSigningKey(
      String secretKey, String dateStamp, String region, String service) {
    byte[] kDate = hmacSha256(("AWS4" + secretKey).getBytes(StandardCharsets.UTF_8), dateStamp);
    byte[] kRegion = hmacSha256(kDate, region);
    byte[] kService = hmacSha256(kRegion, service);
    return hmacSha256(kService, AWS4_REQUEST);
  }

  private static byte[] hmacSha256(byte[] key, String data) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(key, "HmacSHA256"));
      return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new UnknownException(e);
    }
  }

  private static String buildSignedIdentity(URI stsUri, Map<String, String> headers) {
    StringBuilder query = new StringBuilder();
    appendQueryParam(query, "Action", DEFAULT_API_ACTION_NAME);
    appendQueryParam(query, "Version", DEFAULT_API_VERSION);
    headers.forEach((name, value) -> appendQueryParam(query, name, value));
    return stsUri.toString() + "?" + query;
  }

  private static void appendQueryParam(StringBuilder query, String name, String value) {
    if (query.length() > 0) {
      query.append('&');
    }
    query
        .append(URLEncoder.encode(name, StandardCharsets.UTF_8))
        .append('=')
        .append(URLEncoder.encode(value, StandardCharsets.UTF_8));
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder result = new StringBuilder();
    for (byte b : bytes) {
      result.append(String.format("%02x", b));
    }
    return result.toString();
  }

  private static String getSha256Hash(String data) {
    return getSha256Hash(data.getBytes(StandardCharsets.UTF_8));
  }

  private static String getSha256Hash(byte[] data) {
    try {
      return bytesToHex(MessageDigest.getInstance("SHA-256").digest(data));
    } catch (NoSuchAlgorithmException e) {
      throw new UnknownException(e);
    }
  }

  private StsCredentials getSigningCredentials() {
    StsCredentials signingCredentials =
        credentialsOverrider == null ? null : credentialsOverrider.getSessionCredentials();
    if (signingCredentials == null) {
      AwsCredentials resolved = defaultCredentialsProvider().resolveCredentials();
      signingCredentials =
          new StsCredentials(
              resolved.accessKeyId(), resolved.secretAccessKey(), sessionToken(resolved));
    }
    return signingCredentials;
  }

  /** Returns the session token for temporary credentials, or null for long-term credentials. */
  private static String sessionToken(AwsCredentials credentials) {
    if (credentials instanceof AwsSessionCredentials) {
      return ((AwsSessionCredentials) credentials).sessionToken();
    }
    return null;
  }

  private DefaultCredentialsProvider defaultCredentialsProvider() {
    DefaultCredentialsProvider provider = defaultCredentialsProvider;
    if (provider == null) {
      synchronized (this) {
        provider = defaultCredentialsProvider;
        if (provider == null) {
          provider = DefaultCredentialsProvider.builder().build();
          defaultCredentialsProvider = provider;
        }
      }
    }
    return provider;
  }

  public static class Builder extends AbstractStsUtilities.Builder<AwsStsUtilities> {
    protected Builder() {
      providerId("aws");
    }

    @Override
    public AwsStsUtilities build() {
      return new AwsStsUtilities(this);
    }
  }

  @Override
  public SubstrateSdkException mapException(Throwable t) {
    Class<? extends SubstrateSdkException> exceptionClass = UnknownException.class;
    if (t instanceof AwsServiceException) {
      String errorCode = ((AwsServiceException) t).awsErrorDetails().errorCode();
      exceptionClass = ERROR_MAPPING.getOrDefault(errorCode, UnknownException.class);
    }
    return ExceptionHandler.build(exceptionClass, t, AwsRetryClassifier.classify(t));
  }

  // The common error codes as source of truth is here:
  // https://docs.aws.amazon.com/STS/latest/APIReference/CommonErrors.html
  private static final Map<String, Class<? extends SubstrateSdkException>> ERROR_MAPPING =
      Map.of(
          "AccessDenied", UnAuthorizedException.class,
          "IncompleteSignature", InvalidArgumentException.class,
          "InternalFailure", UnknownException.class,
          "InvalidAction", InvalidArgumentException.class,
          "InvalidClientTokenId", InvalidArgumentException.class,
          "NotAuthorized", UnAuthorizedException.class,
          "OptInRequired", UnAuthorizedException.class,
          "RequestExpired", ResourceExhaustedException.class,
          "ThrottlingException", ResourceExhaustedException.class,
          "ValidationError", InvalidArgumentException.class
          // Add more mappings as needed
          );
}
