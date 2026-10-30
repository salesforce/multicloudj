package com.salesforce.multicloudj.sts.aws;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.sts.client.StsUtilities;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.SignOptions;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class AwsStsUtilitiesTest {

  private static final Clock FIXED_CLOCK =
      Clock.fixed(Instant.parse("2026-10-30T12:00:00Z"), ZoneOffset.UTC);
  private static final String REGION = "us-west-2";
  private static final StsCredentials CREDS =
      new StsCredentials(
          "AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "FAKE_SESSION_TOKEN");

  private static StsCredentials credentials;
  private static String region;

  @BeforeAll
  public static void setUp() {
    region = "us-west-2";
    credentials = new StsCredentials("testKeyId", "testSecret", "testToken");
  }

  @Test
  public void testAwsStsInitialization() {
    AwsStsUtilities utilities = new AwsStsUtilities();
    Assertions.assertEquals("aws", utilities.getProviderId());
  }

  @Test
  void signedRequestEchoesSuppliedCredentials() {
    SignedAuthRequest signed = stsUtil().newCloudNativeAuthSignedRequest(sampleRequest());

    Assertions.assertEquals(
        credentials.getAccessKeyId(), signed.getCredentials().getAccessKeyId());
    Assertions.assertEquals(
        credentials.getAccessKeySecret(), signed.getCredentials().getAccessKeySecret());
    Assertions.assertEquals(
        credentials.getSecurityToken(), signed.getCredentials().getSecurityToken());
  }

  @Test
  void defaultSignatureMatchesAwsSdkSigner() {
    SignedAuthRequest signed =
        signer(FIXED_CLOCK).newCloudNativeAuthSignedRequest(formRequest(), null);

    Assertions.assertEquals(
        SigV4Oracle.authorization(signed, CREDS, REGION, FIXED_CLOCK),
        signed.getRequest().headers().firstValue("authorization").orElseThrow());
  }

  @Test
  void excludedHashAndContentTypeSignatureMatchesAwsSdkSigner() {
    SignOptions options =
        SignOptions.builder()
            .withExcludeRequestHashHeader(true)
            .withExcludeContentTypeHeader(true)
            .build();
    SignedAuthRequest signed =
        signer(FIXED_CLOCK).newCloudNativeAuthSignedRequest(formRequest(), options);

    String authorization = signed.getRequest().headers().firstValue("authorization").orElseThrow();
    Assertions.assertTrue(
        authorization.contains("SignedHeaders=host;x-amz-date;x-amz-security-token,"));
    Assertions.assertEquals(
        SigV4Oracle.authorization(signed, CREDS, REGION, FIXED_CLOCK), authorization);
  }

  @Test
  void mixedCaseCustomHeaderIsSignedCanonically() {
    SignOptions options =
        SignOptions.builder()
            .withCustomHeader("X-Vault-AWS-IAM-Server-ID", "vault.example.com")
            .build();
    SignedAuthRequest signed =
        signer(FIXED_CLOCK).newCloudNativeAuthSignedRequest(formRequest(), options);

    String authorization = signed.getRequest().headers().firstValue("authorization").orElseThrow();
    Assertions.assertTrue(
        authorization.contains(
            "SignedHeaders=content-type;host;x-amz-date;x-amz-security-token;"
                + "x-request-hash;x-vault-aws-iam-server-id,"),
        authorization);
    Assertions.assertEquals(
        SigV4Oracle.authorization(signed, CREDS, REGION, FIXED_CLOCK), authorization);
  }

  @Test
  void customHeaderCollidingWithSignerHeaderIsRejected() {
    for (String name :
        new String[] {
          "X-Amz-Date",
          "host",
          "Authorization",
          "x-amz-security-token",
          "content-type",
          "x-request-hash"
        }) {
      SignOptions options = SignOptions.builder().withCustomHeader(name, "v").build();
      Assertions.assertThrows(
          InvalidArgumentException.class,
          () -> signer(FIXED_CLOCK).newCloudNativeAuthSignedRequest(formRequest(), options),
          name);
    }
  }

  @Test
  void signedRequestUsesConventionalHeaderNames() {
    SignedAuthRequest signed =
        signer(FIXED_CLOCK).newCloudNativeAuthSignedRequest(formRequest(), null);

    List<String> names = new ArrayList<>(signed.getRequest().headers().map().keySet());
    Assertions.assertTrue(
        names.containsAll(
            List.of(
                "Authorization",
                "X-Amz-Date",
                "X-Amz-Security-Token",
                "Content-Type",
                "X-Request-Hash")),
        names.toString());
  }

  @Test
  void minimalHeaderSetReplaysWithOnlyAuthorizationDateAndToken() {
    SignOptions options =
        SignOptions.builder()
            .withExcludeRequestHashHeader(true)
            .withExcludeContentTypeHeader(true)
            .build();
    SignedAuthRequest signed =
        signer(FIXED_CLOCK).newCloudNativeAuthSignedRequest(formRequest(), options);

    List<String> names = new ArrayList<>(signed.getRequest().headers().map().keySet());
    Assertions.assertEquals(3, names.size(), names.toString());
    Assertions.assertTrue(
        names.containsAll(List.of("Authorization", "X-Amz-Date", "X-Amz-Security-Token")),
        names.toString());
    Assertions.assertEquals(
        SigV4Oracle.authorization(signed, CREDS, REGION, FIXED_CLOCK),
        signed.getRequest().headers().firstValue("Authorization").orElseThrow());
  }

  @Test
  void signedIdentityKeepsLowercaseHeaderKeys() {
    SignedAuthRequest signed =
        signer(FIXED_CLOCK).newCloudNativeAuthSignedRequest(formRequest(), null);

    Map<String, String> params =
        parseQuery(URI.create(signed.getSignedIdentity()).getRawQuery());
    Assertions.assertTrue(params.containsKey("authorization"), params.keySet().toString());
    Assertions.assertTrue(params.containsKey("x-amz-date"), params.keySet().toString());
    Assertions.assertFalse(params.containsKey("Authorization"), params.keySet().toString());
  }

  @Test
  void endpointOverrideSetsSignedHostAndIdentityRoot() {
    SignedAuthRequest signed =
        signer(FIXED_CLOCK, REGION, URI.create("https://sts.amazonaws.com"))
            .newCloudNativeAuthSignedRequest(formRequest(), null);

    Assertions.assertEquals(URI.create("https://sts.amazonaws.com/"), signed.getRequest().uri());
    Assertions.assertTrue(
        signed.getSignedIdentity().startsWith("https://sts.amazonaws.com/?"),
        signed.getSignedIdentity());
    Assertions.assertEquals(
        SigV4Oracle.authorization(signed, CREDS, REGION, FIXED_CLOCK),
        signed.getRequest().headers().firstValue("authorization").orElseThrow());
  }

  @Test
  void endpointOverrideWithPortSignsHostWithPort() {
    SignedAuthRequest signed =
        signer(FIXED_CLOCK, REGION, URI.create("https://localhost:8443"))
            .newCloudNativeAuthSignedRequest(formRequest(), null);

    Assertions.assertEquals(URI.create("https://localhost:8443/"), signed.getRequest().uri());
    Assertions.assertTrue(
        signed.getSignedIdentity().startsWith("https://localhost:8443/?"),
        signed.getSignedIdentity());
    Assertions.assertEquals(
        SigV4Oracle.authorization(signed, CREDS, REGION, FIXED_CLOCK),
        signed.getRequest().headers().firstValue("authorization").orElseThrow());
  }

  @Test
  void endpointWithPathQueryOrFragmentIsRejected() {
    for (String endpoint : new String[] {"https://h/proxy", "https://h/?a=b", "https://h/#frag"}) {
      AwsStsUtilities utilities = signer(FIXED_CLOCK, REGION, URI.create(endpoint));
      Assertions.assertThrows(
          InvalidArgumentException.class,
          () -> utilities.newCloudNativeAuthSignedRequest(formRequest(), null),
          endpoint);
    }
  }

  @Test
  void endpointWithRootPathIsAccepted() {
    SignedAuthRequest signed =
        signer(FIXED_CLOCK, REGION, URI.create("https://sts.amazonaws.com/"))
            .newCloudNativeAuthSignedRequest(formRequest(), null);

    Assertions.assertEquals(URI.create("https://sts.amazonaws.com/"), signed.getRequest().uri());
  }

  @Test
  void missingRegionIsRejected() {
    for (String blank : new String[] {null, "", "  "}) {
      AwsStsUtilities utilities = signer(FIXED_CLOCK, blank, null);
      InvalidArgumentException e =
          Assertions.assertThrows(
              InvalidArgumentException.class,
              () -> utilities.newCloudNativeAuthSignedRequest(formRequest(), null));
      Assertions.assertTrue(e.getMessage().contains("region is required"), e.getMessage());
    }
  }

  @Test
  void missingRegionIsRejectedEvenWithEndpointOverride() {
    AwsStsUtilities utilities =
        signer(FIXED_CLOCK, null, URI.create("https://sts.amazonaws.com"));
    Assertions.assertThrows(
        InvalidArgumentException.class,
        () -> utilities.newCloudNativeAuthSignedRequest(formRequest(), null));
  }

  @Test
  void noEndpointKeepsRegionalDefault() {
    SignedAuthRequest signed =
        signer(FIXED_CLOCK).newCloudNativeAuthSignedRequest(formRequest(), null);

    Assertions.assertEquals(
        URI.create("https://sts.us-west-2.amazonaws.com/"), signed.getRequest().uri());
    Assertions.assertTrue(
        signed.getSignedIdentity().startsWith("https://sts.us-west-2.amazonaws.com/?"),
        signed.getSignedIdentity());
  }

  private static AwsStsUtilities signer(Clock clock) {
    return signer(clock, REGION, null);
  }

  private static AwsStsUtilities signer(Clock clock, String signerRegion, URI endpoint) {
    AwsStsUtilities.Builder builder = new AwsStsUtilities.Builder();
    if (signerRegion != null) {
      builder.withRegion(signerRegion);
    }
    if (endpoint != null) {
      builder.withEndpoint(endpoint);
    }
    builder.withCredentialsOverrider(
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(CREDS)
            .build());
    return new AwsStsUtilities(builder, clock);
  }

  private static HttpRequest formRequest() {
    return HttpRequest.newBuilder()
        .POST(HttpRequest.BodyPublishers.ofString("Action=GetCallerIdentity&Version=2011-06-15"))
        .uri(URI.create("https://sts.us-west-2.amazonaws.com/"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .build();
  }

  private static StsUtilities stsUtil() {
    return StsUtilities.builder("aws")
        .withRegion(region)
        .withCredentialsOverrider(
            new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(credentials)
                .build())
        .build();
  }

  private static HttpRequest sampleRequest() {
    HttpRequest.BodyPublisher body =
        HttpRequest.BodyPublishers.ofString("Action=GetCallerIdentity&Version=2011-06-15");
    return HttpRequest.newBuilder()
        .POST(body)
        .uri(URI.create("https://sts." + region + ".amazonaws.com:443"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .build();
  }

  @Test
  void defaultOptionsAddRequestHashAndContentTypeHeaders() {
    SignedAuthRequest signed =
        stsUtil().newCloudNativeAuthSignedRequest(sampleRequest(), SignOptions.builder().build());

    Assertions.assertTrue(
        signed.getRequest().headers().firstValue("X-Request-Hash").isPresent(),
        "X-Request-Hash header should be present by default when a request is supplied");
    Assertions.assertEquals(
        "application/x-www-form-urlencoded; charset=utf-8",
        signed.getRequest().headers().firstValue("Content-Type").orElse(null),
        "Content-Type header should be present by default");
  }

  @Test
  void excludeRequestHashHeaderOptionDropsRequestHashHeader() {
    SignedAuthRequest signed =
        stsUtil()
            .newCloudNativeAuthSignedRequest(
                sampleRequest(),
                SignOptions.builder().withExcludeRequestHashHeader(true).build());

    Assertions.assertFalse(
        signed.getRequest().headers().firstValue("X-Request-Hash").isPresent(),
        "X-Request-Hash header should be excluded when the option is set");
    Assertions.assertTrue(
        signed.getRequest().headers().firstValue("Content-Type").isPresent(),
        "Content-Type header should still be present");
  }

  @Test
  void excludeContentTypeHeaderOptionDropsContentTypeHeader() {
    SignedAuthRequest signed =
        stsUtil()
            .newCloudNativeAuthSignedRequest(
                sampleRequest(),
                SignOptions.builder().withExcludeContentTypeHeader(true).build());

    Assertions.assertFalse(
        signed.getRequest().headers().firstValue("Content-Type").isPresent(),
        "Content-Type header should be excluded when the option is set");
    Assertions.assertTrue(
        signed.getRequest().headers().firstValue("X-Request-Hash").isPresent(),
        "X-Request-Hash header should still be present");
  }

  @Test
  void customHeadersAreAppliedToSignedRequest() {
    SignedAuthRequest signed =
        stsUtil()
            .newCloudNativeAuthSignedRequest(
                sampleRequest(),
                SignOptions.builder().withCustomHeader("X-Custom-Header", "custom-value").build());

    Assertions.assertEquals(
        "custom-value",
        signed.getRequest().headers().firstValue("X-Custom-Header").orElse(null),
        "Custom header should be applied to the signed request");
  }

  @Test
  void nullRequestOmitsRequestHashHeaderButKeepsContentType() {
    SignedAuthRequest signed =
        stsUtil().newCloudNativeAuthSignedRequest(null, SignOptions.builder().build());

    Assertions.assertNotNull(signed.getRequest());
    Assertions.assertFalse(
        signed.getRequest().headers().firstValue("X-Request-Hash").isPresent(),
        "X-Request-Hash header should be absent when no request is supplied");
    Assertions.assertTrue(
        signed.getRequest().headers().firstValue("Content-Type").isPresent(),
        "Content-Type header should still be present for a null request");
  }

  @Test
  void signedIdentityIsReplayableUrlRootedAtStsEndpoint() {
    SignedAuthRequest signed =
        stsUtil().newCloudNativeAuthSignedRequest(sampleRequest(), SignOptions.builder().build());

    String signedIdentity = signed.getSignedIdentity();
    Assertions.assertNotNull(signedIdentity, "Signed identity should be produced for AWS");

    URI identityUri = URI.create(signedIdentity);
    Assertions.assertEquals(
        "https", identityUri.getScheme(), "Signed identity should be an https URL");
    Assertions.assertEquals(
        "sts." + region + ".amazonaws.com",
        identityUri.getHost(),
        "Signed identity should be rooted at the STS endpoint");

    Map<String, String> params = parseQuery(identityUri.getRawQuery());
    Assertions.assertEquals(
        "GetCallerIdentity",
        params.get("Action"),
        "Signed identity should carry the STS action");
    Assertions.assertEquals(
        "2011-06-15", params.get("Version"), "Signed identity should carry the STS version");
    Assertions.assertTrue(
        params.containsKey("authorization"),
        "Signed identity should carry the authorization signed header");
  }

  @Test
  void actionInQueryStringSignsActionAndVersionAsQueryParamsWithEmptyBody() {
    SignedAuthRequest signed =
        stsUtil()
            .newCloudNativeAuthSignedRequest(
                null, SignOptions.builder().withActionInQueryString(true).build());

    HttpRequest request = signed.getRequest();
    Assertions.assertEquals("POST", request.method());

    Map<String, String> params = parseQuery(request.uri().getRawQuery());
    Assertions.assertEquals(
        "GetCallerIdentity",
        params.get("Action"),
        "Action should be carried in the request URL query string");
    Assertions.assertEquals(
        "2011-06-15", params.get("Version"), "Version should be carried in the request URL query");

    // The request is signed over an empty body when the action lives in the query string.
    Assertions.assertEquals(
        0L,
        request.bodyPublisher().map(HttpRequest.BodyPublisher::contentLength).orElse(0L),
        "The signed request should carry an empty body");
    Assertions.assertTrue(
        request.headers().firstValue("Authorization").isPresent(),
        "The request should be signed");
  }

  @Test
  void actionInQueryStringSignsMinimalHeaderSetWithoutContentHash() {
    SignedAuthRequest signed =
        stsUtil()
            .newCloudNativeAuthSignedRequest(
                null,
                SignOptions.builder()
                    .withActionInQueryString(true)
                    .withCustomHeader("x-goog-cloud-target-resource", "//example/audience")
                    .build());

    String authorization = signed.getRequest().headers().firstValue("Authorization").orElse("");
    String signedHeaders = extractSignedHeaders(authorization);

    // A verifier that replays this request from its URL and headers alone presents only these
    // headers. The content hash header must NOT be signed, otherwise the replayed request is
    // missing a signed header and AWS rejects the signature.
    Assertions.assertEquals(
        "host;x-amz-date;x-amz-security-token;x-goog-cloud-target-resource",
        signedHeaders,
        "SignedHeaders should be the minimal replayable set with no x-amz-content-sha256");
    Assertions.assertFalse(
        signedHeaders.contains("x-amz-content-sha256"),
        "x-amz-content-sha256 must not be part of SignedHeaders for the query-string path");
  }

  @Test
  void noCredentialsOverriderFallsBackToDefaultCredentialChain() {
    // The AWS default credential chain reads these system properties, so no overrider is needed.
    String priorAccessKey = System.getProperty("aws.accessKeyId");
    String priorSecretKey = System.getProperty("aws.secretAccessKey");
    System.setProperty("aws.accessKeyId", "defaultChainKeyId");
    System.setProperty("aws.secretAccessKey", "defaultChainSecret");
    try {
      StsUtilities stsUtil = StsUtilities.builder("aws").withRegion(region).build();

      SignedAuthRequest signed =
          stsUtil.newCloudNativeAuthSignedRequest(null, SignOptions.builder().build());

      Assertions.assertEquals(
          "defaultChainKeyId",
          signed.getCredentials().getAccessKeyId(),
          "Credentials should be resolved from the default chain when no overrider is supplied");
      Assertions.assertNull(
          signed.getCredentials().getSecurityToken(),
          "Long-term default-chain credentials should carry no session token");
      // A long-term credential has no session token, so it is not part of SignedHeaders.
      String authorization =
          signed.getRequest().headers().firstValue("Authorization").orElse("");
      String signedHeaders = extractSignedHeaders(authorization);
      Assertions.assertFalse(
          signedHeaders.contains("x-amz-security-token"),
          "x-amz-security-token should be absent when no session token is present");
    } finally {
      restoreProperty("aws.accessKeyId", priorAccessKey);
      restoreProperty("aws.secretAccessKey", priorSecretKey);
    }
  }

  private static void restoreProperty(String key, String priorValue) {
    if (priorValue == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, priorValue);
    }
  }

  private static String extractSignedHeaders(String authorization) {
    for (String part : authorization.split(", ")) {
      if (part.startsWith("SignedHeaders=")) {
        return part.substring("SignedHeaders=".length());
      }
    }
    return "";
  }

  private static Map<String, String> parseQuery(String rawQuery) {
    Map<String, String> params = new HashMap<>();
    for (String pair : rawQuery.split("&")) {
      int idx = pair.indexOf('=');
      String key = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
      String value = URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8);
      params.put(key, value);
    }
    return params;
  }
}
