package com.salesforce.multicloudj.sts.gcp;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.json.webtoken.JsonWebSignature;
import com.google.api.client.json.webtoken.JsonWebToken;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.SignOptions;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import com.salesforce.multicloudj.sts.model.ValidateOptions;
import java.math.BigInteger;
import java.net.URI;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GcpStsUtilitiesTest {

  private static final String SERVICE_ACCOUNT = "test-sa@my-project.iam.gserviceaccount.com";
  private static final String KID = "test-key-1";

  private WireMockServer wireMockServer;
  private KeyPair keyPair;

  @BeforeEach
  void setUp() throws Exception {
    keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
    wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
    wireMockServer.start();
  }

  @AfterEach
  void tearDown() {
    if (wireMockServer != null) {
      wireMockServer.stop();
    }
  }

  @Test
  void providerId() {
    Assertions.assertEquals("gcp", new GcpStsUtilities().getProviderId());
  }

  @Test
  void signsJwtWithIssuerAndSubject() throws Exception {
    SignedAuthRequest signed =
        signer().newCloudNativeAuthSignedRequest(null, SignOptions.builder().build());

    String jwt = signed.getSignedIdentity();
    Assertions.assertNotNull(jwt);
    // The signed identity and the credentials' security token are the same self-contained JWT.
    Assertions.assertEquals(jwt, signed.getCredentials().getSecurityToken());

    JsonWebSignature parsed = JsonWebSignature.parse(GsonFactory.getDefaultInstance(), jwt);
    Assertions.assertEquals("RS256", parsed.getHeader().getAlgorithm());
    Assertions.assertEquals(KID, parsed.getHeader().getKeyId());
    Assertions.assertEquals(SERVICE_ACCOUNT, parsed.getPayload().getIssuer());
    Assertions.assertEquals(SERVICE_ACCOUNT, parsed.getPayload().getSubject());
    Assertions.assertNotNull(parsed.getPayload().getIssuedAtTimeSeconds());
    Assertions.assertNotNull(parsed.getPayload().getExpirationTimeSeconds());
    Assertions.assertEquals(
        300L,
        parsed.getPayload().getExpirationTimeSeconds()
            - parsed.getPayload().getIssuedAtTimeSeconds());
  }

  @Test
  void signedJwtRoundTripsThroughVerifier() {
    stubJwks();
    Map<String, String> customHeaders = new LinkedHashMap<>();
    customHeaders.put("x-target-resource", "my-service");

    SignOptions signOptions = SignOptions.builder().withCustomHeaders(customHeaders).build();
    SignedAuthRequest signed = signer().newCloudNativeAuthSignedRequest(null, signOptions);

    ValidateOptions validateOptions =
        ValidateOptions.builder().withExpectedCustomHeaders(customHeaders).build();
    CallerIdentity identity =
        verifier().verifySignedAuthRequest(signed.getSignedIdentity(), validateOptions);

    Assertions.assertEquals(SERVICE_ACCOUNT, identity.getUserId());
    Assertions.assertEquals(SERVICE_ACCOUNT, identity.getCloudResourceName());
    Assertions.assertEquals("my-project", identity.getAccountId());
  }

  @Test
  void customHeadersAreSignedAsClaims() throws Exception {
    Map<String, String> customHeaders = new LinkedHashMap<>();
    customHeaders.put("x-request-source", "example");
    customHeaders.put("x-tenant-id", "tenant-42");

    SignOptions signOptions = SignOptions.builder().withCustomHeaders(customHeaders).build();
    SignedAuthRequest signed = signer().newCloudNativeAuthSignedRequest(null, signOptions);

    JsonWebSignature parsed =
        JsonWebSignature.parse(GsonFactory.getDefaultInstance(), signed.getSignedIdentity());
    Assertions.assertEquals("example", parsed.getPayload().get("x-request-source"));
    Assertions.assertEquals("tenant-42", parsed.getPayload().get("x-tenant-id"));
  }

  @Test
  void nullOptionsProducesBareIdentity() throws Exception {
    SignedAuthRequest signed = signer().newCloudNativeAuthSignedRequest(null);
    JsonWebSignature parsed =
        JsonWebSignature.parse(GsonFactory.getDefaultInstance(), signed.getSignedIdentity());
    Assertions.assertEquals(SERVICE_ACCOUNT, parsed.getPayload().getIssuer());
  }

  @Test
  void signingFailureWrappedAsUnknown() {
    GcpStsUtilities utilities =
        new GcpStsUtilities.Builder()
            .build(
                (name, payload) -> {
                  throw new java.io.IOException("permission denied");
                },
                SERVICE_ACCOUNT);
    Assertions.assertThrows(
        UnknownException.class,
        () -> utilities.newCloudNativeAuthSignedRequest(null, SignOptions.builder().build()));
  }

  @Test
  void mapExceptionWrapsAsUnknown() {
    SubstrateSdkException mapped =
        new GcpStsUtilities().mapException(new RuntimeException("boom"));
    Assertions.assertInstanceOf(UnknownException.class, mapped);
  }

  /** Builds a signer whose signing seam signs the claims payload with the test RSA key. */
  private GcpStsUtilities signer() {
    return new GcpStsUtilities.Builder().build(this::signPayload, SERVICE_ACCOUNT);
  }

  private String signPayload(String serviceAccountName, String payload) throws java.io.IOException {
    Assertions.assertEquals("projects/-/serviceAccounts/" + SERVICE_ACCOUNT, serviceAccountName);
    JsonWebToken.Payload claims =
        GsonFactory.getDefaultInstance()
            .createJsonParser(payload)
            .parse(JsonWebToken.Payload.class);
    JsonWebSignature.Header header =
        new JsonWebSignature.Header().setAlgorithm("RS256").setType("JWT").setKeyId(KID);
    try {
      return JsonWebSignature.signUsingRsaSha256(
          keyPair.getPrivate(), GsonFactory.getDefaultInstance(), header, claims);
    } catch (java.security.GeneralSecurityException e) {
      throw new java.io.IOException(e);
    }
  }

  private GcpStsVerifier verifier() {
    return new GcpStsVerifier.Builder()
        .withEndpoint(URI.create("http://localhost:" + wireMockServer.port()))
        .build();
  }

  private void stubJwks() {
    RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
    Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
    String n = encoder.encodeToString(toUnsignedBytes(publicKey.getModulus()));
    String e = encoder.encodeToString(toUnsignedBytes(publicKey.getPublicExponent()));
    String jwks =
        "{\"keys\":[{\"kty\":\"RSA\",\"alg\":\"RS256\",\"use\":\"sig\",\"kid\":\""
            + KID
            + "\",\"n\":\""
            + n
            + "\",\"e\":\""
            + e
            + "\"}]}";
    wireMockServer.stubFor(
        get(urlMatching("/service_accounts/v1/metadata/jwk/.*"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(jwks)));
  }

  private static byte[] toUnsignedBytes(BigInteger value) {
    byte[] bytes = value.toByteArray();
    if (bytes.length > 1 && bytes[0] == 0) {
      byte[] trimmed = new byte[bytes.length - 1];
      System.arraycopy(bytes, 1, trimmed, 0, trimmed.length);
      return trimmed;
    }
    return bytes;
  }
}
