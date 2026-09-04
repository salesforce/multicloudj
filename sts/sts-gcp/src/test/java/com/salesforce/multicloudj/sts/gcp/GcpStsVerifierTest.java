package com.salesforce.multicloudj.sts.gcp;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.json.webtoken.JsonWebSignature;
import com.google.api.client.json.webtoken.JsonWebToken;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.ValidateOptions;
import java.net.URI;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Base64;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GcpStsVerifierTest {

  private static final String SERVICE_ACCOUNT =
      "test-sa@my-project.iam.gserviceaccount.com";
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
    Assertions.assertEquals("gcp", new GcpStsVerifier().getProviderId());
  }

  @Test
  void returnsCallerIdentityFromValidJwt() throws Exception {
    stubJwks();
    String jwt = signJwt(Instant.now(), null);

    CallerIdentity identity = verifier().verifySignedAuthRequest(jwt);

    Assertions.assertEquals(SERVICE_ACCOUNT, identity.getUserId());
    Assertions.assertEquals(SERVICE_ACCOUNT, identity.getCloudResourceName());
    Assertions.assertEquals("my-project", identity.getAccountId());
  }

  @Test
  void rejectsEmptySignedIdentity() {
    Assertions.assertThrows(
        InvalidArgumentException.class, () -> verifier().verifySignedAuthRequest(""));
  }

  @Test
  void expiredJwtThrowsUnauthorized() throws Exception {
    stubJwks();
    String jwt = signJwt(Instant.now().minusSeconds(3600), null);

    Assertions.assertThrows(
        UnAuthorizedException.class, () -> verifier().verifySignedAuthRequest(jwt));
  }

  @Test
  void unknownKeyIdThrowsNotFound() throws Exception {
    stubJwks();
    String jwt = signJwt(Instant.now(), "other-kid", null);

    Assertions.assertThrows(
        ResourceNotFoundException.class, () -> verifier().verifySignedAuthRequest(jwt));
  }

  @Test
  void tamperedSignatureThrowsUnauthorized() throws Exception {
    stubJwks();
    String jwt = signJwt(Instant.now(), null);
    String tampered = jwt.substring(0, jwt.length() - 4) + "AAAA";

    Assertions.assertThrows(
        UnAuthorizedException.class, () -> verifier().verifySignedAuthRequest(tampered));
  }

  @Test
  void matchingExpectedCustomHeaderPasses() throws Exception {
    stubJwks();
    String jwt = signJwt(Instant.now(), "my-service");
    ValidateOptions options =
        ValidateOptions.builder()
            .withExpectedCustomHeader("x-target-resource", "my-service")
            .build();

    CallerIdentity identity = verifier().verifySignedAuthRequest(jwt, options);
    Assertions.assertEquals("my-project", identity.getAccountId());
  }

  @Test
  void mismatchedExpectedCustomHeaderFails() throws Exception {
    stubJwks();
    String jwt = signJwt(Instant.now(), "my-service");
    ValidateOptions options =
        ValidateOptions.builder().withExpectedCustomHeader("x-target-resource", "other").build();

    Assertions.assertThrows(
        InvalidArgumentException.class, () -> verifier().verifySignedAuthRequest(jwt, options));
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

  private String signJwt(Instant issuedAt, String targetResource) throws Exception {
    return signJwt(issuedAt, KID, targetResource);
  }

  private String signJwt(Instant issuedAt, String kid, String targetResource) throws Exception {
    JsonWebSignature.Header header =
        new JsonWebSignature.Header().setAlgorithm("RS256").setType("JWT").setKeyId(kid);
    JsonWebToken.Payload payload = new JsonWebToken.Payload();
    payload.setIssuer(SERVICE_ACCOUNT);
    payload.setSubject(SERVICE_ACCOUNT);
    payload.setIssuedAtTimeSeconds(issuedAt.getEpochSecond());
    payload.setExpirationTimeSeconds(issuedAt.plusSeconds(300).getEpochSecond());
    if (targetResource != null) {
      payload.set("x-target-resource", targetResource);
    }
    return JsonWebSignature.signUsingRsaSha256(
        keyPair.getPrivate(), GsonFactory.getDefaultInstance(), header, payload);
  }

  private static byte[] toUnsignedBytes(java.math.BigInteger value) {
    byte[] bytes = value.toByteArray();
    if (bytes.length > 1 && bytes[0] == 0) {
      byte[] trimmed = new byte[bytes.length - 1];
      System.arraycopy(bytes, 1, trimmed, 0, trimmed.length);
      return trimmed;
    }
    return bytes;
  }
}
