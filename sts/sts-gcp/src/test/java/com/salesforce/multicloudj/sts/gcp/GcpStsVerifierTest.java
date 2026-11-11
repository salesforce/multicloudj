package com.salesforce.multicloudj.sts.gcp;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.json.webtoken.JsonWebSignature;
import com.google.api.client.json.webtoken.JsonWebToken;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.auth.http.HttpTransportFactory;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.ValidateOptions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GcpStsVerifierTest {

  private static final String SERVICE_ACCOUNT =
      "test-sa@my-project.iam.gserviceaccount.com";
  private static final String KID = "test-key-1";

  private KeyPair keyPair;

  @BeforeEach
  void setUp() throws Exception {
    keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
  }

  @Test
  void providerId() {
    Assertions.assertEquals("gcp", new GcpStsVerifier().getProviderId());
  }

  @Test
  void returnsCallerIdentityFromValidJwt() throws Exception {
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
    String jwt = signJwt(Instant.now().minusSeconds(3600), null);

    Assertions.assertThrows(
        UnAuthorizedException.class, () -> verifier().verifySignedAuthRequest(jwt));
  }

  @Test
  void unknownKeyIdThrowsNotFound() throws Exception {
    String jwt = signJwt(Instant.now(), "other-kid", null);

    Assertions.assertThrows(
        ResourceNotFoundException.class, () -> verifier().verifySignedAuthRequest(jwt));
  }

  @Test
  void tamperedSignatureThrowsUnauthorized() throws Exception {
    String jwt = signJwt(Instant.now(), null);
    String tampered = jwt.substring(0, jwt.length() - 4) + "AAAA";

    Assertions.assertThrows(
        UnAuthorizedException.class, () -> verifier().verifySignedAuthRequest(tampered));
  }

  @Test
  void matchingExpectedCustomHeaderPasses() throws Exception {
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
    String jwt = signJwt(Instant.now(), "my-service");
    ValidateOptions options =
        ValidateOptions.builder().withExpectedCustomHeader("x-target-resource", "other").build();

    Assertions.assertThrows(
        InvalidArgumentException.class, () -> verifier().verifySignedAuthRequest(jwt, options));
  }

  @Test
  void malformedJwtThrowsInvalidArgument() {
    Assertions.assertThrows(
        InvalidArgumentException.class,
        () -> verifier().verifySignedAuthRequest("not-a-jwt-at-all"));
  }

  @Test
  void unsupportedAlgorithmThrowsInvalidArgument() {
    String token = manualToken("{\"alg\":\"HS256\",\"typ\":\"JWT\"}", "{\"iss\":\"x\"}");
    Assertions.assertThrows(
        InvalidArgumentException.class, () -> verifier().verifySignedAuthRequest(token));
  }

  @Test
  void missingAlgorithmThrowsInvalidArgument() {
    String token = manualToken("{\"typ\":\"JWT\"}", "{\"iss\":\"x\"}");
    Assertions.assertThrows(
        InvalidArgumentException.class, () -> verifier().verifySignedAuthRequest(token));
  }

  @Test
  void missingIssuerThrowsInvalidArgument() throws Exception {
    String token = signJwtWithoutIssuer(Instant.now());
    Assertions.assertThrows(
        InvalidArgumentException.class, () -> verifier().verifySignedAuthRequest(token));
  }

  @Test
  void jwksFetchFailureThrowsUnknown() throws Exception {
    // The JWKS endpoint returns 404 and the fetch fails.
    String jwt = signJwt(Instant.now(), null);
    GcpStsVerifier verifier = verifierReturning(404, "");
    Assertions.assertThrows(
        UnknownException.class, () -> verifier.verifySignedAuthRequest(jwt));
  }

  @Test
  void issuedInFutureThrowsUnauthorized() throws Exception {
    String jwt = signJwt(Instant.now().plusSeconds(3600), null);
    Assertions.assertThrows(
        UnAuthorizedException.class, () -> verifier().verifySignedAuthRequest(jwt));
  }

  @Test
  void issuerWithoutServiceAccountDomainYieldsEmptyAccountId() throws Exception {
    String jwt = signJwtWithIssuer(Instant.now(), "plain-issuer-no-at-sign");

    CallerIdentity identity = verifier().verifySignedAuthRequest(jwt);
    Assertions.assertEquals("", identity.getAccountId());
    Assertions.assertEquals("plain-issuer-no-at-sign", identity.getUserId());
  }

  @Test
  void jwksWithoutMatchingKeyThrowsNotFound() throws Exception {
    // JWKS body has a non-RSA key and a key missing modulus - both skipped, none match.
    String jwks =
        "{\"keys\":[{\"kty\":\"oct\",\"kid\":\"" + KID + "\"},"
            + "{\"kty\":\"RSA\",\"kid\":\"" + KID + "\",\"e\":\"AQAB\"}]}";
    String jwt = signJwt(Instant.now(), null);

    GcpStsVerifier verifier = verifierReturning(200, jwks);
    Assertions.assertThrows(
        ResourceNotFoundException.class, () -> verifier.verifySignedAuthRequest(jwt));
  }

  @Test
  void cachedKeysReusedOnSecondCall() throws Exception {
    AtomicInteger fetchCount = new AtomicInteger();
    HttpTransportFactory factory = () -> countingTransport(fetchCount, jwksBody());
    GcpStsVerifier verifier = new GcpStsVerifier.Builder().build(factory);
    String jwt1 = signJwt(Instant.now(), null);
    String jwt2 = signJwt(Instant.now(), null);

    Assertions.assertEquals(SERVICE_ACCOUNT, verifier.verifySignedAuthRequest(jwt1).getUserId());
    Assertions.assertEquals(SERVICE_ACCOUNT, verifier.verifySignedAuthRequest(jwt2).getUserId());
    Assertions.assertEquals(1, fetchCount.get());
  }

  @Test
  void mapExceptionWrapsAsUnknown() {
    SubstrateSdkException mapped =
        new GcpStsVerifier().mapException(new RuntimeException("boom"));
    Assertions.assertInstanceOf(UnknownException.class, mapped);
  }

  @Test
  void builderReturnsGcpBuilder() {
    Assertions.assertEquals("gcp", new GcpStsVerifier().builder().build().getProviderId());
  }

  @Test
  void verifiesThroughSuppliedTransportFactory() throws Exception {
    String jwt = signJwt(Instant.now(), null);
    HttpTransportFactory factory = () -> transportReturning(200, jwksBody());
    GcpStsVerifier verifier = new GcpStsVerifier.Builder().build(factory);

    CallerIdentity identity = verifier.verifySignedAuthRequest(jwt);
    Assertions.assertEquals(SERVICE_ACCOUNT, identity.getUserId());
  }

  @Test
  void missingExpectedCustomHeaderFails() throws Exception {
    String jwt = signJwt(Instant.now(), null);
    ValidateOptions options =
        ValidateOptions.builder().withExpectedCustomHeader("x-absent-header", "value").build();

    Assertions.assertThrows(
        InvalidArgumentException.class, () -> verifier().verifySignedAuthRequest(jwt, options));
  }

  @Test
  void issuerWithNonServiceAccountDomainYieldsEmptyAccountId() throws Exception {
    String jwt = signJwtWithIssuer(Instant.now(), "user@example.com");

    CallerIdentity identity = verifier().verifySignedAuthRequest(jwt);
    Assertions.assertEquals("", identity.getAccountId());
    Assertions.assertEquals("user@example.com", identity.getUserId());
  }

  @Test
  void jwksWithoutKeysArrayThrowsNotFound() throws Exception {
    String jwt = signJwt(Instant.now(), null);
    GcpStsVerifier verifier = verifierReturning(200, "{\"metadata\":\"no keys here\"}");

    Assertions.assertThrows(
        ResourceNotFoundException.class, () -> verifier.verifySignedAuthRequest(jwt));
  }

  @Test
  void jwksWithNonObjectKeyEntryThrowsNotFound() throws Exception {
    String jwt = signJwt(Instant.now(), null);
    GcpStsVerifier verifier = verifierReturning(200, "{\"keys\":[\"not-an-object\"]}");

    Assertions.assertThrows(
        ResourceNotFoundException.class, () -> verifier.verifySignedAuthRequest(jwt));
  }

  @Test
  void jwksWithMalformedModulusThrowsUnknown() throws Exception {
    String jwt = signJwt(Instant.now(), null);
    GcpStsVerifier verifier =
        verifierReturning(
            200,
            "{\"keys\":[{\"kty\":\"RSA\",\"kid\":\"" + KID + "\",\"n\":\"@@@\",\"e\":\"AQAB\"}]}");

    Assertions.assertThrows(
        UnknownException.class, () -> verifier.verifySignedAuthRequest(jwt));
  }

  /** Builds a verifier whose JWKS fetch returns a 200 with the current key pair's JWKS document. */
  private GcpStsVerifier verifier() {
    return verifierReturning(200, jwksBody());
  }

  private static GcpStsVerifier verifierReturning(int status, String body) {
    HttpTransportFactory factory = () -> transportReturning(status, body);
    return new GcpStsVerifier.Builder().build(factory);
  }

  private static HttpTransport transportReturning(int status, String body) {
    return new MockHttpTransport() {
      @Override
      public MockLowLevelHttpRequest buildRequest(String method, String url) {
        return new MockLowLevelHttpRequest() {
          @Override
          public MockLowLevelHttpResponse execute() {
            MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
            response.setStatusCode(status);
            response.setContentType("application/json");
            response.setContent(body);
            return response;
          }
        };
      }
    };
  }

  private static HttpTransport countingTransport(AtomicInteger counter, String body) {
    return new MockHttpTransport() {
      @Override
      public MockLowLevelHttpRequest buildRequest(String method, String url) {
        counter.incrementAndGet();
        return new MockLowLevelHttpRequest() {
          @Override
          public MockLowLevelHttpResponse execute() {
            MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
            response.setStatusCode(200);
            response.setContentType("application/json");
            response.setContent(body);
            return response;
          }
        };
      }
    };
  }

  private static String manualToken(String headerJson, String payloadJson) {
    Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
    return encoder.encodeToString(headerJson.getBytes(StandardCharsets.UTF_8))
        + "."
        + encoder.encodeToString(payloadJson.getBytes(StandardCharsets.UTF_8))
        + "."
        + encoder.encodeToString("sig".getBytes(StandardCharsets.UTF_8));
  }

  private String signJwtWithoutIssuer(Instant issuedAt) throws Exception {
    JsonWebSignature.Header header =
        new JsonWebSignature.Header().setAlgorithm("RS256").setType("JWT").setKeyId(KID);
    JsonWebToken.Payload payload = new JsonWebToken.Payload();
    payload.setIssuedAtTimeSeconds(issuedAt.getEpochSecond());
    payload.setExpirationTimeSeconds(issuedAt.plusSeconds(300).getEpochSecond());
    return JsonWebSignature.signUsingRsaSha256(
        keyPair.getPrivate(), GsonFactory.getDefaultInstance(), header, payload);
  }

  private String signJwtWithIssuer(Instant issuedAt, String issuer) throws Exception {
    JsonWebSignature.Header header =
        new JsonWebSignature.Header().setAlgorithm("RS256").setType("JWT").setKeyId(KID);
    JsonWebToken.Payload payload = new JsonWebToken.Payload();
    payload.setIssuer(issuer);
    payload.setSubject(issuer);
    payload.setIssuedAtTimeSeconds(issuedAt.getEpochSecond());
    payload.setExpirationTimeSeconds(issuedAt.plusSeconds(300).getEpochSecond());
    return JsonWebSignature.signUsingRsaSha256(
        keyPair.getPrivate(), GsonFactory.getDefaultInstance(), header, payload);
  }

  private String jwksBody() {
    RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
    Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
    String n = encoder.encodeToString(toUnsignedBytes(publicKey.getModulus()));
    String e = encoder.encodeToString(toUnsignedBytes(publicKey.getPublicExponent()));
    return "{\"keys\":[{\"kty\":\"RSA\",\"alg\":\"RS256\",\"use\":\"sig\",\"kid\":\""
        + KID
        + "\",\"n\":\""
        + n
        + "\",\"e\":\""
        + e
        + "\"}]}";
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
