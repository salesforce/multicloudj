package com.salesforce.multicloudj.sts.gcp;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.json.webtoken.JsonWebSignature;
import com.google.api.client.json.webtoken.JsonWebToken;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.iam.credentials.v1.IamCredentialsSettings;
import com.google.cloud.iam.credentials.v1.SignJwtResponse;
import com.salesforce.multicloudj.common.exceptions.DeadlineExceededException;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.SignOptions;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import com.salesforce.multicloudj.sts.model.ValidateOptions;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class GcpStsUtilitiesTest {

  private static final String SERVICE_ACCOUNT = "test-sa@my-project.iam.gserviceaccount.com";
  private static final String KID = "test-key-1";

  private KeyPair keyPair;

  @BeforeEach
  void setUp() throws Exception {
    keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
  }

  @Test
  void providerId() {
    Assertions.assertEquals("gcp", new GcpStsUtilities().getProviderId());
  }

  @Test
  void builderAndDefaultBuildProduceGcpUtilities() {
    Assertions.assertEquals("gcp", new GcpStsUtilities().builder().build().getProviderId());
    Assertions.assertEquals("gcp", new GcpStsUtilities.Builder().build().getProviderId());
  }

  @Test
  void signsThroughIamCredentialsClient() throws Exception {
    GoogleCredentials source = Mockito.mock(GoogleCredentials.class);
    Mockito.when(source.createScopedRequired()).thenReturn(true);
    Mockito.when(source.createScoped(Mockito.anyCollection())).thenReturn(source);
    IamCredentialsClient client = Mockito.mock(IamCredentialsClient.class);
    SignJwtResponse response =
        SignJwtResponse.newBuilder().setSignedJwt("signed.jwt.token").build();
    Mockito.when(client.signJwt(Mockito.anyString(), Mockito.anyList(), Mockito.anyString()))
        .thenReturn(response);

    // No injected signer, so the production IAM-backed path runs; the email override skips
    // auto-detection so this isolates the IAM signing seam.
    GcpStsUtilities utilities =
        new GcpStsUtilities.Builder().build((GcpStsUtilities.JwtSigner) null, SERVICE_ACCOUNT);
    try (MockedStatic<GoogleCredentials> mockedGoogleCreds =
            Mockito.mockStatic(GoogleCredentials.class);
        MockedStatic<IamCredentialsClient> mockedClient =
            Mockito.mockStatic(IamCredentialsClient.class)) {
      mockedGoogleCreds.when(GoogleCredentials::getApplicationDefault).thenReturn(source);
      mockedClient
          .when(() -> IamCredentialsClient.create(Mockito.any(IamCredentialsSettings.class)))
          .thenReturn(client);

      SignedAuthRequest signed =
          utilities.newCloudNativeAuthSignedRequest(null, SignOptions.builder().build());
      Assertions.assertEquals("signed.jwt.token", signed.getSignedIdentity());
    }
  }

  @Test
  void signsThroughIamCredentialsClientImpersonatingRole() throws Exception {
    String role = "impersonated-sa@my-project.iam.gserviceaccount.com";
    GoogleCredentials source = Mockito.mock(GoogleCredentials.class);
    Mockito.when(source.createScopedRequired()).thenReturn(false);
    IamCredentialsClient client = Mockito.mock(IamCredentialsClient.class);
    SignJwtResponse response =
        SignJwtResponse.newBuilder().setSignedJwt("impersonated.jwt").build();
    Mockito.when(client.signJwt(Mockito.anyString(), Mockito.anyList(), Mockito.anyString()))
        .thenReturn(response);

    GcpStsUtilities.Builder builder = new GcpStsUtilities.Builder();
    builder.withCredentialsOverrider(
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE).withRole(role).build());
    GcpStsUtilities utilities = builder.build((GcpStsUtilities.JwtSigner) null, null);
    try (MockedStatic<GoogleCredentials> mockedGoogleCreds =
            Mockito.mockStatic(GoogleCredentials.class);
        MockedStatic<IamCredentialsClient> mockedClient =
            Mockito.mockStatic(IamCredentialsClient.class)) {
      mockedGoogleCreds.when(GoogleCredentials::getApplicationDefault).thenReturn(source);
      mockedClient
          .when(() -> IamCredentialsClient.create(Mockito.any(IamCredentialsSettings.class)))
          .thenReturn(client);

      SignedAuthRequest signed =
          utilities.newCloudNativeAuthSignedRequest(null, SignOptions.builder().build());
      Assertions.assertEquals("impersonated.jwt", signed.getSignedIdentity());
    }
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

  @Test
  void mapExceptionMapsApiExceptionStatusCodes() {
    GcpStsUtilities utilities = new GcpStsUtilities();
    assertExceptionMapping(utilities, StatusCode.Code.CANCELLED, UnknownException.class);
    assertExceptionMapping(utilities, StatusCode.Code.UNKNOWN, UnknownException.class);
    assertExceptionMapping(
        utilities, StatusCode.Code.INVALID_ARGUMENT, InvalidArgumentException.class);
    assertExceptionMapping(
        utilities, StatusCode.Code.DEADLINE_EXCEEDED, DeadlineExceededException.class);
    assertExceptionMapping(utilities, StatusCode.Code.NOT_FOUND, ResourceNotFoundException.class);
    assertExceptionMapping(
        utilities, StatusCode.Code.ALREADY_EXISTS, ResourceAlreadyExistsException.class);
    assertExceptionMapping(
        utilities, StatusCode.Code.PERMISSION_DENIED, UnAuthorizedException.class);
    assertExceptionMapping(
        utilities, StatusCode.Code.RESOURCE_EXHAUSTED, ResourceExhaustedException.class);
    assertExceptionMapping(
        utilities, StatusCode.Code.FAILED_PRECONDITION, FailedPreconditionException.class);
    assertExceptionMapping(utilities, StatusCode.Code.ABORTED, DeadlineExceededException.class);
    assertExceptionMapping(
        utilities, StatusCode.Code.OUT_OF_RANGE, InvalidArgumentException.class);
    assertExceptionMapping(
        utilities, StatusCode.Code.UNIMPLEMENTED, UnSupportedOperationException.class);
    assertExceptionMapping(utilities, StatusCode.Code.INTERNAL, UnknownException.class);
    assertExceptionMapping(utilities, StatusCode.Code.UNAVAILABLE, UnknownException.class);
    assertExceptionMapping(utilities, StatusCode.Code.DATA_LOSS, UnknownException.class);
    assertExceptionMapping(
        utilities, StatusCode.Code.UNAUTHENTICATED, UnAuthorizedException.class);
  }

  @Test
  void mapExceptionWithNullStatusCodeWrapsAsUnknown() {
    ApiException apiException = Mockito.mock(ApiException.class);
    Mockito.when(apiException.getStatusCode()).thenReturn(null);
    Assertions.assertInstanceOf(
        UnknownException.class, new GcpStsUtilities().mapException(apiException));
  }

  @Test
  void credentialsOverriderRoleIsUsedAsSigningServiceAccount() throws Exception {
    String role = "impersonated-sa@my-project.iam.gserviceaccount.com";
    GcpStsUtilities.Builder builder = new GcpStsUtilities.Builder();
    builder.withCredentialsOverrider(
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE).withRole(role).build());
    GcpStsUtilities utilities = builder.build(this::signAnyPayload, null);

    SignedAuthRequest signed =
        utilities.newCloudNativeAuthSignedRequest(null, SignOptions.builder().build());
    JsonWebSignature parsed =
        JsonWebSignature.parse(GsonFactory.getDefaultInstance(), signed.getSignedIdentity());
    Assertions.assertEquals(role, parsed.getPayload().getIssuer());
    Assertions.assertEquals(role, parsed.getPayload().getSubject());
  }

  @Test
  void resolvesServiceAccountFromServiceAccountCredentials() throws Exception {
    ServiceAccountCredentials credentials = Mockito.mock(ServiceAccountCredentials.class);
    Mockito.when(credentials.getClientEmail()).thenReturn(SERVICE_ACCOUNT);
    Assertions.assertEquals(SERVICE_ACCOUNT, resolvedIssuerWithApplicationDefault(credentials));
  }

  @Test
  void resolvesServiceAccountFromImpersonatedCredentials() throws Exception {
    ImpersonatedCredentials credentials = Mockito.mock(ImpersonatedCredentials.class);
    Mockito.when(credentials.getAccount()).thenReturn(SERVICE_ACCOUNT);
    Assertions.assertEquals(SERVICE_ACCOUNT, resolvedIssuerWithApplicationDefault(credentials));
  }

  @Test
  void resolvesServiceAccountFromComputeEngineCredentials() throws Exception {
    ComputeEngineCredentials credentials = Mockito.mock(ComputeEngineCredentials.class);
    Mockito.when(credentials.getAccount()).thenReturn(SERVICE_ACCOUNT);
    Assertions.assertEquals(SERVICE_ACCOUNT, resolvedIssuerWithApplicationDefault(credentials));
  }

  /**
   * Signs an identity while Application Default Credentials resolve to {@code credentials}, and
   * returns the {@code iss} claim of the produced JWT (i.e. the auto-detected service account).
   */
  private String resolvedIssuerWithApplicationDefault(GoogleCredentials credentials)
      throws Exception {
    GcpStsUtilities utilities =
        new GcpStsUtilities.Builder().build(this::signAnyPayload, null);
    try (MockedStatic<GoogleCredentials> mockedGoogleCreds =
        Mockito.mockStatic(GoogleCredentials.class)) {
      mockedGoogleCreds
          .when(GoogleCredentials::getApplicationDefault)
          .thenReturn(credentials);
      SignedAuthRequest signed =
          utilities.newCloudNativeAuthSignedRequest(null, SignOptions.builder().build());
      return JsonWebSignature.parse(GsonFactory.getDefaultInstance(), signed.getSignedIdentity())
          .getPayload()
          .getIssuer();
    }
  }

  private void assertExceptionMapping(
      GcpStsUtilities utilities,
      StatusCode.Code statusCode,
      Class<? extends SubstrateSdkException> expected) {
    ApiException apiException = Mockito.mock(ApiException.class);
    StatusCode mockStatusCode = Mockito.mock(StatusCode.class);
    Mockito.when(apiException.getStatusCode()).thenReturn(mockStatusCode);
    Mockito.when(mockStatusCode.getCode()).thenReturn(statusCode);
    Assertions.assertInstanceOf(expected, utilities.mapException(apiException));
  }

  /** Builds a signer whose signing seam signs the claims payload with the test RSA key. */
  private GcpStsUtilities signer() {
    return new GcpStsUtilities.Builder().build(this::signPayload, SERVICE_ACCOUNT);
  }

  /** Signs the claims for any service account without asserting a specific resource name. */
  private String signAnyPayload(String serviceAccountName, String payload)
      throws java.io.IOException {
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

  /** Builds a verifier whose JWKS fetch returns the current key pair's public key document. */
  private GcpStsVerifier verifier() {
    HttpTransportFactory factory = () -> transportReturning(jwksBody());
    return new GcpStsVerifier.Builder().build(factory);
  }

  private static HttpTransport transportReturning(String body) {
    return new MockHttpTransport() {
      @Override
      public MockLowLevelHttpRequest buildRequest(String method, String url) {
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
