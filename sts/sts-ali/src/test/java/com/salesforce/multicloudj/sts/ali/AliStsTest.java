package com.salesforce.multicloudj.sts.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.http.HttpClientConfig;
import com.aliyuncs.profile.IClientProfile;
import com.aliyuncs.sts.model.v20150401.AssumeRoleRequest;
import com.aliyuncs.sts.model.v20150401.AssumeRoleResponse;
import com.aliyuncs.sts.model.v20150401.AssumeRoleWithOIDCRequest;
import com.aliyuncs.sts.model.v20150401.AssumeRoleWithOIDCResponse;
import com.aliyuncs.sts.model.v20150401.GetCallerIdentityRequest;
import com.aliyuncs.sts.model.v20150401.GetCallerIdentityResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.CredentialScope;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.net.URI;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class AliStsTest {

  private static DefaultAcsClient mockStsClient;

  @BeforeAll
  public static void setUp() throws ClientException {
    mockStsClient = Mockito.mock(DefaultAcsClient.class);
    AssumeRoleResponse.Credentials credentials = new AssumeRoleResponse.Credentials();
    credentials.setAccessKeyId("testKeyId");
    credentials.setAccessKeySecret("testSecret");
    credentials.setSecurityToken("testToken");

    AssumeRoleResponse mockResponse = new AssumeRoleResponse();
    mockResponse.setCredentials(credentials);
    GetCallerIdentityResponse callerIdentityResponse = new GetCallerIdentityResponse();
    callerIdentityResponse.setPrincipalId("testUserId");
    callerIdentityResponse.setArn("testResourceName");
    callerIdentityResponse.setAccountId("testAccountId");
    Mockito.when(mockStsClient.getAcsResponse(any(AssumeRoleRequest.class)))
        .thenReturn(mockResponse);
    Mockito.when(mockStsClient.getAcsResponse(any(GetCallerIdentityRequest.class)))
        .thenReturn(callerIdentityResponse);

    AssumeRoleWithOIDCResponse.Credentials oidcCredentials =
        new AssumeRoleWithOIDCResponse.Credentials();
    oidcCredentials.setAccessKeyId("oidcKeyId");
    oidcCredentials.setAccessKeySecret("oidcSecret");
    oidcCredentials.setSecurityToken("oidcToken");
    AssumeRoleWithOIDCResponse oidcResponse = new AssumeRoleWithOIDCResponse();
    oidcResponse.setCredentials(oidcCredentials);
    Mockito.when(mockStsClient.getAcsResponse(any(AssumeRoleWithOIDCRequest.class)))
        .thenReturn(oidcResponse);
  }

  @Test
  public void testAliStsInitialization() {
    AliSts sts = new AliSts();
    Assertions.assertEquals("ali", sts.getProviderId());
  }

  @Test
  public void testAliStsClientProfileBadEndpoint() {
    URI badEndpoint = URI.create("https://a.bad.endpoint");

    // Expect InvalidArgumentException to be thrown
    InvalidArgumentException exception =
        assertThrows(InvalidArgumentException.class, () -> AliSts.getClientProfile(badEndpoint));

    assertEquals("The endpoint is invalid", exception.getMessage());

    URI anotherBadEndpoint = URI.create("https://a.endpoint");

    // Expect InvalidArgumentException to be thrown
    exception =
        assertThrows(
            InvalidArgumentException.class, () -> AliSts.getClientProfile(anotherBadEndpoint));

    assertEquals("The endpoint is invalid", exception.getMessage());
  }

  @Test
  public void testAliStsClientProfileGoodEndpoint() {
    URI goodEndpoint = URI.create("https://a.good.endpoint.com");

    IClientProfile profile = AliSts.getClientProfile(goodEndpoint);
    assertEquals("good", profile.getRegionId());

    goodEndpoint = URI.create("https://a-vpc.good.endpoint.com");

    profile = AliSts.getClientProfile(goodEndpoint);
    assertEquals("good", profile.getRegionId());
  }

  @Test
  public void testAssumedRoleSts() {
    AliSts sts = new AliSts().builder().build(mockStsClient);
    AssumedRoleRequest request =
        AssumedRoleRequest.newBuilder().withRole("testRole").withSessionName("testSession").build();
    StsCredentials credentials = sts.assumeRole(request);
    Assertions.assertEquals("testKeyId", credentials.getAccessKeyId());
    Assertions.assertEquals("testSecret", credentials.getAccessKeySecret());
    Assertions.assertEquals("testToken", credentials.getSecurityToken());
  }

  @Test
  public void testAssumeRoleWithWebIdentity_explicitProviderArn() throws ClientException {
    // Use a dedicated mock so the captured request is unambiguous (the class-level mock is shared).
    DefaultAcsClient client = Mockito.mock(DefaultAcsClient.class);
    AssumeRoleWithOIDCResponse.Credentials creds = new AssumeRoleWithOIDCResponse.Credentials();
    creds.setAccessKeyId("oidcKeyId");
    creds.setAccessKeySecret("oidcSecret");
    creds.setSecurityToken("oidcToken");
    AssumeRoleWithOIDCResponse response = new AssumeRoleWithOIDCResponse();
    response.setCredentials(creds);
    Mockito.when(client.getAcsResponse(any(AssumeRoleWithOIDCRequest.class))).thenReturn(response);

    AliSts sts = new AliSts().builder().build(client);
    AssumeRoleWebIdentityRequest request =
        AssumeRoleWebIdentityRequest.builder()
            .role("testRole")
            .webIdentityToken("testOidcToken")
            .sessionName("testSession")
            .webIdentityProviderArn("acs:ram::123:oidc-provider/test")
            .build();

    StsCredentials credentials = sts.assumeRoleWithWebIdentity(request);

    Assertions.assertEquals("oidcKeyId", credentials.getAccessKeyId());
    Assertions.assertEquals("oidcSecret", credentials.getAccessKeySecret());
    Assertions.assertEquals("oidcToken", credentials.getSecurityToken());

    // Verify the cloud-agnostic request was wired to the Aliyun OIDC request correctly.
    ArgumentCaptor<AssumeRoleWithOIDCRequest> captor =
        ArgumentCaptor.forClass(AssumeRoleWithOIDCRequest.class);
    Mockito.verify(client).getAcsResponse(captor.capture());
    AssumeRoleWithOIDCRequest sent = captor.getValue();
    Assertions.assertEquals("testRole", sent.getRoleArn());
    Assertions.assertEquals("testOidcToken", sent.getOIDCToken());
    Assertions.assertEquals("testSession", sent.getRoleSessionName());
    Assertions.assertEquals("acs:ram::123:oidc-provider/test", sent.getOIDCProviderArn());
  }

  @Test
  public void testAssumeRoleWithWebIdentity_missingProviderArn_throws() {
    AliSts sts = new AliSts().builder().build(mockStsClient);
    AssumeRoleWebIdentityRequest request =
        AssumeRoleWebIdentityRequest.builder()
            .role("testRole")
            .webIdentityToken("testOidcToken")
            .sessionName("testSession")
            .build();

    // No provider ARN on the request. Guarded so the assertion is only made when the env-var
    // fallback is also absent (keeps the test correct in any environment).
    if (System.getenv(AliSts.OIDC_PROVIDER_ARN_ENV) == null) {
      assertThrows(InvalidArgumentException.class, () -> sts.assumeRoleWithWebIdentity(request));
    }
  }

  @Test
  public void testGetCallerIdentitySts() {
    AliSts sts = new AliSts().builder().build(mockStsClient);
    CallerIdentity identity =
        sts.getCallerIdentity(
            com.salesforce.multicloudj.sts.model.GetCallerIdentityRequest.builder().build());
    Assertions.assertEquals("testAccountId", identity.getAccountId());
    Assertions.assertEquals("testUserId", identity.getUserId());
    Assertions.assertEquals("testResourceName", identity.getCloudResourceName());
  }

  @Test
  public void testRuntimeExceptionType() {
    AliSts sts = new AliSts().builder().build(mockStsClient);
    Assertions.assertInstanceOf(UnknownException.class, sts.mapException(new RuntimeException()));
  }

  @Test
  public void testBuildHttpClientConfigWithExplicitProxyEndpoint() {
    URI proxyEndpoint = URI.create("http://proxy.example.com:8080");
    AliSts.Builder builder =
        new AliSts().builder().withRegion("cn-hangzhou").withProxyEndpoint(proxyEndpoint);

    HttpClientConfig config = AliSts.buildHttpClientConfig(builder);
    Assertions.assertNotNull(config);
    Assertions.assertEquals("http://proxy.example.com:8080", config.getHttpProxy());
    Assertions.assertEquals("http://proxy.example.com:8080", config.getHttpsProxy());
  }

  @Test
  public void testBuildHttpClientConfigWithNullProxyEndpoint() {
    AliSts.Builder builder = new AliSts().builder().withRegion("cn-hangzhou");

    HttpClientConfig config = AliSts.buildHttpClientConfig(builder);
    Assertions.assertNotNull(config);
    Assertions.assertNull(config.getHttpProxy());
    Assertions.assertNull(config.getHttpsProxy());
  }

  @Test
  public void testBuildHttpClientConfigVerifiesBothHttpAndHttps() {
    URI proxyEndpoint = URI.create("http://proxy.example.com:8080");
    AliSts.Builder builder =
        new AliSts().builder().withRegion("cn-hangzhou").withProxyEndpoint(proxyEndpoint);

    HttpClientConfig config = AliSts.buildHttpClientConfig(builder);
    String expectedUrl = "http://proxy.example.com:8080";
    Assertions.assertEquals(expectedUrl, config.getHttpProxy());
    Assertions.assertEquals(expectedUrl, config.getHttpsProxy());
  }

  // Captures the AssumeRoleRequest issued for the given cloud-agnostic request, using a dedicated
  // mock so the captured request is unambiguous.
  private static AssumeRoleRequest capturedAssumeRoleRequest(AssumedRoleRequest request)
      throws ClientException {
    DefaultAcsClient client = Mockito.mock(DefaultAcsClient.class);
    AssumeRoleResponse.Credentials creds = new AssumeRoleResponse.Credentials();
    creds.setAccessKeyId("k");
    creds.setAccessKeySecret("s");
    creds.setSecurityToken("t");
    AssumeRoleResponse response = new AssumeRoleResponse();
    response.setCredentials(creds);
    Mockito.when(client.getAcsResponse(any(AssumeRoleRequest.class))).thenReturn(response);

    AliSts sts = new AliSts().builder().build(client);
    sts.assumeRole(request);

    ArgumentCaptor<AssumeRoleRequest> captor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Mockito.verify(client).getAcsResponse(captor.capture());
    return captor.getValue();
  }

  @Test
  public void testAssumeRoleWithCredentialScope() throws Exception {
    // The Capstone shape: read-write access downscoped to a bucket + key prefix. Object operations
    // and the list operation must land in separate RAM statements with different resource forms.
    CredentialScope credentialScope =
        CredentialScope.builder()
            .rule(
                CredentialScope.ScopeRule.builder()
                    .availableResource("storage://my-bucket")
                    .availablePermission("storage:GetObject")
                    .availablePermission("storage:PutObject")
                    .availablePermission("storage:ListBucket")
                    .availabilityCondition(
                        CredentialScope.AvailabilityCondition.builder()
                            .resourcePrefix("storage://my-bucket/documents/")
                            .title("Limit to documents folder")
                            .description("Only allow access to the documents folder")
                            .build())
                    .build())
            .build();

    AssumedRoleRequest request =
        AssumedRoleRequest.newBuilder()
            .withRole("acs:ram::123456789:role/my-bucket-ro")
            .withSessionName("my-session")
            .withCredentialScope(credentialScope)
            .build();

    AssumeRoleRequest captured = capturedAssumeRoleRequest(request);

    String expectedPolicy =
        "{\"Version\":\"1\",\"Statement\":["
            + "{\"Effect\":\"Allow\",\"Action\":[\"oss:GetObject\",\"oss:PutObject\"],"
            + "\"Resource\":\"acs:oss:*:*:my-bucket/documents/*\"},"
            + "{\"Effect\":\"Allow\",\"Action\":[\"oss:ListObjects\"],"
            + "\"Resource\":\"acs:oss:*:*:my-bucket\","
            + "\"Condition\":{\"StringLike\":{\"oss:Prefix\":[\"documents/\",\"documents/*\"]}}}]}";
    assertJsonEquals(expectedPolicy, captured.getPolicy());
  }

  @Test
  public void testAssumeRoleWithCredentialScopeNoPrefix() throws Exception {
    // No availability condition -> object ops cover the whole bucket, list statement has no
    // prefix condition.
    CredentialScope credentialScope =
        CredentialScope.builder()
            .rule(
                CredentialScope.ScopeRule.builder()
                    .availableResource("storage://my-bucket")
                    .availablePermission("storage:GetObject")
                    .availablePermission("storage:ListBucket")
                    .build())
            .build();

    AssumedRoleRequest request =
        AssumedRoleRequest.newBuilder()
            .withRole("acs:ram::123456789:role/my-bucket-ro")
            .withSessionName("my-session")
            .withCredentialScope(credentialScope)
            .build();

    AssumeRoleRequest captured = capturedAssumeRoleRequest(request);

    String expectedPolicy =
        "{\"Version\":\"1\",\"Statement\":["
            + "{\"Effect\":\"Allow\",\"Action\":[\"oss:GetObject\"],"
            + "\"Resource\":\"acs:oss:*:*:my-bucket/*\"},"
            + "{\"Effect\":\"Allow\",\"Action\":[\"oss:ListObjects\"],"
            + "\"Resource\":\"acs:oss:*:*:my-bucket\"}]}";
    assertJsonEquals(expectedPolicy, captured.getPolicy());
  }

  @Test
  public void testAssumeRoleWithoutCredentialScopeSetsNoPolicy() throws Exception {
    AssumedRoleRequest request =
        AssumedRoleRequest.newBuilder().withRole("testRole").withSessionName("testSession").build();

    AssumeRoleRequest captured = capturedAssumeRoleRequest(request);

    Assertions.assertNull(captured.getPolicy(), "no credential scope should mean no inline policy");
  }

  @Test
  public void testAssumeRoleWithMismatchedPrefixBucketThrows() {
    // A resourcePrefix that names a different (or missing) bucket than availableResource must be
    // rejected, not silently ignored -- ignoring it would widen the grant to the whole bucket.
    CredentialScope credentialScope =
        CredentialScope.builder()
            .rule(
                CredentialScope.ScopeRule.builder()
                    .availableResource("storage://my-bucket")
                    .availablePermission("storage:GetObject")
                    .availabilityCondition(
                        CredentialScope.AvailabilityCondition.builder()
                            .resourcePrefix("storage://other-bucket/documents/")
                            .build())
                    .build())
            .build();

    AssumedRoleRequest request =
        AssumedRoleRequest.newBuilder()
            .withRole("acs:ram::123456789:role/my-bucket-ro")
            .withSessionName("my-session")
            .withCredentialScope(credentialScope)
            .build();

    AliSts sts = new AliSts().builder().build(mockStsClient);
    assertThrows(InvalidArgumentException.class, () -> sts.assumeRole(request));
  }

  @Test
  public void testAssumeRoleWithEmptyCredentialScopeThrows() {
    // A credential scope was supplied but has no rules -> it would downscope to an empty policy.
    // Reject it up front rather than sending a deny-all policy that fails opaquely at request time.
    CredentialScope emptyScope = CredentialScope.builder().build();
    AssumedRoleRequest request =
        AssumedRoleRequest.newBuilder()
            .withRole("acs:ram::123456789:role/my-bucket-ro")
            .withSessionName("my-session")
            .withCredentialScope(emptyScope)
            .build();

    AliSts sts = new AliSts().builder().build(mockStsClient);
    assertThrows(InvalidArgumentException.class, () -> sts.assumeRole(request));
  }

  // Compares two JSON strings for structural equality, ignoring object key ordering.
  private static void assertJsonEquals(String expected, String actual) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    Assertions.assertEquals(mapper.readTree(expected), mapper.readTree(actual));
  }
}
