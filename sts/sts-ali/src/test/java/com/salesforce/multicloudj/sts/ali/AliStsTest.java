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
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
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
}
