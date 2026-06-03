package com.salesforce.multicloudj.sts.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.profile.IClientProfile;
import com.aliyuncs.sts.model.v20150401.AssumeRoleRequest;
import com.aliyuncs.sts.model.v20150401.AssumeRoleResponse;
import com.aliyuncs.sts.model.v20150401.GetCallerIdentityRequest;
import com.aliyuncs.sts.model.v20150401.GetCallerIdentityResponse;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.net.URI;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
    Assertions.assertEquals(UnknownException.class, sts.getException(new RuntimeException()));
  }

  @Test
  public void testBuilderWithProxyEndpoint() {
    java.net.URI proxyEndpoint = java.net.URI.create("http://proxy.example.com:8080");
    AliSts.Builder builder =
        new AliSts().builder().withRegion("cn-hangzhou").withProxyEndpoint(proxyEndpoint);

    Assertions.assertEquals(proxyEndpoint, builder.getProxyEndpoint());
  }

  @Test
  public void testBuilderWithUseSystemPropertyProxyValues() {
    AliSts.Builder builder =
        new AliSts().builder().withRegion("cn-hangzhou").withUseSystemPropertyProxyValues(true);

    Assertions.assertEquals(Boolean.TRUE, builder.getUseSystemPropertyProxyValues());
  }

  @Test
  public void testBuilderWithUseEnvironmentVariableProxyValues() {
    AliSts.Builder builder =
        new AliSts()
            .builder()
            .withRegion("cn-hangzhou")
            .withUseEnvironmentVariableProxyValues(true);

    Assertions.assertEquals(Boolean.TRUE, builder.getUseEnvironmentVariableProxyValues());
  }

  @Test
  public void testBuilderWithAllProxySettings() {
    java.net.URI proxyEndpoint = java.net.URI.create("http://proxy.example.com:8080");
    AliSts.Builder builder =
        new AliSts()
            .builder()
            .withRegion("cn-hangzhou")
            .withProxyEndpoint(proxyEndpoint)
            .withUseSystemPropertyProxyValues(true)
            .withUseEnvironmentVariableProxyValues(true);

    Assertions.assertEquals(proxyEndpoint, builder.getProxyEndpoint());
    Assertions.assertEquals(Boolean.TRUE, builder.getUseSystemPropertyProxyValues());
    Assertions.assertEquals(Boolean.TRUE, builder.getUseEnvironmentVariableProxyValues());
  }

  @Test
  public void testBuilderWithDisabledSystemPropertyProxyValues() {
    AliSts.Builder builder =
        new AliSts().builder().withRegion("cn-hangzhou").withUseSystemPropertyProxyValues(false);

    Assertions.assertEquals(Boolean.FALSE, builder.getUseSystemPropertyProxyValues());
  }

  @Test
  public void testBuilderWithDisabledEnvironmentVariableProxyValues() {
    AliSts.Builder builder =
        new AliSts()
            .builder()
            .withRegion("cn-hangzhou")
            .withUseEnvironmentVariableProxyValues(false);

    Assertions.assertEquals(Boolean.FALSE, builder.getUseEnvironmentVariableProxyValues());
  }

  @Test
  public void testClientCreationWithProxyEndpoint() {
    java.net.URI proxyEndpoint = java.net.URI.create("http://proxy.example.com:8080");
    AliSts.Builder builder =
        new AliSts().builder().withRegion("cn-hangzhou").withProxyEndpoint(proxyEndpoint);

    // Build the client to exercise proxy configuration code
    AliSts sts = builder.build(mockStsClient);
    Assertions.assertNotNull(sts);
    Assertions.assertEquals("ali", sts.getProviderId());
  }

  @Test
  public void testClientCreationWithSystemPropertyProxyValues() {
    AliSts.Builder builder =
        new AliSts().builder().withRegion("cn-hangzhou").withUseSystemPropertyProxyValues(true);

    // Build the client to exercise proxy configuration code
    AliSts sts = builder.build(mockStsClient);
    Assertions.assertNotNull(sts);
    Assertions.assertEquals("ali", sts.getProviderId());
  }

  @Test
  public void testClientCreationWithEnvironmentVariableProxyValues() {
    AliSts.Builder builder =
        new AliSts()
            .builder()
            .withRegion("cn-hangzhou")
            .withUseEnvironmentVariableProxyValues(true);

    // Build the client to exercise proxy configuration code
    AliSts sts = builder.build(mockStsClient);
    Assertions.assertNotNull(sts);
    Assertions.assertEquals("ali", sts.getProviderId());
  }

  @Test
  public void testClientCreationWithAllProxySettingsEnabled() {
    java.net.URI proxyEndpoint = java.net.URI.create("http://proxy.example.com:8080");
    AliSts.Builder builder =
        new AliSts()
            .builder()
            .withRegion("cn-hangzhou")
            .withProxyEndpoint(proxyEndpoint)
            .withUseSystemPropertyProxyValues(true)
            .withUseEnvironmentVariableProxyValues(true);

    // Build the client to exercise proxy configuration code
    AliSts sts = builder.build(mockStsClient);
    Assertions.assertNotNull(sts);
    Assertions.assertEquals("ali", sts.getProviderId());
  }

  @Test
  public void testClientCreationWithDisabledProxyFlags() {
    AliSts.Builder builder =
        new AliSts()
            .builder()
            .withRegion("cn-hangzhou")
            .withUseSystemPropertyProxyValues(false)
            .withUseEnvironmentVariableProxyValues(false);

    // Build the client to exercise proxy configuration code
    // Even with flags set to false, the SDK will still auto-detect (documented limitation)
    AliSts sts = builder.build(mockStsClient);
    Assertions.assertNotNull(sts);
    Assertions.assertEquals("ali", sts.getProviderId());
  }
}
