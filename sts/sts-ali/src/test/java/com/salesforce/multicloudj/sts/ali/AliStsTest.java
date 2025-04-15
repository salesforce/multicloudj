package com.salesforce.multicloudj.sts.ali;

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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;

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
        Mockito.when(mockStsClient.getAcsResponse(any(AssumeRoleRequest.class))).thenReturn(mockResponse);
        Mockito.when(mockStsClient.getAcsResponse(any(GetCallerIdentityRequest.class))).thenReturn(callerIdentityResponse);
    }

    @Test
    public void TestAliStsInitialization() {
        AliSts sts = new AliSts();
        Assertions.assertEquals("ali", sts.getProviderId());
    }

    @Test
    public void TestAliStsClientProfileBadEndpoint() {
        URI badEndpoint = URI.create("https://a.bad.endpoint");

        // Expect InvalidArgumentException to be thrown
        InvalidArgumentException exception = assertThrows(
                InvalidArgumentException.class,
                () -> AliSts.getClientProfile(badEndpoint)
        );

        assertEquals("The endpoint is invalid", exception.getMessage());

        URI anotherBadEndpoint = URI.create("https://a.endpoint");

        // Expect InvalidArgumentException to be thrown
        exception = assertThrows(
                InvalidArgumentException.class,
                () -> AliSts.getClientProfile(anotherBadEndpoint)
        );

        assertEquals("The endpoint is invalid", exception.getMessage());
    }

    @Test
    public void TestAliStsClientProfileGoodEndpoint() {
        URI goodEndpoint = URI.create("https://a.good.endpoint.com");

        IClientProfile profile = AliSts.getClientProfile(goodEndpoint);
        assertEquals("good", profile.getRegionId());

        goodEndpoint = URI.create("https://a-vpc.good.endpoint.com");

        profile = AliSts.getClientProfile(goodEndpoint);
        assertEquals("good", profile.getRegionId());
    }

    @Test
    public void TestAssumedRoleSts() {
        AliSts sts = new AliSts().builder().build(mockStsClient);
        AssumedRoleRequest request = AssumedRoleRequest.newBuilder().withRole("testRole").withSessionName("testSession").build();
        StsCredentials credentials = sts.assumeRole(request);
        Assertions.assertEquals("testKeyId", credentials.getAccessKeyId());
        Assertions.assertEquals("testSecret", credentials.getAccessKeySecret());
        Assertions.assertEquals("testToken", credentials.getSecurityToken());
    }

    @Test
    public void TestGetCallerIdentitySts() {
        AliSts sts = new AliSts().builder().build(mockStsClient);
        CallerIdentity identity = sts.getCallerIdentity();
        Assertions.assertEquals("testAccountId", identity.getAccountId());
        Assertions.assertEquals("testUserId", identity.getUserId());
        Assertions.assertEquals("testResourceName", identity.getCloudResourceName());
    }

    @Test
    public void TestRuntimeExceptionType() {
        AliSts sts = new AliSts().builder().build(mockStsClient);
        Assertions.assertEquals(UnknownException.class, sts.getException(new RuntimeException()));
    }
}
