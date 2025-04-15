package com.salesforce.multicloudj.sts.aws;

import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;
import software.amazon.awssdk.services.sts.model.GetSessionTokenRequest;
import software.amazon.awssdk.services.sts.model.GetSessionTokenResponse;

import static org.mockito.ArgumentMatchers.any;

public class AwsStsTest {

    private static StsClient mockStsClient;

    @BeforeAll
    public static void setUp() {
        mockStsClient = Mockito.mock(StsClient.class);
        Credentials credentials = Credentials.builder()
                .accessKeyId("testKeyId")
                .secretAccessKey("testSecret")
                .sessionToken("testToken").build();
        AssumeRoleResponse mockResponse = AssumeRoleResponse.builder().credentials(credentials).build();
        GetSessionTokenResponse sessionTokenResponse = GetSessionTokenResponse.builder().credentials(credentials).build();
        GetCallerIdentityResponse callerIdentityResponse = GetCallerIdentityResponse.builder()
                .userId("testUserId").arn("testResourceName").account("testAccountId").build();
        Mockito.when(mockStsClient.assumeRole(any(AssumeRoleRequest.class))).thenReturn(mockResponse);
        Mockito.when(mockStsClient.getSessionToken(any(GetSessionTokenRequest.class))).thenReturn(sessionTokenResponse);
        Mockito.when(mockStsClient.getCallerIdentity(any(GetCallerIdentityRequest.class))).thenReturn(callerIdentityResponse);
    }

    @Test
    public void TestAwsStsInitialization() {
        AwsSts sts = new AwsSts();
        Assertions.assertEquals("aws", sts.getProviderId());
    }

    @Test
    public void TestAssumedRoleSts() {
        AwsSts sts = new AwsSts().builder().build(mockStsClient);
        AssumedRoleRequest request = AssumedRoleRequest.newBuilder().withRole("testRole").withSessionName("testSession").build();
        StsCredentials credentials = sts.assumeRole(request);
        Assertions.assertEquals("testKeyId", credentials.getAccessKeyId());
        Assertions.assertEquals("testSecret", credentials.getAccessKeySecret());
        Assertions.assertEquals("testToken", credentials.getSecurityToken());
    }

    @Test
    public void TestGetCallerIdentitySts() {
        AwsSts sts = new AwsSts().builder().build(mockStsClient);
        GetAccessTokenRequest request = GetAccessTokenRequest.newBuilder().withDurationSeconds(60).build();
        StsCredentials credentials = sts.getAccessToken(request);
        Assertions.assertEquals("testKeyId", credentials.getAccessKeyId());
        Assertions.assertEquals("testSecret", credentials.getAccessKeySecret());
        Assertions.assertEquals("testToken", credentials.getSecurityToken());
    }

    @Test
    public void TestGetSessionTokenSts() {
        AwsSts sts = new AwsSts().builder().build(mockStsClient);
        CallerIdentity identity = sts.getCallerIdentity();
        Assertions.assertEquals("testUserId", identity.getUserId());
        Assertions.assertEquals("testResourceName", identity.getCloudResourceName());
        Assertions.assertEquals("testAccountId", identity.getAccountId());
    }
}
