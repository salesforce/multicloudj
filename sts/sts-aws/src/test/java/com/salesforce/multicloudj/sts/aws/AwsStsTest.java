package com.salesforce.multicloudj.sts.aws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.CredentialScope;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityResponse;
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
        AssumeRoleWithWebIdentityResponse webIdentityResponse = AssumeRoleWithWebIdentityResponse.builder().credentials(credentials).build();
        GetSessionTokenResponse sessionTokenResponse = GetSessionTokenResponse.builder().credentials(credentials).build();
        GetCallerIdentityResponse callerIdentityResponse = GetCallerIdentityResponse.builder()
                .userId("testUserId").arn("testResourceName").account("testAccountId").build();
        Mockito.when(mockStsClient.assumeRole(any(AssumeRoleRequest.class))).thenReturn(mockResponse);
        Mockito.when(mockStsClient.assumeRoleWithWebIdentity(any(AssumeRoleWithWebIdentityRequest.class))).thenReturn(webIdentityResponse);
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
        CallerIdentity identity = sts.getCallerIdentity((com.salesforce.multicloudj.sts.model.GetCallerIdentityRequest.builder().build()));
        Assertions.assertEquals("testUserId", identity.getUserId());
        Assertions.assertEquals("testResourceName", identity.getCloudResourceName());
        Assertions.assertEquals("testAccountId", identity.getAccountId());
    }

    @Test
    public void TestAssumeRoleWithWebIdentity() {
        AwsSts sts = new AwsSts().builder().build(mockStsClient);
        AssumeRoleWebIdentityRequest request = AssumeRoleWebIdentityRequest.builder()
                .role("testRole")
                .webIdentityToken("testWebIdentityToken")
                .sessionName("testSession")
                .build();
        StsCredentials credentials = sts.assumeRoleWithWebIdentity(request);
        Assertions.assertEquals("testKeyId", credentials.getAccessKeyId());
        Assertions.assertEquals("testSecret", credentials.getAccessKeySecret());
        Assertions.assertEquals("testToken", credentials.getSecurityToken());
    }

    @Test
    public void TestAssumeRoleWithWebIdentityWithExpiration() {
        AwsSts sts = new AwsSts().builder().build(mockStsClient);
        AssumeRoleWebIdentityRequest request = AssumeRoleWebIdentityRequest.builder()
                .role("testRole")
                .webIdentityToken("testWebIdentityToken")
                .sessionName("testSession")
                .expiration(3600)
                .build();
        StsCredentials credentials = sts.assumeRoleWithWebIdentity(request);
        Assertions.assertEquals("testKeyId", credentials.getAccessKeyId());
        Assertions.assertEquals("testSecret", credentials.getAccessKeySecret());
        Assertions.assertEquals("testToken", credentials.getSecurityToken());
    }

    @Test
    public void TestAssumeRoleWithWebIdentityWithoutSessionName() {
        AwsSts sts = new AwsSts().builder().build(mockStsClient);
        AssumeRoleWebIdentityRequest request = AssumeRoleWebIdentityRequest.builder()
                .role("testRole")
                .webIdentityToken("testWebIdentityToken")
                .build();
        StsCredentials credentials = sts.assumeRoleWithWebIdentity(request);
        Assertions.assertEquals("testKeyId", credentials.getAccessKeyId());
        Assertions.assertEquals("testSecret", credentials.getAccessKeySecret());
        Assertions.assertEquals("testToken", credentials.getSecurityToken());
    }

    @Test
    public void TestAssumeRoleWithCredentialScope() throws Exception {
        AwsSts sts = new AwsSts().builder().build(mockStsClient);

        // Create cloud-agnostic CredentialScope using storage:// format
        CredentialScope.ScopeRule rule = CredentialScope.ScopeRule.builder()
                .availableResource("storage://my-bucket")
                .availablePermission("storage:GetObject")
                .build();

        CredentialScope credentialScope = CredentialScope.builder()
                .rule(rule)
                .build();

        AssumedRoleRequest request = AssumedRoleRequest.newBuilder()
                .withRole("arn:aws:iam::123456789012:role/test-role")
                .withSessionName("testSession")
                .withCredentialScope(credentialScope)
                .build();

        StsCredentials credentials = sts.assumeRole(request);
        Assertions.assertEquals("testKeyId", credentials.getAccessKeyId());
        Assertions.assertEquals("testSecret", credentials.getAccessKeySecret());
        Assertions.assertEquals("testToken", credentials.getSecurityToken());

        // Verify that policy was set with correct conversion (using ArgumentCaptor)
        org.mockito.ArgumentCaptor<AssumeRoleRequest> captor = org.mockito.ArgumentCaptor.forClass(AssumeRoleRequest.class);
        Mockito.verify(mockStsClient).assumeRole(captor.capture());
        AssumeRoleRequest capturedRequest = captor.getValue();

        // Verify the complete policy structure (parse JSON to ignore key ordering)
        String expectedPolicy = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\"],\"Resource\":\"arn:aws:s3:::my-bucket/*\"}]}";
        assertJsonEquals(expectedPolicy, capturedRequest.policy());
    }

    private void assertJsonEquals(String expected, String actual) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode expectedNode = mapper.readTree(expected);
        JsonNode actualNode = mapper.readTree(actual);
        Assertions.assertEquals(expectedNode, actualNode);
    }

    @Test
    public void TestAssumeRoleWithCredentialScopeAndCondition() throws Exception {
        AwsSts sts = new AwsSts().builder().build(mockStsClient);

        // Create cloud-agnostic CredentialScope with availability condition
        CredentialScope.AvailabilityCondition condition = CredentialScope.AvailabilityCondition.builder()
                .resourcePrefix("storage://my-bucket/documents/")
                .title("Limit to documents folder")
                .description("Only allow access to objects in the documents folder")
                .build();

        CredentialScope.ScopeRule rule = CredentialScope.ScopeRule.builder()
                .availableResource("storage://my-bucket")
                .availablePermission("storage:GetObject")
                .availablePermission("storage:PutObject")
                .availabilityCondition(condition)
                .build();

        CredentialScope credentialScope = CredentialScope.builder()
                .rule(rule)
                .build();

        AssumedRoleRequest request = AssumedRoleRequest.newBuilder()
                .withRole("arn:aws:iam::123456789012:role/test-role")
                .withSessionName("testSession")
                .withCredentialScope(credentialScope)
                .build();

        StsCredentials credentials = sts.assumeRole(request);
        Assertions.assertNotNull(credentials);

        // Verify that policy was set with correct conversion including condition
        org.mockito.ArgumentCaptor<AssumeRoleRequest> captor = org.mockito.ArgumentCaptor.forClass(AssumeRoleRequest.class);
        Mockito.verify(mockStsClient, Mockito.atLeastOnce()).assumeRole(captor.capture());
        AssumeRoleRequest capturedRequest = captor.getValue();

        // Verify the complete policy structure with condition (parse JSON to ignore key ordering)
        String expectedPolicy = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:PutObject\"],\"Resource\":\"arn:aws:s3:::my-bucket/*\",\"Condition\":{\"StringLike\":{\"s3:prefix\":\"documents/\"}}}]}";
        assertJsonEquals(expectedPolicy, capturedRequest.policy());
    }
}
