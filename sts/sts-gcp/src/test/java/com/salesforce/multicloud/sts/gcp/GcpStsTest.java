package com.salesforce.multicloud.sts.gcp;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenRequest;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenResponse;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.salesforce.multicloudj.sts.gcp.GcpSts;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.Collection;

public class GcpStsTest {

    private static IamCredentialsClient mockStsClient;
    private static GoogleCredentials mockGoogleCredentials;

    @BeforeAll
    public static void setUp() throws IOException {
        mockStsClient = Mockito.mock(IamCredentialsClient.class);
        mockGoogleCredentials = Mockito.mock(GoogleCredentials.class);
        GenerateAccessTokenResponse mockAccessTokenResponse = Mockito.mock(GenerateAccessTokenResponse.class);
        AccessToken mockAccessToken = Mockito.mock(AccessToken.class);
        Mockito.when(mockStsClient.generateAccessToken(Mockito.any(GenerateAccessTokenRequest.class))).thenReturn(mockAccessTokenResponse);
        Mockito.when(mockAccessTokenResponse.getAccessToken()).thenReturn("testAccessToken");
        Mockito.when(mockGoogleCredentials.createScoped(Mockito.any(Collection.class))).thenReturn(mockGoogleCredentials);
        Mockito.doNothing().when(mockGoogleCredentials).refreshIfExpired();
        Mockito.when(mockGoogleCredentials.getAccessToken()).thenReturn(mockAccessToken);
        Mockito.when(mockAccessToken.getTokenValue()).thenReturn("testAccessTokenValue");
    }

    @Test
    public void TestAssumedRoleSts() {
        GcpSts sts = new GcpSts().builder().build(mockStsClient);
        AssumedRoleRequest request = AssumedRoleRequest.newBuilder().withRole("testRole").withSessionName("testSession").build();
        StsCredentials credentials = sts.assumeRole(request);
        Assertions.assertEquals(null, credentials.getAccessKeyId());
        Assertions.assertEquals(null, credentials.getAccessKeySecret());
        Assertions.assertEquals("testAccessToken", credentials.getSecurityToken());
    }

    @Test
    public void TestGetCallerIdentitySts() {
        try (MockedStatic<GoogleCredentials> mockedGoogleCreds = Mockito.mockStatic(GoogleCredentials.class)) {
            mockedGoogleCreds.when(GoogleCredentials::getApplicationDefault).thenReturn(mockGoogleCredentials);
            
            GcpSts sts = new GcpSts().builder().build(mockStsClient);
            CallerIdentity identity = sts.getCallerIdentity();
            Assertions.assertEquals(null, identity.getUserId());
            Assertions.assertEquals("testAccessTokenValue", identity.getCloudResourceName());
            Assertions.assertEquals(null, identity.getAccountId());
        }
    }

    @Test
    public void TestGetCallerIdentityStsThrowsException() throws IOException {
        try (MockedStatic<GoogleCredentials> mockedGoogleCreds = Mockito.mockStatic(GoogleCredentials.class)) {
            mockedGoogleCreds.when(GoogleCredentials::getApplicationDefault).thenReturn(mockGoogleCredentials);

            GcpSts sts = new GcpSts().builder().build(mockStsClient);
            Mockito.doThrow(new IOException("Test error")).when(mockGoogleCredentials).refreshIfExpired();

            Assertions.assertThrows(RuntimeException.class, () -> {
                sts.getCallerIdentity();
            });
        }
    }

    @Test
    public void TestGetSessionTokenSts() throws IOException {
        try (MockedStatic<GoogleCredentials> mockedGoogleCreds = Mockito.mockStatic(GoogleCredentials.class)) {
            mockedGoogleCreds.when(GoogleCredentials::getApplicationDefault).thenReturn(mockGoogleCredentials);
            Mockito.when(mockGoogleCredentials.createScoped(Mockito.any(Collection.class))).thenReturn(mockGoogleCredentials);
            Mockito.doNothing().when(mockGoogleCredentials).refreshIfExpired();
            
            GcpSts sts = new GcpSts().builder().build(mockStsClient);
            GetAccessTokenRequest request = GetAccessTokenRequest.newBuilder().withDurationSeconds(60).build();
            StsCredentials credentials = sts.getAccessToken(request);
            Assertions.assertEquals(null, credentials.getAccessKeyId());
            Assertions.assertEquals(null, credentials.getAccessKeySecret());
            Assertions.assertEquals("testAccessTokenValue", credentials.getSecurityToken());
        }
    }

    @Test
    public void TestGetSessionTokenStsThrowsException() throws IOException {
        try (MockedStatic<GoogleCredentials> mockedGoogleCreds = Mockito.mockStatic(GoogleCredentials.class)) {
            mockedGoogleCreds.when(GoogleCredentials::getApplicationDefault).thenReturn(mockGoogleCredentials);

            GcpSts sts = new GcpSts().builder().build(mockStsClient);
            Mockito.doThrow(new IOException("Test error")).when(mockGoogleCredentials).refreshIfExpired();

            Assertions.assertThrows(RuntimeException.class, () -> {
                sts.getAccessToken(GetAccessTokenRequest.newBuilder().withDurationSeconds(60).build());
            });
        }
    }

    @Test
    public void TestGcpStsConstructorWithBuilder() {
        try (MockedStatic<IamCredentialsClient> mockedIamClient = Mockito.mockStatic(IamCredentialsClient.class)) {
            mockedIamClient.when(IamCredentialsClient::create).thenReturn(mockStsClient);
            GcpSts sts = new GcpSts(new GcpSts().builder());
            Assertions.assertNotNull(sts);
            Assertions.assertEquals("gcp", sts.getProviderId());
            mockedIamClient.when(IamCredentialsClient::create).thenThrow(new IOException("Failed to create client"));
            Assertions.assertThrows(RuntimeException.class, () -> {
                new GcpSts(new GcpSts().builder());
            });
        }
    }
}
