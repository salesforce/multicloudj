package com.salesforce.multicloudj.sts.gcp;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdToken;
import com.google.auth.oauth2.IdTokenProvider;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenRequest;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenResponse;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.salesforce.multicloudj.common.exceptions.DeadlineExceededException;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CredentialScope;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.GetCallerIdentityRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.apache.commons.lang3.StringUtils;
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
    private static GoogleCredentials mockGoogleCredentialsWithIdToken;

    @BeforeAll
    public static void setUp() throws IOException {
        mockStsClient = Mockito.mock(IamCredentialsClient.class);
        mockGoogleCredentials = Mockito.mock(GoogleCredentials.class);

        // Create a mock that is both GoogleCredentials and IdTokenProvider
        mockGoogleCredentialsWithIdToken = Mockito.mock(GoogleCredentials.class, Mockito.withSettings().extraInterfaces(IdTokenProvider.class));

        GenerateAccessTokenResponse mockAccessTokenResponse = Mockito.mock(GenerateAccessTokenResponse.class);
        AccessToken mockAccessToken = Mockito.mock(AccessToken.class);

        // Create a real IdToken instead of mocking it
        String mockJwt = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJzdWIiOiJtb2NrLXVzZXIiLCJhdWQiOiJtdWx0aWNsb3VkaiIsImV4cCI6OTk5OTk5OTk5OSwiaWF0IjoxMjM0NTY3ODkwfQ.mock-signature";
        IdToken mockIdToken = IdToken.create(mockJwt);

        Mockito.when(mockStsClient.generateAccessToken(Mockito.any(GenerateAccessTokenRequest.class))).thenReturn(mockAccessTokenResponse);
        Mockito.when(mockAccessTokenResponse.getAccessToken()).thenReturn("testAccessToken");
        Mockito.when(mockGoogleCredentials.createScoped(Mockito.any(Collection.class))).thenReturn(mockGoogleCredentials);
        Mockito.doNothing().when(mockGoogleCredentials).refreshIfExpired();
        Mockito.when(mockGoogleCredentials.getAccessToken()).thenReturn(mockAccessToken);
        Mockito.when(mockAccessToken.getTokenValue()).thenReturn("testAccessTokenValue");

        // Setup for GoogleCredentials with IdTokenProvider
        Mockito.when(mockGoogleCredentialsWithIdToken.createScoped(Mockito.any(Collection.class))).thenReturn(mockGoogleCredentialsWithIdToken);
        Mockito.doNothing().when(mockGoogleCredentialsWithIdToken).refreshIfExpired();
        Mockito.when(mockGoogleCredentialsWithIdToken.getAccessToken()).thenReturn(mockAccessToken);
        Mockito.when(((IdTokenProvider) mockGoogleCredentialsWithIdToken).idTokenWithAudience(Mockito.anyString(), Mockito.any())).thenReturn(mockIdToken);
    }

    @Test
    public void TestAssumedRoleSts() {
        GcpSts sts = new GcpSts().builder().build(mockStsClient);
        AssumedRoleRequest request = AssumedRoleRequest.newBuilder().withRole("testRole").withSessionName("testSession").build();
        StsCredentials credentials = sts.assumeRole(request);
        Assertions.assertEquals(StringUtils.EMPTY, credentials.getAccessKeyId());
        Assertions.assertEquals(StringUtils.EMPTY, credentials.getAccessKeySecret());
        Assertions.assertEquals("testAccessToken", credentials.getSecurityToken());
    }

    @Test
    public void TestGetCallerIdentitySts() {
        try (MockedStatic<GoogleCredentials> mockedGoogleCreds = Mockito.mockStatic(GoogleCredentials.class)) {
            mockedGoogleCreds.when(GoogleCredentials::getApplicationDefault).thenReturn(mockGoogleCredentialsWithIdToken);

            GcpSts sts = new GcpSts().builder().build(mockStsClient);
            CallerIdentity identity = sts.getCallerIdentity(GetCallerIdentityRequest.builder().build());
            Assertions.assertEquals(StringUtils.EMPTY, identity.getUserId());
            Assertions.assertTrue(identity.getCloudResourceName().startsWith("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9"));
            Assertions.assertEquals(StringUtils.EMPTY, identity.getAccountId());
        }
    }

    @Test
    public void TestGetCallerIdentityWithCustomAud() {
        try (MockedStatic<GoogleCredentials> mockedGoogleCreds = Mockito.mockStatic(GoogleCredentials.class)) {
            mockedGoogleCreds.when(GoogleCredentials::getApplicationDefault).thenReturn(mockGoogleCredentialsWithIdToken);

            GcpSts sts = new GcpSts().builder().build(mockStsClient);
            CallerIdentity identity = sts.getCallerIdentity(GetCallerIdentityRequest.builder().aud("customAudience").build());
            Assertions.assertEquals(StringUtils.EMPTY, identity.getUserId());
            Assertions.assertTrue(identity.getCloudResourceName().startsWith("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9"));
            Assertions.assertEquals(StringUtils.EMPTY, identity.getAccountId());
        }
    }

    @Test
    public void TestGetCallerIdentityStsThrowsException() throws IOException {
        try (MockedStatic<GoogleCredentials> mockedGoogleCreds = Mockito.mockStatic(GoogleCredentials.class)) {
            mockedGoogleCreds.when(GoogleCredentials::getApplicationDefault).thenReturn(mockGoogleCredentialsWithIdToken);

            GcpSts sts = new GcpSts().builder().build(mockStsClient);
            Mockito.doThrow(new IOException("Test error")).when(mockGoogleCredentialsWithIdToken).refreshIfExpired();

            Assertions.assertThrows(RuntimeException.class, () -> {
                sts.getCallerIdentity(GetCallerIdentityRequest.builder().build());
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
            Assertions.assertEquals(StringUtils.EMPTY, credentials.getAccessKeyId());
            Assertions.assertEquals(StringUtils.EMPTY, credentials.getAccessKeySecret());
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

    @Test
    public void TestGetExceptionWithApiException() {
        GcpSts sts = new GcpSts().builder().build(mockStsClient);
        
        // Test various status codes
        assertExceptionMapping(sts, StatusCode.Code.CANCELLED, UnknownException.class);
        assertExceptionMapping(sts, StatusCode.Code.UNKNOWN, UnknownException.class);
        assertExceptionMapping(sts, StatusCode.Code.INVALID_ARGUMENT, InvalidArgumentException.class);
        assertExceptionMapping(sts, StatusCode.Code.DEADLINE_EXCEEDED, DeadlineExceededException.class);
        assertExceptionMapping(sts, StatusCode.Code.NOT_FOUND, ResourceNotFoundException.class);
        assertExceptionMapping(sts, StatusCode.Code.ALREADY_EXISTS, ResourceAlreadyExistsException.class);
        assertExceptionMapping(sts, StatusCode.Code.PERMISSION_DENIED, UnAuthorizedException.class);
        assertExceptionMapping(sts, StatusCode.Code.RESOURCE_EXHAUSTED, ResourceExhaustedException.class);
        assertExceptionMapping(sts, StatusCode.Code.FAILED_PRECONDITION, FailedPreconditionException.class);
        assertExceptionMapping(sts, StatusCode.Code.ABORTED, DeadlineExceededException.class);
        assertExceptionMapping(sts, StatusCode.Code.OUT_OF_RANGE, InvalidArgumentException.class);
        assertExceptionMapping(sts, StatusCode.Code.UNIMPLEMENTED, UnSupportedOperationException.class);
        assertExceptionMapping(sts, StatusCode.Code.INTERNAL, UnknownException.class);
        assertExceptionMapping(sts, StatusCode.Code.UNAVAILABLE, UnknownException.class);
        assertExceptionMapping(sts, StatusCode.Code.DATA_LOSS, UnknownException.class);
        assertExceptionMapping(sts, StatusCode.Code.UNAUTHENTICATED, UnAuthorizedException.class);
    }

    @Test
    public void TestGetExceptionWithNonApiException() {
        GcpSts sts = new GcpSts().builder().build(mockStsClient);
        Class<? extends SubstrateSdkException> exceptionClass = sts.getException(new RuntimeException("Test error"));
        Assertions.assertEquals(UnknownException.class, exceptionClass);
    }

    @Test
    public void TestAssumeRoleWithWebIdentityReturnsNull() {
        GcpSts sts = new GcpSts().builder().build(mockStsClient);
        com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest request =
                com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest.builder()
                        .role("testRole")
                        .webIdentityToken("testToken")
                        .build();
        Assertions.assertThrows(UnSupportedOperationException.class, () -> sts.assumeRoleWithWebIdentity(request));
    }

    @Test
    public void TestAssumedRoleStsWithCredentialScope() {
        GcpSts sts = new GcpSts().builder().build(mockStsClient);

        // Create a cloud-agnostic CredentialScope using storage:// format
        CredentialScope.ScopeRule rule = CredentialScope.ScopeRule.builder()
                .availableResource("storage://test-bucket/*")
                .availablePermission("storage:GetObject")
                .build();

        CredentialScope credentialScope = CredentialScope.builder()
                .rule(rule)
                .build();

        AssumedRoleRequest request = AssumedRoleRequest.newBuilder()
                .withRole("testRole@test-project.iam.gserviceaccount.com")
                .withSessionName("testSession")
                .withCredentialScope(credentialScope)
                .build();

        // This test verifies the code path is taken; actual credential generation
        // requires valid GCP credentials which are tested in integration tests
        Assertions.assertThrows(SubstrateSdkException.class, () -> sts.assumeRole(request));
    }


    private void assertExceptionMapping(GcpSts sts, StatusCode.Code statusCode, Class<? extends SubstrateSdkException> expectedExceptionClass) {
        ApiException apiException = Mockito.mock(ApiException.class);
        StatusCode mockStatusCode = Mockito.mock(StatusCode.class);
        Mockito.when(apiException.getStatusCode()).thenReturn(mockStatusCode);
        Mockito.when(mockStatusCode.getCode()).thenReturn(statusCode);

        Class<? extends SubstrateSdkException> actualExceptionClass = sts.getException(apiException);
        Assertions.assertEquals(expectedExceptionClass, actualExceptionClass,
            "Expected " + expectedExceptionClass.getSimpleName() + " for status code " + statusCode);
    }
}
