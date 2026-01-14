package com.salesforce.multicloudj.sts.gcp;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdToken;
import com.google.auth.oauth2.IdTokenProvider;
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
import com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest;
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
import java.lang.reflect.Method;
import java.util.Collection;

public class GcpStsTest {

    private static GoogleCredentials mockGoogleCredentials;
    private static GoogleCredentials mockGoogleCredentialsWithIdToken;

    @BeforeAll
    public static void setUp() throws IOException {
        mockGoogleCredentials = Mockito.mock(GoogleCredentials.class);

        // Create a mock that is both GoogleCredentials and IdTokenProvider
        mockGoogleCredentialsWithIdToken = Mockito.mock(GoogleCredentials.class, Mockito.withSettings().extraInterfaces(IdTokenProvider.class));

        AccessToken mockAccessToken = Mockito.mock(AccessToken.class);

        // Create a real IdToken instead of mocking it
        String mockJwt = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJzdWIiOiJtb2NrLXVzZXIiLCJhdWQiOiJtdWx0aWNsb3VkaiIsImV4cCI6OTk5OTk5OTk5OSwiaWF0IjoxMjM0NTY3ODkwfQ.mock-signature";
        IdToken mockIdToken = IdToken.create(mockJwt);

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
    public void testAssumedRoleSts() throws IOException {
        // Reset the mock to ensure no interference from other tests
        Mockito.reset(mockGoogleCredentials);
        Mockito.when(mockGoogleCredentials.createScoped(Mockito.any(Collection.class))).thenReturn(mockGoogleCredentials);
        Mockito.doNothing().when(mockGoogleCredentials).refreshIfExpired();
        Mockito.when(mockGoogleCredentials.getAccessToken()).thenReturn(Mockito.mock(AccessToken.class));
        Mockito.when(mockGoogleCredentials.getAccessToken().getTokenValue()).thenReturn("testAccessTokenValue");

        GcpSts sts = new GcpSts().builder().build(mockGoogleCredentials);
        // Test without role to avoid ImpersonatedCredentials (which can't be easily mocked)
        AssumedRoleRequest request = AssumedRoleRequest.newBuilder().withSessionName("testSession").build();
        StsCredentials credentials = sts.assumeRole(request);
        Assertions.assertEquals(StringUtils.EMPTY, credentials.getAccessKeyId());
        Assertions.assertEquals(StringUtils.EMPTY, credentials.getAccessKeySecret());
        Assertions.assertEquals("testAccessTokenValue", credentials.getSecurityToken());
    }

    @Test
    public void testGetCallerIdentitySts() {
        try (MockedStatic<GoogleCredentials> mockedGoogleCreds = Mockito.mockStatic(GoogleCredentials.class)) {
            mockedGoogleCreds.when(GoogleCredentials::getApplicationDefault).thenReturn(mockGoogleCredentialsWithIdToken);

            GcpSts sts = new GcpSts().builder().build(mockGoogleCredentialsWithIdToken);
            CallerIdentity identity = sts.getCallerIdentity(GetCallerIdentityRequest.builder().build());
            Assertions.assertEquals(StringUtils.EMPTY, identity.getUserId());
            Assertions.assertTrue(identity.getCloudResourceName().startsWith("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9"));
            Assertions.assertEquals(StringUtils.EMPTY, identity.getAccountId());
        }
    }

    @Test
    public void testGetCallerIdentityWithCustomAud() {
        try (MockedStatic<GoogleCredentials> mockedGoogleCreds = Mockito.mockStatic(GoogleCredentials.class)) {
            mockedGoogleCreds.when(GoogleCredentials::getApplicationDefault).thenReturn(mockGoogleCredentialsWithIdToken);

            GcpSts sts = new GcpSts().builder().build(mockGoogleCredentialsWithIdToken);
            CallerIdentity identity = sts.getCallerIdentity(GetCallerIdentityRequest.builder().aud("customAudience").build());
            Assertions.assertEquals(StringUtils.EMPTY, identity.getUserId());
            Assertions.assertTrue(identity.getCloudResourceName().startsWith("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9"));
            Assertions.assertEquals(StringUtils.EMPTY, identity.getAccountId());
        }
    }

    @Test
    public void testGetCallerIdentityStsThrowsException() throws IOException {
        try (MockedStatic<GoogleCredentials> mockedGoogleCreds = Mockito.mockStatic(GoogleCredentials.class)) {
            mockedGoogleCreds.when(GoogleCredentials::getApplicationDefault).thenReturn(mockGoogleCredentialsWithIdToken);

            GcpSts sts = new GcpSts().builder().build(mockGoogleCredentialsWithIdToken);
            Mockito.doThrow(new IOException("Test error")).when(mockGoogleCredentialsWithIdToken).refreshIfExpired();

            Assertions.assertThrows(RuntimeException.class, () -> {
                sts.getCallerIdentity(GetCallerIdentityRequest.builder().build());
            });
        }
    }

    @Test
    public void testGetSessionTokenSts() throws IOException {
        try (MockedStatic<GoogleCredentials> mockedGoogleCreds = Mockito.mockStatic(GoogleCredentials.class)) {
            mockedGoogleCreds.when(GoogleCredentials::getApplicationDefault).thenReturn(mockGoogleCredentials);
            Mockito.when(mockGoogleCredentials.createScoped(Mockito.any(Collection.class))).thenReturn(mockGoogleCredentials);
            Mockito.doNothing().when(mockGoogleCredentials).refreshIfExpired();
            
            GcpSts sts = new GcpSts().builder().build(mockGoogleCredentialsWithIdToken);
            GetAccessTokenRequest request = GetAccessTokenRequest.newBuilder().withDurationSeconds(60).build();
            StsCredentials credentials = sts.getAccessToken(request);
            Assertions.assertEquals(StringUtils.EMPTY, credentials.getAccessKeyId());
            Assertions.assertEquals(StringUtils.EMPTY, credentials.getAccessKeySecret());
            Assertions.assertEquals("testAccessTokenValue", credentials.getSecurityToken());
        }
    }

    @Test
    public void testGetSessionTokenStsThrowsException() throws IOException {
        try (MockedStatic<GoogleCredentials> mockedGoogleCreds = Mockito.mockStatic(GoogleCredentials.class)) {
            mockedGoogleCreds.when(GoogleCredentials::getApplicationDefault).thenReturn(mockGoogleCredentials);

            GcpSts sts = new GcpSts().builder().build(mockGoogleCredentials);
            Mockito.doThrow(new IOException("Test error")).when(mockGoogleCredentials).refreshIfExpired();

            Assertions.assertThrows(RuntimeException.class, () -> {
                sts.getAccessToken(GetAccessTokenRequest.newBuilder().withDurationSeconds(60).build());
            });
        }
    }

    @Test
    public void testGcpStsConstructorWithBuilder() {
        GcpSts sts = new GcpSts(new GcpSts().builder());
        Assertions.assertNotNull(sts);
        Assertions.assertEquals("gcp", sts.getProviderId());
    }

    @Test
    public void testGetExceptionWithApiException() {
        GcpSts sts = new GcpSts().builder().build(mockGoogleCredentials);
        
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
    public void testGetExceptionWithNonApiException() {
        GcpSts sts = new GcpSts().builder().build(mockGoogleCredentials);
        Class<? extends SubstrateSdkException> exceptionClass = sts.getException(new RuntimeException("Test error"));
        Assertions.assertEquals(UnknownException.class, exceptionClass);
    }

    @Test
    public void testAssumeRoleWithWebIdentityNullRequest() {
        GcpSts sts = new GcpSts().builder().build(mockGoogleCredentials);
        Assertions.assertThrows(InvalidArgumentException.class, () -> {
            sts.assumeRoleWithWebIdentity(null);
        });
    }

    @Test
    public void testAssumeRoleWithWebIdentityMissingRole() {
        GcpSts sts = new GcpSts().builder().build(mockGoogleCredentials);
        AssumeRoleWebIdentityRequest request = AssumeRoleWebIdentityRequest.builder()
                .webIdentityToken("test-token")
                .build();
        Assertions.assertThrows(InvalidArgumentException.class, () -> {
            sts.assumeRoleWithWebIdentity(request);
        });
    }

    @Test
    public void testAssumeRoleWithWebIdentityMissingToken() {
        GcpSts sts = new GcpSts().builder().build(mockGoogleCredentials);
        AssumeRoleWebIdentityRequest request = AssumeRoleWebIdentityRequest.builder()
                .role("test-role")
                .build();
        Assertions.assertThrows(InvalidArgumentException.class, () -> {
            sts.assumeRoleWithWebIdentity(request);
        });
    }

    @Test
    public void testGetSTSCredentialsWithAssumeRoleWebIdentityHappyPath() throws IOException {
        // Mock response content
        String responseJson = "{\"access_token\":\"test-access-token\",\"expires_in\":3600}";
        
        // Create MockHttpTransport that returns the expected response
        MockHttpTransport mockHttpTransport = new MockHttpTransport() {
            @Override
            public MockLowLevelHttpRequest buildRequest(String method, String url) throws IOException {
                return new MockLowLevelHttpRequest() {
                    @Override
                    public MockLowLevelHttpResponse execute() throws IOException {
                        MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                        response.setStatusCode(200);
                        response.setContentType("application/json");
                        response.setContent(responseJson);
                        return response;
                    }
                };
            }
        };

        // Wrap MockHttpTransport in HttpTransportFactory
        HttpTransportFactory httpTransportFactory = () -> mockHttpTransport;

        // Create GcpSts with mocked HttpTransportFactory
        GcpSts sts = new GcpSts().builder().build(httpTransportFactory);
        
        // Create request
        AssumeRoleWebIdentityRequest request = AssumeRoleWebIdentityRequest.builder()
                .role("//iam.googleapis.com/projects/test-project/locations/global/workloadIdentityPools/test-pool/providers/test-provider")
                .webIdentityToken("test-oidc-token")
                .sessionName("test-session")
                .build();

        // Execute
        StsCredentials credentials = sts.assumeRoleWithWebIdentity(request);

        // Verify
        Assertions.assertNotNull(credentials);
        Assertions.assertEquals(StringUtils.EMPTY, credentials.getAccessKeyId());
        Assertions.assertEquals(StringUtils.EMPTY, credentials.getAccessKeySecret());
        Assertions.assertEquals("test-access-token", credentials.getSecurityToken());
    }

    @Test
    public void testAssumedRoleStsWithCredentialScopeConversion() throws Exception {
        GcpSts sts = new GcpSts().builder().build(mockGoogleCredentials);
        CredentialScope.AvailabilityCondition condition = CredentialScope.AvailabilityCondition.builder()
                .resourcePrefix("storage://my-bucket/documents/")
                .title("Limit to documents folder")
                .description("Only allow access to objects in the documents folder")
                .build();
        // Create a cloud-agnostic CredentialScope using storage:// format
        CredentialScope.ScopeRule rule = CredentialScope.ScopeRule.builder()
                .availableResource("storage://test-bucket")
                .availablePermission("storage:GetObject")
                .availablePermission("storage:PutObject")
                .availablePermission("storage:DeleteObject")
                .availablePermission("storage:ListBucket")
                .availabilityCondition(condition)
                .build();

        CredentialScope credentialScope = CredentialScope.builder()
                .rule(rule)
                .build();

        // Test conversion logic using reflection to access private method
        Method convertMethod = GcpSts.class.getDeclaredMethod("convertToGcpAccessBoundary", CredentialScope.class);
        convertMethod.setAccessible(true);
        CredentialAccessBoundary boundary =
            (CredentialAccessBoundary) convertMethod.invoke(sts, credentialScope);

        // Verify the converted boundary structure
        Assertions.assertNotNull(boundary);
        Assertions.assertEquals(1, boundary.getAccessBoundaryRules().size());

        CredentialAccessBoundary.AccessBoundaryRule boundaryRule = boundary.getAccessBoundaryRules().get(0);

        // Verify resource conversion: storage://test-bucket -> //storage.googleapis.com/projects/_/buckets/test-bucket
        Assertions.assertEquals("//storage.googleapis.com/projects/_/buckets/test-bucket", boundaryRule.getAvailableResource());

        // Verify permission conversion: storage:GetObject -> inRole:roles/storage.objectViewer, storage:PutObject -> inRole:roles/storage.objectCreator
        Assertions.assertEquals(4, boundaryRule.getAvailablePermissions().size());
        Assertions.assertTrue(boundaryRule.getAvailablePermissions().contains("inRole:roles/storage.objectViewer"));
        Assertions.assertTrue(boundaryRule.getAvailablePermissions().contains("inRole:roles/storage.objectCreator"));

        // Verify condition conversion
        CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition gcpCondition =
            boundaryRule.getAvailabilityCondition();
        Assertions.assertNotNull(gcpCondition);
        Assertions.assertEquals("Limit to documents folder", gcpCondition.getTitle());
        Assertions.assertEquals("Only allow access to objects in the documents folder", gcpCondition.getDescription());
        Assertions.assertEquals("resource.name.startsWith('projects/_/buckets/my-bucket/objects/documents/')", gcpCondition.getExpression());
    }

    @Test
    public void testAssumedRoleStsWithCredentialScopeExecutionWithMockedCredentials() throws IOException {
        GcpSts sts = new GcpSts().builder().build(mockGoogleCredentials);

        CredentialScope.ScopeRule rule = CredentialScope.ScopeRule.builder()
                .availableResource("storage://test-bucket")
                .availablePermission("storage:GetObject")
                .build();

        CredentialScope credentialScope = CredentialScope.builder()
                .rule(rule)
                .build();

        AssumedRoleRequest request = AssumedRoleRequest.newBuilder()
                .withSessionName("testSession")
                .withCredentialScope(credentialScope)
                .build();

        try {
            sts.assumeRole(request);
            Assertions.fail("Expected exception from DownscopedCredentials");
        } catch (IllegalArgumentException e) {
            Assertions.assertNotNull(e, "mock credentials should throw for downscoped credentials");
        }
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
