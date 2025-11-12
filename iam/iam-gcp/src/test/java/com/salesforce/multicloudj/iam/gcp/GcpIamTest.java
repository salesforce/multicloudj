package com.salesforce.multicloudj.iam.gcp;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.iam.admin.v1.IAMClient;
import com.google.iam.admin.v1.CreateServiceAccountRequest;
import com.google.iam.admin.v1.ServiceAccount;
import com.google.iam.v1.Binding;
import com.google.iam.v1.GetIamPolicyRequest;
import com.google.iam.v1.Policy;
import com.google.iam.v1.SetIamPolicyRequest;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Test class for GcpIam.
 */
public class GcpIamTest {
    
    private GcpIam gcpIam;
    private IAMClient mockIamClient;
    
    private static final String TEST_PROJECT_ID = "test-project-123";
    private static final String TEST_IDENTITY_NAME = "test-service-account";
    private static final String TEST_DESCRIPTION = "Test service account description";
    private static final String TEST_REGION = "us-central1";
    private static final String TEST_SERVICE_ACCOUNT_EMAIL = "test-service-account@test-project-123.iam.gserviceaccount.com";
    private static final String TEST_SERVICE_ACCOUNT_NAME = "projects/test-project-123/serviceAccounts/test-service-account@test-project-123.iam.gserviceaccount.com";
    
    @BeforeEach
    void setUp() {
        mockIamClient = mock(IAMClient.class);
        
        gcpIam = new GcpIam(new GcpIam.Builder(), mockIamClient);
    }
    
    @AfterEach
    void tearDown() throws Exception {
        if (gcpIam != null) {
            gcpIam.close();
        }
    }
    
    @Test
    void testConstructorWithNullClient() {
        assertThrows(InvalidArgumentException.class, () -> 
            new GcpIam(new GcpIam.Builder(), null));
    }
    
    @Test
    void testCreateIdentityWithoutTrustConfig() {
        // Arrange
        ServiceAccount mockServiceAccount = ServiceAccount.newBuilder()
                .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
                .setName(TEST_SERVICE_ACCOUNT_NAME)
                .setDisplayName(TEST_IDENTITY_NAME)
                .setDescription(TEST_DESCRIPTION)
                .build();
        
        when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
                .thenReturn(mockServiceAccount);
        
        // Act
        String result = gcpIam.createIdentity(
                TEST_IDENTITY_NAME,
                TEST_DESCRIPTION,
                TEST_PROJECT_ID,
                TEST_REGION,
                Optional.empty(),
                Optional.empty()
        );
        
        // Assert
        assertEquals(TEST_SERVICE_ACCOUNT_EMAIL, result);
        
        // Verify the request
        ArgumentCaptor<CreateServiceAccountRequest> requestCaptor = 
                ArgumentCaptor.forClass(CreateServiceAccountRequest.class);
        verify(mockIamClient).createServiceAccount(requestCaptor.capture());
        
        CreateServiceAccountRequest capturedRequest = requestCaptor.getValue();
        assertEquals("projects/" + TEST_PROJECT_ID, capturedRequest.getName());
        assertEquals(TEST_IDENTITY_NAME, capturedRequest.getAccountId());
        assertEquals(TEST_IDENTITY_NAME, capturedRequest.getServiceAccount().getDisplayName());
        assertEquals(TEST_DESCRIPTION, capturedRequest.getServiceAccount().getDescription());
        
        // Verify no IAM policy calls were made
        verify(mockIamClient, never()).getIamPolicy(any(GetIamPolicyRequest.class));
        verify(mockIamClient, never()).setIamPolicy(any(SetIamPolicyRequest.class));
    }
    
    @Test
    void testCreateIdentityWithProjectsPrefix() {
        // Arrange
        ServiceAccount mockServiceAccount = ServiceAccount.newBuilder()
                .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
                .setName(TEST_SERVICE_ACCOUNT_NAME)
                .setDisplayName(TEST_IDENTITY_NAME)
                .setDescription(TEST_DESCRIPTION)
                .build();
        
        when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
                .thenReturn(mockServiceAccount);
        
        // Act - pass tenantId with "projects/" prefix
        String result = gcpIam.createIdentity(
                TEST_IDENTITY_NAME,
                TEST_DESCRIPTION,
                "projects/" + TEST_PROJECT_ID,
                TEST_REGION,
                Optional.empty(),
                Optional.empty()
        );
        
        // Assert
        assertEquals(TEST_SERVICE_ACCOUNT_EMAIL, result);
        
        // Verify the request still has correct format
        ArgumentCaptor<CreateServiceAccountRequest> requestCaptor = 
                ArgumentCaptor.forClass(CreateServiceAccountRequest.class);
        verify(mockIamClient).createServiceAccount(requestCaptor.capture());
        
        CreateServiceAccountRequest capturedRequest = requestCaptor.getValue();
        assertEquals("projects/" + TEST_PROJECT_ID, capturedRequest.getName());
    }
    
    @Test
    void testCreateIdentityWithNullDescription() {
        // Arrange
        ServiceAccount mockServiceAccount = ServiceAccount.newBuilder()
                .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
                .setName(TEST_SERVICE_ACCOUNT_NAME)
                .setDisplayName(TEST_IDENTITY_NAME)
                .build();
        
        when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
                .thenReturn(mockServiceAccount);
        
        // Act
        String result = gcpIam.createIdentity(
                TEST_IDENTITY_NAME,
                null,
                TEST_PROJECT_ID,
                TEST_REGION,
                Optional.empty(),
                Optional.empty()
        );
        
        // Assert
        assertEquals(TEST_SERVICE_ACCOUNT_EMAIL, result);
        
        // Verify the request has empty description
        ArgumentCaptor<CreateServiceAccountRequest> requestCaptor = 
                ArgumentCaptor.forClass(CreateServiceAccountRequest.class);
        verify(mockIamClient).createServiceAccount(requestCaptor.capture());
        
        CreateServiceAccountRequest capturedRequest = requestCaptor.getValue();
        assertEquals("", capturedRequest.getServiceAccount().getDescription());
    }
    
    @Test
    void testCreateIdentityWithTrustConfigSinglePrincipal() {
        // Arrange
        String trustedPrincipal = "another-sa@test-project-123.iam.gserviceaccount.com";
        TrustConfiguration trustConfig = TrustConfiguration.builder()
                .addTrustedPrincipal(trustedPrincipal)
                .build();
        
        ServiceAccount mockServiceAccount = ServiceAccount.newBuilder()
                .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
                .setName(TEST_SERVICE_ACCOUNT_NAME)
                .setDisplayName(TEST_IDENTITY_NAME)
                .setDescription(TEST_DESCRIPTION)
                .build();
        
        Policy mockPolicy = Policy.newBuilder().build();
        
        when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
                .thenReturn(mockServiceAccount);
        when(mockIamClient.getIamPolicy(any(GetIamPolicyRequest.class)))
                .thenReturn(mockPolicy);
        when(mockIamClient.setIamPolicy(any(SetIamPolicyRequest.class)))
                .thenReturn(mockPolicy);
        
        // Act
        String result = gcpIam.createIdentity(
                TEST_IDENTITY_NAME,
                TEST_DESCRIPTION,
                TEST_PROJECT_ID,
                TEST_REGION,
                Optional.of(trustConfig),
                Optional.empty()
        );
        
        // Assert
        assertEquals(TEST_SERVICE_ACCOUNT_EMAIL, result);
        
        // Verify service account creation
        verify(mockIamClient).createServiceAccount(any(CreateServiceAccountRequest.class));
        
        // Verify IAM policy operations
        ArgumentCaptor<GetIamPolicyRequest> getRequestCaptor = 
                ArgumentCaptor.forClass(GetIamPolicyRequest.class);
        verify(mockIamClient).getIamPolicy(getRequestCaptor.capture());
        assertEquals(TEST_SERVICE_ACCOUNT_NAME, getRequestCaptor.getValue().getResource());
        
        ArgumentCaptor<SetIamPolicyRequest> setRequestCaptor = 
                ArgumentCaptor.forClass(SetIamPolicyRequest.class);
        verify(mockIamClient).setIamPolicy(setRequestCaptor.capture());
        
        SetIamPolicyRequest capturedSetRequest = setRequestCaptor.getValue();
        assertEquals(TEST_SERVICE_ACCOUNT_NAME, capturedSetRequest.getResource());
        
        // Verify the policy has the correct binding
        Policy capturedPolicy = capturedSetRequest.getPolicy();
        assertEquals(1, capturedPolicy.getBindingsCount());
        
        Binding binding = capturedPolicy.getBindings(0);
        assertEquals("roles/iam.serviceAccountTokenCreator", binding.getRole());
        assertEquals(1, binding.getMembersCount());
        assertEquals("serviceAccount:" + trustedPrincipal, binding.getMembers(0));
    }
    
    @Test
    void testCreateIdentityWithTrustConfigMultiplePrincipals() {
        // Arrange
        String principal1 = "sa1@test-project-123.iam.gserviceaccount.com";
        String principal2 = "sa2@test-project-123.iam.gserviceaccount.com";
        String principal3 = "user:user@example.com"; // Already formatted
        
        TrustConfiguration trustConfig = TrustConfiguration.builder()
                .addTrustedPrincipal(principal1)
                .addTrustedPrincipal(principal2)
                .addTrustedPrincipal(principal3)
                .build();
        
        ServiceAccount mockServiceAccount = ServiceAccount.newBuilder()
                .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
                .setName(TEST_SERVICE_ACCOUNT_NAME)
                .setDisplayName(TEST_IDENTITY_NAME)
                .setDescription(TEST_DESCRIPTION)
                .build();
        
        Policy mockPolicy = Policy.newBuilder().build();
        
        when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
                .thenReturn(mockServiceAccount);
        when(mockIamClient.getIamPolicy(any(GetIamPolicyRequest.class)))
                .thenReturn(mockPolicy);
        when(mockIamClient.setIamPolicy(any(SetIamPolicyRequest.class)))
                .thenReturn(mockPolicy);
        
        // Act
        String result = gcpIam.createIdentity(
                TEST_IDENTITY_NAME,
                TEST_DESCRIPTION,
                TEST_PROJECT_ID,
                TEST_REGION,
                Optional.of(trustConfig),
                Optional.empty()
        );
        
        // Assert
        assertEquals(TEST_SERVICE_ACCOUNT_EMAIL, result);
        
        // Verify IAM policy operations
        ArgumentCaptor<SetIamPolicyRequest> setRequestCaptor = 
                ArgumentCaptor.forClass(SetIamPolicyRequest.class);
        verify(mockIamClient).setIamPolicy(setRequestCaptor.capture());
        
        SetIamPolicyRequest capturedSetRequest = setRequestCaptor.getValue();
        Policy capturedPolicy = capturedSetRequest.getPolicy();
        
        // Verify the policy has the correct binding with all members
        assertEquals(1, capturedPolicy.getBindingsCount());
        
        Binding binding = capturedPolicy.getBindings(0);
        assertEquals("roles/iam.serviceAccountTokenCreator", binding.getRole());
        assertEquals(3, binding.getMembersCount());
        
        // Verify all principals are present
        assertTrue(binding.getMembersList().contains("serviceAccount:" + principal1));
        assertTrue(binding.getMembersList().contains("serviceAccount:" + principal2));
        assertTrue(binding.getMembersList().contains(principal3)); // Already formatted, should remain as-is
    }
    
    @Test
    void testCreateIdentityWithTrustConfigEmptyPrincipals() {
        // Arrange
        TrustConfiguration trustConfig = TrustConfiguration.builder().build();
        
        ServiceAccount mockServiceAccount = ServiceAccount.newBuilder()
                .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
                .setName(TEST_SERVICE_ACCOUNT_NAME)
                .setDisplayName(TEST_IDENTITY_NAME)
                .setDescription(TEST_DESCRIPTION)
                .build();
        
        when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
                .thenReturn(mockServiceAccount);
        
        // Act
        String result = gcpIam.createIdentity(
                TEST_IDENTITY_NAME,
                TEST_DESCRIPTION,
                TEST_PROJECT_ID,
                TEST_REGION,
                Optional.of(trustConfig),
                Optional.empty()
        );
        
        // Assert
        assertEquals(TEST_SERVICE_ACCOUNT_EMAIL, result);
        
        // Verify service account creation
        verify(mockIamClient).createServiceAccount(any(CreateServiceAccountRequest.class));
        
        // Verify no IAM policy calls were made (empty principals list)
        verify(mockIamClient, never()).getIamPolicy(any(GetIamPolicyRequest.class));
        verify(mockIamClient, never()).setIamPolicy(any(SetIamPolicyRequest.class));
    }
    
    @Test
    void testCreateIdentityWithTrustConfigPolicyNotFound() {
        // Arrange
        String trustedPrincipal = "another-sa@test-project-123.iam.gserviceaccount.com";
        TrustConfiguration trustConfig = TrustConfiguration.builder()
                .addTrustedPrincipal(trustedPrincipal)
                .build();
        
        ServiceAccount mockServiceAccount = ServiceAccount.newBuilder()
                .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
                .setName(TEST_SERVICE_ACCOUNT_NAME)
                .setDisplayName(TEST_IDENTITY_NAME)
                .setDescription(TEST_DESCRIPTION)
                .build();
        
        ApiException notFoundException = mock(ApiException.class);
        StatusCode statusCode = mock(StatusCode.class);
        when(statusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);
        when(notFoundException.getStatusCode()).thenReturn(statusCode);
        
        Policy emptyPolicy = Policy.newBuilder().build();
        
        when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
                .thenReturn(mockServiceAccount);
        when(mockIamClient.getIamPolicy(any(GetIamPolicyRequest.class)))
                .thenThrow(notFoundException);
        when(mockIamClient.setIamPolicy(any(SetIamPolicyRequest.class)))
                .thenReturn(emptyPolicy);
        
        // Act
        String result = gcpIam.createIdentity(
                TEST_IDENTITY_NAME,
                TEST_DESCRIPTION,
                TEST_PROJECT_ID,
                TEST_REGION,
                Optional.of(trustConfig),
                Optional.empty()
        );
        
        // Assert
        assertEquals(TEST_SERVICE_ACCOUNT_EMAIL, result);
        
        // Verify that even with policy not found, we still set the policy
        verify(mockIamClient).getIamPolicy(any(GetIamPolicyRequest.class));
        verify(mockIamClient).setIamPolicy(any(SetIamPolicyRequest.class));
    }
    
    @Test
    void testCreateIdentityApiExceptionDuringCreation() {
        // Arrange
        ApiException apiException = mock(ApiException.class);
        StatusCode statusCode = mock(StatusCode.class);
        when(statusCode.getCode()).thenReturn(StatusCode.Code.ALREADY_EXISTS);
        when(apiException.getStatusCode()).thenReturn(statusCode);
        when(apiException.getMessage()).thenReturn("Service account already exists");
        
        when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
                .thenThrow(apiException);
        
        // Act & Assert
        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () ->
                gcpIam.createIdentity(
                        TEST_IDENTITY_NAME,
                        TEST_DESCRIPTION,
                        TEST_PROJECT_ID,
                        TEST_REGION,
                        Optional.empty(),
                        Optional.empty()
                )
        );
        
        assertTrue(exception.getMessage().contains("Failed to create service account"));
        assertEquals(apiException, exception.getCause());
    }
    
    @Test
    void testCreateIdentityApiExceptionDuringPolicySet() {
        // Arrange
        String trustedPrincipal = "another-sa@test-project-123.iam.gserviceaccount.com";
        TrustConfiguration trustConfig = TrustConfiguration.builder()
                .addTrustedPrincipal(trustedPrincipal)
                .build();
        
        ServiceAccount mockServiceAccount = ServiceAccount.newBuilder()
                .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
                .setName(TEST_SERVICE_ACCOUNT_NAME)
                .setDisplayName(TEST_IDENTITY_NAME)
                .setDescription(TEST_DESCRIPTION)
                .build();
        
        Policy mockPolicy = Policy.newBuilder().build();
        
        ApiException apiException = mock(ApiException.class);
        StatusCode statusCode = mock(StatusCode.class);
        when(statusCode.getCode()).thenReturn(StatusCode.Code.PERMISSION_DENIED);
        when(apiException.getStatusCode()).thenReturn(statusCode);
        when(apiException.getMessage()).thenReturn("Permission denied");
        
        when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
                .thenReturn(mockServiceAccount);
        when(mockIamClient.getIamPolicy(any(GetIamPolicyRequest.class)))
                .thenReturn(mockPolicy);
        when(mockIamClient.setIamPolicy(any(SetIamPolicyRequest.class)))
                .thenThrow(apiException);
        
        // Act & Assert
        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () ->
                gcpIam.createIdentity(
                        TEST_IDENTITY_NAME,
                        TEST_DESCRIPTION,
                        TEST_PROJECT_ID,
                        TEST_REGION,
                        Optional.of(trustConfig),
                        Optional.empty()
                )
        );
        
        assertTrue(exception.getMessage().contains("Failed to create service account"));
        assertEquals(apiException, exception.getCause());
    }
    
    @Test
    void testFormatPrincipalAsMemberWithServiceAccountEmail() {
        // This tests the private method indirectly through createIdentity
        String principal = "test@project.iam.gserviceaccount.com";
        TrustConfiguration trustConfig = TrustConfiguration.builder()
                .addTrustedPrincipal(principal)
                .build();
        
        ServiceAccount mockServiceAccount = ServiceAccount.newBuilder()
                .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
                .setName(TEST_SERVICE_ACCOUNT_NAME)
                .build();
        
        Policy mockPolicy = Policy.newBuilder().build();
        
        when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
                .thenReturn(mockServiceAccount);
        when(mockIamClient.getIamPolicy(any(GetIamPolicyRequest.class)))
                .thenReturn(mockPolicy);
        when(mockIamClient.setIamPolicy(any(SetIamPolicyRequest.class)))
                .thenReturn(mockPolicy);
        
        // Act
        gcpIam.createIdentity(TEST_IDENTITY_NAME, TEST_DESCRIPTION, TEST_PROJECT_ID, 
                TEST_REGION, Optional.of(trustConfig), Optional.empty());
        
        // Assert
        ArgumentCaptor<SetIamPolicyRequest> captor = ArgumentCaptor.forClass(SetIamPolicyRequest.class);
        verify(mockIamClient).setIamPolicy(captor.capture());
        
        Binding binding = captor.getValue().getPolicy().getBindings(0);
        assertEquals("serviceAccount:" + principal, binding.getMembers(0));
    }
    
    @Test
    void testFormatPrincipalAsMemberWithAlreadyFormattedPrincipal() {
        // This tests the private method indirectly through createIdentity
        String principal = "user:test@example.com";
        TrustConfiguration trustConfig = TrustConfiguration.builder()
                .addTrustedPrincipal(principal)
                .build();
        
        ServiceAccount mockServiceAccount = ServiceAccount.newBuilder()
                .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
                .setName(TEST_SERVICE_ACCOUNT_NAME)
                .build();
        
        Policy mockPolicy = Policy.newBuilder().build();
        
        when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
                .thenReturn(mockServiceAccount);
        when(mockIamClient.getIamPolicy(any(GetIamPolicyRequest.class)))
                .thenReturn(mockPolicy);
        when(mockIamClient.setIamPolicy(any(SetIamPolicyRequest.class)))
                .thenReturn(mockPolicy);
        
        // Act
        gcpIam.createIdentity(TEST_IDENTITY_NAME, TEST_DESCRIPTION, TEST_PROJECT_ID, 
                TEST_REGION, Optional.of(trustConfig), Optional.empty());
        
        // Assert
        ArgumentCaptor<SetIamPolicyRequest> captor = ArgumentCaptor.forClass(SetIamPolicyRequest.class);
        verify(mockIamClient).setIamPolicy(captor.capture());
        
        Binding binding = captor.getValue().getPolicy().getBindings(0);
        assertEquals(principal, binding.getMembers(0)); // Should remain unchanged
    }
    
    @Test
    void testGetProviderId() {
        assertEquals("gcp", gcpIam.getProviderId());
    }
}
