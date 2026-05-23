package com.salesforce.multicloudj.iam.gcp;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.iam.admin.v1.IAMClient;
import com.google.cloud.resourcemanager.v3.ProjectsClient;
import com.google.iam.admin.v1.CreateServiceAccountRequest;
import com.google.iam.admin.v1.DeleteServiceAccountRequest;
import com.google.iam.admin.v1.GetServiceAccountRequest;
import com.google.iam.admin.v1.ServiceAccount;
import com.google.iam.v1.Binding;
import com.google.iam.v1.GetIamPolicyRequest;
import com.google.iam.v1.Policy;
import com.google.iam.v1.SetIamPolicyRequest;
import com.salesforce.multicloudj.common.exceptions.DeadlineExceededException;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.iam.model.Action;
import com.salesforce.multicloudj.iam.model.AttachInlinePolicyRequest;
import com.salesforce.multicloudj.iam.model.ComputeActions;
import com.salesforce.multicloudj.iam.model.ConditionOperator;
import com.salesforce.multicloudj.iam.model.Effect;
import com.salesforce.multicloudj.iam.model.GetAttachedPoliciesRequest;
import com.salesforce.multicloudj.iam.model.GetInlinePolicyDetailsRequest;
import com.salesforce.multicloudj.iam.model.IamActions;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.RemovePolicyRequest;
import com.salesforce.multicloudj.iam.model.Statement;
import com.salesforce.multicloudj.iam.model.StorageActions;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class GcpIamTest {

  @Mock private ProjectsClient mockProjectsClient;
  @Mock private IAMClient mockIamClient;
  private GcpIam gcpIam;
  private static final String TEST_TENANT_ID = "projects/test-project";
  private static final String TEST_REGION = "us-west1";
  private static final String TEST_SERVICE_ACCOUNT =
      "serviceAccount:test-sa@test-project.iam.gserviceaccount.com";
  private static final String TEST_ROLE = "roles/iam.serviceAccountUser";

  private static final String TEST_PROJECT_ID = "test-project-123";
  private static final String TEST_IDENTITY_NAME = "test-service-account";
  private static final String TEST_DESCRIPTION = "Test service account description";
  private static final String TEST_SERVICE_ACCOUNT_EMAIL =
      "test-service-account@test-project-123.iam.gserviceaccount.com";
  private static final String TEST_SERVICE_ACCOUNT_NAME =
      "projects/test-project-123/serviceAccounts/"
          + "test-service-account@test-project-123.iam.gserviceaccount.com";

  @BeforeEach
  void setUp() {
    gcpIam =
        new GcpIam.Builder()
            .withProjectsClient(mockProjectsClient)
            .withIamClient(mockIamClient)
            .build();
  }

  @Test
  void testConstructorWithBuilderAndProjectsClient() {
    GcpIam iam =
        new GcpIam.Builder()
            .withProjectsClient(mockProjectsClient)
            .withIamClient(mockIamClient)
            .build();
    Assertions.assertNotNull(iam);
    assertEquals("gcp", iam.getProviderId());
  }

  @Test
  void testConstructorWithNullClients() throws IOException {
    // ProjectsClient can be null - it will be created in Builder.build() if not provided
    try (MockedStatic<ProjectsClient> mockedProjectsClient = mockStatic(ProjectsClient.class);
        MockedStatic<IAMClient> mockedIamClient = mockStatic(IAMClient.class)) {
      ProjectsClient mockClient = mock(ProjectsClient.class);
      IAMClient mockIamClient = mock(IAMClient.class);
      mockedProjectsClient.when(ProjectsClient::create).thenReturn(mockClient);
      mockedIamClient.when(IAMClient::create).thenReturn(mockIamClient);

      Assertions.assertDoesNotThrow(
          () -> {
            new GcpIam.Builder().build();
          });
    }
  }

  @Test
  void testConstructorWithBuilder() throws IOException {
    try (MockedStatic<ProjectsClient> mockedProjectsClient = mockStatic(ProjectsClient.class);
        MockedStatic<IAMClient> mockedIamClient = mockStatic(IAMClient.class)) {
      ProjectsClient mockProjectsClient = mock(ProjectsClient.class);
      IAMClient mockIamClient = mock(IAMClient.class);
      mockedProjectsClient.when(ProjectsClient::create).thenReturn(mockProjectsClient);
      mockedIamClient.when(IAMClient::create).thenReturn(mockIamClient);

      GcpIam iam = new GcpIam(new GcpIam.Builder());
      Assertions.assertNotNull(iam);
      assertEquals("gcp", iam.getProviderId());
    }
  }

  @Test
  void testDoAttachInlinePolicySuccess() {
    // Setup: Create a policy with existing bindings
    Policy existingPolicy =
        Policy.newBuilder()
            .addBindings(
                Binding.newBuilder()
                    .setRole("roles/storage.objectViewer")
                    .addMembers("serviceAccount:other@test-project.iam.gserviceaccount.com")
                    .build())
            .build();

    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class)))
        .thenReturn(existingPolicy);
    when(mockProjectsClient.setIamPolicy(any(SetIamPolicyRequest.class)))
        .thenReturn(existingPolicy);

    // Create policy document with substrate-neutral action
    PolicyDocument policyDocument =
        PolicyDocument.builder()
            .name("TestPolicy")
            .version("2024-01-01")
            .statement(
                Statement.builder()
                    .sid("TestPolicy")
                    .effect(Effect.ALLOW)
                    .action(IamActions.ASSUME_ROLE) // Translates to roles/iam.serviceAccountUser
                    .build())
            .build();

    // Execute
    AttachInlinePolicyRequest request =
        AttachInlinePolicyRequest.builder()
            .policyDocument(policyDocument)
            .tenantId(TEST_TENANT_ID)
            .region(TEST_REGION)
            .identityName(TEST_SERVICE_ACCOUNT)
            .build();
    Assertions.assertDoesNotThrow(() -> gcpIam.doAttachInlinePolicy(request));

    // Verify
    verify(mockProjectsClient, times(1)).getIamPolicy(any(GetIamPolicyRequest.class));
    ArgumentCaptor<SetIamPolicyRequest> setRequestCaptor =
        ArgumentCaptor.forClass(SetIamPolicyRequest.class);
    verify(mockProjectsClient, times(1)).setIamPolicy(setRequestCaptor.capture());

    SetIamPolicyRequest setRequest = setRequestCaptor.getValue();
    assertEquals(TEST_TENANT_ID, setRequest.getResource());
    Policy updatedPolicy = setRequest.getPolicy();
    Assertions.assertNotNull(updatedPolicy);

    // Verify the new binding was added (translated role)
    boolean foundBinding = false;
    for (Binding binding : updatedPolicy.getBindingsList()) {
      if (binding.getRole().equals("roles/iam.serviceAccountUser")) {
        assertTrue(binding.getMembersList().contains(TEST_SERVICE_ACCOUNT));
        foundBinding = true;
      }
    }
    assertTrue(foundBinding, "New binding should be added");
  }

  @Test
  void testDoAttachInlinePolicyWithNullPolicy() {
    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(null);
    when(mockProjectsClient.setIamPolicy(any(SetIamPolicyRequest.class)))
        .thenReturn(Policy.newBuilder().build());

    PolicyDocument policyDocument =
        PolicyDocument.builder()
            .name("TestPolicy")
            .version("2024-01-01")
            .statement(
                Statement.builder()
                    .sid("TestPolicy")
                    .effect(Effect.ALLOW)
                    .action(StorageActions.GET_OBJECT) // Translates to roles/storage.objectViewer
                    .build())
            .build();

    AttachInlinePolicyRequest request =
        AttachInlinePolicyRequest.builder()
            .policyDocument(policyDocument)
            .tenantId(TEST_TENANT_ID)
            .region(TEST_REGION)
            .identityName(TEST_SERVICE_ACCOUNT)
            .build();
    Assertions.assertDoesNotThrow(() -> gcpIam.doAttachInlinePolicy(request));

    verify(mockProjectsClient, times(1)).setIamPolicy(any(SetIamPolicyRequest.class));
  }

  @Test
  void testDoAttachInlinePolicyMergesExistingBinding() {
    // Setup: Create a policy with existing binding for the same role
    Policy existingPolicy =
        Policy.newBuilder()
            .addBindings(
                Binding.newBuilder()
                    .setRole("roles/iam.serviceAccountUser")
                    .addMembers("serviceAccount:existing@test-project.iam.gserviceaccount.com")
                    .build())
            .build();

    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class)))
        .thenReturn(existingPolicy);
    when(mockProjectsClient.setIamPolicy(any(SetIamPolicyRequest.class)))
        .thenReturn(existingPolicy);

    PolicyDocument policyDocument =
        PolicyDocument.builder()
            .name("TestPolicy")
            .version("2024-01-01")
            .statement(
                Statement.builder()
                    .sid("TestPolicy")
                    .effect(Effect.ALLOW)
                    .action(IamActions.ASSUME_ROLE) // Translates to roles/iam.serviceAccountUser
                    .build())
            .build();

    AttachInlinePolicyRequest request =
        AttachInlinePolicyRequest.builder()
            .policyDocument(policyDocument)
            .tenantId(TEST_TENANT_ID)
            .region(TEST_REGION)
            .identityName(TEST_SERVICE_ACCOUNT)
            .build();
    Assertions.assertDoesNotThrow(() -> gcpIam.doAttachInlinePolicy(request));

    ArgumentCaptor<SetIamPolicyRequest> setRequestCaptor =
        ArgumentCaptor.forClass(SetIamPolicyRequest.class);
    verify(mockProjectsClient, times(1)).setIamPolicy(setRequestCaptor.capture());

    Policy updatedPolicy = setRequestCaptor.getValue().getPolicy();
    Binding updatedBinding =
        updatedPolicy.getBindingsList().stream()
            .filter(b -> b.getRole().equals("roles/iam.serviceAccountUser"))
            .findFirst()
            .orElse(null);

    Assertions.assertNotNull(updatedBinding);
    assertEquals(2, updatedBinding.getMembersCount(), "Should have both existing and new members");
    assertTrue(updatedBinding.getMembersList().contains(TEST_SERVICE_ACCOUNT));
  }

  @Test
  void testDoAttachInlinePolicySkipsDenyStatements() {
    Policy existingPolicy = Policy.newBuilder().build();
    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class)))
        .thenReturn(existingPolicy);
    when(mockProjectsClient.setIamPolicy(any(SetIamPolicyRequest.class)))
        .thenReturn(existingPolicy);

    PolicyDocument policyDocument =
        PolicyDocument.builder()
            .name("DenyPolicy")
            .version("2024-01-01")
            .statement(
                Statement.builder()
                    .sid("DenyPolicy")
                    .effect(Effect.DENY)
                    .action(StorageActions.GET_OBJECT)
                    .build())
            .statement(
                Statement.builder()
                    .sid("AllowPolicy")
                    .effect(Effect.ALLOW)
                    .action(StorageActions.PUT_OBJECT)
                    .build())
            .build();

    // Verify: Deny statements are skipped, Allow statements are processed
    AttachInlinePolicyRequest request =
        AttachInlinePolicyRequest.builder()
            .policyDocument(policyDocument)
            .tenantId(TEST_TENANT_ID)
            .region(TEST_REGION)
            .identityName(TEST_SERVICE_ACCOUNT)
            .build();
    Assertions.assertDoesNotThrow(() -> gcpIam.doAttachInlinePolicy(request));

    verify(mockProjectsClient, times(1)).getIamPolicy(any(GetIamPolicyRequest.class));
    verify(mockProjectsClient, times(1)).setIamPolicy(any(SetIamPolicyRequest.class));

    // Verify the policy was updated with only the Allow statement's role
    ArgumentCaptor<SetIamPolicyRequest> setRequestCaptor =
        ArgumentCaptor.forClass(SetIamPolicyRequest.class);
    verify(mockProjectsClient, times(1)).setIamPolicy(setRequestCaptor.capture());

    SetIamPolicyRequest setRequest = setRequestCaptor.getValue();
    Policy updatedPolicy = setRequest.getPolicy();

    // Should have only one binding for the Allow statement's role
    assertEquals(1, updatedPolicy.getBindingsCount());
    assertEquals("roles/storage.objectCreator", updatedPolicy.getBindings(0).getRole());
    assertTrue(updatedPolicy.getBindings(0).getMembersList().contains(TEST_SERVICE_ACCOUNT));
  }

  @Test
  void testGetExceptionWithApiException() {
    // Test various status codes
    assertExceptionMapping(StatusCode.Code.CANCELLED, UnknownException.class);
    assertExceptionMapping(StatusCode.Code.UNKNOWN, UnknownException.class);
    assertExceptionMapping(StatusCode.Code.INVALID_ARGUMENT, InvalidArgumentException.class);
    assertExceptionMapping(StatusCode.Code.DEADLINE_EXCEEDED, DeadlineExceededException.class);
    assertExceptionMapping(StatusCode.Code.NOT_FOUND, ResourceNotFoundException.class);
    assertExceptionMapping(StatusCode.Code.ALREADY_EXISTS, ResourceAlreadyExistsException.class);
    assertExceptionMapping(StatusCode.Code.PERMISSION_DENIED, UnAuthorizedException.class);
    assertExceptionMapping(StatusCode.Code.RESOURCE_EXHAUSTED, ResourceExhaustedException.class);
    assertExceptionMapping(StatusCode.Code.FAILED_PRECONDITION, FailedPreconditionException.class);
    assertExceptionMapping(StatusCode.Code.ABORTED, DeadlineExceededException.class);
    assertExceptionMapping(StatusCode.Code.OUT_OF_RANGE, InvalidArgumentException.class);
    assertExceptionMapping(StatusCode.Code.UNIMPLEMENTED, UnSupportedOperationException.class);
    assertExceptionMapping(StatusCode.Code.INTERNAL, UnknownException.class);
    assertExceptionMapping(StatusCode.Code.UNAVAILABLE, UnknownException.class);
    assertExceptionMapping(StatusCode.Code.DATA_LOSS, UnknownException.class);
    assertExceptionMapping(StatusCode.Code.UNAUTHENTICATED, UnAuthorizedException.class);
  }

  @Test
  void testGetExceptionWithNonApiException() {
    Class<? extends SubstrateSdkException> exceptionClass =
        gcpIam.getException(new RuntimeException("Test error"));
    assertEquals(UnknownException.class, exceptionClass);
  }

  @Test
  void testBuilder() {
    GcpIam.Builder builder = gcpIam.builder();
    Assertions.assertNotNull(builder);
    Assertions.assertInstanceOf(GcpIam.Builder.class, builder);
  }

  @Test
  void testDoGetAttachedPoliciesSuccess() {
    // Setup: Create a policy with multiple bindings, some including our service account
    Policy policy =
        Policy.newBuilder()
            .addBindings(
                Binding.newBuilder()
                    .setRole("roles/iam.serviceAccountUser")
                    .addMembers(TEST_SERVICE_ACCOUNT)
                    .addMembers("serviceAccount:other@test-project.iam.gserviceaccount.com")
                    .build())
            .addBindings(
                Binding.newBuilder()
                    .setRole("roles/storage.objectViewer")
                    .addMembers(TEST_SERVICE_ACCOUNT)
                    .build())
            .addBindings(
                Binding.newBuilder()
                    .setRole("roles/compute.instanceAdmin")
                    .addMembers("serviceAccount:another@test-project.iam.gserviceaccount.com")
                    .build())
            .build();

    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(policy);

    // Execute
    List<String> result =
        gcpIam.doGetAttachedPolicies(
            GetAttachedPoliciesRequest.builder()
                .identityName(TEST_SERVICE_ACCOUNT)
                .tenantId(TEST_TENANT_ID)
                .region(TEST_REGION)
                .build());

    // Verify
    Assertions.assertNotNull(result);
    assertEquals(2, result.size());
    assertTrue(result.contains("roles/iam.serviceAccountUser"));
    assertTrue(result.contains("roles/storage.objectViewer"));
    Assertions.assertFalse(result.contains("roles/compute.instanceAdmin"));

    ArgumentCaptor<GetIamPolicyRequest> requestCaptor =
        ArgumentCaptor.forClass(GetIamPolicyRequest.class);
    verify(mockProjectsClient, times(1)).getIamPolicy(requestCaptor.capture());
    assertEquals(TEST_TENANT_ID, requestCaptor.getValue().getResource());
  }

  @Test
  void testDoGetAttachedPoliciesWithNoAttachedRoles() {
    // Setup: Create a policy with bindings, but none for our service account
    Policy policy =
        Policy.newBuilder()
            .addBindings(
                Binding.newBuilder()
                    .setRole("roles/iam.serviceAccountUser")
                    .addMembers("serviceAccount:other@test-project.iam.gserviceaccount.com")
                    .build())
            .build();

    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(policy);

    // Execute
    List<String> result =
        gcpIam.doGetAttachedPolicies(
            GetAttachedPoliciesRequest.builder()
                .identityName(TEST_SERVICE_ACCOUNT)
                .tenantId(TEST_TENANT_ID)
                .region(TEST_REGION)
                .build());

    // Verify
    Assertions.assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  void testDoGetAttachedPoliciesWithNullPolicy() {
    // Setup: Return null policy
    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(null);

    // Execute
    List<String> result =
        gcpIam.doGetAttachedPolicies(
            GetAttachedPoliciesRequest.builder()
                .identityName(TEST_SERVICE_ACCOUNT)
                .tenantId(TEST_TENANT_ID)
                .region(TEST_REGION)
                .build());

    // Verify
    Assertions.assertNotNull(result);
    assertTrue(result.isEmpty());
    verify(mockProjectsClient, times(1)).getIamPolicy(any(GetIamPolicyRequest.class));
  }

  @Test
  void testDoGetAttachedPoliciesWithEmptyPolicy() {
    // Setup: Create an empty policy
    Policy policy = Policy.newBuilder().build();

    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(policy);

    // Execute
    List<String> result =
        gcpIam.doGetAttachedPolicies(
            GetAttachedPoliciesRequest.builder()
                .identityName(TEST_SERVICE_ACCOUNT)
                .tenantId(TEST_TENANT_ID)
                .region(TEST_REGION)
                .build());

    // Verify
    Assertions.assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  void testDoGetAttachedPoliciesWithSingleRole() {
    // Setup: Service account has exactly one role
    Policy policy =
        Policy.newBuilder()
            .addBindings(
                Binding.newBuilder()
                    .setRole("roles/storage.objectViewer")
                    .addMembers(TEST_SERVICE_ACCOUNT)
                    .build())
            .build();
    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(policy);

    // Execute
    List<String> result =
        gcpIam.doGetAttachedPolicies(
            GetAttachedPoliciesRequest.builder()
                .identityName(TEST_SERVICE_ACCOUNT)
                .tenantId(TEST_TENANT_ID)
                .region(TEST_REGION)
                .build());

    // Verify
    Assertions.assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals("roles/storage.objectViewer", result.get(0));
  }

  @Test
  void testClose() throws Exception {
    gcpIam.close();
    verify(mockProjectsClient, times(1)).close();
  }

  @Test
  void testCloseWithException() throws Exception {
    Mockito.doThrow(new IOException("Close failed")).when(mockProjectsClient).close();
    assertThrows(IOException.class, () -> gcpIam.close());
    verify(mockProjectsClient, times(1)).close();
  }

  @Test
  void testDoGetAttachedPoliciesWithApiException() {
    // Setup: Mock API exception
    ApiException apiException = mock(ApiException.class);
    StatusCode mockStatusCode = mock(StatusCode.class);
    when(apiException.getStatusCode()).thenReturn(mockStatusCode);
    when(mockStatusCode.getCode()).thenReturn(StatusCode.Code.PERMISSION_DENIED);
    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenThrow(apiException);

    // Execute and verify exception is thrown
    assertThrows(
        ApiException.class,
        () -> {
          gcpIam.doGetAttachedPolicies(
              GetAttachedPoliciesRequest.builder()
                  .identityName(TEST_SERVICE_ACCOUNT)
                  .tenantId(TEST_TENANT_ID)
                  .region(TEST_REGION)
                  .build());
        });

    // Verify that the exception would be mapped correctly
    Class<? extends SubstrateSdkException> mappedException = gcpIam.getException(apiException);
    assertEquals(UnAuthorizedException.class, mappedException);
  }

  @Test
  void testDoRemovePolicySuccess() {
    // Setup: Create a policy with a binding containing multiple members
    Policy originalPolicy =
        Policy.newBuilder()
            .addBindings(
                Binding.newBuilder()
                    .setRole(TEST_ROLE)
                    .addMembers(TEST_SERVICE_ACCOUNT)
                    .addMembers("serviceAccount:other@test-project.iam.gserviceaccount.com")
                    .build())
            .addBindings(
                Binding.newBuilder()
                    .setRole("roles/storage.objectViewer")
                    .addMembers(TEST_SERVICE_ACCOUNT)
                    .build())
            .build();

    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class)))
        .thenReturn(originalPolicy);

    // Execute
    gcpIam.doRemovePolicy(
        RemovePolicyRequest.builder()
            .identityName(TEST_SERVICE_ACCOUNT)
            .policyName(TEST_ROLE)
            .tenantId(TEST_TENANT_ID)
            .region(TEST_REGION)
            .build());

    // Verify: getIamPolicy was called once
    ArgumentCaptor<GetIamPolicyRequest> getRequestCaptor =
        ArgumentCaptor.forClass(GetIamPolicyRequest.class);
    verify(mockProjectsClient, times(1)).getIamPolicy(getRequestCaptor.capture());
    assertEquals(TEST_TENANT_ID, getRequestCaptor.getValue().getResource());

    // Verify: setIamPolicy was called with updated policy
    ArgumentCaptor<SetIamPolicyRequest> setRequestCaptor =
        ArgumentCaptor.forClass(SetIamPolicyRequest.class);
    verify(mockProjectsClient, times(1)).setIamPolicy(setRequestCaptor.capture());
    SetIamPolicyRequest setRequest = setRequestCaptor.getValue();
    assertEquals(TEST_TENANT_ID, setRequest.getResource());

    // Verify: The member was removed from the binding, but binding still exists
    Policy updatedPolicy = setRequest.getPolicy();
    Binding updatedBinding =
        updatedPolicy.getBindingsList().stream()
            .filter(b -> b.getRole().equals(TEST_ROLE))
            .findFirst()
            .orElse(null);
    Assertions.assertNotNull(updatedBinding, "Binding should still exist");
    Assertions.assertFalse(
        updatedBinding.getMembersList().contains(TEST_SERVICE_ACCOUNT),
        "Service account should be removed from binding");
    assertTrue(
        updatedBinding
            .getMembersList()
            .contains("serviceAccount:other@test-project.iam.gserviceaccount.com"),
        "Other member should remain in binding");

    // Verify: Other binding with the same service account should remain unchanged
    Binding otherBinding =
        updatedPolicy.getBindingsList().stream()
            .filter(b -> b.getRole().equals("roles/storage.objectViewer"))
            .findFirst()
            .orElse(null);
    Assertions.assertNotNull(otherBinding, "Other binding should still exist");
    assertTrue(
        otherBinding.getMembersList().contains(TEST_SERVICE_ACCOUNT),
        "Service account should remain in other binding");
  }

  @Test
  void testDoRemovePolicyRemovesEmptyBinding() {
    // Setup: Create a policy with a binding containing only our service account
    Policy originalPolicy =
        Policy.newBuilder()
            .addBindings(
                Binding.newBuilder().setRole(TEST_ROLE).addMembers(TEST_SERVICE_ACCOUNT).build())
            .addBindings(
                Binding.newBuilder()
                    .setRole("roles/storage.objectViewer")
                    .addMembers("serviceAccount:other@test-project.iam.gserviceaccount.com")
                    .build())
            .build();

    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class)))
        .thenReturn(originalPolicy);

    // Execute
    gcpIam.doRemovePolicy(
        RemovePolicyRequest.builder()
            .identityName(TEST_SERVICE_ACCOUNT)
            .policyName(TEST_ROLE)
            .tenantId(TEST_TENANT_ID)
            .region(TEST_REGION)
            .build());

    // Verify: setIamPolicy was called
    ArgumentCaptor<SetIamPolicyRequest> setRequestCaptor =
        ArgumentCaptor.forClass(SetIamPolicyRequest.class);
    verify(mockProjectsClient, times(1)).setIamPolicy(setRequestCaptor.capture());
    Policy updatedPolicy = setRequestCaptor.getValue().getPolicy();

    // Verify: The binding should be completely removed since it's now empty
    boolean bindingExists =
        updatedPolicy.getBindingsList().stream().anyMatch(b -> b.getRole().equals(TEST_ROLE));
    Assertions.assertFalse(bindingExists, "Empty binding should be removed");

    // Verify: Other binding should remain
    boolean otherBindingExists =
        updatedPolicy.getBindingsList().stream()
            .anyMatch(b -> b.getRole().equals("roles/storage.objectViewer"));
    assertTrue(otherBindingExists, "Other binding should remain");
  }

  @Test
  void testDoRemovePolicyWithNonExistentBinding() {
    // Setup: Create a policy without the binding we're trying to remove
    Policy originalPolicy =
        Policy.newBuilder()
            .addBindings(
                Binding.newBuilder()
                    .setRole("roles/storage.objectViewer")
                    .addMembers(TEST_SERVICE_ACCOUNT)
                    .build())
            .build();

    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class)))
        .thenReturn(originalPolicy);

    // Execute
    gcpIam.doRemovePolicy(
        RemovePolicyRequest.builder()
            .identityName(TEST_SERVICE_ACCOUNT)
            .policyName(TEST_ROLE)
            .tenantId(TEST_TENANT_ID)
            .region(TEST_REGION)
            .build());

    // Verify: getIamPolicy was called
    verify(mockProjectsClient, times(1)).getIamPolicy(any(GetIamPolicyRequest.class));

    // Verify: setIamPolicy should not be called since binding doesn't exist (nothing to remove)
    verify(mockProjectsClient, times(0)).setIamPolicy(any(SetIamPolicyRequest.class));
  }

  @Test
  void testDoRemovePolicyWithMemberNotInBinding() {
    // Setup: Create a policy with the binding, but our service account is not a member
    Policy originalPolicy =
        Policy.newBuilder()
            .addBindings(
                Binding.newBuilder()
                    .setRole(TEST_ROLE)
                    .addMembers("serviceAccount:other@test-project.iam.gserviceaccount.com")
                    .build())
            .build();

    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class)))
        .thenReturn(originalPolicy);

    // Execute
    gcpIam.doRemovePolicy(
        RemovePolicyRequest.builder()
            .identityName(TEST_SERVICE_ACCOUNT)
            .policyName(TEST_ROLE)
            .tenantId(TEST_TENANT_ID)
            .region(TEST_REGION)
            .build());

    // Verify: setIamPolicy should not be called since member is not in binding (nothing to remove)
    verify(mockProjectsClient, times(0)).setIamPolicy(any(SetIamPolicyRequest.class));
  }

  @Test
  void testDoRemovePolicyWithNullPolicy() {
    // Setup: Return null policy
    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(null);

    // Execute
    gcpIam.doRemovePolicy(
        RemovePolicyRequest.builder()
            .identityName(TEST_SERVICE_ACCOUNT)
            .policyName(TEST_ROLE)
            .tenantId(TEST_TENANT_ID)
            .region(TEST_REGION)
            .build());

    // Verify: getIamPolicy was called
    verify(mockProjectsClient, times(1)).getIamPolicy(any(GetIamPolicyRequest.class));

    // Verify: setIamPolicy should not be called when policy is null
    verify(mockProjectsClient, times(0)).setIamPolicy(any(SetIamPolicyRequest.class));
  }

  @Test
  void testDoRemovePolicyWithApiExceptionOnGet() {
    // Setup: Mock API exception on getIamPolicy
    ApiException apiException = mock(ApiException.class);
    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenThrow(apiException);

    // Execute and verify exception is thrown
    assertThrows(
        ApiException.class,
        () -> {
          gcpIam.doRemovePolicy(
              RemovePolicyRequest.builder()
                  .identityName(TEST_SERVICE_ACCOUNT)
                  .policyName(TEST_ROLE)
                  .tenantId(TEST_TENANT_ID)
                  .region(TEST_REGION)
                  .build());
        });

    // Verify: setIamPolicy should not be called
    verify(mockProjectsClient, times(0)).setIamPolicy(any(SetIamPolicyRequest.class));
  }

  @Test
  void testDoRemovePolicyWithApiExceptionOnSet() {
    // Setup: Create a policy and mock exception on setIamPolicy
    Policy originalPolicy =
        Policy.newBuilder()
            .addBindings(
                Binding.newBuilder().setRole(TEST_ROLE).addMembers(TEST_SERVICE_ACCOUNT).build())
            .build();

    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class)))
        .thenReturn(originalPolicy);

    ApiException apiException = mock(ApiException.class);
    when(mockProjectsClient.setIamPolicy(any(SetIamPolicyRequest.class))).thenThrow(apiException);

    // Execute and verify exception is thrown
    assertThrows(
        ApiException.class,
        () -> {
          gcpIam.doRemovePolicy(
              RemovePolicyRequest.builder()
                  .identityName(TEST_SERVICE_ACCOUNT)
                  .policyName(TEST_ROLE)
                  .tenantId(TEST_TENANT_ID)
                  .region(TEST_REGION)
                  .build());
        });
  }

  @Test
  void testDoGetInlinePolicyDetailsSuccess() {
    // Setup: Create a policy with a binding for our service account
    Policy originalPolicy =
        Policy.newBuilder()
            .addBindings(
                Binding.newBuilder()
                    .setRole(TEST_ROLE)
                    .addMembers(TEST_SERVICE_ACCOUNT)
                    .addMembers("serviceAccount:other@test-project.iam.gserviceaccount.com")
                    .build())
            .build();

    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class)))
        .thenReturn(originalPolicy);

    // Execute
    String result =
        gcpIam.doGetInlinePolicyDetails(
            GetInlinePolicyDetailsRequest.builder()
                .identityName(TEST_SERVICE_ACCOUNT)
                .roleName(TEST_ROLE)
                .tenantId(TEST_TENANT_ID)
                .region(TEST_REGION)
                .build());

    // Verify
    Assertions.assertNotNull(result);
    assertTrue(result.contains("\"version\""));
    assertTrue(result.contains("\"statements\""));
    assertTrue(result.contains("\"effect\":\"Allow\""));
    assertTrue(result.contains("\"actions\""));
    assertTrue(result.contains(TEST_ROLE));

    // Verify getIamPolicy was called
    ArgumentCaptor<GetIamPolicyRequest> requestCaptor =
        ArgumentCaptor.forClass(GetIamPolicyRequest.class);
    verify(mockProjectsClient, times(1)).getIamPolicy(requestCaptor.capture());
    assertEquals(TEST_TENANT_ID, requestCaptor.getValue().getResource());
  }

  @Test
  void testDoGetInlinePolicyDetailsWithNonExistentBinding() {
    // Setup: Create a policy without the binding we're looking for
    Policy originalPolicy =
        Policy.newBuilder()
            .addBindings(
                Binding.newBuilder()
                    .setRole("roles/storage.objectViewer")
                    .addMembers(TEST_SERVICE_ACCOUNT)
                    .build())
            .build();

    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class)))
        .thenReturn(originalPolicy);

    // Execute
    String result =
        gcpIam.doGetInlinePolicyDetails(
            GetInlinePolicyDetailsRequest.builder()
                .identityName(TEST_SERVICE_ACCOUNT)
                .roleName(TEST_ROLE)
                .tenantId(TEST_TENANT_ID)
                .region(TEST_REGION)
                .build());

    // Verify: Should return null when binding doesn't exist
    Assertions.assertNull(result);
  }

  @Test
  void testDoGetInlinePolicyDetailsWithMemberNotInBinding() {
    // Setup: Create a policy with the binding, but our service account is not a member
    Policy originalPolicy =
        Policy.newBuilder()
            .addBindings(
                Binding.newBuilder()
                    .setRole(TEST_ROLE)
                    .addMembers("serviceAccount:other@test-project.iam.gserviceaccount.com")
                    .build())
            .build();

    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class)))
        .thenReturn(originalPolicy);

    // Execute
    String result =
        gcpIam.doGetInlinePolicyDetails(
            GetInlinePolicyDetailsRequest.builder()
                .identityName(TEST_SERVICE_ACCOUNT)
                .roleName(TEST_ROLE)
                .tenantId(TEST_TENANT_ID)
                .region(TEST_REGION)
                .build());

    // Verify: Should return null when service account is not in binding
    Assertions.assertNull(result);
  }

  @Test
  void testDoGetInlinePolicyDetailsWithNullPolicy() {
    // Setup: Return null policy
    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(null);

    // Execute
    String result =
        gcpIam.doGetInlinePolicyDetails(
            GetInlinePolicyDetailsRequest.builder()
                .identityName(TEST_SERVICE_ACCOUNT)
                .roleName(TEST_ROLE)
                .tenantId(TEST_TENANT_ID)
                .region(TEST_REGION)
                .build());

    // Verify: Should return null when policy is null
    Assertions.assertNull(result);
  }

  @Test
  void testDoGetInlinePolicyDetailsWithApiException() {
    // Setup: Mock API exception
    ApiException apiException = mock(ApiException.class);
    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenThrow(apiException);

    // Execute and verify exception is thrown
    Assertions.assertThrows(
        ApiException.class,
        () -> {
          gcpIam.doGetInlinePolicyDetails(
              GetInlinePolicyDetailsRequest.builder()
                  .identityName(TEST_SERVICE_ACCOUNT)
                  .roleName(TEST_ROLE)
                  .tenantId(TEST_TENANT_ID)
                  .region(TEST_REGION)
                  .build());
        });
  }

  @Test
  void testDoGetInlinePolicyDetailsWithNullRoleName() {
    // Execute and verify InvalidArgumentException is thrown when roleName is null
    Assertions.assertThrows(
        InvalidArgumentException.class,
        () -> {
          gcpIam.doGetInlinePolicyDetails(
              GetInlinePolicyDetailsRequest.builder()
                  .identityName(TEST_SERVICE_ACCOUNT)
                  .tenantId(TEST_TENANT_ID)
                  .region(TEST_REGION)
                  .build());
        });
  }

  @Test
  void testDoGetInlinePolicyDetailsWithEmptyRoleName() {
    // Execute and verify InvalidArgumentException is thrown when roleName is empty
    Assertions.assertThrows(
        InvalidArgumentException.class,
        () -> {
          gcpIam.doGetInlinePolicyDetails(
              GetInlinePolicyDetailsRequest.builder()
                  .identityName(TEST_SERVICE_ACCOUNT)
                  .roleName("")
                  .tenantId(TEST_TENANT_ID)
                  .region(TEST_REGION)
                  .build());
        });
  }

  private void assertExceptionMapping(
      StatusCode.Code statusCode, Class<? extends SubstrateSdkException> expectedExceptionClass) {
    ApiException apiException = mock(ApiException.class);
    StatusCode mockStatusCode = mock(StatusCode.class);
    when(apiException.getStatusCode()).thenReturn(mockStatusCode);
    when(mockStatusCode.getCode()).thenReturn(statusCode);

    Class<? extends SubstrateSdkException> actualExceptionClass = gcpIam.getException(apiException);
    assertEquals(
        expectedExceptionClass,
        actualExceptionClass,
        "Expected " + expectedExceptionClass.getSimpleName() + " for status code " + statusCode);
  }

  @Test
  void testCreateIdentityWithoutTrustConfig() {
    // Arrange
    ServiceAccount mockServiceAccount =
        ServiceAccount.newBuilder()
            .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
            .setName(TEST_SERVICE_ACCOUNT_NAME)
            .setDisplayName(TEST_IDENTITY_NAME)
            .setDescription(TEST_DESCRIPTION)
            .build();

    when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
        .thenReturn(mockServiceAccount);

    // Act
    String result =
        gcpIam.createIdentity(
            TEST_IDENTITY_NAME,
            TEST_DESCRIPTION,
            TEST_PROJECT_ID,
            TEST_REGION,
            Optional.empty(),
            Optional.empty());

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
    ServiceAccount mockServiceAccount =
        ServiceAccount.newBuilder()
            .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
            .setName(TEST_SERVICE_ACCOUNT_NAME)
            .setDisplayName(TEST_IDENTITY_NAME)
            .setDescription(TEST_DESCRIPTION)
            .build();

    when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
        .thenReturn(mockServiceAccount);

    // Act - pass tenantId with "projects/" prefix
    String result =
        gcpIam.createIdentity(
            TEST_IDENTITY_NAME,
            TEST_DESCRIPTION,
            "projects/" + TEST_PROJECT_ID,
            TEST_REGION,
            Optional.empty(),
            Optional.empty());

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
    ServiceAccount mockServiceAccount =
        ServiceAccount.newBuilder()
            .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
            .setName(TEST_SERVICE_ACCOUNT_NAME)
            .setDisplayName(TEST_IDENTITY_NAME)
            .build();

    when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
        .thenReturn(mockServiceAccount);

    // Act
    String result =
        gcpIam.createIdentity(
            TEST_IDENTITY_NAME,
            null,
            TEST_PROJECT_ID,
            TEST_REGION,
            Optional.empty(),
            Optional.empty());

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
    TrustConfiguration trustConfig =
        TrustConfiguration.builder().addTrustedPrincipal(trustedPrincipal).build();

    ServiceAccount mockServiceAccount =
        ServiceAccount.newBuilder()
            .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
            .setName(TEST_SERVICE_ACCOUNT_NAME)
            .setDisplayName(TEST_IDENTITY_NAME)
            .setDescription(TEST_DESCRIPTION)
            .build();

    Policy mockPolicy = Policy.newBuilder().build();

    when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
        .thenReturn(mockServiceAccount);
    when(mockIamClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(mockPolicy);
    when(mockIamClient.setIamPolicy(any(SetIamPolicyRequest.class))).thenReturn(mockPolicy);

    // Act
    String result =
        gcpIam.createIdentity(
            TEST_IDENTITY_NAME,
            TEST_DESCRIPTION,
            TEST_PROJECT_ID,
            TEST_REGION,
            Optional.of(trustConfig),
            Optional.empty());

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

    TrustConfiguration trustConfig =
        TrustConfiguration.builder()
            .addTrustedPrincipal(principal1)
            .addTrustedPrincipal(principal2)
            .addTrustedPrincipal(principal3)
            .build();

    ServiceAccount mockServiceAccount =
        ServiceAccount.newBuilder()
            .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
            .setName(TEST_SERVICE_ACCOUNT_NAME)
            .setDisplayName(TEST_IDENTITY_NAME)
            .setDescription(TEST_DESCRIPTION)
            .build();

    Policy mockPolicy = Policy.newBuilder().build();

    when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
        .thenReturn(mockServiceAccount);
    when(mockIamClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(mockPolicy);
    when(mockIamClient.setIamPolicy(any(SetIamPolicyRequest.class))).thenReturn(mockPolicy);

    // Act
    String result =
        gcpIam.createIdentity(
            TEST_IDENTITY_NAME,
            TEST_DESCRIPTION,
            TEST_PROJECT_ID,
            TEST_REGION,
            Optional.of(trustConfig),
            Optional.empty());

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
    assertTrue(
        binding.getMembersList().contains(principal3)); // Already formatted, should remain as-is
  }

  @Test
  void testCreateIdentityWithTrustConfigEmptyPrincipals() {
    // Arrange
    TrustConfiguration trustConfig = TrustConfiguration.builder().build();

    ServiceAccount mockServiceAccount =
        ServiceAccount.newBuilder()
            .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
            .setName(TEST_SERVICE_ACCOUNT_NAME)
            .setDisplayName(TEST_IDENTITY_NAME)
            .setDescription(TEST_DESCRIPTION)
            .build();

    when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
        .thenReturn(mockServiceAccount);

    // Act
    String result =
        gcpIam.createIdentity(
            TEST_IDENTITY_NAME,
            TEST_DESCRIPTION,
            TEST_PROJECT_ID,
            TEST_REGION,
            Optional.of(trustConfig),
            Optional.empty());

    // Assert
    assertEquals(TEST_SERVICE_ACCOUNT_EMAIL, result);

    // Verify service account creation
    verify(mockIamClient).createServiceAccount(any(CreateServiceAccountRequest.class));

    // Verify no IAM policy calls were made (empty principals list)
    verify(mockIamClient, never()).getIamPolicy(any(GetIamPolicyRequest.class));
    verify(mockIamClient, never()).setIamPolicy(any(SetIamPolicyRequest.class));
  }

  @Test
  void testCreateIdentityWithTrustConfigNullPolicy() {
    // Arrange
    String trustedPrincipal = "another-sa@test-project-123.iam.gserviceaccount.com";
    TrustConfiguration trustConfig =
        TrustConfiguration.builder().addTrustedPrincipal(trustedPrincipal).build();

    ServiceAccount mockServiceAccount =
        ServiceAccount.newBuilder()
            .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
            .setName(TEST_SERVICE_ACCOUNT_NAME)
            .setDisplayName(TEST_IDENTITY_NAME)
            .setDescription(TEST_DESCRIPTION)
            .build();

    Policy emptyPolicy = Policy.newBuilder().build();

    when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
        .thenReturn(mockServiceAccount);
    when(mockIamClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(null);
    when(mockIamClient.setIamPolicy(any(SetIamPolicyRequest.class))).thenReturn(emptyPolicy);

    // Act
    String result =
        gcpIam.createIdentity(
            TEST_IDENTITY_NAME,
            TEST_DESCRIPTION,
            TEST_PROJECT_ID,
            TEST_REGION,
            Optional.of(trustConfig),
            Optional.empty());

    // Assert
    assertEquals(TEST_SERVICE_ACCOUNT_EMAIL, result);

    // Verify that even with policy not found, we still set the policy
    verify(mockIamClient).getIamPolicy(any(GetIamPolicyRequest.class));
    verify(mockIamClient).setIamPolicy(any(SetIamPolicyRequest.class));
  }

  @Test
  void testCreateIdentityWithAlreadyExistsException() {
    // Arrange
    String trustedPrincipal = "another-sa@test-project-123.iam.gserviceaccount.com";
    TrustConfiguration trustConfig =
        TrustConfiguration.builder().addTrustedPrincipal(trustedPrincipal).build();

    ServiceAccount mockServiceAccount =
        ServiceAccount.newBuilder()
            .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
            .setName(TEST_SERVICE_ACCOUNT_NAME)
            .setDisplayName(TEST_IDENTITY_NAME)
            .setDescription(TEST_DESCRIPTION)
            .build();

    Policy mockPolicy = Policy.newBuilder().build();

    AlreadyExistsException alreadyExistsException = mock(AlreadyExistsException.class);

    when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
        .thenThrow(alreadyExistsException);
    when(mockIamClient.getServiceAccount(any(GetServiceAccountRequest.class)))
        .thenReturn(mockServiceAccount);
    when(mockIamClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(mockPolicy);
    when(mockIamClient.setIamPolicy(any(SetIamPolicyRequest.class))).thenReturn(mockPolicy);

    // Act
    String result =
        gcpIam.createIdentity(
            TEST_IDENTITY_NAME,
            TEST_DESCRIPTION,
            TEST_PROJECT_ID,
            TEST_REGION,
            Optional.of(trustConfig),
            Optional.empty());

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
  void testCreateIdentityApiExceptionDuringCreation() {
    // Arrange
    ApiException apiException = mock(ApiException.class);

    when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
        .thenThrow(apiException);

    // Act & Assert - Exception should propagate to IamClient for centralized handling
    ApiException exception =
        assertThrows(
            ApiException.class,
            () ->
                gcpIam.createIdentity(
                    TEST_IDENTITY_NAME,
                    TEST_DESCRIPTION,
                    TEST_PROJECT_ID,
                    TEST_REGION,
                    Optional.empty(),
                    Optional.empty()));

    assertEquals(apiException, exception);
  }

  @Test
  void testCreateIdentityApiExceptionDuringPolicySet() {
    // Arrange
    String trustedPrincipal = "another-sa@test-project-123.iam.gserviceaccount.com";
    TrustConfiguration trustConfig =
        TrustConfiguration.builder().addTrustedPrincipal(trustedPrincipal).build();

    ServiceAccount mockServiceAccount =
        ServiceAccount.newBuilder()
            .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
            .setName(TEST_SERVICE_ACCOUNT_NAME)
            .setDisplayName(TEST_IDENTITY_NAME)
            .setDescription(TEST_DESCRIPTION)
            .build();

    Policy mockPolicy = Policy.newBuilder().build();

    ApiException apiException = mock(ApiException.class);

    when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
        .thenReturn(mockServiceAccount);
    when(mockIamClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(mockPolicy);
    when(mockIamClient.setIamPolicy(any(SetIamPolicyRequest.class))).thenThrow(apiException);

    // Act & Assert
    ApiException exception =
        assertThrows(
            ApiException.class,
            () ->
                gcpIam.createIdentity(
                    TEST_IDENTITY_NAME,
                    TEST_DESCRIPTION,
                    TEST_PROJECT_ID,
                    TEST_REGION,
                    Optional.of(trustConfig),
                    Optional.empty()));

    // Verify exception is thrown without rollback
    assertNotNull(exception);
  }

  @Test
  void testFormatPrincipalAsMemberWithServiceAccountEmail() {
    // This tests the private method indirectly through createIdentity
    String principal = "test@project.iam.gserviceaccount.com";
    TrustConfiguration trustConfig =
        TrustConfiguration.builder().addTrustedPrincipal(principal).build();

    ServiceAccount mockServiceAccount =
        ServiceAccount.newBuilder()
            .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
            .setName(TEST_SERVICE_ACCOUNT_NAME)
            .build();

    Policy mockPolicy = Policy.newBuilder().build();

    when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
        .thenReturn(mockServiceAccount);
    when(mockIamClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(mockPolicy);
    when(mockIamClient.setIamPolicy(any(SetIamPolicyRequest.class))).thenReturn(mockPolicy);

    // Act
    gcpIam.createIdentity(
        TEST_IDENTITY_NAME,
        TEST_DESCRIPTION,
        TEST_PROJECT_ID,
        TEST_REGION,
        Optional.of(trustConfig),
        Optional.empty());

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
    TrustConfiguration trustConfig =
        TrustConfiguration.builder().addTrustedPrincipal(principal).build();

    ServiceAccount mockServiceAccount =
        ServiceAccount.newBuilder()
            .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
            .setName(TEST_SERVICE_ACCOUNT_NAME)
            .build();

    Policy mockPolicy = Policy.newBuilder().build();

    when(mockIamClient.createServiceAccount(any(CreateServiceAccountRequest.class)))
        .thenReturn(mockServiceAccount);
    when(mockIamClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(mockPolicy);
    when(mockIamClient.setIamPolicy(any(SetIamPolicyRequest.class))).thenReturn(mockPolicy);

    // Act
    gcpIam.createIdentity(
        TEST_IDENTITY_NAME,
        TEST_DESCRIPTION,
        TEST_PROJECT_ID,
        TEST_REGION,
        Optional.of(trustConfig),
        Optional.empty());

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

  // ==================== Tests for doGetIdentity ====================

  @Test
  void testGetIdentityWithAccountId() {
    // Arrange
    ServiceAccount mockServiceAccount =
        ServiceAccount.newBuilder()
            .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
            .setName(TEST_SERVICE_ACCOUNT_NAME)
            .setDisplayName(TEST_IDENTITY_NAME)
            .setDescription(TEST_DESCRIPTION)
            .build();

    when(mockIamClient.getServiceAccount(any(GetServiceAccountRequest.class)))
        .thenReturn(mockServiceAccount);

    // Act
    String result = gcpIam.getIdentity(TEST_IDENTITY_NAME, TEST_PROJECT_ID, TEST_REGION);

    // Assert
    assertEquals(TEST_SERVICE_ACCOUNT_EMAIL, result);

    // Verify the request
    ArgumentCaptor<GetServiceAccountRequest> requestCaptor =
        ArgumentCaptor.forClass(GetServiceAccountRequest.class);
    verify(mockIamClient).getServiceAccount(requestCaptor.capture());

    GetServiceAccountRequest capturedRequest = requestCaptor.getValue();
    assertEquals(TEST_SERVICE_ACCOUNT_NAME, capturedRequest.getName());
  }

  @Test
  void testGetIdentityWithFullEmail() {
    // Arrange
    ServiceAccount mockServiceAccount =
        ServiceAccount.newBuilder()
            .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
            .setName(TEST_SERVICE_ACCOUNT_NAME)
            .setDisplayName(TEST_IDENTITY_NAME)
            .setDescription(TEST_DESCRIPTION)
            .build();

    when(mockIamClient.getServiceAccount(any(GetServiceAccountRequest.class)))
        .thenReturn(mockServiceAccount);

    // Act - pass full email instead of just account ID
    String result = gcpIam.getIdentity(TEST_SERVICE_ACCOUNT_EMAIL, TEST_PROJECT_ID, TEST_REGION);

    // Assert
    assertEquals(TEST_SERVICE_ACCOUNT_EMAIL, result);

    // Verify the request
    ArgumentCaptor<GetServiceAccountRequest> requestCaptor =
        ArgumentCaptor.forClass(GetServiceAccountRequest.class);
    verify(mockIamClient).getServiceAccount(requestCaptor.capture());

    GetServiceAccountRequest capturedRequest = requestCaptor.getValue();
    assertEquals(TEST_SERVICE_ACCOUNT_NAME, capturedRequest.getName());
  }

  @Test
  void testGetIdentityWithProjectsPrefix() {
    // Arrange
    ServiceAccount mockServiceAccount =
        ServiceAccount.newBuilder()
            .setEmail(TEST_SERVICE_ACCOUNT_EMAIL)
            .setName(TEST_SERVICE_ACCOUNT_NAME)
            .build();

    when(mockIamClient.getServiceAccount(any(GetServiceAccountRequest.class)))
        .thenReturn(mockServiceAccount);

    // Act - pass tenantId with "projects/" prefix
    String result =
        gcpIam.getIdentity(TEST_IDENTITY_NAME, "projects/" + TEST_PROJECT_ID, TEST_REGION);

    // Assert
    assertEquals(TEST_SERVICE_ACCOUNT_EMAIL, result);

    // Verify the request still has correct format
    ArgumentCaptor<GetServiceAccountRequest> requestCaptor =
        ArgumentCaptor.forClass(GetServiceAccountRequest.class);
    verify(mockIamClient).getServiceAccount(requestCaptor.capture());

    GetServiceAccountRequest capturedRequest = requestCaptor.getValue();
    assertEquals(TEST_SERVICE_ACCOUNT_NAME, capturedRequest.getName());
  }

  @Test
  void testGetIdentityThrowsException() {
    // Arrange
    RuntimeException genericException = new RuntimeException("Unexpected error");

    when(mockIamClient.getServiceAccount(any(GetServiceAccountRequest.class)))
        .thenThrow(genericException);

    // Act & Assert - Exception should propagate to IamClient for centralized handling
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> gcpIam.getIdentity(TEST_IDENTITY_NAME, TEST_PROJECT_ID, TEST_REGION));

    assertEquals(genericException, exception);
  }

  // ==================== Tests for doDeleteIdentity ====================

  @Test
  void testDeleteIdentityWithAccountId() {
    // Arrange
    doNothing().when(mockIamClient).deleteServiceAccount(any(DeleteServiceAccountRequest.class));

    // Act
    gcpIam.deleteIdentity(TEST_IDENTITY_NAME, TEST_PROJECT_ID, TEST_REGION);

    // Assert - Verify the request
    ArgumentCaptor<DeleteServiceAccountRequest> requestCaptor =
        ArgumentCaptor.forClass(DeleteServiceAccountRequest.class);
    verify(mockIamClient).deleteServiceAccount(requestCaptor.capture());

    DeleteServiceAccountRequest capturedRequest = requestCaptor.getValue();
    assertEquals(TEST_SERVICE_ACCOUNT_NAME, capturedRequest.getName());
  }

  @Test
  void testDeleteIdentityWithFullEmail() {
    // Arrange
    doNothing().when(mockIamClient).deleteServiceAccount(any(DeleteServiceAccountRequest.class));

    // Act - pass full email instead of just account ID
    gcpIam.deleteIdentity(TEST_SERVICE_ACCOUNT_EMAIL, TEST_PROJECT_ID, TEST_REGION);

    // Assert - Verify the request
    ArgumentCaptor<DeleteServiceAccountRequest> requestCaptor =
        ArgumentCaptor.forClass(DeleteServiceAccountRequest.class);
    verify(mockIamClient).deleteServiceAccount(requestCaptor.capture());

    DeleteServiceAccountRequest capturedRequest = requestCaptor.getValue();
    assertEquals(TEST_SERVICE_ACCOUNT_NAME, capturedRequest.getName());
  }

  @Test
  void testDeleteIdentityWithProjectsPrefix() {
    // Arrange
    doNothing().when(mockIamClient).deleteServiceAccount(any(DeleteServiceAccountRequest.class));

    // Act - pass tenantId with "projects/" prefix
    gcpIam.deleteIdentity(TEST_IDENTITY_NAME, "projects/" + TEST_PROJECT_ID, TEST_REGION);

    // Assert - Verify the request still has correct format
    ArgumentCaptor<DeleteServiceAccountRequest> requestCaptor =
        ArgumentCaptor.forClass(DeleteServiceAccountRequest.class);
    verify(mockIamClient).deleteServiceAccount(requestCaptor.capture());

    DeleteServiceAccountRequest capturedRequest = requestCaptor.getValue();
    assertEquals(TEST_SERVICE_ACCOUNT_NAME, capturedRequest.getName());
  }

  @Test
  void testDeleteIdentityThrowsException() {
    // Arrange
    RuntimeException genericException = new RuntimeException("Unexpected error");

    doThrow(genericException)
        .when(mockIamClient)
        .deleteServiceAccount(any(DeleteServiceAccountRequest.class));

    // Act & Assert - Exception should propagate to IamClient for centralized handling
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> gcpIam.deleteIdentity(TEST_IDENTITY_NAME, TEST_PROJECT_ID, TEST_REGION));

    assertEquals(genericException, exception);
  }

  @Test
  void testDeleteIdentitySuccessfullyDeletesServiceAccount() {
    // Arrange
    doNothing().when(mockIamClient).deleteServiceAccount(any(DeleteServiceAccountRequest.class));

    // Act - should not throw any exception
    assertDoesNotThrow(
        () -> gcpIam.deleteIdentity(TEST_IDENTITY_NAME, TEST_PROJECT_ID, TEST_REGION));

    // Assert - Verify the method was called
    verify(mockIamClient, times(1)).deleteServiceAccount(any(DeleteServiceAccountRequest.class));
  }

  @Test
  void testAttachInlinePolicyWithStorageActions() {
    Policy existingPolicy = Policy.newBuilder().build();
    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class)))
        .thenReturn(existingPolicy);
    when(mockProjectsClient.setIamPolicy(any(SetIamPolicyRequest.class)))
        .thenReturn(existingPolicy);

    PolicyDocument policyDocument =
        PolicyDocument.builder()
            .version("2024-01-01")
            .statement(
                Statement.builder()
                    .effect(Effect.ALLOW)
                    .action(StorageActions.GET_OBJECT)
                    .action(StorageActions.PUT_OBJECT)
                    .build())
            .build();

    // policyDocument, TEST_TENANT_ID, TEST_REGION, TEST_SERVICE_ACCOUNT
    gcpIam.doAttachInlinePolicy(
        AttachInlinePolicyRequest.builder()
            .policyDocument(policyDocument)
            .tenantId(TEST_TENANT_ID)
            .region(TEST_REGION)
            .identityName(TEST_SERVICE_ACCOUNT)
            .build());

    ArgumentCaptor<SetIamPolicyRequest> captor = ArgumentCaptor.forClass(SetIamPolicyRequest.class);
    verify(mockProjectsClient, times(1)).setIamPolicy(captor.capture());

    Policy updatedPolicy = captor.getValue().getPolicy();
    List<String> roles =
        updatedPolicy.getBindingsList().stream()
            .map(Binding::getRole)
            .collect(java.util.stream.Collectors.toList());

    assertTrue(roles.contains("roles/storage.objectViewer"));
    assertTrue(roles.contains("roles/storage.objectCreator"));
  }

  @Test
  void testAttachInlinePolicyWithComputeActions() {
    Policy existingPolicy = Policy.newBuilder().build();
    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class)))
        .thenReturn(existingPolicy);
    when(mockProjectsClient.setIamPolicy(any(SetIamPolicyRequest.class)))
        .thenReturn(existingPolicy);

    PolicyDocument policyDocument =
        PolicyDocument.builder()
            .version("2024-01-01")
            .statement(
                Statement.builder()
                    .effect(Effect.ALLOW)
                    .action(ComputeActions.CREATE_INSTANCE)
                    .action(ComputeActions.GET_INSTANCE)
                    .build())
            .build();

    gcpIam.doAttachInlinePolicy(
        AttachInlinePolicyRequest.builder()
            .policyDocument(policyDocument)
            .tenantId(TEST_TENANT_ID)
            .region(TEST_REGION)
            .identityName(TEST_SERVICE_ACCOUNT)
            .build());

    ArgumentCaptor<SetIamPolicyRequest> captor = ArgumentCaptor.forClass(SetIamPolicyRequest.class);
    verify(mockProjectsClient, times(1)).setIamPolicy(captor.capture());

    Policy updatedPolicy = captor.getValue().getPolicy();
    List<String> roles =
        updatedPolicy.getBindingsList().stream()
            .map(Binding::getRole)
            .collect(java.util.stream.Collectors.toList());

    assertTrue(roles.contains("roles/compute.instanceAdmin.v1"));
    assertTrue(roles.contains("roles/compute.viewer"));
  }

  @Test
  void testAttachInlinePolicyWithIamActions() {
    Policy existingPolicy = Policy.newBuilder().build();
    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class)))
        .thenReturn(existingPolicy);
    when(mockProjectsClient.setIamPolicy(any(SetIamPolicyRequest.class)))
        .thenReturn(existingPolicy);

    PolicyDocument policyDocument =
        PolicyDocument.builder()
            .version("2024-01-01")
            .statement(
                Statement.builder().effect(Effect.ALLOW).action(IamActions.ASSUME_ROLE).build())
            .build();

    // policyDocument, TEST_TENANT_ID, TEST_REGION, TEST_SERVICE_ACCOUNT
    gcpIam.doAttachInlinePolicy(
        AttachInlinePolicyRequest.builder()
            .policyDocument(policyDocument)
            .tenantId(TEST_TENANT_ID)
            .region(TEST_REGION)
            .identityName(TEST_SERVICE_ACCOUNT)
            .build());

    ArgumentCaptor<SetIamPolicyRequest> captor = ArgumentCaptor.forClass(SetIamPolicyRequest.class);
    verify(mockProjectsClient, times(1)).setIamPolicy(captor.capture());

    Policy updatedPolicy = captor.getValue().getPolicy();
    Binding binding = updatedPolicy.getBindings(0);
    assertEquals("roles/iam.serviceAccountUser", binding.getRole());
    assertTrue(binding.getMembersList().contains(TEST_SERVICE_ACCOUNT));
  }

  @Test
  void testAttachInlinePolicyWithConditionsThrowsException() {
    Policy existingPolicy = Policy.newBuilder().build();
    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class)))
        .thenReturn(existingPolicy);

    PolicyDocument policyDocument =
        PolicyDocument.builder()
            .version("2024-01-01")
            .statement(
                Statement.builder()
                    .effect(Effect.ALLOW)
                    .action(StorageActions.GET_OBJECT)
                    .condition(ConditionOperator.STRING_EQUALS, "key", "value")
                    .build())
            .build();

    SubstrateSdkException exception =
        assertThrows(
            SubstrateSdkException.class,
            () -> {
              // policyDocument, TEST_TENANT_ID, TEST_REGION, TEST_SERVICE_ACCOUNT
              gcpIam.doAttachInlinePolicy(
                  AttachInlinePolicyRequest.builder()
                      .policyDocument(policyDocument)
                      .tenantId(TEST_TENANT_ID)
                      .region(TEST_REGION)
                      .identityName(TEST_SERVICE_ACCOUNT)
                      .build());
            });

    assertTrue(exception.getMessage().contains("GCP IAM policy conditions are not yet supported"));
    verify(mockProjectsClient, times(1)).getIamPolicy(any(GetIamPolicyRequest.class));
    verify(mockProjectsClient, times(0)).setIamPolicy(any(SetIamPolicyRequest.class));
  }

  @Test
  void testAttachInlinePolicyWithUnknownActionThrowsException() {
    Policy existingPolicy = Policy.newBuilder().build();
    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class)))
        .thenReturn(existingPolicy);

    PolicyDocument policyDocument =
        PolicyDocument.builder()
            .version("2024-01-01")
            .statement(
                Statement.builder()
                    .effect(Effect.ALLOW)
                    .action(Action.of("unknown:Action"))
                    .build())
            .build();

    SubstrateSdkException exception =
        assertThrows(
            SubstrateSdkException.class,
            () -> {
              // policyDocument, TEST_TENANT_ID, TEST_REGION, TEST_SERVICE_ACCOUNT
              gcpIam.doAttachInlinePolicy(
                  AttachInlinePolicyRequest.builder()
                      .policyDocument(policyDocument)
                      .tenantId(TEST_TENANT_ID)
                      .region(TEST_REGION)
                      .identityName(TEST_SERVICE_ACCOUNT)
                      .build());
            });

    assertTrue(exception.getMessage().contains("Unknown substrate-neutral action: unknown:Action"));
    verify(mockProjectsClient, times(1)).getIamPolicy(any(GetIamPolicyRequest.class));
    verify(mockProjectsClient, times(0)).setIamPolicy(any(SetIamPolicyRequest.class));
  }

  @Test
  void testAttachInlinePolicyWithMultipleStatements() {
    Policy existingPolicy = Policy.newBuilder().build();
    when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class)))
        .thenReturn(existingPolicy);
    when(mockProjectsClient.setIamPolicy(any(SetIamPolicyRequest.class)))
        .thenReturn(existingPolicy);

    PolicyDocument policyDocument =
        PolicyDocument.builder()
            .version("2024-01-01")
            .statement(
                Statement.builder()
                    .sid("StorageAccess")
                    .effect(Effect.ALLOW)
                    .action(StorageActions.GET_OBJECT)
                    .build())
            .statement(
                Statement.builder()
                    .sid("ComputeAccess")
                    .effect(Effect.ALLOW)
                    .action(ComputeActions.CREATE_INSTANCE)
                    .build())
            .build();

    // policyDocument, TEST_TENANT_ID, TEST_REGION, TEST_SERVICE_ACCOUNT
    gcpIam.doAttachInlinePolicy(
        AttachInlinePolicyRequest.builder()
            .policyDocument(policyDocument)
            .tenantId(TEST_TENANT_ID)
            .region(TEST_REGION)
            .identityName(TEST_SERVICE_ACCOUNT)
            .build());

    ArgumentCaptor<SetIamPolicyRequest> captor = ArgumentCaptor.forClass(SetIamPolicyRequest.class);
    verify(mockProjectsClient, times(1)).setIamPolicy(captor.capture());

    Policy updatedPolicy = captor.getValue().getPolicy();
    List<String> roles =
        updatedPolicy.getBindingsList().stream()
            .map(Binding::getRole)
            .collect(java.util.stream.Collectors.toList());

    assertTrue(roles.contains("roles/storage.objectViewer"));
    assertTrue(roles.contains("roles/compute.instanceAdmin.v1"));
  }
}
