package com.salesforce.multicloudj.iam.gcp;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.resourcemanager.v3.ProjectsClient;
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
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GcpIamTest {

	@Mock
	private ProjectsClient mockProjectsClient;
	private GcpIam gcpIam;
	private static final String TEST_TENANT_ID = "projects/test-project";
	private static final String TEST_REGION = "us-west1";
	private static final String TEST_SERVICE_ACCOUNT = "serviceAccount:test-sa@test-project.iam.gserviceaccount.com";
	private static final String TEST_ROLE = "roles/iam.serviceAccountUser";

	@BeforeEach
	void setUp() {
		gcpIam = new GcpIam.Builder().withProjectsClient(mockProjectsClient).build();
	}

	@Test
	void testConstructorWithBuilderAndProjectsClient() {
		GcpIam iam = new GcpIam.Builder().withProjectsClient(mockProjectsClient).build();
		Assertions.assertNotNull(iam);
		Assertions.assertEquals("gcp", iam.getProviderId());
	}

	@Test
	void testConstructorWithNullProjectsClient() throws IOException {
		// ProjectsClient can be null - it will be created in Builder.build() if not provided
		try (MockedStatic<ProjectsClient> mockedClient = mockStatic(ProjectsClient.class)) {
			ProjectsClient mockClient = mock(ProjectsClient.class);
			mockedClient.when(ProjectsClient::create).thenReturn(mockClient);

			Assertions.assertDoesNotThrow(() -> {
				new GcpIam.Builder().build();
			});
		}
	}

	@Test
	void testConstructorWithBuilder() throws IOException {
		try (MockedStatic<ProjectsClient> mockedClient = mockStatic(ProjectsClient.class)) {
			ProjectsClient mockClient = mock(ProjectsClient.class);
			mockedClient.when(ProjectsClient::create).thenReturn(mockClient);

			GcpIam iam = new GcpIam(new GcpIam.Builder());
			Assertions.assertNotNull(iam);
			Assertions.assertEquals("gcp", iam.getProviderId());
		}
	}

	@Test
	void testDoAttachInlinePolicySuccess() {
		// Setup: Create a policy with existing bindings
		Policy existingPolicy = Policy.newBuilder()
				.addBindings(Binding.newBuilder()
						.setRole("roles/storage.objectViewer")
						.addMembers("serviceAccount:other@test-project.iam.gserviceaccount.com")
						.build())
				.build();

		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(existingPolicy);
		when(mockProjectsClient.setIamPolicy(any(SetIamPolicyRequest.class))).thenReturn(existingPolicy);

		// Create policy document
		PolicyDocument policyDocument = PolicyDocument.builder()
				.version("2024-01-01")
				.statement(Statement.builder()
						.sid("TestPolicy")
						.effect("Allow")
						.action(TEST_ROLE)
						.build())
				.build();

		// Execute
		Assertions.assertDoesNotThrow(() -> {
			gcpIam.doAttachInlinePolicy(policyDocument, TEST_TENANT_ID, TEST_REGION, TEST_SERVICE_ACCOUNT);
		});

		// Verify
		verify(mockProjectsClient, times(1)).getIamPolicy(any(GetIamPolicyRequest.class));
		ArgumentCaptor<SetIamPolicyRequest> setRequestCaptor = ArgumentCaptor.forClass(SetIamPolicyRequest.class);
		verify(mockProjectsClient, times(1)).setIamPolicy(setRequestCaptor.capture());

		SetIamPolicyRequest setRequest = setRequestCaptor.getValue();
		Assertions.assertEquals(TEST_TENANT_ID, setRequest.getResource());
		Policy updatedPolicy = setRequest.getPolicy();
		Assertions.assertNotNull(updatedPolicy);

		// Verify the new binding was added
		boolean foundBinding = false;
		for (Binding binding : updatedPolicy.getBindingsList()) {
			if (binding.getRole().equals(TEST_ROLE)) {
				Assertions.assertTrue(binding.getMembersList().contains(TEST_SERVICE_ACCOUNT));
				foundBinding = true;
			}
		}
		Assertions.assertTrue(foundBinding, "New binding should be added");
	}

	@Test
	void testDoAttachInlinePolicyWithNullPolicy() {
		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(null);
		when(mockProjectsClient.setIamPolicy(any(SetIamPolicyRequest.class))).thenReturn(Policy.newBuilder().build());

		PolicyDocument policyDocument = PolicyDocument.builder()
				.version("2024-01-01")
				.statement(Statement.builder()
						.sid("TestPolicy")
						.effect("Allow")
						.action(TEST_ROLE)
						.build())
				.build();

		Assertions.assertDoesNotThrow(() -> {
			gcpIam.doAttachInlinePolicy(policyDocument, TEST_TENANT_ID, TEST_REGION, TEST_SERVICE_ACCOUNT);
		});

		verify(mockProjectsClient, times(1)).setIamPolicy(any(SetIamPolicyRequest.class));
	}

	@Test
	void testDoAttachInlinePolicyMergesExistingBinding() {
		// Setup: Create a policy with existing binding for the same role
		Policy existingPolicy = Policy.newBuilder()
				.addBindings(Binding.newBuilder()
						.setRole(TEST_ROLE)
						.addMembers("serviceAccount:existing@test-project.iam.gserviceaccount.com")
						.build())
				.build();

		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(existingPolicy);
		when(mockProjectsClient.setIamPolicy(any(SetIamPolicyRequest.class))).thenReturn(existingPolicy);

		PolicyDocument policyDocument = PolicyDocument.builder()
				.version("2024-01-01")
				.statement(Statement.builder()
						.sid("TestPolicy")
						.effect("Allow")
						.action(TEST_ROLE)
						.build())
				.build();

		Assertions.assertDoesNotThrow(() -> {
			gcpIam.doAttachInlinePolicy(policyDocument, TEST_TENANT_ID, TEST_REGION, TEST_SERVICE_ACCOUNT);
		});

		ArgumentCaptor<SetIamPolicyRequest> setRequestCaptor = ArgumentCaptor.forClass(SetIamPolicyRequest.class);
		verify(mockProjectsClient, times(1)).setIamPolicy(setRequestCaptor.capture());

		Policy updatedPolicy = setRequestCaptor.getValue().getPolicy();
		Binding updatedBinding = updatedPolicy.getBindingsList().stream()
				.filter(b -> b.getRole().equals(TEST_ROLE))
				.findFirst()
				.orElse(null);

		Assertions.assertNotNull(updatedBinding);
		Assertions.assertEquals(2, updatedBinding.getMembersCount(), "Should have both existing and new members");
		Assertions.assertTrue(updatedBinding.getMembersList().contains(TEST_SERVICE_ACCOUNT));
	}


	@Test
	void testDoAttachInlinePolicySkipsDenyStatements() {
		Policy existingPolicy = Policy.newBuilder().build();
		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(existingPolicy);

		PolicyDocument policyDocument = PolicyDocument.builder()
				.version("2024-01-01")
				.statement(Statement.builder()
						.sid("DenyPolicy")
						.effect("Deny")
						.action(TEST_ROLE)
						.build())
				.build();

		Assertions.assertDoesNotThrow(() -> {
			gcpIam.doAttachInlinePolicy(policyDocument, TEST_TENANT_ID, TEST_REGION, TEST_SERVICE_ACCOUNT);
		});

		// Verify: setIamPolicy should not be called since Deny statements are skipped and nothing changes
		verify(mockProjectsClient, times(0)).setIamPolicy(any(SetIamPolicyRequest.class));
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
		Class<? extends SubstrateSdkException> exceptionClass = gcpIam.getException(new RuntimeException("Test error"));
		Assertions.assertEquals(UnknownException.class, exceptionClass);
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
		Policy policy = Policy.newBuilder()
				.addBindings(Binding.newBuilder()
						.setRole("roles/iam.serviceAccountUser")
						.addMembers(TEST_SERVICE_ACCOUNT)
						.addMembers("serviceAccount:other@test-project.iam.gserviceaccount.com")
						.build())
				.addBindings(Binding.newBuilder()
						.setRole("roles/storage.objectViewer")
						.addMembers(TEST_SERVICE_ACCOUNT)
						.build())
				.addBindings(Binding.newBuilder()
						.setRole("roles/compute.instanceAdmin")
						.addMembers("serviceAccount:another@test-project.iam.gserviceaccount.com")
						.build())
				.build();

		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(policy);

		// Execute
		List<String> result = gcpIam.doGetAttachedPolicies(
				TEST_SERVICE_ACCOUNT, TEST_TENANT_ID, TEST_REGION);

		// Verify
		Assertions.assertNotNull(result);
		Assertions.assertEquals(2, result.size());
		Assertions.assertTrue(result.contains("roles/iam.serviceAccountUser"));
		Assertions.assertTrue(result.contains("roles/storage.objectViewer"));
		Assertions.assertFalse(result.contains("roles/compute.instanceAdmin"));

		ArgumentCaptor<GetIamPolicyRequest> requestCaptor = ArgumentCaptor.forClass(GetIamPolicyRequest.class);
		verify(mockProjectsClient, times(1)).getIamPolicy(requestCaptor.capture());
		Assertions.assertEquals(TEST_TENANT_ID, requestCaptor.getValue().getResource());
	}

	@Test
	void testDoGetAttachedPoliciesWithNoAttachedRoles() {
		// Setup: Create a policy with bindings, but none for our service account
		Policy policy = Policy.newBuilder()
				.addBindings(Binding.newBuilder()
						.setRole("roles/iam.serviceAccountUser")
						.addMembers("serviceAccount:other@test-project.iam.gserviceaccount.com")
						.build())
				.build();

		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(policy);

		// Execute
		List<String> result = gcpIam.doGetAttachedPolicies(
				TEST_SERVICE_ACCOUNT, TEST_TENANT_ID, TEST_REGION);

		// Verify
		Assertions.assertNotNull(result);
		Assertions.assertTrue(result.isEmpty());
	}

	@Test
	void testDoGetAttachedPoliciesWithNullPolicy() {
		// Setup: Return null policy
		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(null);

		// Execute
		List<String> result = gcpIam.doGetAttachedPolicies(
				TEST_SERVICE_ACCOUNT, TEST_TENANT_ID, TEST_REGION);

		// Verify
		Assertions.assertNotNull(result);
		Assertions.assertTrue(result.isEmpty());
		verify(mockProjectsClient, times(1)).getIamPolicy(any(GetIamPolicyRequest.class));
	}

	@Test
	void testDoGetAttachedPoliciesWithEmptyPolicy() {
		// Setup: Create an empty policy
		Policy policy = Policy.newBuilder().build();

		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(policy);

		// Execute
		List<String> result = gcpIam.doGetAttachedPolicies(
				TEST_SERVICE_ACCOUNT, TEST_TENANT_ID, TEST_REGION);

		// Verify
		Assertions.assertNotNull(result);
		Assertions.assertTrue(result.isEmpty());
	}

	@Test
	void testDoGetAttachedPoliciesWithSingleRole() {
		// Setup: Service account has exactly one role
		Policy policy = Policy.newBuilder()
				.addBindings(Binding.newBuilder()
						.setRole("roles/storage.objectViewer")
						.addMembers(TEST_SERVICE_ACCOUNT)
						.build())
				.build();
		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(policy);

		// Execute
		List<String> result = gcpIam.doGetAttachedPolicies(
				TEST_SERVICE_ACCOUNT, TEST_TENANT_ID, TEST_REGION);

		// Verify
		Assertions.assertNotNull(result);
		Assertions.assertEquals(1, result.size());
		Assertions.assertEquals("roles/storage.objectViewer", result.get(0));
	}

	@Test
	void testClose() throws Exception {
		gcpIam.close();
		verify(mockProjectsClient, times(1)).close();
	}

	@Test
	void testCloseWithException() throws Exception {
		Mockito.doThrow(new IOException("Close failed")).when(mockProjectsClient).close();
		Assertions.assertThrows(IOException.class, () -> gcpIam.close());
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
		Assertions.assertThrows(ApiException.class, () -> {
			gcpIam.doGetAttachedPolicies(TEST_SERVICE_ACCOUNT, TEST_TENANT_ID, TEST_REGION);
		});
		
		// Verify that the exception would be mapped correctly
		Class<? extends SubstrateSdkException> mappedException = gcpIam.getException(apiException);
		Assertions.assertEquals(UnAuthorizedException.class, mappedException);
	}

	@Test
	void testDoRemovePolicySuccess() {
		// Setup: Create a policy with a binding containing multiple members
		Policy originalPolicy = Policy.newBuilder()
				.addBindings(Binding.newBuilder()
						.setRole(TEST_ROLE)
						.addMembers(TEST_SERVICE_ACCOUNT)
						.addMembers("serviceAccount:other@test-project.iam.gserviceaccount.com")
						.build())
				.addBindings(Binding.newBuilder()
						.setRole("roles/storage.objectViewer")
						.addMembers(TEST_SERVICE_ACCOUNT)
						.build())
				.build();

		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(originalPolicy);

		// Execute
		gcpIam.doRemovePolicy(TEST_SERVICE_ACCOUNT, TEST_ROLE, TEST_TENANT_ID, TEST_REGION);

		// Verify: getIamPolicy was called once
		ArgumentCaptor<GetIamPolicyRequest> getRequestCaptor = ArgumentCaptor.forClass(GetIamPolicyRequest.class);
		verify(mockProjectsClient, times(1)).getIamPolicy(getRequestCaptor.capture());
		Assertions.assertEquals(TEST_TENANT_ID, getRequestCaptor.getValue().getResource());

		// Verify: setIamPolicy was called with updated policy
		ArgumentCaptor<SetIamPolicyRequest> setRequestCaptor = ArgumentCaptor.forClass(SetIamPolicyRequest.class);
		verify(mockProjectsClient, times(1)).setIamPolicy(setRequestCaptor.capture());
		SetIamPolicyRequest setRequest = setRequestCaptor.getValue();
		Assertions.assertEquals(TEST_TENANT_ID, setRequest.getResource());

		// Verify: The member was removed from the binding, but binding still exists
		Policy updatedPolicy = setRequest.getPolicy();
		Binding updatedBinding = updatedPolicy.getBindingsList().stream()
				.filter(b -> b.getRole().equals(TEST_ROLE))
				.findFirst()
				.orElse(null);
		Assertions.assertNotNull(updatedBinding, "Binding should still exist");
		Assertions.assertFalse(updatedBinding.getMembersList().contains(TEST_SERVICE_ACCOUNT),
				"Service account should be removed from binding");
		Assertions.assertTrue(updatedBinding.getMembersList().contains("serviceAccount:other@test-project.iam.gserviceaccount.com"),
				"Other member should remain in binding");

		// Verify: Other binding with the same service account should remain unchanged
		Binding otherBinding = updatedPolicy.getBindingsList().stream()
				.filter(b -> b.getRole().equals("roles/storage.objectViewer"))
				.findFirst()
				.orElse(null);
		Assertions.assertNotNull(otherBinding, "Other binding should still exist");
		Assertions.assertTrue(otherBinding.getMembersList().contains(TEST_SERVICE_ACCOUNT),
				"Service account should remain in other binding");
	}

	@Test
	void testDoRemovePolicyRemovesEmptyBinding() {
		// Setup: Create a policy with a binding containing only our service account
		Policy originalPolicy = Policy.newBuilder()
				.addBindings(Binding.newBuilder()
						.setRole(TEST_ROLE)
						.addMembers(TEST_SERVICE_ACCOUNT)
						.build())
				.addBindings(Binding.newBuilder()
						.setRole("roles/storage.objectViewer")
						.addMembers("serviceAccount:other@test-project.iam.gserviceaccount.com")
						.build())
				.build();

		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(originalPolicy);

		// Execute
		gcpIam.doRemovePolicy(TEST_SERVICE_ACCOUNT, TEST_ROLE, TEST_TENANT_ID, TEST_REGION);

		// Verify: setIamPolicy was called
		ArgumentCaptor<SetIamPolicyRequest> setRequestCaptor = ArgumentCaptor.forClass(SetIamPolicyRequest.class);
		verify(mockProjectsClient, times(1)).setIamPolicy(setRequestCaptor.capture());
		Policy updatedPolicy = setRequestCaptor.getValue().getPolicy();

		// Verify: The binding should be completely removed since it's now empty
		boolean bindingExists = updatedPolicy.getBindingsList().stream()
				.anyMatch(b -> b.getRole().equals(TEST_ROLE));
		Assertions.assertFalse(bindingExists, "Empty binding should be removed");

		// Verify: Other binding should remain
		boolean otherBindingExists = updatedPolicy.getBindingsList().stream()
				.anyMatch(b -> b.getRole().equals("roles/storage.objectViewer"));
		Assertions.assertTrue(otherBindingExists, "Other binding should remain");
	}

	@Test
	void testDoRemovePolicyWithNonExistentBinding() {
		// Setup: Create a policy without the binding we're trying to remove
		Policy originalPolicy = Policy.newBuilder()
				.addBindings(Binding.newBuilder()
						.setRole("roles/storage.objectViewer")
						.addMembers(TEST_SERVICE_ACCOUNT)
						.build())
				.build();

		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(originalPolicy);

		// Execute
		gcpIam.doRemovePolicy(TEST_SERVICE_ACCOUNT, TEST_ROLE, TEST_TENANT_ID, TEST_REGION);

		// Verify: getIamPolicy was called
		verify(mockProjectsClient, times(1)).getIamPolicy(any(GetIamPolicyRequest.class));

		// Verify: setIamPolicy should not be called since binding doesn't exist (nothing to remove)
		verify(mockProjectsClient, times(0)).setIamPolicy(any(SetIamPolicyRequest.class));
	}

	@Test
	void testDoRemovePolicyWithMemberNotInBinding() {
		// Setup: Create a policy with the binding, but our service account is not a member
		Policy originalPolicy = Policy.newBuilder()
				.addBindings(Binding.newBuilder()
						.setRole(TEST_ROLE)
						.addMembers("serviceAccount:other@test-project.iam.gserviceaccount.com")
						.build())
				.build();

		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(originalPolicy);

		// Execute
		gcpIam.doRemovePolicy(TEST_SERVICE_ACCOUNT, TEST_ROLE, TEST_TENANT_ID, TEST_REGION);

		// Verify: setIamPolicy should not be called since member is not in binding (nothing to remove)
		verify(mockProjectsClient, times(0)).setIamPolicy(any(SetIamPolicyRequest.class));
	}

	@Test
	void testDoRemovePolicyWithNullPolicy() {
		// Setup: Return null policy
		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(null);

		// Execute
		gcpIam.doRemovePolicy(TEST_SERVICE_ACCOUNT, TEST_ROLE, TEST_TENANT_ID, TEST_REGION);

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
		Assertions.assertThrows(ApiException.class, () -> {
			gcpIam.doRemovePolicy(TEST_SERVICE_ACCOUNT, TEST_ROLE, TEST_TENANT_ID, TEST_REGION);
		});

		// Verify: setIamPolicy should not be called
		verify(mockProjectsClient, times(0)).setIamPolicy(any(SetIamPolicyRequest.class));
	}

	@Test
	void testDoRemovePolicyWithApiExceptionOnSet() {
		// Setup: Create a policy and mock exception on setIamPolicy
		Policy originalPolicy = Policy.newBuilder()
				.addBindings(Binding.newBuilder()
						.setRole(TEST_ROLE)
						.addMembers(TEST_SERVICE_ACCOUNT)
						.build())
				.build();

		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(originalPolicy);

		ApiException apiException = mock(ApiException.class);
		when(mockProjectsClient.setIamPolicy(any(SetIamPolicyRequest.class))).thenThrow(apiException);

		// Execute and verify exception is thrown
		Assertions.assertThrows(ApiException.class, () -> {
			gcpIam.doRemovePolicy(TEST_SERVICE_ACCOUNT, TEST_ROLE, TEST_TENANT_ID, TEST_REGION);
		});
	}

	@Test
	void testDoGetInlinePolicyDetailsSuccess() {
		// Setup: Create a policy with a binding for our service account
		Policy originalPolicy = Policy.newBuilder()
				.addBindings(Binding.newBuilder()
						.setRole(TEST_ROLE)
						.addMembers(TEST_SERVICE_ACCOUNT)
						.addMembers("serviceAccount:other@test-project.iam.gserviceaccount.com")
						.build())
				.build();

		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(originalPolicy);

		// Execute
		String result = gcpIam.doGetInlinePolicyDetails(
				TEST_SERVICE_ACCOUNT, TEST_ROLE, TEST_TENANT_ID, TEST_REGION);

		// Verify
		Assertions.assertNotNull(result);
		Assertions.assertTrue(result.contains("\"version\""));
		Assertions.assertTrue(result.contains("\"statements\""));
		Assertions.assertTrue(result.contains("\"effect\":\"Allow\""));
		Assertions.assertTrue(result.contains("\"actions\""));
		Assertions.assertTrue(result.contains(TEST_ROLE));

		// Verify getIamPolicy was called
		ArgumentCaptor<GetIamPolicyRequest> requestCaptor = ArgumentCaptor.forClass(GetIamPolicyRequest.class);
		verify(mockProjectsClient, times(1)).getIamPolicy(requestCaptor.capture());
		Assertions.assertEquals(TEST_TENANT_ID, requestCaptor.getValue().getResource());
	}

	@Test
	void testDoGetInlinePolicyDetailsWithNonExistentBinding() {
		// Setup: Create a policy without the binding we're looking for
		Policy originalPolicy = Policy.newBuilder()
				.addBindings(Binding.newBuilder()
						.setRole("roles/storage.objectViewer")
						.addMembers(TEST_SERVICE_ACCOUNT)
						.build())
				.build();

		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(originalPolicy);

		// Execute
		String result = gcpIam.doGetInlinePolicyDetails(
				TEST_SERVICE_ACCOUNT, TEST_ROLE, TEST_TENANT_ID, TEST_REGION);

		// Verify: Should return null when binding doesn't exist
		Assertions.assertNull(result);
	}

	@Test
	void testDoGetInlinePolicyDetailsWithMemberNotInBinding() {
		// Setup: Create a policy with the binding, but our service account is not a member
		Policy originalPolicy = Policy.newBuilder()
				.addBindings(Binding.newBuilder()
						.setRole(TEST_ROLE)
						.addMembers("serviceAccount:other@test-project.iam.gserviceaccount.com")
						.build())
				.build();

		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(originalPolicy);

		// Execute
		String result = gcpIam.doGetInlinePolicyDetails(
				TEST_SERVICE_ACCOUNT, TEST_ROLE, TEST_TENANT_ID, TEST_REGION);

		// Verify: Should return null when service account is not in binding
		Assertions.assertNull(result);
	}

	@Test
	void testDoGetInlinePolicyDetailsWithNullPolicy() {
		// Setup: Return null policy
		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenReturn(null);

		// Execute
		String result = gcpIam.doGetInlinePolicyDetails(
				TEST_SERVICE_ACCOUNT, TEST_ROLE, TEST_TENANT_ID, TEST_REGION);

		// Verify: Should return null when policy is null
		Assertions.assertNull(result);
	}

	@Test
	void testDoGetInlinePolicyDetailsWithApiException() {
		// Setup: Mock API exception
		ApiException apiException = mock(ApiException.class);
		when(mockProjectsClient.getIamPolicy(any(GetIamPolicyRequest.class))).thenThrow(apiException);

		// Execute and verify exception is thrown
		Assertions.assertThrows(ApiException.class, () -> {
			gcpIam.doGetInlinePolicyDetails(TEST_SERVICE_ACCOUNT, TEST_ROLE, TEST_TENANT_ID, TEST_REGION);
		});
	}

	private void assertExceptionMapping(StatusCode.Code statusCode,
			Class<? extends SubstrateSdkException> expectedExceptionClass) {
		ApiException apiException = mock(ApiException.class);
		StatusCode mockStatusCode = mock(StatusCode.class);
		when(apiException.getStatusCode()).thenReturn(mockStatusCode);
		when(mockStatusCode.getCode()).thenReturn(statusCode);

		Class<? extends SubstrateSdkException> actualExceptionClass =
				gcpIam.getException(apiException);
		Assertions.assertEquals(expectedExceptionClass, actualExceptionClass,
				"Expected " + expectedExceptionClass.getSimpleName() + " for status code "
						+ statusCode);
	}
}
