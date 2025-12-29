package com.salesforce.multicloudj.iam.aws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleResponse;
import software.amazon.awssdk.services.iam.model.DeleteRoleRequest;
import software.amazon.awssdk.services.iam.model.DeleteRoleResponse;
import software.amazon.awssdk.services.iam.model.EntityAlreadyExistsException;
import software.amazon.awssdk.services.iam.model.GetRoleRequest;
import software.amazon.awssdk.services.iam.model.GetRoleResponse;
import software.amazon.awssdk.services.iam.model.Role;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AwsIamTest {

    private static final String TEST_ROLE_NAME = "TestRole";
    private static final String TEST_DESCRIPTION = "Test role";
    private static final String TEST_TENANT_ID = "123456789012";
    private static final String TEST_REGION = "us-west-2";
    private static final String TEST_ROLE_ARN = "arn:aws:iam::123456789012:role/TestRole";

    @Mock
    private IamClient mockIamClient;

    private AwsIam awsIam;

    @BeforeEach
    void setUp() {
        awsIam = new AwsIam.Builder()
                .withIamClient(mockIamClient)
                .withRegion(TEST_REGION)
                .build();
    }

    @Test
    void testCreateIdentityWithTrustConfigAddsConditionsToPolicy() throws Exception {
        Role role = Role.builder().arn(TEST_ROLE_ARN).roleName(TEST_ROLE_NAME).build();
        when(mockIamClient.createRole(any(CreateRoleRequest.class)))
                .thenReturn(CreateRoleResponse.builder().role(role).build());

        TrustConfiguration trustConfiguration = TrustConfiguration.builder()
                .addTrustedPrincipal("arn:aws:iam::999999999999:root")
                .addCondition("StringEquals", "sts:ExternalId", "external-id")
                .build();

        String result = awsIam.createIdentity(
                TEST_ROLE_NAME,
                TEST_DESCRIPTION,
                TEST_TENANT_ID,
                TEST_REGION,
                Optional.of(trustConfiguration),
                Optional.empty());

        assertEquals(TEST_ROLE_ARN, result);

        ArgumentCaptor<CreateRoleRequest> captor = ArgumentCaptor.forClass(CreateRoleRequest.class);
        verify(mockIamClient, times(1)).createRole(captor.capture());

        JsonNode doc = new ObjectMapper().readTree(captor.getValue().assumeRolePolicyDocument());
        JsonNode condition = doc.get("Statement").get(0).get("Condition");
        assertEquals("external-id", condition.get("StringEquals").get("sts:ExternalId").asText());
    }

    @Test
    void testCreateIdentityWithServicePrincipalInTrustConfig() throws Exception {
        Role role = Role.builder().arn(TEST_ROLE_ARN).roleName(TEST_ROLE_NAME).build();
        when(mockIamClient.createRole(any(CreateRoleRequest.class)))
                .thenReturn(CreateRoleResponse.builder().role(role).build());

        TrustConfiguration trustConfiguration = TrustConfiguration.builder()
                .addTrustedPrincipal("ec2.amazonaws.com")
                .build();

        String result = awsIam.createIdentity(
                TEST_ROLE_NAME,
                TEST_DESCRIPTION,
                TEST_TENANT_ID,
                TEST_REGION,
                Optional.of(trustConfiguration),
                Optional.empty());

        assertEquals(TEST_ROLE_ARN, result);

        ArgumentCaptor<CreateRoleRequest> captor = ArgumentCaptor.forClass(CreateRoleRequest.class);
        verify(mockIamClient, times(1)).createRole(captor.capture());

        JsonNode doc = new ObjectMapper().readTree(captor.getValue().assumeRolePolicyDocument());
        JsonNode principal = doc.get("Statement").get(0).get("Principal");
        assertEquals("ec2.amazonaws.com", principal.get("Service").asText());
    }

    @Test
    void testCreateIdentityWithoutTrustConfigDefaultsToSameAccountRoot() throws Exception {
        Role role = Role.builder().arn(TEST_ROLE_ARN).roleName(TEST_ROLE_NAME).build();
        when(mockIamClient.createRole(any(CreateRoleRequest.class)))
                .thenReturn(CreateRoleResponse.builder().role(role).build());

        String result = awsIam.createIdentity(
                TEST_ROLE_NAME,
                TEST_DESCRIPTION,
                TEST_TENANT_ID,
                TEST_REGION,
                Optional.empty(),
                Optional.empty());

        assertEquals(TEST_ROLE_ARN, result);

        ArgumentCaptor<CreateRoleRequest> captor = ArgumentCaptor.forClass(CreateRoleRequest.class);
        verify(mockIamClient, times(1)).createRole(captor.capture());

        CreateRoleRequest request = captor.getValue();
        assertEquals(TEST_ROLE_NAME, request.roleName());
        assertEquals(TEST_DESCRIPTION, request.description());

        JsonNode doc = new ObjectMapper().readTree(request.assumeRolePolicyDocument());
        assertEquals("2012-10-17", doc.get("Version").asText());
        JsonNode stmt = doc.get("Statement").get(0);
        assertEquals("Allow", stmt.get("Effect").asText());
        assertEquals("sts:AssumeRole", stmt.get("Action").asText());
        assertEquals("arn:aws:iam::" + TEST_TENANT_ID + ":root",
                stmt.get("Principal").get("AWS").asText());
    }

    @Test
    void testCreateIdentityWithTrustConfigUsesTrustedPrincipals() throws Exception {
        Role role = Role.builder().arn(TEST_ROLE_ARN).roleName(TEST_ROLE_NAME).build();
        when(mockIamClient.createRole(any(CreateRoleRequest.class)))
                .thenReturn(CreateRoleResponse.builder().role(role).build());

        TrustConfiguration trustConfiguration = TrustConfiguration.builder()
                .addTrustedPrincipal("arn:aws:iam::999999999999:root")
                .addTrustedPrincipal("arn:aws:iam::888888888888:role/SomeRole")
                .build();

        String result = awsIam.createIdentity(
                TEST_ROLE_NAME,
                TEST_DESCRIPTION,
                TEST_TENANT_ID,
                TEST_REGION,
                Optional.of(trustConfiguration),
                Optional.empty());

        assertEquals(TEST_ROLE_ARN, result);

        ArgumentCaptor<CreateRoleRequest> captor = ArgumentCaptor.forClass(CreateRoleRequest.class);
        verify(mockIamClient, times(1)).createRole(captor.capture());

        JsonNode doc = new ObjectMapper().readTree(captor.getValue().assumeRolePolicyDocument());
        JsonNode principals = doc.get("Statement").get(0).get("Principal").get("AWS");
        assertTrue(principals.isArray());
        assertEquals(2, principals.size());
    }

    @Test
    void testCreateIdentityWithCreateOptionsSetsFields() {
        Role role = Role.builder().arn(TEST_ROLE_ARN).roleName(TEST_ROLE_NAME).build();
        when(mockIamClient.createRole(any(CreateRoleRequest.class)))
                .thenReturn(CreateRoleResponse.builder().role(role).build());

        CreateOptions options = CreateOptions.builder()
                .path("/service-roles/")
                .maxSessionDuration(3600)
                .permissionBoundary("arn:aws:iam::123456789012:policy/Boundary")
                .build();

        String result = awsIam.createIdentity(
                TEST_ROLE_NAME,
                TEST_DESCRIPTION,
                TEST_TENANT_ID,
                TEST_REGION,
                Optional.empty(),
                Optional.of(options));

        assertEquals(TEST_ROLE_ARN, result);

        ArgumentCaptor<CreateRoleRequest> captor = ArgumentCaptor.forClass(CreateRoleRequest.class);
        verify(mockIamClient, times(1)).createRole(captor.capture());

        CreateRoleRequest request = captor.getValue();
        assertEquals("/service-roles/", request.path());
        assertEquals(3600, request.maxSessionDuration());
        assertEquals("arn:aws:iam::123456789012:policy/Boundary", request.permissionsBoundary());
    }

    @Test
    void testCreateIdentityAlreadyExistsReturnsExistingArn() {
        when(mockIamClient.createRole(any(CreateRoleRequest.class)))
                .thenThrow(EntityAlreadyExistsException.builder().message("exists").build());

        Role role = Role.builder().arn(TEST_ROLE_ARN).roleName(TEST_ROLE_NAME).build();
        when(mockIamClient.getRole(any(GetRoleRequest.class)))
                .thenReturn(GetRoleResponse.builder().role(role).build());

        String result = awsIam.createIdentity(
                TEST_ROLE_NAME,
                TEST_DESCRIPTION,
                TEST_TENANT_ID,
                TEST_REGION,
                Optional.empty(),
                Optional.empty());

        assertEquals(TEST_ROLE_ARN, result);

        ArgumentCaptor<GetRoleRequest> getRoleCaptor = ArgumentCaptor.forClass(GetRoleRequest.class);
        verify(mockIamClient, times(1)).getRole(getRoleCaptor.capture());
        assertEquals(TEST_ROLE_NAME, getRoleCaptor.getValue().roleName());
    }

    @Test
    void testGetIdentityReturnsRoleArn() {
        Role role = Role.builder().arn(TEST_ROLE_ARN).roleName(TEST_ROLE_NAME).build();
        when(mockIamClient.getRole(any(GetRoleRequest.class)))
                .thenReturn(GetRoleResponse.builder().role(role).build());

        String result = awsIam.getIdentity(TEST_ROLE_NAME, TEST_TENANT_ID, TEST_REGION);

        assertEquals(TEST_ROLE_ARN, result);

        ArgumentCaptor<GetRoleRequest> captor = ArgumentCaptor.forClass(GetRoleRequest.class);
        verify(mockIamClient, times(1)).getRole(captor.capture());
        assertEquals(TEST_ROLE_NAME, captor.getValue().roleName());
    }

    @Test
    void testGetIdentityThrowsException() {
        RuntimeException genericException = new RuntimeException("Role not found");

        when(mockIamClient.getRole(any(GetRoleRequest.class)))
                .thenThrow(genericException);

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                awsIam.getIdentity(TEST_ROLE_NAME, TEST_TENANT_ID, TEST_REGION)
        );

        assertEquals(genericException, exception);
    }

    @Test
    void testDeleteIdentitySuccessfully() {
        when(mockIamClient.deleteRole(any(DeleteRoleRequest.class)))
                .thenReturn(DeleteRoleResponse.builder().build());

        assertDoesNotThrow(() ->
                awsIam.deleteIdentity(TEST_ROLE_NAME, TEST_TENANT_ID, TEST_REGION)
        );

        ArgumentCaptor<DeleteRoleRequest> captor = ArgumentCaptor.forClass(DeleteRoleRequest.class);
        verify(mockIamClient, times(1)).deleteRole(captor.capture());
        assertEquals(TEST_ROLE_NAME, captor.getValue().roleName());
    }

    @Test
    void testDeleteIdentityThrowsException() {
        RuntimeException genericException = new RuntimeException("Delete failed");

        when(mockIamClient.deleteRole(any(DeleteRoleRequest.class)))
                .thenThrow(genericException);

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                awsIam.deleteIdentity(TEST_ROLE_NAME, TEST_TENANT_ID, TEST_REGION)
        );

        assertEquals(genericException, exception);
    }

    @Test
    void testDeleteIdentityVerifiesRoleName() {
        when(mockIamClient.deleteRole(any(DeleteRoleRequest.class)))
                .thenReturn(DeleteRoleResponse.builder().build());

        awsIam.deleteIdentity(TEST_ROLE_NAME, TEST_TENANT_ID, TEST_REGION);

        ArgumentCaptor<DeleteRoleRequest> captor = ArgumentCaptor.forClass(DeleteRoleRequest.class);
        verify(mockIamClient).deleteRole(captor.capture());

        DeleteRoleRequest capturedRequest = captor.getValue();
        assertEquals(TEST_ROLE_NAME, capturedRequest.roleName());
    }
}
