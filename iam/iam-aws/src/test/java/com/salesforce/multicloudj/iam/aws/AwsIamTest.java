package com.salesforce.multicloudj.iam.aws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.GetAttachedPoliciesRequest;
import com.salesforce.multicloudj.iam.model.GetInlinePolicyDetailsRequest;
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
import software.amazon.awssdk.services.iam.model.GetRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.GetRolePolicyResponse;
import software.amazon.awssdk.services.iam.model.NoSuchEntityException;
import software.amazon.awssdk.services.iam.model.Role;
import software.amazon.awssdk.services.iam.model.UpdateAssumeRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.UpdateAssumeRolePolicyResponse;
import software.amazon.awssdk.services.iam.model.UpdateRoleRequest;
import software.amazon.awssdk.services.iam.model.UpdateRoleResponse;
import software.amazon.awssdk.services.iam.model.DeleteRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.DeleteRolePolicyResponse;
import software.amazon.awssdk.services.iam.paginators.ListRolePoliciesIterable;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
    private static final String TEST_POLICY_NAME = "TestPolicy";
    private static final String TEST_POLICY_DOCUMENT = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"s3:GetObject\",\"Resource\":\"*\"}]}";
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

        JsonNode doc = OBJECT_MAPPER.readTree(captor.getValue().assumeRolePolicyDocument());
        assertNotNull(doc);

        JsonNode externalId = doc.at("/Statement/0/Condition/StringEquals/sts:ExternalId");
        assertFalse(externalId.isMissingNode(), "ExternalId should not be missing");
        assertEquals("external-id", externalId.asText());
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

        JsonNode doc = OBJECT_MAPPER.readTree(captor.getValue().assumeRolePolicyDocument());
        assertNotNull(doc);

        JsonNode service = doc.at("/Statement/0/Principal/Service");
        assertFalse(service.isMissingNode(), "Service principal should not be missing");
        assertEquals("ec2.amazonaws.com", service.isArray() ? service.get(0).asText() : service.asText());
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

        JsonNode doc = OBJECT_MAPPER.readTree(request.assumeRolePolicyDocument());
        assertNotNull(doc);
        assertFalse(doc.at("/Version").isMissingNode(), "Version should not be missing");
        assertEquals("2012-10-17", doc.at("/Version").asText());

        JsonNode stmt = doc.at("/Statement/0");
        assertFalse(stmt.isMissingNode(), "Statement should not be missing");
        assertEquals("Allow", stmt.at("/Effect").asText());
        assertEquals("sts:AssumeRole", stmt.at("/Action").asText());
        JsonNode awsPrincipal = stmt.at("/Principal/AWS");
        assertEquals("arn:aws:iam::" + TEST_TENANT_ID + ":root",
                awsPrincipal.isArray() ? awsPrincipal.get(0).asText() : awsPrincipal.asText());
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

        JsonNode doc = OBJECT_MAPPER.readTree(captor.getValue().assumeRolePolicyDocument());
        assertNotNull(doc);

        JsonNode principals = doc.at("/Statement/0/Principal/AWS");
        assertFalse(principals.isMissingNode(), "AWS Principal should not be missing");
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

        awsIam.deleteIdentity(TEST_ROLE_NAME, TEST_TENANT_ID, TEST_REGION);

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

    @Test
    void testCreateIdentityAlreadyExistsUpdatesDescriptionWhenDifferent() throws Exception {
        when(mockIamClient.createRole(any(CreateRoleRequest.class)))
                .thenThrow(EntityAlreadyExistsException.builder().message("exists").build());

        String existingDescription = "Old description";
        String newDescription = "New description";
        String assumeRolePolicy = buildDefaultAssumeRolePolicy();
        String encodedPolicy = URLEncoder.encode(assumeRolePolicy, StandardCharsets.UTF_8);

        Role existingRole = Role.builder()
                .arn(TEST_ROLE_ARN)
                .roleName(TEST_ROLE_NAME)
                .description(existingDescription)
                .assumeRolePolicyDocument(encodedPolicy)
                .maxSessionDuration(3600)
                .build();

        when(mockIamClient.getRole(any(GetRoleRequest.class)))
                .thenReturn(GetRoleResponse.builder().role(existingRole).build());
        when(mockIamClient.updateRole(any(UpdateRoleRequest.class)))
                .thenReturn(UpdateRoleResponse.builder().build());

        String result = awsIam.createIdentity(
                TEST_ROLE_NAME,
                newDescription,
                TEST_TENANT_ID,
                TEST_REGION,
                Optional.empty(),
                Optional.empty());

        assertEquals(TEST_ROLE_ARN, result);

        ArgumentCaptor<UpdateRoleRequest> updateCaptor = ArgumentCaptor.forClass(UpdateRoleRequest.class);
        verify(mockIamClient, times(1)).updateRole(updateCaptor.capture());
        assertEquals(TEST_ROLE_NAME, updateCaptor.getValue().roleName());
        assertEquals(newDescription, updateCaptor.getValue().description());
    }

    @Test
    void testCreateIdentityAlreadyExistsUpdatesMaxSessionDurationWhenDifferent() {
        when(mockIamClient.createRole(any(CreateRoleRequest.class)))
                .thenThrow(EntityAlreadyExistsException.builder().message("exists").build());

        String assumeRolePolicy = buildDefaultAssumeRolePolicy();
        String encodedPolicy = URLEncoder.encode(assumeRolePolicy, StandardCharsets.UTF_8);

        Role existingRole = Role.builder()
                .arn(TEST_ROLE_ARN)
                .roleName(TEST_ROLE_NAME)
                .description(TEST_DESCRIPTION)
                .assumeRolePolicyDocument(encodedPolicy)
                .maxSessionDuration(3600)
                .build();

        when(mockIamClient.getRole(any(GetRoleRequest.class)))
                .thenReturn(GetRoleResponse.builder().role(existingRole).build());
        when(mockIamClient.updateRole(any(UpdateRoleRequest.class)))
                .thenReturn(UpdateRoleResponse.builder().build());

        CreateOptions options = CreateOptions.builder()
                .maxSessionDuration(7200)
                .build();

        String result = awsIam.createIdentity(
                TEST_ROLE_NAME,
                TEST_DESCRIPTION,
                TEST_TENANT_ID,
                TEST_REGION,
                Optional.empty(),
                Optional.of(options));

        assertEquals(TEST_ROLE_ARN, result);

        ArgumentCaptor<UpdateRoleRequest> updateCaptor = ArgumentCaptor.forClass(UpdateRoleRequest.class);
        verify(mockIamClient, times(1)).updateRole(updateCaptor.capture());
        assertEquals(TEST_ROLE_NAME, updateCaptor.getValue().roleName());
        assertEquals(7200, updateCaptor.getValue().maxSessionDuration());
    }

    @Test
    void testCreateIdentityAlreadyExistsUpdatesTrustPolicyWhenDifferent() throws Exception {
        when(mockIamClient.createRole(any(CreateRoleRequest.class)))
                .thenThrow(EntityAlreadyExistsException.builder().message("exists").build());

        String oldAssumeRolePolicy = buildDefaultAssumeRolePolicy();
        String encodedOldPolicy = URLEncoder.encode(oldAssumeRolePolicy, StandardCharsets.UTF_8);

        Role existingRole = Role.builder()
                .arn(TEST_ROLE_ARN)
                .roleName(TEST_ROLE_NAME)
                .description(TEST_DESCRIPTION)
                .assumeRolePolicyDocument(encodedOldPolicy)
                .maxSessionDuration(3600)
                .build();

        when(mockIamClient.getRole(any(GetRoleRequest.class)))
                .thenReturn(GetRoleResponse.builder().role(existingRole).build());
        when(mockIamClient.updateAssumeRolePolicy(any(UpdateAssumeRolePolicyRequest.class)))
                .thenReturn(UpdateAssumeRolePolicyResponse.builder().build());

        TrustConfiguration newTrustConfig = TrustConfiguration.builder()
                .addTrustedPrincipal("arn:aws:iam::999999999999:root")
                .build();

        String result = awsIam.createIdentity(
                TEST_ROLE_NAME,
                TEST_DESCRIPTION,
                TEST_TENANT_ID,
                TEST_REGION,
                Optional.of(newTrustConfig),
                Optional.empty());

        assertEquals(TEST_ROLE_ARN, result);

        ArgumentCaptor<UpdateAssumeRolePolicyRequest> updatePolicyCaptor =
                ArgumentCaptor.forClass(UpdateAssumeRolePolicyRequest.class);
        verify(mockIamClient, times(1)).updateAssumeRolePolicy(updatePolicyCaptor.capture());
        assertEquals(TEST_ROLE_NAME, updatePolicyCaptor.getValue().roleName());
        assertNotNull(updatePolicyCaptor.getValue().policyDocument());

        JsonNode updatedPolicy = OBJECT_MAPPER.readTree(updatePolicyCaptor.getValue().policyDocument());
        JsonNode principal = updatedPolicy.at("/Statement/0/Principal/AWS");
        assertEquals("arn:aws:iam::999999999999:root", principal.isArray() ? principal.get(0).asText() : principal.asText());
    }

    @Test
    void testCreateIdentityAlreadyExistsDoesNotUpdateWhenAttributesAreSame() {
        when(mockIamClient.createRole(any(CreateRoleRequest.class)))
                .thenThrow(EntityAlreadyExistsException.builder().message("exists").build());

        String assumeRolePolicy = buildDefaultAssumeRolePolicy();
        String encodedPolicy = URLEncoder.encode(assumeRolePolicy, StandardCharsets.UTF_8);

        Role existingRole = Role.builder()
                .arn(TEST_ROLE_ARN)
                .roleName(TEST_ROLE_NAME)
                .description(TEST_DESCRIPTION)
                .assumeRolePolicyDocument(encodedPolicy)
                .maxSessionDuration(3600)
                .build();

        when(mockIamClient.getRole(any(GetRoleRequest.class)))
                .thenReturn(GetRoleResponse.builder().role(existingRole).build());

        String result = awsIam.createIdentity(
                TEST_ROLE_NAME,
                TEST_DESCRIPTION,
                TEST_TENANT_ID,
                TEST_REGION,
                Optional.empty(),
                Optional.empty());

        assertEquals(TEST_ROLE_ARN, result);

        verify(mockIamClient, never()).updateRole(any(UpdateRoleRequest.class));
        verify(mockIamClient, never()).updateAssumeRolePolicy(any(UpdateAssumeRolePolicyRequest.class));
    }

    @Test
    void testCreateIdentityAlreadyExistsUpdatesMultipleAttributesWhenDifferent() throws Exception {
        when(mockIamClient.createRole(any(CreateRoleRequest.class)))
                .thenThrow(EntityAlreadyExistsException.builder().message("exists").build());

        String oldDescription = "Old description";
        String newDescription = "New description";
        String oldAssumeRolePolicy = buildDefaultAssumeRolePolicy();
        String encodedOldPolicy = URLEncoder.encode(oldAssumeRolePolicy, StandardCharsets.UTF_8);

        Role existingRole = Role.builder()
                .arn(TEST_ROLE_ARN)
                .roleName(TEST_ROLE_NAME)
                .description(oldDescription)
                .assumeRolePolicyDocument(encodedOldPolicy)
                .maxSessionDuration(3600)
                .build();

        when(mockIamClient.getRole(any(GetRoleRequest.class)))
                .thenReturn(GetRoleResponse.builder().role(existingRole).build());
        when(mockIamClient.updateRole(any(UpdateRoleRequest.class)))
                .thenReturn(UpdateRoleResponse.builder().build());
        when(mockIamClient.updateAssumeRolePolicy(any(UpdateAssumeRolePolicyRequest.class)))
                .thenReturn(UpdateAssumeRolePolicyResponse.builder().build());

        TrustConfiguration newTrustConfig = TrustConfiguration.builder()
                .addTrustedPrincipal("arn:aws:iam::999999999999:root")
                .build();

        CreateOptions options = CreateOptions.builder()
                .maxSessionDuration(7200)
                .build();

        String result = awsIam.createIdentity(
                TEST_ROLE_NAME,
                newDescription,
                TEST_TENANT_ID,
                TEST_REGION,
                Optional.of(newTrustConfig),
                Optional.of(options));

        assertEquals(TEST_ROLE_ARN, result);

        ArgumentCaptor<UpdateRoleRequest> updateCaptor = ArgumentCaptor.forClass(UpdateRoleRequest.class);
        verify(mockIamClient, times(1)).updateRole(updateCaptor.capture());
        assertEquals(newDescription, updateCaptor.getValue().description());
        assertEquals(7200, updateCaptor.getValue().maxSessionDuration());

        ArgumentCaptor<UpdateAssumeRolePolicyRequest> updatePolicyCaptor =
                ArgumentCaptor.forClass(UpdateAssumeRolePolicyRequest.class);
        verify(mockIamClient, times(1)).updateAssumeRolePolicy(updatePolicyCaptor.capture());
        assertEquals(TEST_ROLE_NAME, updatePolicyCaptor.getValue().roleName());
    }

    private String buildDefaultAssumeRolePolicy() {
        return "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\"," +
                "\"Action\":\"sts:AssumeRole\",\"Principal\":{\"AWS\":[\"arn:aws:iam::" +
                TEST_TENANT_ID + ":root\"]}}]}";
    }

    @Test
    void testGetInlinePolicyDetailsReturnsDocument() {
        when(mockIamClient.getRolePolicy(any(GetRolePolicyRequest.class)))
                .thenReturn(GetRolePolicyResponse.builder()
                        .policyDocument(TEST_POLICY_DOCUMENT)
                        .build());

        String result = awsIam.getInlinePolicyDetails(
                GetInlinePolicyDetailsRequest.builder()
                        .identityName(TEST_ROLE_NAME)
                        .policyName(TEST_POLICY_NAME)
                        .roleName(TEST_ROLE_NAME)
                        .tenantId(TEST_TENANT_ID)
                        .region(TEST_REGION)
                        .build());

        assertEquals(TEST_POLICY_DOCUMENT, result);

        ArgumentCaptor<GetRolePolicyRequest> captor = ArgumentCaptor.forClass(GetRolePolicyRequest.class);
        verify(mockIamClient, times(1)).getRolePolicy(captor.capture());
        assertEquals(TEST_ROLE_NAME, captor.getValue().roleName());
        assertEquals(TEST_POLICY_NAME, captor.getValue().policyName());
    }

    @Test
    void testGetInlinePolicyDetailsWithNoSuchEntityException() {
        when(mockIamClient.getRolePolicy(any(GetRolePolicyRequest.class)))
                .thenThrow(NoSuchEntityException.builder()
                        .message("Policy not found")
                        .build());

        assertThrows(NoSuchEntityException.class, () ->
                awsIam.getInlinePolicyDetails(
                        GetInlinePolicyDetailsRequest.builder()
                                .identityName(TEST_ROLE_NAME)
                                .policyName(TEST_POLICY_NAME)
                                .roleName(TEST_ROLE_NAME)
                                .tenantId(TEST_TENANT_ID)
                                .region(TEST_REGION)
                                .build())
        );

        ArgumentCaptor<GetRolePolicyRequest> captor = ArgumentCaptor.forClass(GetRolePolicyRequest.class);
        verify(mockIamClient, times(1)).getRolePolicy(captor.capture());
        assertEquals(TEST_ROLE_NAME, captor.getValue().roleName());
        assertEquals(TEST_POLICY_NAME, captor.getValue().policyName());
    }

    @Test
    void testGetInlinePolicyDetailsVerifiesParameters() {
        when(mockIamClient.getRolePolicy(any(GetRolePolicyRequest.class)))
                .thenReturn(GetRolePolicyResponse.builder()
                        .policyDocument(TEST_POLICY_DOCUMENT)
                        .build());

        awsIam.getInlinePolicyDetails(
                GetInlinePolicyDetailsRequest.builder()
                        .identityName(TEST_ROLE_NAME)
                        .policyName(TEST_POLICY_NAME)
                        .roleName(TEST_ROLE_NAME)
                        .tenantId(TEST_TENANT_ID)
                        .region(TEST_REGION)
                        .build());

        ArgumentCaptor<GetRolePolicyRequest> captor = ArgumentCaptor.forClass(GetRolePolicyRequest.class);
        verify(mockIamClient).getRolePolicy(captor.capture());

        GetRolePolicyRequest capturedRequest = captor.getValue();
        assertEquals(TEST_ROLE_NAME, capturedRequest.roleName());
        assertEquals(TEST_POLICY_NAME, capturedRequest.policyName());
    }

    @Test
    void testGetInlinePolicyDetailsThrowsGenericException() {
        RuntimeException genericException = new RuntimeException("Service error");

        when(mockIamClient.getRolePolicy(any(GetRolePolicyRequest.class)))
                .thenThrow(genericException);

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                awsIam.getInlinePolicyDetails(
                        GetInlinePolicyDetailsRequest.builder()
                                .identityName(TEST_ROLE_NAME)
                                .policyName(TEST_POLICY_NAME)
                                .roleName(TEST_ROLE_NAME)
                                .tenantId(TEST_TENANT_ID)
                                .region(TEST_REGION)
                                .build())
        );

        assertEquals(genericException, exception);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testGetAttachedPoliciesReturnsPolicyNames() {
        List<String> policyNames = Arrays.asList("Policy1", "Policy2", "Policy3");
        
        // Mock the paginator
        ListRolePoliciesIterable mockPaginator = mock(ListRolePoliciesIterable.class);
        SdkIterable<String> mockPolicyNames = mock(SdkIterable.class);
        when(mockIamClient.listRolePoliciesPaginator(any(Consumer.class)))
                .thenReturn(mockPaginator);
        when(mockPaginator.policyNames()).thenReturn(mockPolicyNames);
        when(mockPolicyNames.stream()).thenReturn(policyNames.stream());

        List<String> result = awsIam.getAttachedPolicies(
                GetAttachedPoliciesRequest.builder()
                        .roleName(TEST_ROLE_NAME)
                        .tenantId(TEST_TENANT_ID)
                        .region(TEST_REGION)
                        .build());

        assertEquals(3, result.size());
        assertEquals(policyNames, result);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testGetAttachedPoliciesHandlesPagination() {
        // The paginator automatically handles pagination, so we just verify all results are returned
        List<String> allPolicies = Arrays.asList("Policy1", "Policy2", "Policy3", "Policy4");
        
        // Mock the paginator
        ListRolePoliciesIterable mockPaginator = mock(ListRolePoliciesIterable.class);
        SdkIterable<String> mockPolicyNames = mock(SdkIterable.class);
        when(mockIamClient.listRolePoliciesPaginator(any(Consumer.class)))
                .thenReturn(mockPaginator);
        when(mockPaginator.policyNames()).thenReturn(mockPolicyNames);
        when(mockPolicyNames.stream()).thenReturn(allPolicies.stream());

        List<String> result = awsIam.getAttachedPolicies(
                GetAttachedPoliciesRequest.builder()
                        .roleName(TEST_ROLE_NAME)
                        .tenantId(TEST_TENANT_ID)
                        .region(TEST_REGION)
                        .build());

        assertEquals(4, result.size());
        assertEquals(allPolicies, result);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testGetAttachedPoliciesThrowsException() {
        // Mock the paginator to throw exception when policyNames() is called
        ListRolePoliciesIterable mockPaginator = mock(ListRolePoliciesIterable.class);
        when(mockIamClient.listRolePoliciesPaginator(any(Consumer.class)))
                .thenReturn(mockPaginator);
        when(mockPaginator.policyNames()).thenThrow(NoSuchEntityException.builder().build());

        assertThrows(NoSuchEntityException.class, () ->
                awsIam.getAttachedPolicies(
                        GetAttachedPoliciesRequest.builder()
                                .roleName(TEST_ROLE_NAME)
                                .tenantId(TEST_TENANT_ID)
                                .region(TEST_REGION)
                                .build())
        );
    }

    @Test
    void testRemovePolicySuccessfully() {
        when(mockIamClient.deleteRolePolicy(any(DeleteRolePolicyRequest.class)))
                .thenReturn(DeleteRolePolicyResponse.builder().build());

        awsIam.removePolicy(TEST_ROLE_NAME, TEST_POLICY_NAME, TEST_TENANT_ID, TEST_REGION);

        ArgumentCaptor<DeleteRolePolicyRequest> captor = ArgumentCaptor.forClass(DeleteRolePolicyRequest.class);
        verify(mockIamClient, times(1)).deleteRolePolicy(captor.capture());
        assertEquals(TEST_ROLE_NAME, captor.getValue().roleName());
        assertEquals(TEST_POLICY_NAME, captor.getValue().policyName());
    }

    @Test
    void testRemovePolicyThrowsException() {
        when(mockIamClient.deleteRolePolicy(any(DeleteRolePolicyRequest.class)))
                .thenThrow(NoSuchEntityException.builder().build());

        assertThrows(NoSuchEntityException.class, () ->
                awsIam.removePolicy(TEST_ROLE_NAME, TEST_POLICY_NAME, TEST_TENANT_ID, TEST_REGION)
        );
    }
}
