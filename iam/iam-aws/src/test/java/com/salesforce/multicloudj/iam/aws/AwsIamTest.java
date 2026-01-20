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
import software.amazon.awssdk.services.iam.model.GetRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.GetRolePolicyResponse;
import software.amazon.awssdk.services.iam.model.NoSuchEntityException;
import software.amazon.awssdk.services.iam.model.Role;
import software.amazon.awssdk.services.iam.model.UpdateAssumeRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.UpdateAssumeRolePolicyResponse;
import software.amazon.awssdk.services.iam.model.UpdateRoleRequest;
import software.amazon.awssdk.services.iam.model.UpdateRoleResponse;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String DEFAULT_ASSUME_ROLE_POLICY_TEMPLATE = 
        "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\"," +
        "\"Action\":\"sts:AssumeRole\",\"Principal\":{\"AWS\":\"arn:aws:iam::%s:root\"}}]}";

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
        setupCreateRoleSuccess();

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
        setupCreateRoleSuccess();

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
        assertEquals("ec2.amazonaws.com", service.asText());
    }

    @Test
    void testCreateIdentityWithoutTrustConfigDefaultsToSameAccountRoot() throws Exception {
        setupCreateRoleSuccess();

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
        assertEquals("arn:aws:iam::" + TEST_TENANT_ID + ":root",
                stmt.at("/Principal/AWS").asText());
    }

    @Test
    void testCreateIdentityWithTrustConfigUsesTrustedPrincipals() throws Exception {
        setupCreateRoleSuccess();

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
        setupCreateRoleSuccess();

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
        Role role = Role.builder().arn(TEST_ROLE_ARN).roleName(TEST_ROLE_NAME).build();
        setupEntityAlreadyExists(role);

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

    @Test
    void testCreateIdentityAlreadyExistsUpdatesDescriptionWhenDifferent() throws Exception {
        String existingDescription = "Old description";
        String newDescription = "New description";
        String assumeRolePolicy = buildDefaultAssumeRolePolicy();
        Role existingRole = buildTestRole(existingDescription, assumeRolePolicy, 3600);
        setupEntityAlreadyExists(existingRole);
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
        String assumeRolePolicy = buildDefaultAssumeRolePolicy();
        Role existingRole = buildTestRole(TEST_DESCRIPTION, assumeRolePolicy, 3600);
        setupEntityAlreadyExists(existingRole);
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
        String oldAssumeRolePolicy = buildDefaultAssumeRolePolicy();
        Role existingRole = buildTestRole(TEST_DESCRIPTION, oldAssumeRolePolicy, 3600);
        setupEntityAlreadyExists(existingRole);
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
        assertEquals("arn:aws:iam::999999999999:root", principal.asText());
    }

    @Test
    void testCreateIdentityAlreadyExistsDoesNotUpdateWhenAttributesAreSame() {
        String assumeRolePolicy = buildDefaultAssumeRolePolicy();
        Role existingRole = buildTestRole(TEST_DESCRIPTION, assumeRolePolicy, 3600);
        setupEntityAlreadyExists(existingRole);

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
        String oldDescription = "Old description";
        String newDescription = "New description";
        String oldAssumeRolePolicy = buildDefaultAssumeRolePolicy();
        Role existingRole = buildTestRole(oldDescription, oldAssumeRolePolicy, 3600);
        setupEntityAlreadyExists(existingRole);
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
        return String.format(DEFAULT_ASSUME_ROLE_POLICY_TEMPLATE, TEST_TENANT_ID);
    }

    private Role buildTestRole(String description, String assumeRolePolicy, Integer maxSessionDuration) {
        String encodedPolicy = assumeRolePolicy != null ? 
            URLEncoder.encode(assumeRolePolicy, StandardCharsets.UTF_8) : null;
        return Role.builder()
                .arn(TEST_ROLE_ARN)
                .roleName(TEST_ROLE_NAME)
                .description(description)
                .assumeRolePolicyDocument(encodedPolicy)
                .maxSessionDuration(maxSessionDuration)
                .build();
    }

    private void setupCreateRoleSuccess() {
        Role role = Role.builder().arn(TEST_ROLE_ARN).roleName(TEST_ROLE_NAME).build();
        when(mockIamClient.createRole(any(CreateRoleRequest.class)))
                .thenReturn(CreateRoleResponse.builder().role(role).build());
    }

    private void setupEntityAlreadyExists(Role existingRole) {
        when(mockIamClient.createRole(any(CreateRoleRequest.class)))
                .thenThrow(EntityAlreadyExistsException.builder().message("exists").build());
        when(mockIamClient.getRole(any(GetRoleRequest.class)))
                .thenReturn(GetRoleResponse.builder().role(existingRole).build());
    }

    @Test
    void testGetInlinePolicyDetailsReturnsDocument() {
        when(mockIamClient.getRolePolicy(any(GetRolePolicyRequest.class)))
                .thenReturn(GetRolePolicyResponse.builder()
                        .policyDocument(TEST_POLICY_DOCUMENT)
                        .build());

        String result = awsIam.getInlinePolicyDetails(
                TEST_ROLE_NAME,
                TEST_POLICY_NAME,
                TEST_ROLE_NAME,
                TEST_TENANT_ID,
                TEST_REGION);

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
                        TEST_ROLE_NAME,
                        TEST_POLICY_NAME,
                        TEST_ROLE_NAME,
                        TEST_TENANT_ID,
                        TEST_REGION)
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
                TEST_ROLE_NAME,
                TEST_POLICY_NAME,
                TEST_ROLE_NAME,
                TEST_TENANT_ID,
                TEST_REGION);

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
                        TEST_ROLE_NAME,
                        TEST_POLICY_NAME,
                        TEST_ROLE_NAME,
                        TEST_TENANT_ID,
                        TEST_REGION)
        );

        assertEquals(genericException, exception);
    }

    @Test
    void testGetExceptionWithSubstrateSdkException() {
        com.salesforce.multicloudj.common.exceptions.InvalidArgumentException testException = 
            new com.salesforce.multicloudj.common.exceptions.InvalidArgumentException("test");
        
        Class<? extends com.salesforce.multicloudj.common.exceptions.SubstrateSdkException> result = 
            awsIam.getException(testException);
        
        assertEquals(com.salesforce.multicloudj.common.exceptions.InvalidArgumentException.class, result);
    }

    @Test
    void testGetExceptionWithAwsServiceException() {
        software.amazon.awssdk.awscore.exception.AwsErrorDetails errorDetails = 
            software.amazon.awssdk.awscore.exception.AwsErrorDetails.builder()
                .errorCode("NoSuchEntity")
                .errorMessage("Not found")
                .build();
        
        software.amazon.awssdk.services.iam.model.NoSuchEntityException awsException = 
            (NoSuchEntityException) NoSuchEntityException.builder()
                .message("Not found")
                .awsErrorDetails(errorDetails)
                .build();
        
        Class<? extends com.salesforce.multicloudj.common.exceptions.SubstrateSdkException> result = 
            awsIam.getException(awsException);
        
        assertEquals(com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException.class, result);
    }

    @Test
    void testGetExceptionWithAwsServiceExceptionNoErrorDetails() {
        software.amazon.awssdk.awscore.exception.AwsServiceException awsException = 
            software.amazon.awssdk.awscore.exception.AwsServiceException.builder()
                .message("Service error")
                .build();
        
        Class<? extends com.salesforce.multicloudj.common.exceptions.SubstrateSdkException> result = 
            awsIam.getException(awsException);
        
        assertEquals(com.salesforce.multicloudj.common.exceptions.UnknownException.class, result);
    }

    @Test
    void testGetExceptionWithSdkClientException() {
        software.amazon.awssdk.core.exception.SdkClientException sdkException = 
            software.amazon.awssdk.core.exception.SdkClientException.builder()
                .message("Client error")
                .build();
        
        Class<? extends com.salesforce.multicloudj.common.exceptions.SubstrateSdkException> result = 
            awsIam.getException(sdkException);
        
        assertEquals(com.salesforce.multicloudj.common.exceptions.InvalidArgumentException.class, result);
    }

    @Test
    void testGetExceptionWithIllegalArgumentException() {
        IllegalArgumentException illegalArgException = new IllegalArgumentException("Invalid arg");
        
        Class<? extends com.salesforce.multicloudj.common.exceptions.SubstrateSdkException> result = 
            awsIam.getException(illegalArgException);
        
        assertEquals(com.salesforce.multicloudj.common.exceptions.InvalidArgumentException.class, result);
    }

    @Test
    void testGetExceptionWithUnknownException() {
        RuntimeException unknownException = new RuntimeException("Unknown error");
        
        Class<? extends com.salesforce.multicloudj.common.exceptions.SubstrateSdkException> result = 
            awsIam.getException(unknownException);
        
        assertEquals(com.salesforce.multicloudj.common.exceptions.UnknownException.class, result);
    }

    @Test
    void testCloseClosesIamClient() throws Exception {
        awsIam.close();
        verify(mockIamClient, times(1)).close();
    }

    @Test
    void testBuilderReturnsNewBuilder() {
        com.salesforce.multicloudj.common.provider.Provider.Builder builder = awsIam.builder();
        assertNotNull(builder);
        assertTrue(builder instanceof AwsIam.Builder);
    }

    @Test
    void testBuilderSelfReturnsThis() {
        AwsIam.Builder builder = new AwsIam.Builder();
        assertEquals(builder, builder.self());
    }

    @Test
    void testBuilderBuildCreatesAwsIam() {
        AwsIam.Builder builder = new AwsIam.Builder()
                .withIamClient(mockIamClient)
                .withRegion(TEST_REGION);
        AwsIam result = builder.build();
        assertNotNull(result);
    }

    @Test
    void testBuilderBuildWithoutIamClientCreatesDefault() {
        AwsIam.Builder builder = new AwsIam.Builder()
                .withRegion(TEST_REGION);
        AwsIam result = builder.build();
        assertNotNull(result);
    }

    @Test
    void testDefaultConstructorCreatesAwsIam() {
        AwsIam result = new AwsIam();
        assertNotNull(result);
    }

    @Test
    void testDoAttachInlinePolicyThrowsUnsupportedOperation() {
        assertThrows(UnsupportedOperationException.class, () ->
            awsIam.attachInlinePolicy(null, TEST_TENANT_ID, TEST_REGION, TEST_ROLE_NAME)
        );
    }

    @Test
    void testDoGetAttachedPoliciesThrowsUnsupportedOperation() {
        assertThrows(UnsupportedOperationException.class, () ->
            awsIam.getAttachedPolicies(TEST_ROLE_NAME, TEST_TENANT_ID, TEST_REGION)
        );
    }

    @Test
    void testDoRemovePolicyThrowsUnsupportedOperation() {
        assertThrows(UnsupportedOperationException.class, () ->
            awsIam.removePolicy(TEST_ROLE_NAME, TEST_POLICY_NAME, TEST_TENANT_ID, TEST_REGION)
        );
    }

    @Test
    void testGetInlinePolicyDetailsWithBlankIdentityNameThrowsException() {
        assertThrows(com.salesforce.multicloudj.common.exceptions.InvalidArgumentException.class, () ->
            awsIam.getInlinePolicyDetails("", TEST_POLICY_NAME, TEST_ROLE_NAME, TEST_TENANT_ID, TEST_REGION)
        );
    }

    @Test
    void testGetInlinePolicyDetailsWithBlankPolicyNameThrowsException() {
        assertThrows(com.salesforce.multicloudj.common.exceptions.InvalidArgumentException.class, () ->
            awsIam.getInlinePolicyDetails(TEST_ROLE_NAME, "", TEST_ROLE_NAME, TEST_TENANT_ID, TEST_REGION)
        );
    }

    @Test
    void testCreateIdentityWithBlankTrustedPrincipalSkipsIt() throws Exception {
        setupCreateRoleSuccess();

        TrustConfiguration trustConfiguration = TrustConfiguration.builder()
                .addTrustedPrincipal("")
                .addTrustedPrincipal("arn:aws:iam::999999999999:root")
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
        JsonNode principals = doc.at("/Statement/0/Principal/AWS");
        assertEquals("arn:aws:iam::999999999999:root", principals.asText());
    }

    @Test
    void testCreateIdentityWithAccountIdConvertsToArn() throws Exception {
        setupCreateRoleSuccess();

        TrustConfiguration trustConfiguration = TrustConfiguration.builder()
                .addTrustedPrincipal("999999999999")
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
        JsonNode principals = doc.at("/Statement/0/Principal/AWS");
        assertEquals("arn:aws:iam::999999999999:root", principals.asText());
    }

    @Test
    void testCreateIdentityWithMixedAwsAndServicePrincipals() throws Exception {
        setupCreateRoleSuccess();

        TrustConfiguration trustConfiguration = TrustConfiguration.builder()
                .addTrustedPrincipal("arn:aws:iam::999999999999:root")
                .addTrustedPrincipal("ec2.amazonaws.com")
                .addTrustedPrincipal("lambda.amazonaws.com")
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
        JsonNode awsPrincipals = doc.at("/Statement/0/Principal/AWS");
        assertEquals("arn:aws:iam::999999999999:root", awsPrincipals.asText());
        
        JsonNode servicePrincipals = doc.at("/Statement/0/Principal/Service");
        assertTrue(servicePrincipals.isArray());
        assertEquals(2, servicePrincipals.size());
    }

    @Test
    void testCreateIdentityWithNonMatchingPrincipalTreatedAsAws() throws Exception {
        setupCreateRoleSuccess();

        TrustConfiguration trustConfiguration = TrustConfiguration.builder()
                .addTrustedPrincipal("some-custom-principal")
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
        JsonNode principals = doc.at("/Statement/0/Principal/AWS");
        assertEquals("some-custom-principal", principals.asText());
    }

    @Test
    void testCreateIdentityWithNullDescriptionUsesEmpty() {
        setupCreateRoleSuccess();

        String result = awsIam.createIdentity(
                TEST_ROLE_NAME,
                null,
                TEST_TENANT_ID,
                TEST_REGION,
                Optional.empty(),
                Optional.empty());

        assertEquals(TEST_ROLE_ARN, result);

        ArgumentCaptor<CreateRoleRequest> captor = ArgumentCaptor.forClass(CreateRoleRequest.class);
        verify(mockIamClient, times(1)).createRole(captor.capture());
        assertEquals("", captor.getValue().description());
    }

    @Test
    void testCreateIdentityWithBlankPathNotSet() {
        setupCreateRoleSuccess();

        CreateOptions options = CreateOptions.builder()
                .path("")
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
        assertEquals(null, captor.getValue().path());
    }

    @Test
    void testCreateIdentityWithBlankPermissionBoundaryNotSet() {
        setupCreateRoleSuccess();

        CreateOptions options = CreateOptions.builder()
                .permissionBoundary("")
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
        assertEquals(null, captor.getValue().permissionsBoundary());
    }

    @Test
    void testCreateIdentityAlreadyExistsWithNullDescription() {
        String assumeRolePolicy = buildDefaultAssumeRolePolicy();
        Role existingRole = buildTestRole(null, assumeRolePolicy, 3600);
        setupEntityAlreadyExists(existingRole);
        when(mockIamClient.updateRole(any(UpdateRoleRequest.class)))
                .thenReturn(UpdateRoleResponse.builder().build());

        String result = awsIam.createIdentity(
                TEST_ROLE_NAME,
                TEST_DESCRIPTION,
                TEST_TENANT_ID,
                TEST_REGION,
                Optional.empty(),
                Optional.empty());

        assertEquals(TEST_ROLE_ARN, result);
        verify(mockIamClient, times(1)).updateRole(any(UpdateRoleRequest.class));
    }

    @Test
    void testGetIdentityReturnsNullWhenRoleIsNull() {
        when(mockIamClient.getRole(any(GetRoleRequest.class)))
                .thenReturn(GetRoleResponse.builder().build());

        String result = awsIam.getIdentity(TEST_ROLE_NAME, TEST_TENANT_ID, TEST_REGION);

        assertEquals(null, result);
    }

    @Test
    void testCreateIdentityReturnsNullWhenRoleIsNull() {
        when(mockIamClient.createRole(any(CreateRoleRequest.class)))
                .thenReturn(CreateRoleResponse.builder().build());

        String result = awsIam.createIdentity(
                TEST_ROLE_NAME,
                TEST_DESCRIPTION,
                TEST_TENANT_ID,
                TEST_REGION,
                Optional.empty(),
                Optional.empty());

        assertEquals(null, result);
    }

    @Test
    void testCreateIdentityAlreadyExistsReturnsNullWhenRoleIsNull() {
        when(mockIamClient.createRole(any(CreateRoleRequest.class)))
                .thenThrow(EntityAlreadyExistsException.builder().message("exists").build());
        when(mockIamClient.getRole(any(GetRoleRequest.class)))
                .thenReturn(GetRoleResponse.builder().build());

        String result = awsIam.createIdentity(
                TEST_ROLE_NAME,
                TEST_DESCRIPTION,
                TEST_TENANT_ID,
                TEST_REGION,
                Optional.empty(),
                Optional.empty());

        assertEquals(null, result);
    }
}
