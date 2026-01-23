package com.salesforce.multicloudj.sts.model;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AssumedRoleRequestTest {
    @Test
    public void testAssumedRoleRequestBuilderWithProvidedValues() {
        AssumedRoleRequest request = AssumedRoleRequest.newBuilder().withRole("testRole").withSessionName("testSession").withExpiration(60).build();
        Assertions.assertEquals("testRole", request.getRole());
        Assertions.assertEquals("testSession", request.getSessionName());
        Assertions.assertEquals(60, request.getExpiration());
    }

    @Test
    public void testAssumedRoleRequestBuilderWithDefaultValues() {
        AssumedRoleRequest request = AssumedRoleRequest.newBuilder().build();
        Assertions.assertNull(request.getRole());
        Assertions.assertNull(request.getSessionName());
        Assertions.assertEquals(0, request.getExpiration());
        Assertions.assertNull(request.getCredentialScope());
    }

    @Test
    public void testAssumedRoleRequestBuilderWithCredentialScope() {
        CredentialScope.ScopeRule rule = CredentialScope.ScopeRule.builder()
                .availableResource("storage://test-bucket/*")
                .availablePermission("storage:GetObject")
                .build();

        CredentialScope credentialScope = CredentialScope.builder()
                .rule(rule)
                .build();

        AssumedRoleRequest request = AssumedRoleRequest.newBuilder()
                .withRole("testRole")
                .withCredentialScope(credentialScope)
                .build();
        Assertions.assertEquals("testRole", request.getRole());
        Assertions.assertNotNull(request.getCredentialScope());
        Assertions.assertEquals(1, request.getCredentialScope().getRules().size());
    }
}
