package com.salesforce.multicloudj.sts.model;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AssumedRoleRequestTest {
    @Test
    public void TestAssumedRoleRequestBuilderWithProvidedValues() {
        AssumedRoleRequest request = AssumedRoleRequest.newBuilder().withRole("testRole").withSessionName("testSession").withExpiration(60).build();
        Assertions.assertEquals("testRole", request.getRole());
        Assertions.assertEquals("testSession", request.getSessionName());
        Assertions.assertEquals(60, request.getExpiration());
    }

    @Test
    public void TestAssumedRoleRequestBuilderWithDefaultValues() {
        AssumedRoleRequest request = AssumedRoleRequest.newBuilder().build();
        Assertions.assertNull(request.getRole());
        Assertions.assertNull(request.getSessionName());
        Assertions.assertEquals(0, request.getExpiration());
        Assertions.assertNull(request.getCredentialScope());
    }

    @Test
    public void TestAssumedRoleRequestBuilderWithCredentialScope() {
        CredentialScope.ScopeRule rule = CredentialScope.ScopeRule.newBuilder()
                .withAvailableResource("storage://test-bucket/*")
                .addAvailablePermission("storage:GetObject")
                .build();

        CredentialScope credentialScope = CredentialScope.newBuilder()
                .addRule(rule)
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
