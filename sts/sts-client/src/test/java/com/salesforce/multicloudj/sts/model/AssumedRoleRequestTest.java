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
    }
}
