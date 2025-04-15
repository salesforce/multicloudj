package com.salesforce.multicloudj.sts.model;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GetAccessTokenRequestTest {
    @Test
    public void testGetAccessTokenRequestWithProvidedValues() {
        GetAccessTokenRequest request = GetAccessTokenRequest.newBuilder().withDurationSeconds(60).build();
        Assertions.assertEquals(Integer.valueOf(60), request.getDuration());
    }

    @Test
    public void testAssumedRoleRequestBuilderWithDefaultValues() {
        GetAccessTokenRequest request = GetAccessTokenRequest.newBuilder().build();
        Assertions.assertNull(request.getDuration());
    }
}
