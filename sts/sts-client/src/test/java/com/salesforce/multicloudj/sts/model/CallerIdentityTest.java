package com.salesforce.multicloudj.sts.model;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CallerIdentityTest {

    @Test
    public void stsCredentials() {
        String testUserId = "testUserId";
        String testResourceName = "testResourceName";
        String testAccountId = "testAccountId";

        CallerIdentity identity = new CallerIdentity(testUserId, testResourceName, testAccountId);
        Assertions.assertEquals(testUserId, identity.getUserId(), "User ID doesn't match");
        Assertions.assertEquals(testResourceName, identity.getCloudResourceName(), "Cloud Resource Name doesn't match");
        Assertions.assertEquals(testAccountId, identity.getAccountId(), "Account ID doesn't match");
    }
}