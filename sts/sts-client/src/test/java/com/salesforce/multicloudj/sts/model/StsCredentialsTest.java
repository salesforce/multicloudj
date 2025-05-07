package com.salesforce.multicloudj.sts.model;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StsCredentialsTest {

    @Test
    void stsCredentials() {
        String accessKeyId = "testKeyId";
        String secretAccessKey = "testSecretKey";
        String token = "testToken";

        StsCredentials credentials = new StsCredentials(accessKeyId, secretAccessKey, token);
        Assertions.assertEquals(accessKeyId, credentials.getAccessKeyId(), "AccessKey Id doesn't match");
        Assertions.assertEquals(secretAccessKey, credentials.getAccessKeySecret(), "Secret Key doesn't match");
        Assertions.assertEquals(token, credentials.getSecurityToken(), "Token doesn't match");
    }
}