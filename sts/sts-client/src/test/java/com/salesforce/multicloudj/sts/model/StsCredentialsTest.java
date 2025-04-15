package com.salesforce.multicloudj.sts.model;


import org.junit.Assert;
import org.junit.Test;

public class StsCredentialsTest {

    @Test
    public void stsCredentials() {
        String accessKeyId = "testKeyId";
        String secretAccessKey = "testSecretKey";
        String token = "testToken";

        StsCredentials credentials = new StsCredentials(accessKeyId, secretAccessKey, token);
        Assert.assertEquals("AccessKey Id doesn't match", accessKeyId, credentials.getAccessKeyId());
        Assert.assertEquals("Secret Key doesn't match", secretAccessKey, credentials.getAccessKeySecret());
        Assert.assertEquals("Token doesn't match", token, credentials.getSecurityToken());
    }
}