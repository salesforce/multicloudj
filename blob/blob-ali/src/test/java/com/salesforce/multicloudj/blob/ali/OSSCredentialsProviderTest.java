package com.salesforce.multicloudj.blob.ali;

import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.common.auth.STSAssumeRoleSessionCredentialsProvider;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OSSCredentialsProviderTest {

    @Test
    public void testAssumeRoleCredentialsProvider() {
        CredentialsOverrider credentialsOverrider =
                new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE).withRole("testRole").build();
        com.aliyun.oss.common.auth.CredentialsProvider aliCredsProvider =
                OSSCredentialsProvider.getCredentialsProvider(credentialsOverrider, "cn-shanghai");
        Assertions.assertNotNull(aliCredsProvider);
        Assertions.assertInstanceOf(STSAssumeRoleSessionCredentialsProvider.class, aliCredsProvider);
    }

    @Test
    public void testStaticCredentialsProvider() {
        StsCredentials stsCredentials = new StsCredentials("key", "secret", "token");
        CredentialsOverrider credentialsOverrider =
                new CredentialsOverrider.Builder(CredentialsType.SESSION).withSessionCredentials(stsCredentials).build();
        com.aliyun.oss.common.auth.CredentialsProvider aliCredsProvider =
                OSSCredentialsProvider.getCredentialsProvider(credentialsOverrider, "cn-shanghai");
        Assertions.assertNotNull(aliCredsProvider);
        Assertions.assertInstanceOf(DefaultCredentialProvider.class, aliCredsProvider);
    }
}