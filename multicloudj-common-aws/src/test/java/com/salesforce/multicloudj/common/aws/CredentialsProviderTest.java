package com.salesforce.multicloudj.common.aws;

import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

public class CredentialsProviderTest {

    @Test
    public void testAssumeRoleCredentialsProvider() {
        CredentialsOverrider credentialsOverrider =
                new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE).withRole("testRole").build();
        AwsCredentialsProvider awsCredsProvider =
                CredentialsProvider.getCredentialsProvider(credentialsOverrider, Region.AF_SOUTH_1);
        Assertions.assertNotNull(awsCredsProvider);
        Assertions.assertInstanceOf(StsAssumeRoleCredentialsProvider.class, awsCredsProvider);
    }

    @Test
    public void testStaticCredentialsProvider() {
        StsCredentials stsCredentials = new StsCredentials("key", "secret", "token");
        CredentialsOverrider credentialsOverrider =
                new CredentialsOverrider.Builder(CredentialsType.SESSION).withSessionCredentials(stsCredentials).build();
        AwsCredentialsProvider awsCredsProvider =
                CredentialsProvider.getCredentialsProvider(credentialsOverrider, Region.AF_SOUTH_1);
        Assertions.assertNotNull(awsCredsProvider);
        Assertions.assertInstanceOf(StaticCredentialsProvider.class, awsCredsProvider);
    }
}