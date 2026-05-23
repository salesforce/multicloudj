package com.salesforce.multicloudj.blob.ali;

import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.credentials.StaticCredentialsProvider;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OSSCredentialsProviderTest {

  @Test
  public void testSessionCredentialsProvider() {
    StsCredentials stsCredentials = new StsCredentials("key", "secret", "token");
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(stsCredentials)
            .build();
    CredentialsProvider provider =
        OSSCredentialsProvider.getCredentialsProvider(credentialsOverrider, "cn-shanghai");
    Assertions.assertNotNull(provider);
    Assertions.assertInstanceOf(StaticCredentialsProvider.class, provider);
    Assertions.assertEquals("key", provider.getCredentials().accessKeyId());
    Assertions.assertEquals("secret", provider.getCredentials().accessKeySecret());
    Assertions.assertEquals("token", provider.getCredentials().securityToken());
  }

  @Test
  public void testAssumeRoleCredentialsProvider() {
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE)
            .withRole("acs:ram::123456:role/test-role")
            .withSessionName("test-session")
            .withDurationSeconds(1800)
            .build();
    CredentialsProvider provider =
        OSSCredentialsProvider.getCredentialsProvider(credentialsOverrider, "cn-shanghai");
    Assertions.assertNotNull(provider);
    Assertions.assertInstanceOf(
        OSSCredentialsProvider.AssumeRoleCredentialsProvider.class, provider);
  }

  @Test
  public void testNullOverrider() {
    CredentialsProvider provider =
        OSSCredentialsProvider.getCredentialsProvider(null, "cn-shanghai");
    Assertions.assertNull(provider);
  }

  @Test
  public void testNullType() {
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(null).build();
    CredentialsProvider provider =
        OSSCredentialsProvider.getCredentialsProvider(credentialsOverrider, "cn-shanghai");
    Assertions.assertNull(provider);
  }
}
