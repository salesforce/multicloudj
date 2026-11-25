package com.salesforce.multicloudj.blob.ali;

import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.credentials.StaticCredentialsProvider;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OssCredentialsProviderTest {

  @Test
  public void testSessionCredentialsProvider() {
    StsCredentials stsCredentials = new StsCredentials("key", "secret", "token");
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(stsCredentials)
            .build();
    CredentialsProvider provider =
        OssCredentialsProvider.getCredentialsProvider(credentialsOverrider, "cn-shanghai");
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
        OssCredentialsProvider.getCredentialsProvider(credentialsOverrider, "cn-shanghai");
    Assertions.assertNotNull(provider);
    Assertions.assertInstanceOf(
        OssCredentialsProvider.AssumeRoleCredentialsProvider.class, provider);
  }

  @Test
  public void testSessionCredentialsSupplierIsRejectedExplicitly() {
    AtomicInteger supplierCalls = new AtomicInteger();
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentialsSupplier(
                () -> {
                  supplierCalls.incrementAndGet();
                  return new StsCredentials("key", "secret", "token");
                })
            .withRefreshInterval(Duration.ofHours(1))
            .build();

    Assertions.assertThrows(
        UnSupportedOperationException.class,
        () -> OssCredentialsProvider.getCredentialsProvider(credentialsOverrider, "cn-shanghai"));
    Assertions.assertEquals(0, supplierCalls.get());
  }

  @Test
  public void testAssumeRoleWebIdentityStillReturnsNull() {
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
            .withRole("acs:ram::123456:role/test-role")
            .withWebIdentityTokenSupplier(() -> "token")
            .build();

    Assertions.assertNull(
        OssCredentialsProvider.getCredentialsProvider(credentialsOverrider, "cn-shanghai"));
  }

  @Test
  public void testNullOverrider() {
    CredentialsProvider provider =
        OssCredentialsProvider.getCredentialsProvider(null, "cn-shanghai");
    Assertions.assertNull(provider);
  }

  @Test
  public void testNullType() {
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(null).build();
    CredentialsProvider provider =
        OssCredentialsProvider.getCredentialsProvider(credentialsOverrider, "cn-shanghai");
    Assertions.assertNull(provider);
  }
}
