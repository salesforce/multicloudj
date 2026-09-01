package com.salesforce.multicloudj.pubsub.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.aliyuncs.auth.AlibabaCloudCredentials;
import com.aliyuncs.auth.AlibabaCloudCredentialsProvider;
import com.aliyuncs.auth.BasicSessionCredentials;
import com.aliyuncs.auth.StaticCredentialsProvider;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.Test;

public class MnsCredentialsProviderTest {

  @Test
  void sessionMapsToStaticCredentialsProvider() throws Exception {
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(new StsCredentials("key", "secret", "token"))
            .build();

    AlibabaCloudCredentialsProvider provider =
        MnsCredentialsProvider.getCredentialsProvider(overrider);

    assertNotNull(provider);
    assertInstanceOf(StaticCredentialsProvider.class, provider);
    AlibabaCloudCredentials credentials = provider.getCredentials();
    assertEquals("key", credentials.getAccessKeyId());
    assertEquals("secret", credentials.getAccessKeySecret());
    assertInstanceOf(BasicSessionCredentials.class, credentials);
    assertEquals("token", ((BasicSessionCredentials) credentials).getSessionToken());
  }

  @Test
  void nullOverriderReturnsNull() {
    assertNull(MnsCredentialsProvider.getCredentialsProvider(null));
  }

  @Test
  void nullTypeReturnsNull() {
    CredentialsOverrider overrider = new CredentialsOverrider.Builder(null).build();
    assertNull(MnsCredentialsProvider.getCredentialsProvider(overrider));
  }

  @Test
  void assumeRoleIsDeferredAndReturnsNull() {
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE)
            .withRole("acs:ram::123456:role/test-role")
            .withSessionName("test-session")
            .build();
    assertNull(MnsCredentialsProvider.getCredentialsProvider(overrider));
  }
}
