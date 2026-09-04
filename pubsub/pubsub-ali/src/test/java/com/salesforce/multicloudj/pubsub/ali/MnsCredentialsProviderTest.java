package com.salesforce.multicloudj.pubsub.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.aliyuncs.auth.AlibabaCloudCredentials;
import com.aliyuncs.auth.AlibabaCloudCredentialsProvider;
import com.aliyuncs.auth.BasicSessionCredentials;
import com.aliyuncs.auth.StaticCredentialsProvider;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.Test;

public class MnsCredentialsProviderTest {

  private static CredentialsOverrider session(StsCredentials credentials) {
    return new CredentialsOverrider.Builder(CredentialsType.SESSION)
        .withSessionCredentials(credentials)
        .build();
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
  void sessionMapsToStaticCredentialsProvider() throws Exception {
    AlibabaCloudCredentialsProvider provider =
        MnsCredentialsProvider.getCredentialsProvider(
            session(new StsCredentials("key", "secret", "token")));

    assertInstanceOf(StaticCredentialsProvider.class, provider);
    AlibabaCloudCredentials credentials = provider.getCredentials();
    assertEquals("key", credentials.getAccessKeyId());
    assertEquals("secret", credentials.getAccessKeySecret());
    assertInstanceOf(BasicSessionCredentials.class, credentials);
    assertEquals("token", ((BasicSessionCredentials) credentials).getSessionToken());
  }

  @Test
  void sessionValuesArePassedThroughWithoutValidation() throws Exception {
    // Blank/empty field values are not validated here; they are passed to the SDK, which
    // evaluates them at request time.
    AlibabaCloudCredentialsProvider provider =
        MnsCredentialsProvider.getCredentialsProvider(session(new StsCredentials("", "", "")));
    assertInstanceOf(StaticCredentialsProvider.class, provider);
    assertEquals("", provider.getCredentials().getAccessKeyId());
  }

  @Test
  void sessionWithoutCredentialsThrows() {
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION).build();
    assertThrows(
        InvalidArgumentException.class,
        () -> MnsCredentialsProvider.getCredentialsProvider(overrider));
  }

  @Test
  void assumeRoleThrows() {
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE)
            .withRole("acs:ram::123456:role/test-role")
            .withSessionName("test-session")
            .build();
    assertThrows(
        UnSupportedOperationException.class,
        () -> MnsCredentialsProvider.getCredentialsProvider(overrider));
  }

  @Test
  void assumeRoleWebIdentityThrows() {
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY).build();
    assertThrows(
        UnSupportedOperationException.class,
        () -> MnsCredentialsProvider.getCredentialsProvider(overrider));
  }
}
