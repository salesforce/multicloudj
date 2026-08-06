package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TableStoreCredentialsProviderTest {

  @Test
  public void testSessionCredentialsProvider() {
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(new StsCredentials("key", "secret", "token"))
            .build();

    CredentialsProvider provider =
        TableStoreCredentialsProvider.getCredentialsProvider(credentialsOverrider, "cn-shanghai");

    Assertions.assertNotNull(provider);
    Assertions.assertEquals("key", provider.getCredentials().getAccessKeyId());
    Assertions.assertEquals("secret", provider.getCredentials().getAccessKeySecret());
    Assertions.assertEquals("token", provider.getCredentials().getSecurityToken());
  }

  @Test
  public void testSessionCredentialsSupplierIsRejected() {
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentialsSupplier(() -> new StsCredentials("key", "secret", "token"))
            .build();

    InvalidArgumentException failure =
        Assertions.assertThrows(
            InvalidArgumentException.class,
            () ->
                TableStoreCredentialsProvider.getCredentialsProvider(
                    credentialsOverrider, "cn-shanghai"));
    Assertions.assertTrue(
        failure.getMessage().contains("withSessionCredentialsSupplier"),
        "unexpected message: " + failure.getMessage());
  }

  @Test
  public void testNullOverrider() {
    Assertions.assertNull(
        TableStoreCredentialsProvider.getCredentialsProvider(null, "cn-shanghai"));
  }

  @Test
  public void testNullType() {
    CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(null).build();

    Assertions.assertNull(
        TableStoreCredentialsProvider.getCredentialsProvider(credentialsOverrider, "cn-shanghai"));
  }
}
