package com.salesforce.multicloudj.docstore.ali;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class TableStoreCredentialsProviderTest {

  private static final String REGION = "cn-shanghai";

  @Test
  void refreshableSessionIsRejectedInsteadOfFallingBackToDefaultCredentials() {
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.REFRESHABLE_SESSION)
            .withSessionCredentialsSupplier(() -> new StsCredentials("key", "secret", "token"))
            .withRefreshInterval(Duration.ofMinutes(1))
            .build();

    UnSupportedOperationException failure =
        assertThrows(
            UnSupportedOperationException.class,
            () -> TableStoreCredentialsProvider.getCredentialsProvider(overrider, REGION));

    assertTrue(failure.getMessage().contains("REFRESHABLE_SESSION"));
  }

  @Test
  void sessionStillMapsToAProvider() {
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(new StsCredentials("key", "secret", "token"))
            .build();

    assertNotNull(TableStoreCredentialsProvider.getCredentialsProvider(overrider, REGION));
  }

  @Test
  void assumeRoleAndWebIdentityStillReturnNull() {
    CredentialsOverrider assumeRole =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE)
            .withRole("acs:ram::123456:role/test-role")
            .build();
    CredentialsOverrider webIdentity =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
            .withRole("acs:ram::123456:role/test-role")
            .withWebIdentityTokenSupplier(() -> "token")
            .build();

    assertNull(TableStoreCredentialsProvider.getCredentialsProvider(assumeRole, REGION));
    assertNull(TableStoreCredentialsProvider.getCredentialsProvider(webIdentity, REGION));
  }

  @Test
  void nullOverriderAndNullTypeReturnNull() {
    assertNull(TableStoreCredentialsProvider.getCredentialsProvider(null, REGION));
    assertNull(
        TableStoreCredentialsProvider.getCredentialsProvider(
            new CredentialsOverrider.Builder(null).build(), REGION));
  }
}
