package com.salesforce.multicloudj.sts.model;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CredentialsOverriderTest {
  @Test
  public void testCredentialsOverriderBuilderWithProvidedValues() {
    StsCredentials stsCredentials = new StsCredentials("key", "secret", "token");
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(stsCredentials)
            .withRole("some-role")
            .withDurationSeconds(60)
            .build();
    Assertions.assertEquals(stsCredentials, credentialsOverrider.getSessionCredentials());
    StsCredentials actualStsCredentials = credentialsOverrider.getSessionCredentials();
    Assertions.assertEquals(stsCredentials.getAccessKeyId(), actualStsCredentials.getAccessKeyId());
    Assertions.assertEquals(
        stsCredentials.getAccessKeySecret(), actualStsCredentials.getAccessKeySecret());
    Assertions.assertEquals(
        stsCredentials.getSecurityToken(), actualStsCredentials.getSecurityToken());
    Assertions.assertEquals("some-role", credentialsOverrider.getRole());
    Assertions.assertEquals(60, credentialsOverrider.getDurationSeconds());
  }

  @Test
  public void testCredentialsOverriderBuilderWithNoValues() {
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION).build();
    Assertions.assertNull(credentialsOverrider.getSessionCredentials());
    Assertions.assertNull(credentialsOverrider.getRole());
    Assertions.assertNull(credentialsOverrider.getDurationSeconds());
  }

  @Test
  public void testCredentialsOverriderWithSessionName() {
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE)
            .withRole("test-role")
            .withSessionName("testSession")
            .build();
    Assertions.assertEquals("test-role", credentialsOverrider.getRole());
    Assertions.assertEquals("testSession", credentialsOverrider.getSessionName());
  }

  @Test
  public void testCredentialsOverriderWithWebIdentityTokenSupplier() {
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
            .withRole("test-role")
            .withWebIdentityTokenSupplier(() -> "test-token")
            .build();
    Assertions.assertEquals("test-role", credentialsOverrider.getRole());
    Assertions.assertNotNull(credentialsOverrider.getWebIdentityTokenSupplier());
    Assertions.assertEquals("test-token", credentialsOverrider.getWebIdentityTokenSupplier().get());
  }

  @Test
  public void testCredentialsOverriderWithWebIdentityTokenSupplierAndSessionName() {
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
            .withRole("test-role")
            .withWebIdentityTokenSupplier(() -> "test-token")
            .withSessionName("webSession")
            .withDurationSeconds(3600)
            .build();
    Assertions.assertEquals("test-role", credentialsOverrider.getRole());
    Assertions.assertNotNull(credentialsOverrider.getWebIdentityTokenSupplier());
    Assertions.assertEquals("test-token", credentialsOverrider.getWebIdentityTokenSupplier().get());
    Assertions.assertEquals("webSession", credentialsOverrider.getSessionName());
    Assertions.assertEquals(3600, credentialsOverrider.getDurationSeconds());
  }

  @Test
  public void testSessionWithSupplierAndInterval() {
    Instant expiration = Instant.parse("2030-01-01T00:00:00Z");
    StsCredentials credentials = new StsCredentials("key", "secret", "token", expiration);
    Supplier<StsCredentials> supplier = () -> credentials;
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentialsSupplier(supplier)
            .withRefreshInterval(Duration.ofMinutes(30))
            .build();
    Assertions.assertEquals(CredentialsType.SESSION, credentialsOverrider.getType());
    Assertions.assertSame(supplier, credentialsOverrider.getSessionCredentialsSupplier());
    Assertions.assertSame(credentials, credentialsOverrider.getSessionCredentialsSupplier().get());
    Assertions.assertEquals(Duration.ofMinutes(30), credentialsOverrider.getRefreshInterval());
    Assertions.assertNull(credentialsOverrider.getSessionCredentials());
  }

  @Test
  public void testSessionIntervalWithoutSupplierIsRejected() {
    CredentialsOverrider.Builder builder =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withRefreshInterval(Duration.ofMinutes(30));
    Assertions.assertThrows(InvalidArgumentException.class, builder::build);
  }

  @Test
  public void testSessionSupplierWithoutIntervalIsRejected() {
    CredentialsOverrider.Builder builder =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentialsSupplier(() -> new StsCredentials("key", "secret", "token"));
    Assertions.assertThrows(InvalidArgumentException.class, builder::build);
  }

  @Test
  public void testSessionSupplierWithZeroIntervalIsRejected() {
    CredentialsOverrider.Builder builder =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentialsSupplier(() -> new StsCredentials("key", "secret", "token"))
            .withRefreshInterval(Duration.ZERO);
    Assertions.assertThrows(InvalidArgumentException.class, builder::build);
  }

  @Test
  public void testSessionSupplierWithNegativeIntervalIsRejected() {
    CredentialsOverrider.Builder builder =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentialsSupplier(() -> new StsCredentials("key", "secret", "token"))
            .withRefreshInterval(Duration.ofSeconds(-1));
    Assertions.assertThrows(InvalidArgumentException.class, builder::build);
  }

  @Test
  public void testSessionSupplierWithFixedCredentialsIsRejected() {
    CredentialsOverrider.Builder builder =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(new StsCredentials("key", "secret", "token"))
            .withSessionCredentialsSupplier(() -> new StsCredentials("key", "secret", "token"))
            .withRefreshInterval(Duration.ofMinutes(30));
    Assertions.assertThrows(InvalidArgumentException.class, builder::build);
  }

  @Test
  public void testSupplierOnANonSessionTypeIsRejected() {
    for (CredentialsType type :
        new CredentialsType[] {
          CredentialsType.ASSUME_ROLE, CredentialsType.ASSUME_ROLE_WEB_IDENTITY
        }) {
      CredentialsOverrider.Builder builder =
          new CredentialsOverrider.Builder(type)
              .withSessionCredentialsSupplier(() -> new StsCredentials("key", "secret", "token"))
              .withRefreshInterval(Duration.ofMinutes(30));
      Assertions.assertThrows(InvalidArgumentException.class, builder::build, type.name());
    }
  }

  @Test
  public void testRefreshIntervalOnANonSessionTypeIsRejected() {
    for (CredentialsType type :
        new CredentialsType[] {
          CredentialsType.ASSUME_ROLE, CredentialsType.ASSUME_ROLE_WEB_IDENTITY
        }) {
      CredentialsOverrider.Builder builder =
          new CredentialsOverrider.Builder(type).withRefreshInterval(Duration.ofMinutes(30));
      Assertions.assertThrows(InvalidArgumentException.class, builder::build, type.name());
    }
  }

  @Test
  public void testSessionWithFixedCredentialsAndAnIntervalIsRejected() {
    CredentialsOverrider.Builder builder =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(new StsCredentials("key", "secret", "token"))
            .withRefreshInterval(Duration.ofMinutes(30));
    Assertions.assertThrows(InvalidArgumentException.class, builder::build);
  }

  @Test
  public void testSessionWithNeitherFixedCredentialsNorSupplierIsStillAccepted() {
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION).build();

    Assertions.assertEquals(CredentialsType.SESSION, overrider.getType());
    Assertions.assertNull(overrider.getSessionCredentials());
    Assertions.assertNull(overrider.getSessionCredentialsSupplier());
    Assertions.assertNull(overrider.getCredentialsRefresher());
  }

  @Test
  public void testSessionWithOnlyFixedCredentialsHasNoRefresher() {
    StsCredentials fixed = new StsCredentials("key", "secret", "token");
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(fixed)
            .build();

    Assertions.assertSame(fixed, overrider.getSessionCredentials());
    Assertions.assertNull(overrider.getSessionCredentialsSupplier());
    Assertions.assertNull(overrider.getRefreshInterval());
    Assertions.assertNull(overrider.getCredentialsRefresher());
  }

  @Test
  public void testExistingTypesLeaveRefreshFieldsUnset() {
    CredentialsOverrider session =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(new StsCredentials("key", "secret", "token"))
            .build();
    CredentialsOverrider assumeRole =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE).withRole("role").build();
    CredentialsOverrider webIdentity =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
            .withRole("role")
            .withWebIdentityTokenSupplier(() -> "token")
            .build();
    CredentialsOverrider[] overriders = {session, assumeRole, webIdentity};
    for (CredentialsOverrider overrider : overriders) {
      Assertions.assertNull(overrider.getSessionCredentialsSupplier());
      Assertions.assertNull(overrider.getRefreshInterval());
      Assertions.assertNull(overrider.getCredentialsRefresher());
    }
  }

  @Test
  public void testSessionOverriderWithASupplierOwnsOneRefresherBackedByIt() {
    StsCredentials supplied = new StsCredentials("key", "secret", "token");
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentialsSupplier(() -> supplied)
            .withRefreshInterval(Duration.ofMinutes(30))
            .build();

    CredentialsRefresher refresher = overrider.getCredentialsRefresher();

    Assertions.assertNotNull(refresher);
    Assertions.assertSame(refresher, overrider.getCredentialsRefresher());
    Assertions.assertSame(supplied, refresher.getCredentials());
  }

  @Test
  public void testSeparateOverridersDoNotShareARefresher() {
    CredentialsOverrider.Builder builder =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentialsSupplier(() -> new StsCredentials("key", "secret", "token"))
            .withRefreshInterval(Duration.ofMinutes(30));

    CredentialsOverrider first = builder.build();
    CredentialsOverrider second = builder.build();

    Assertions.assertNotSame(first.getCredentialsRefresher(), second.getCredentialsRefresher());
  }

  @Test
  void buildRejectsARefreshIntervalThatOverflowsTheCutoffArithmetic() {
    CredentialsOverrider.Builder builder =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentialsSupplier(() -> new StsCredentials("id", "secret", "token"))
            .withRefreshInterval(Duration.ofSeconds(Long.MAX_VALUE));

    Assertions.assertThrows(InvalidArgumentException.class, builder::build);
  }
}
