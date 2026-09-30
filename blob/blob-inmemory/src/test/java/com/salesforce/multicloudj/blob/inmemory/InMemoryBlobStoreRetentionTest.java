package com.salesforce.multicloudj.blob.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.multicloudj.blob.driver.ObjectLockConfiguration;
import com.salesforce.multicloudj.blob.driver.ObjectLockInfo;
import com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig;
import com.salesforce.multicloudj.blob.driver.RetentionMode;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import java.io.ByteArrayInputStream;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Rules-table tests for {@link InMemoryBlobStore#updateObjectRetention(String, String,
 * ObjectRetentionConfig)}.
 *
 * <p>The in-memory provider must mirror the AWS/GCP server-side rules table exactly, so
 * unit-test parity here is the cheapest way to validate the rules helper before wiring it
 * into providers that talk to real clouds.
 */
class InMemoryBlobStoreRetentionTest {

  private static final Instant INITIAL = Instant.parse("2030-01-01T00:00:00Z");
  private static final Instant LATER = Instant.parse("2030-06-01T00:00:00Z");
  private static final Instant EARLIER = Instant.parse("2029-06-01T00:00:00Z");

  private InMemoryBlobStore store;

  @BeforeEach
  void setUp() {
    store =
        new InMemoryBlobStore.Builder()
            .withBucket("bucket-1")
            .withRegion("local")
            .build();
    InMemoryBlobStore.createBucket("bucket-1");
  }

  // ---- helpers -----------------------------------------------------------------

  private void uploadWithRetention(String key, RetentionMode mode, Instant retainUntil) {
    byte[] content = "x".getBytes();
    store.upload(
        new UploadRequest.Builder()
            .withKey(key)
            .withContentLength(content.length)
            .withObjectLock(
                ObjectLockConfiguration.builder()
                    .mode(mode)
                    .retainUntilDate(retainUntil)
                    .legalHold(false)
                    .build())
            .build(),
        new ByteArrayInputStream(content));
  }

  private ObjectRetentionConfig cfg(RetentionMode mode, Instant date, Boolean bypass) {
    return ObjectRetentionConfig.builder()
        .mode(mode)
        .retainUntilDate(date)
        .bypassGovernanceRetention(bypass)
        .build();
  }

  // ---- rules table -------------------------------------------------------------

  @Test
  void noCurrentRetention_throws() {
    String key = "k/no-retention";
    byte[] content = "x".getBytes();
    store.upload(
        new UploadRequest.Builder().withKey(key).withContentLength(content.length).build(),
        new ByteArrayInputStream(content));

    assertThrows(
        FailedPreconditionException.class,
        () -> store.updateObjectRetention(key, null, cfg(RetentionMode.GOVERNANCE, LATER, null)));
  }

  @Test
  void governance_extending_succeeds() {
    String key = "k/gov-extend";
    uploadWithRetention(key, RetentionMode.GOVERNANCE, INITIAL);

    store.updateObjectRetention(key, null, cfg(RetentionMode.GOVERNANCE, LATER, null));

    ObjectLockInfo info = store.getObjectLock(key, null);
    assertEquals(RetentionMode.GOVERNANCE, info.getMode());
    assertEquals(LATER, info.getRetainUntilDate());
  }

  @Test
  void compliance_extending_succeeds() {
    String key = "k/comp-extend";
    uploadWithRetention(key, RetentionMode.COMPLIANCE, INITIAL);

    store.updateObjectRetention(key, null, cfg(RetentionMode.COMPLIANCE, LATER, null));

    assertEquals(LATER, store.getObjectLock(key, null).getRetainUntilDate());
  }

  @Test
  void governance_shortening_withoutBypass_throws() {
    String key = "k/gov-shorten-no-bypass";
    uploadWithRetention(key, RetentionMode.GOVERNANCE, LATER);

    assertThrows(
        FailedPreconditionException.class,
        () ->
            store.updateObjectRetention(
                key, null, cfg(RetentionMode.GOVERNANCE, EARLIER, Boolean.FALSE)));
  }

  @Test
  void governance_shortening_withBypass_succeeds() {
    String key = "k/gov-shorten-bypass";
    uploadWithRetention(key, RetentionMode.GOVERNANCE, LATER);

    store.updateObjectRetention(
        key, null, cfg(RetentionMode.GOVERNANCE, EARLIER, Boolean.TRUE));

    assertEquals(EARLIER, store.getObjectLock(key, null).getRetainUntilDate());
  }

  @Test
  void compliance_shortening_evenWithBypass_throws() {
    // Bypass is meaningless on the immutable mode in both AWS and GCP — mirror that here.
    String key = "k/comp-shorten-bypass";
    uploadWithRetention(key, RetentionMode.COMPLIANCE, LATER);

    assertThrows(
        FailedPreconditionException.class,
        () ->
            store.updateObjectRetention(
                key, null, cfg(RetentionMode.COMPLIANCE, EARLIER, Boolean.TRUE)));
  }

  @Test
  void modeUpgrade_governanceToCompliance_withBypass_succeeds() {
    // Mode upgrade requires bypassGovernanceRetention=true on both AWS and GCP — the cloud
    // treats the lock-mode change as a modification of the existing lock. We mirror that here.
    String key = "k/upgrade";
    uploadWithRetention(key, RetentionMode.GOVERNANCE, INITIAL);

    store.updateObjectRetention(key, null, cfg(RetentionMode.COMPLIANCE, LATER, Boolean.TRUE));

    ObjectLockInfo info = store.getObjectLock(key, null);
    assertEquals(RetentionMode.COMPLIANCE, info.getMode());
    assertEquals(LATER, info.getRetainUntilDate());
  }

  @Test
  void modeUpgrade_governanceToCompliance_withoutBypass_throws() {
    String key = "k/upgrade-no-bypass";
    uploadWithRetention(key, RetentionMode.GOVERNANCE, INITIAL);

    assertThrows(
        com.salesforce.multicloudj.common.exceptions.FailedPreconditionException.class,
        () ->
            store.updateObjectRetention(
                key, null, cfg(RetentionMode.COMPLIANCE, LATER, null)));
  }

  @Test
  void modeDowngrade_complianceToGovernance_throws() {
    String key = "k/downgrade";
    uploadWithRetention(key, RetentionMode.COMPLIANCE, INITIAL);

    assertThrows(
        FailedPreconditionException.class,
        () ->
            store.updateObjectRetention(
                key, null, cfg(RetentionMode.GOVERNANCE, LATER, Boolean.TRUE)));
  }

  @Test
  void modeNull_preservesCurrentMode_extending() {
    String key = "k/null-mode";
    uploadWithRetention(key, RetentionMode.COMPLIANCE, INITIAL);

    store.updateObjectRetention(key, null, cfg(null, LATER, null));

    assertEquals(RetentionMode.COMPLIANCE, store.getObjectLock(key, null).getMode());
    assertEquals(LATER, store.getObjectLock(key, null).getRetainUntilDate());
  }

  @Test
  void legalHoldPreservedAcrossUpdate() {
    String key = "k/legalhold";
    byte[] content = "x".getBytes();
    store.upload(
        new UploadRequest.Builder()
            .withKey(key)
            .withContentLength(content.length)
            .withObjectLock(
                ObjectLockConfiguration.builder()
                    .mode(RetentionMode.GOVERNANCE)
                    .retainUntilDate(INITIAL)
                    .legalHold(true)
                    .build())
            .build(),
        new ByteArrayInputStream(content));

    store.updateObjectRetention(key, null, cfg(RetentionMode.GOVERNANCE, LATER, null));

    org.junit.jupiter.api.Assertions.assertTrue(store.getObjectLock(key, null).isLegalHold());
  }
}
