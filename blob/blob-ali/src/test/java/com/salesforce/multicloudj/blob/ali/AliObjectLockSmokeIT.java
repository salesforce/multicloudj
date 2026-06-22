package com.salesforce.multicloudj.blob.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.OperationOptions;
import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.models.DeleteObjectRequest;
import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.ObjectLockConfiguration;
import com.salesforce.multicloudj.blob.driver.ObjectLockInfo;
import com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig;
import com.salesforce.multicloudj.blob.driver.RetentionMode;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.ali.AliConstants;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Standalone smoke test that verifies the OSS object lock (WORM) feature is enabled and working
 * end-to-end against a real bucket, BEFORE investing in the full object-lock conformance suite.
 *
 * <p>Unlike the WireMock-backed conformance tests, this hits the real OSS service directly. It is
 * gated on {@code ALIBABA_CLOUD_ACCESS_KEY_ID} so it never runs in CI/replay.
 *
 * <p><b>Requires:</b> {@link #BUCKET} must be a versioned bucket with object lock / WORM enabled.
 * If WORM is NOT enabled, the upload-with-lock or the enforcement check (step 3) will fail — which
 * is exactly the signal this test is meant to surface.
 *
 * <p>Uses GOVERNANCE mode (not COMPLIANCE) on purpose: a COMPLIANCE lock cannot be shortened or
 * bypassed by anyone until expiry, which would leave an undeletable object behind. GOVERNANCE
 * supports bypass, so the test can clean up after itself and remain repeatable.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfEnvironmentVariable(named = "ALIBABA_CLOUD_ACCESS_KEY_ID", matches = ".+")
public class AliObjectLockSmokeIT {

  private static final String ENDPOINT = "https://oss-cn-shanghai.aliyuncs.com";
  // Must be a versioned bucket with object lock / WORM enabled.
  private static final String BUCKET = "chameleon-multicloudj-test-versioned";
  private static final String REGION = "cn-shanghai";

  private BucketClient client;
  // Raw OSS client used only for teardown cleanup of retained/held versions, which
  // require the bypass-governance-retention header on the delete itself (OSS bypass cannot
  // set a past retain date, so "shorten then delete" is not viable for synchronous cleanup).
  private OSSClient rawClient;
  private String key;
  private String controlKey;
  private String legalHoldKey;
  private String lockedVersionId;
  private String controlVersionId;
  private String legalHoldVersionId;

  @BeforeAll
  void setup() {
    StsCredentials creds =
        new StsCredentials(
            System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"),
            System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"),
            System.getenv("ALIBABA_CLOUD_SECURITY_TOKEN"));
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(creds)
            .build();

    client =
        BucketClient.builder(AliConstants.PROVIDER_ID)
            .withBucket(BUCKET)
            .withRegion(REGION)
            .withEndpoint(URI.create(ENDPOINT))
            .withCredentialsOverrider(credentialsOverrider)
            .build();

    CredentialsProvider rawCreds =
        OSSCredentialsProvider.getCredentialsProvider(credentialsOverrider, REGION);
    rawClient =
        OSSClient.newBuilder()
            .region(REGION)
            .endpoint(ENDPOINT)
            .credentialsProvider(rawCreds)
            .build();

    String runId = UUID.randomUUID().toString();
    key = "objectlock-smoke/" + runId + "/locked";
    controlKey = "objectlock-smoke/" + runId + "/control-no-lock";
    legalHoldKey = "objectlock-smoke/" + runId + "/legal-hold";
  }

  @AfterAll
  void teardown() throws Exception {
    // Best-effort cleanup of the locked object. The version still carries a future GOVERNANCE
    // retain-until date, and OSS bypass cannot set a past date, so we cannot "shorten then
    // delete". Instead delete the version directly with the bypass-governance-retention header
    // (the same mechanism the OSS console's "permanent delete" uses). This requires the raw
    // OSS client because the multicloudj delete() API does not expose the bypass flag.
    if (rawClient != null && lockedVersionId != null) {
      try {
        rawClient.deleteObject(
            DeleteObjectRequest.newBuilder()
                .bucket(BUCKET)
                .key(key)
                .versionId(lockedVersionId)
                .header("x-oss-bypass-governance-retention", "true")
                .build(),
            OperationOptions.defaults());
      } catch (Exception e) {
        System.out.printf("  teardown: failed to clean up locked object %s (version %s): %s%n",
            key, lockedVersionId, e.getMessage());
      }
    }

    if (client != null) {
      // Best-effort cleanup of the control object — delete the specific version so the
      // underlying version is removed rather than just adding a delete marker on the
      // versioned bucket. Guard on controlVersionId to avoid creating a delete marker
      // when the wormRoundTrip test didn't run.
      try {
        if (controlVersionId != null) {
          client.delete(controlKey, controlVersionId);
        }
      } catch (Exception ignored) {
        // ignore
      }
      // Legal hold object: hold was cleared by the test; delete the specific version.
      try {
        if (legalHoldVersionId != null) {
          client.delete(legalHoldKey, legalHoldVersionId);
        }
      } catch (Exception ignored) {
        // ignore
      }
      client.close();
    }
    if (rawClient != null) {
      rawClient.close();
    }
  }

  @Test
  void wormRoundTrip() throws Exception {
    byte[] content = "worm smoke test".getBytes(StandardCharsets.UTF_8);
    // OSS stores retention timestamps at (at most) millisecond precision, so truncate to
    // seconds to keep the round-trip comparison exact (Instant.now() carries micro/nanos).
    Instant retainUntil =
        Instant.now().plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.SECONDS);

    // 0. Control upload: a second blob with NO object lock, for side-by-side comparison
    //    against the locked object in the OSS console.
    byte[] controlContent = "control no-lock blob".getBytes(StandardCharsets.UTF_8);
    try (ByteArrayInputStream in = new ByteArrayInputStream(controlContent)) {
      UploadResponse controlResponse = client.upload(
          new UploadRequest.Builder()
              .withKey(controlKey)
              .withContentLength(controlContent.length)
              .build(),
          in);
      controlVersionId = controlResponse.getVersionId();
    }

    // 1. Upload with GOVERNANCE retention applied at write time.
    UploadResponse uploadResponse;
    try (ByteArrayInputStream in = new ByteArrayInputStream(content)) {
      uploadResponse = client.upload(
          new UploadRequest.Builder()
              .withKey(key)
              .withContentLength(content.length)
              .withObjectLock(
                  ObjectLockConfiguration.builder()
                      .mode(RetentionMode.GOVERNANCE)
                      .retainUntilDate(retainUntil)
                      .legalHold(false)
                      .build())
              .build(),
          in);
    }
    // Retention is bound to this specific object VERSION. On a versioned bucket a
    // delete without a versionId only writes a delete marker (and always succeeds);
    // retention only blocks deleting the locked version itself.
    lockedVersionId = uploadResponse.getVersionId();
    System.out.printf("  locked versionId = %s%n", lockedVersionId);

    // 2. Read the lock back on both objects and print for live console comparison.
    ObjectLockInfo info = client.getObjectLock(key, null);
    printLockState("LOCKED   blob", key, info);

    ObjectLockInfo controlInfo = readLockStateQuietly(controlKey);
    printLockState("CONTROL  blob (no lock)", controlKey, controlInfo);

    assertNotNull(info, "getObjectLock should return non-null for a locked object");
    assertEquals(RetentionMode.GOVERNANCE, info.getMode());
    assertNotNull(info.getRetainUntilDate(), "retainUntilDate should be set");

    // 3. Enforcement check: deleting the locked object VERSION without bypass must fail.
    //    This is the real proof that WORM is enabled and enforced on the bucket.
    assertThrows(
        Exception.class,
        () -> client.delete(key, lockedVersionId),
        "Locked object version should not be deletable without governance bypass");

    // 4. Extend retention — proves updateObjectRetention works.
    Instant extended = retainUntil.plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.SECONDS);
    client.updateObjectRetention(
        key,
        null,
        ObjectRetentionConfig.builder()
            .mode(RetentionMode.GOVERNANCE)
            .retainUntilDate(extended)
            .build());
    // OSS may return the retain-until at coarser (e.g. millisecond) precision than sent,
    // so compare truncated to seconds rather than exact-equals.
    Instant actual = client.getObjectLock(key, null).getRetainUntilDate();
    assertEquals(
        extended,
        actual.truncatedTo(ChronoUnit.SECONDS),
        "Retention should reflect the extended date (compared at second precision)");

    // 5. Shorten with bypass so cleanup can delete it — proves bypass works.
    client.updateObjectRetention(
        key,
        null,
        ObjectRetentionConfig.builder()
            .mode(RetentionMode.GOVERNANCE)
            .retainUntilDate(Instant.now().plusSeconds(1))
            .bypassGovernanceRetention(Boolean.TRUE)
            .build());

    // Both objects are left in place for live inspection in the OSS console; the
    // @AfterAll teardown performs the final cleanup of both the locked and control blobs.
  }

  @Test
  void legalHoldRoundTrip() throws Exception {
    byte[] content = "legal hold smoke test".getBytes(StandardCharsets.UTF_8);

    // 1. Upload a plain object (no retention, no legal hold at upload time).
    UploadRequest uploadRequest = new UploadRequest.Builder()
        .withKey(legalHoldKey)
        .withContentLength(content.length)
        .build();
    UploadResponse uploadResponse =
        client.upload(uploadRequest, new ByteArrayInputStream(content));
    legalHoldVersionId = uploadResponse.getVersionId();
    assertNotNull(legalHoldVersionId, "Versioned bucket should return a versionId");
    System.out.printf("  legalHold test: uploaded %s (version %s)%n",
        legalHoldKey, legalHoldVersionId);

    // 2. Verify no legal hold initially.
    ObjectLockInfo infoBeforeHold = readLockStateQuietly(legalHoldKey);
    if (infoBeforeHold != null) {
      assertFalse(infoBeforeHold.isLegalHold(),
          "Freshly uploaded object should have no legal hold");
    }
    System.out.println("  legalHold test: confirmed no hold initially");

    // 3. Set legal hold ON.
    client.updateLegalHold(legalHoldKey, legalHoldVersionId, true);
    System.out.println("  legalHold test: set legal hold ON");

    // 4. Read back — verify legal hold is ON.
    ObjectLockInfo infoWithHold = client.getObjectLock(legalHoldKey, legalHoldVersionId);
    assertNotNull(infoWithHold, "getObjectLock should return non-null after setting hold");
    assertTrue(infoWithHold.isLegalHold(), "Legal hold should be ON after setting it");
    printLockState("after setLegalHold(true)", legalHoldKey, infoWithHold);

    // 5. Enforcement: deleting the held version should fail.
    assertThrows(Exception.class,
        () -> client.delete(legalHoldKey, legalHoldVersionId),
        "Delete should fail while legal hold is ON");
    System.out.println("  legalHold test: delete correctly rejected (hold is ON)");

    // 6. Clear legal hold.
    client.updateLegalHold(legalHoldKey, legalHoldVersionId, false);
    System.out.println("  legalHold test: cleared legal hold");

    // 7. Verify legal hold is OFF.
    ObjectLockInfo infoAfterClear = client.getObjectLock(legalHoldKey, legalHoldVersionId);
    assertNotNull(infoAfterClear);
    assertFalse(infoAfterClear.isLegalHold(), "Legal hold should be OFF after clearing");
    printLockState("after setLegalHold(false)", legalHoldKey, infoAfterClear);

    // 8. Delete should now succeed (no hold, no retention).
    client.delete(legalHoldKey, legalHoldVersionId);
    legalHoldVersionId = null; // prevent double-delete in teardown
    System.out.println("  legalHold test: delete succeeded after clearing hold");
  }

  @Test
  void uploadWithLegalHoldAtWriteTime() throws Exception {
    // Validates the upload-time ObjectLockConfiguration.legalHold(true) path,
    // which applies legal hold via a separate API call after the PutObject.
    byte[] content = "upload-time legal hold".getBytes(StandardCharsets.UTF_8);
    String uploadHoldKey = legalHoldKey + "-at-upload";
    String uploadHoldVersionId = null;

    try {
      ObjectLockConfiguration lockConfig = ObjectLockConfiguration.builder()
          .legalHold(true)
          .build();
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(uploadHoldKey)
          .withContentLength(content.length)
          .withObjectLock(lockConfig)
          .build();
      UploadResponse response =
          client.upload(uploadRequest, new ByteArrayInputStream(content));
      uploadHoldVersionId = response.getVersionId();
      System.out.printf("  uploadWithHold: uploaded %s (version %s)%n",
          uploadHoldKey, uploadHoldVersionId);

      // Verify hold applied at upload time.
      ObjectLockInfo info = client.getObjectLock(uploadHoldKey, uploadHoldVersionId);
      assertNotNull(info);
      assertTrue(info.isLegalHold(),
          "Legal hold should be ON when set at upload time");
      printLockState("upload-time legal hold", uploadHoldKey, info);

      // Enforcement: delete should fail.
      String vid = uploadHoldVersionId;
      assertThrows(Exception.class,
          () -> client.delete(uploadHoldKey, vid),
          "Delete should fail with legal hold set at upload time");
      System.out.println("  uploadWithHold: delete correctly rejected");
    } finally {
      // Clean up: clear hold, then delete.
      if (uploadHoldVersionId != null) {
        try {
          client.updateLegalHold(uploadHoldKey, uploadHoldVersionId, false);
          client.delete(uploadHoldKey, uploadHoldVersionId);
          System.out.println("  uploadWithHold: cleaned up");
        } catch (Exception e) {
          System.out.printf("  uploadWithHold: cleanup failed: %s%n", e.getMessage());
        }
      }
    }
  }

  @Test
  void shortRetentionPeriodAcceptance() throws Exception {
    // Validates whether OSS accepts a retention period of less than 1 day.
    // Uploads two objects: one GOVERNANCE, one COMPLIANCE, both with retainUntilDate
    // set to 5 minutes from now. If OSS enforces a minimum retention period (e.g. 1 day),
    // the upload or putObjectRetention call will throw.
    byte[] content = "short retention test".getBytes(StandardCharsets.UTF_8);
    Instant fiveMinutesFromNow =
        Instant.now().plus(5, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.SECONDS);

    String govKey = key + "-short-gov";
    String compKey = key + "-short-comp";
    String govVersionId = null;
    String compVersionId = null;

    try {
      // Upload with GOVERNANCE + 5 min retention
      ObjectLockConfiguration govLock = ObjectLockConfiguration.builder()
          .mode(RetentionMode.GOVERNANCE)
          .retainUntilDate(fiveMinutesFromNow)
          .build();
      UploadResponse govResponse = client.upload(
          new UploadRequest.Builder()
              .withKey(govKey)
              .withContentLength(content.length)
              .withObjectLock(govLock)
              .build(),
          new ByteArrayInputStream(content));
      govVersionId = govResponse.getVersionId();
      System.out.printf("  shortRetention: GOVERNANCE uploaded %s (version %s, retainUntil %s)%n",
          govKey, govVersionId, fiveMinutesFromNow);

      // Verify GOVERNANCE retention was accepted
      ObjectLockInfo govInfo = client.getObjectLock(govKey, govVersionId);
      assertNotNull(govInfo);
      assertEquals(RetentionMode.GOVERNANCE, govInfo.getMode());
      assertNotNull(govInfo.getRetainUntilDate());
      printLockState("GOVERNANCE 5-min retention", govKey, govInfo);

      // Upload with COMPLIANCE + 5 min retention
      ObjectLockConfiguration compLock = ObjectLockConfiguration.builder()
          .mode(RetentionMode.COMPLIANCE)
          .retainUntilDate(fiveMinutesFromNow)
          .build();
      UploadResponse compResponse = client.upload(
          new UploadRequest.Builder()
              .withKey(compKey)
              .withContentLength(content.length)
              .withObjectLock(compLock)
              .build(),
          new ByteArrayInputStream(content));
      compVersionId = compResponse.getVersionId();
      System.out.printf("  shortRetention: COMPLIANCE uploaded %s (version %s, retainUntil %s)%n",
          compKey, compVersionId, fiveMinutesFromNow);

      // Verify COMPLIANCE retention was accepted
      ObjectLockInfo compInfo = client.getObjectLock(compKey, compVersionId);
      assertNotNull(compInfo);
      assertEquals(RetentionMode.COMPLIANCE, compInfo.getMode());
      assertNotNull(compInfo.getRetainUntilDate());
      printLockState("COMPLIANCE 5-min retention", compKey, compInfo);

      System.out.println("  shortRetention: BOTH accepted 5-min retention without error");

    } finally {
      // Clean up GOVERNANCE object (use bypass to shorten, then delete)
      if (govVersionId != null) {
        try {
          rawClient.deleteObject(
              DeleteObjectRequest.newBuilder()
                  .bucket(BUCKET)
                  .key(govKey)
                  .versionId(govVersionId)
                  .header("x-oss-bypass-governance-retention", "true")
                  .build(),
              OperationOptions.defaults());
          System.out.println("  shortRetention: cleaned up GOVERNANCE object");
        } catch (Exception e) {
          System.out.printf("  shortRetention: GOVERNANCE cleanup failed: %s%n", e.getMessage());
        }
      }
      // COMPLIANCE object: cannot be deleted until retention expires (5 min).
      // Leave it — it auto-unlocks after 5 minutes, then can be manually deleted.
      if (compVersionId != null) {
        System.out.printf(
            "  shortRetention: COMPLIANCE object %s (version %s) will unlock at %s"
                + " — delete manually after expiry%n",
            compKey, compVersionId, fiveMinutesFromNow);
      }
    }
  }

  private ObjectLockInfo readLockStateQuietly(String objectKey) {
    try {
      return client.getObjectLock(objectKey, null);
    } catch (Exception e) {
      // Some providers throw when an object has no lock config; treat as "no lock".
      System.out.printf("  (getObjectLock threw for %s: %s)%n", objectKey, e.getMessage());
      return null;
    }
  }

  private static void printLockState(String label, String objectKey, ObjectLockInfo info) {
    System.out.println("=== Object lock state: " + label + " ===");
    System.out.println("  key            = " + objectKey);
    if (info == null) {
      System.out.println("  objectLockInfo = <null> (no lock)");
      return;
    }
    System.out.println("  mode           = " + info.getMode());
    System.out.println("  retainUntil    = " + info.getRetainUntilDate());
    System.out.println("  legalHold      = " + info.isLegalHold());
    System.out.println("  useEventBased  = " + info.getUseEventBasedHold());
  }
}
