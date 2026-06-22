package com.salesforce.multicloudj.blob.ali;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.OperationOptions;
import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.models.DeleteObjectRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectRetentionRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectRetentionResult;
import com.aliyun.sdk.service.oss2.models.ObjectRetentionModeType;
import com.aliyun.sdk.service.oss2.models.PutObjectRetentionRequest;
import com.aliyun.sdk.service.oss2.models.Retention;
import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.ObjectLockConfiguration;
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
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Standalone direct test that empirically confirms Alibaba OSS does NOT support upgrading an
 * object's retention mode from GOVERNANCE to COMPLIANCE — the single scenario behind the skipped
 * conformance test
 * {@code testUpdateObjectRetention_modeUpgrade_governanceToCompliance_withBypass_succeeds}.
 *
 * <p>This hits the real OSS service directly (gated on {@code ALIBABA_CLOUD_ACCESS_KEY_ID}, so it
 * never runs in CI/replay). It deliberately uses the <b>raw {@link OSSClient}</b> for the upgrade
 * attempt rather than the multicloudj client: the multicloudj sync path now guards this transition
 * and throws {@code UnSupportedOperationException} before reaching OSS, which would mask the
 * server's actual response. Driving the SDK directly proves the platform limitation itself —
 * OSS rejects the mode change with an HTTP 409 (FileImmutable) even when the
 * {@code x-oss-bypass-governance-retention} header / bypass flag is supplied.
 *
 * <p><b>Requires:</b> {@link #BUCKET} must be a versioned bucket with object lock / WORM enabled.
 * Uses GOVERNANCE for the initial lock so the object can be cleaned up via a bypass delete.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfEnvironmentVariable(named = "ALIBABA_CLOUD_ACCESS_KEY_ID", matches = ".+")
public class AliRetentionModeUpgradeIT {

  private static final String ENDPOINT = "https://oss-cn-shanghai.aliyuncs.com";
  // Must be a versioned bucket with object lock / WORM enabled.
  private static final String BUCKET = "chameleon-multicloudj-test-versioned";
  private static final String REGION = "cn-shanghai";

  private static final Instant RETAIN_UNTIL =
      Instant.now().plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.SECONDS);
  private static final Instant UPGRADE_RETAIN_UNTIL =
      RETAIN_UNTIL.plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.SECONDS);

  private BucketClient client;
  // Raw OSS client: used both to attempt the upgrade (bypassing our guard) and to clean up the
  // GOVERNANCE-locked version with the bypass-governance-retention header.
  private OSSClient rawClient;
  private String key;
  private String lockedVersionId;

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

    key = "retention-mode-upgrade/" + UUID.randomUUID() + "/object";
  }

  @AfterAll
  void teardown() throws Exception {
    // The version still carries a future GOVERNANCE retain-until date. OSS bypass cannot set a
    // past date, so delete the version directly with the bypass-governance-retention header.
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
        System.out.printf("  teardown: failed to clean up %s (version %s): %s%n",
            key, lockedVersionId, e.getMessage());
      }
    }
    if (rawClient != null) {
      rawClient.close();
    }
    if (client != null) {
      client.close();
    }
  }

  @Test
  void governanceToComplianceUpgrade_isRejectedByOss() throws Exception {
    // 1. Upload an object with GOVERNANCE retention applied at write time (via multicloudj).
    byte[] content = "mode-upgrade test".getBytes(StandardCharsets.UTF_8);
    UploadResponse uploadResponse;
    try (ByteArrayInputStream in = new ByteArrayInputStream(content)) {
      uploadResponse = client.upload(
          new UploadRequest.Builder()
              .withKey(key)
              .withContentLength(content.length)
              .withObjectLock(
                  ObjectLockConfiguration.builder()
                      .mode(RetentionMode.GOVERNANCE)
                      .retainUntilDate(RETAIN_UNTIL)
                      .legalHold(false)
                      .build())
              .build(),
          in);
    }
    lockedVersionId = uploadResponse.getVersionId();
    assertNotNull(lockedVersionId, "Versioned bucket should return a versionId for cleanup");
    System.out.printf("  uploaded GOVERNANCE-locked object %s (version %s)%n",
        key, lockedVersionId);

    // 2. Read back the current retention via the RAW OSS client and inspect the values OSS
    //    returns. Confirms the GOVERNANCE lock is actually in place before the upgrade attempt,
    //    and lets the operator eyeball the exact mode/retainUntilDate format OSS reports.
    GetObjectRetentionResult currentRetention = rawClient.getObjectRetention(
        GetObjectRetentionRequest.newBuilder()
            .bucket(BUCKET)
            .key(key)
            .build(),
        OperationOptions.defaults());
    assertNotNull(currentRetention.retention(),
        "OSS should report retention for the GOVERNANCE-locked object");
    Retention current = currentRetention.retention();
    System.out.printf("  getObjectRetention before upgrade: mode=%s, retainUntilDate=%s%n",
        current.mode(), current.retainUntilDate());

    // 3. Attempt GOVERNANCE -> COMPLIANCE upgrade via the RAW OSS client, WITH bypass.
    //    We bypass the multicloudj guard on purpose to observe OSS's own response.
    Retention complianceRetention = Retention.newBuilder()
        .mode(ObjectRetentionModeType.COMPLIANCE)
        .retainUntilDate(DateTimeFormatter.ISO_INSTANT.format(UPGRADE_RETAIN_UNTIL))
        .build();
    PutObjectRetentionRequest upgradeRequest = PutObjectRetentionRequest.newBuilder()
        .bucket(BUCKET)
        .key(key)
        .retention(complianceRetention)
        .bypassGovernanceRetention(true)
        .build();

    // 4. OSS must reject the mode change. Even with bypass, OSS treats the object version's
    //    retention mode as immutable and returns HTTP 409 (FileImmutable). We assert that the
    //    call throws; the printed exception lets the operator eyeball the exact status/code.
    Exception ex = assertThrows(Exception.class,
        () -> rawClient.putObjectRetention(upgradeRequest, OperationOptions.defaults()),
        "OSS should reject a GOVERNANCE -> COMPLIANCE mode upgrade even with bypass");
    System.out.printf("  OSS rejected mode upgrade as expected: %s: %s%n",
        ex.getClass().getName(), ex.getMessage());
  }
}
