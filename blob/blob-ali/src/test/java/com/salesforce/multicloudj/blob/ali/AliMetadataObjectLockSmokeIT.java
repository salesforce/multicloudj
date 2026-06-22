package com.salesforce.multicloudj.blob.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.OperationOptions;
import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.models.DeleteObjectRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectResult;
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
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Standalone direct test that empirically determines whether Alibaba OSS returns object-lock
 * (retention + legal hold) information in <code>HeadObject</code> response headers, the way S3
 * does via <code>x-amz-object-lock-mode</code> / <code>x-amz-object-lock-retain-until-date</code>
 * / <code>x-amz-object-lock-legal-hold</code>.
 *
 * <p>This is a feasibility probe for the planned <code>objectLockInfo</code> in
 * <code>BlobMetadata.fromGetMetadata()</code> on Ali (gap #4 in the feature gap analysis):
 * <ul>
 *   <li>If OSS DOES return lock info on HEAD, the implementation can read it directly from the
 *       response headers — one round trip, cheap.</li>
 *   <li>If OSS does NOT, the implementation must follow up the HEAD with separate
 *       <code>GetObjectRetention</code> + <code>GetObjectLegalHold</code> calls — up to 3 round
 *       trips per <code>getMetadata()</code>, with attendant latency cost.</li>
 * </ul>
 * The bytecode of {@link HeadObjectResult} has no typed accessor for retention/lock fields, so
 * the only way to learn the answer is to inspect the raw response headers from a real HEAD against
 * a versioned + WORM-enabled bucket holding a locked object — which is what this test does.
 *
 * <p>This hits the real OSS service directly (gated on <code>ALIBABA_CLOUD_ACCESS_KEY_ID</code>,
 * so it never runs in CI/replay). Uses GOVERNANCE retention so the locked object can be cleaned
 * up with a bypass delete after the probe.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfEnvironmentVariable(named = "ALIBABA_CLOUD_ACCESS_KEY_ID", matches = ".+")
public class AliMetadataObjectLockSmokeIT {

  private static final String ENDPOINT = "https://oss-cn-shanghai.aliyuncs.com";
  // Must be a versioned bucket with object lock / WORM enabled.
  private static final String BUCKET = "chameleon-multicloudj-test-versioned";
  private static final String REGION = "cn-shanghai";

  private BucketClient client;
  // Raw OSS client — used both for the HeadObject call (so the headers map is observable
  // unmodified by the multicloudj transformer) and for the bypass-governance-retention delete
  // during teardown.
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

    key = "metadata-objectlock-smoke/" + UUID.randomUUID() + "/object";
  }

  @AfterAll
  void teardown() throws Exception {
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
  void headObject_onLockedObject_responseHeaders() throws Exception {
    // 1. Upload an object with GOVERNANCE retention + legal hold applied at write time. This
    //    exercises both retention surfaces on a single object so the HEAD response can be
    //    inspected for either signal.
    byte[] content = "metadata objectlock smoke".getBytes(StandardCharsets.UTF_8);
    Instant retainUntil =
        Instant.now().plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.SECONDS);
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
                      .legalHold(true)
                      .build())
              .build(),
          in);
    }
    lockedVersionId = uploadResponse.getVersionId();
    assertNotNull(lockedVersionId, "Versioned bucket should return a versionId for cleanup");
    System.out.printf("  uploaded GOVERNANCE+legalHold-locked object %s (version %s)%n",
        key, lockedVersionId);

    // 2. Raw HeadObject — observe what OSS returns. The typed HeadObjectResult has no retention
    //    accessor (verified via javap on alibabacloud-oss-v2:0.4.0), so the question is purely
    //    about the raw response headers map.
    HeadObjectResult headResult = rawClient.headObject(
        HeadObjectRequest.newBuilder().bucket(BUCKET).key(key).build(),
        OperationOptions.defaults());
    assertEquals(200, headResult.statusCode(), "HEAD on locked object should succeed");

    System.out.printf("  HeadObject statusCode=%d%n", headResult.statusCode());
    Map<String, String> headers = headResult.headers();
    System.out.println("  HeadObject response headers (full dump):");
    if (headers != null) {
      headers.forEach((k, v) -> System.out.printf("    %s: %s%n", k, v));
    } else {
      System.out.println("    (null)");
    }

    // 3. Look specifically for any retention/lock/hold-related header. The substrings cover
    //    every plausible spelling Alibaba might use (the SDK has no documented constants).
    boolean anyLockHeader = headers != null && headers.entrySet().stream()
        .anyMatch(e -> {
          String name = e.getKey() == null ? "" : e.getKey().toLowerCase();
          return name.contains("retention")
              || name.contains("legal-hold")
              || name.contains("legalhold")
              || name.contains("object-lock")
              || name.contains("objectlock")
              || name.contains("worm");
        });

    System.out.printf(
        "  Any retention/legal-hold/object-lock header present on HEAD? %s%n", anyLockHeader);
    System.out.println(
        "  Verdict for gap #4 implementation: "
            + (anyLockHeader
                ? "OSS surfaces lock info on HEAD — single-call impl is feasible."
                : "OSS does NOT surface lock info on HEAD — impl needs follow-up "
                    + "GetObjectRetention + GetObjectLegalHold calls."));

    // No assertion on the verdict itself — this is a probe to inform implementation. The key
    // assertion is that the HEAD succeeded and we observed the headers.
    assertNotNull(headers, "HEAD response should expose a headers map even if it's empty");
  }
}
