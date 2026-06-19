package com.salesforce.multicloudj.blob.ali;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.OperationOptions;
import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.models.DeleteObjectRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectResult;
import com.aliyun.sdk.service.oss2.models.PutObjectRequest;
import com.aliyun.sdk.service.oss2.transport.BinaryData;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Standalone smoke test that probes UNCONFIRMED OSS server-side behavior for the gap #2
 * ("useKmsManagedKey") feasibility question: what does OSS do when a PutObject sets
 * {@code x-oss-server-side-encryption: KMS} but does NOT supply
 * {@code x-oss-server-side-encryption-key-id}?
 *
 * <p>Context: the multicloudj driver wires an explicit {@code kmsKeyId} today, but ignores the
 * {@code useKmsManagedKey=true} (KMS-with-no-explicit-key) flag that AWS honors. The OSS SDK lets
 * us build such a request (the key-id setter is independent and optional), and the OSS REST API
 * docs confirm the key-id header is NOT required with KMS — but NO doc states what key OSS uses
 * when it is omitted. Three outcomes are possible and undocumented:
 * <ol>
 *   <li>OSS encrypts with a default / service-managed CMK (the AWS-equivalent we want), or</li>
 *   <li>OSS rejects the request (e.g. 400 demanding a key-id), or</li>
 *   <li>some other behavior.</li>
 * </ol>
 * This test exercises the raw {@link OSSClient} to establish ground truth BEFORE we invest in any
 * driver wiring. Mirrors the standalone real-bucket approach of the other Ali smoke ITs.
 *
 * <p>Probes (each PUTs, then HEADs to read back the encryption response headers):
 * <ul>
 *   <li><b>Case A</b> — SSE=KMS, NO key id. The crux. Observe: does the PUT succeed? If so, what
 *       are the {@code x-oss-server-side-encryption} / {@code -key-id} headers on HEAD?</li>
 *   <li><b>Case B</b> — SSE=KMS, WITH an explicit key id (control). Only meaningful if a real CMK
 *       id is supplied via {@code ALI_SMOKE_KMS_KEY_ID}; skipped/best-effort otherwise.</li>
 *   <li><b>Case C</b> — no SSE headers at all (baseline). Shows whether the bucket applies a
 *       default encryption (so we can distinguish bucket-default from request-driven KMS).</li>
 * </ul>
 *
 * <p>This test makes NO assertions about the encryption outcome (that is exactly what we are
 * trying to discover); it only asserts the object was created where the PUT is expected to
 * succeed, and prints the observed headers for inspection. Gated on
 * {@code ALIBABA_CLOUD_ACCESS_KEY_ID} so it never runs in CI/replay. Uses the non-versioned
 * conformance bucket and cleans up after itself.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfEnvironmentVariable(named = "ALIBABA_CLOUD_ACCESS_KEY_ID", matches = ".+")
public class AliKmsManagedKeySmokeIT {

  private static final String ENDPOINT = "https://oss-cn-shanghai.aliyuncs.com";
  private static final String BUCKET = "chameleon-multicloudj-test";
  private static final String REGION = "cn-shanghai";
  private static final String SSE_KMS = "KMS";

  private OSSClient rawClient;
  private String runId;

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
    CredentialsProvider rawCreds =
        OSSCredentialsProvider.getCredentialsProvider(credentialsOverrider, REGION);
    rawClient =
        OSSClient.newBuilder()
            .region(REGION)
            .endpoint(ENDPOINT)
            .credentialsProvider(rawCreds)
            .build();
    runId = UUID.randomUUID().toString();
  }

  @AfterAll
  void teardown() throws Exception {
    if (rawClient != null) {
      rawClient.close();
    }
  }

  /**
   * Case A — the crux: SSE=KMS header set, NO key id. Print whether the PUT succeeds and what
   * encryption headers OSS reports on HEAD. This tells us whether "useKmsManagedKey" is honored
   * by OSS (default/managed CMK) or rejected.
   */
  @Test
  void ssekms_noKeyId_behavior() {
    String key = keyFor("kms-no-keyid");
    byte[] content = "kms managed-key smoke — no key id".getBytes(StandardCharsets.UTF_8);

    System.out.println("=== Case A: PutObject SSE=KMS, NO key-id ===");
    boolean created = false;
    try {
      var result = rawClient.putObject(
          PutObjectRequest.newBuilder()
              .bucket(BUCKET)
              .key(key)
              .serverSideEncryption(SSE_KMS)
              .body(BinaryData.fromBytes(content))
              .build(),
          OperationOptions.defaults());
      created = true;
      System.out.println("  PUT RESULT: 200 OK, etag=" + result.eTag());
      printEncryptionHeaders(key);
      assertNotNull(result.eTag());
    } catch (Exception e) {
      System.out.println("  PUT RESULT: rejected — " + e.getClass().getSimpleName()
          + ": " + e.getMessage());
      System.out.println("  >> Interpretation: OSS does NOT apply a default CMK for SSE=KMS"
          + " without a key id; useKmsManagedKey would be a NO-OP / error on Ali.");
    } finally {
      if (created) {
        safeDelete(key);
      }
    }
  }

  /**
   * Case B — control: SSE=KMS WITH an explicit key id. Only runs a meaningful request when a real
   * CMK id is provided via ALI_SMOKE_KMS_KEY_ID; otherwise prints a skip note (we don't want to
   * guess a key id and conflate a bad-key error with the no-key behavior in Case A).
   */
  @Test
  void ssekms_withKeyId_control() {
    String kmsKeyId = System.getenv("ALI_SMOKE_KMS_KEY_ID");
    if (kmsKeyId == null || kmsKeyId.isEmpty()) {
      System.out.println("=== Case B: SKIPPED (set ALI_SMOKE_KMS_KEY_ID to a real CMK id"
          + " to run the explicit-key control) ===");
      return;
    }
    String key = keyFor("kms-with-keyid");
    byte[] content = "kms managed-key smoke — explicit key".getBytes(StandardCharsets.UTF_8);

    System.out.println("=== Case B: PutObject SSE=KMS, WITH key-id=" + kmsKeyId + " ===");
    boolean created = false;
    try {
      var result = rawClient.putObject(
          PutObjectRequest.newBuilder()
              .bucket(BUCKET)
              .key(key)
              .serverSideEncryption(SSE_KMS)
              .serverSideEncryptionKeyId(kmsKeyId)
              .body(BinaryData.fromBytes(content))
              .build(),
          OperationOptions.defaults());
      created = true;
      System.out.println("  PUT RESULT: 200 OK, etag=" + result.eTag());
      printEncryptionHeaders(key);
    } catch (Exception e) {
      System.out.println("  PUT RESULT: rejected — " + e.getClass().getSimpleName()
          + ": " + e.getMessage());
    } finally {
      if (created) {
        safeDelete(key);
      }
    }
  }

  /**
   * Case C — baseline: no SSE headers at all. Reveals whether the bucket has default encryption,
   * so Case A's observed headers can be attributed to the request vs. a pre-existing bucket policy.
   */
  @Test
  void noSse_baseline() {
    String key = keyFor("no-sse");
    byte[] content = "kms managed-key smoke — baseline no sse".getBytes(StandardCharsets.UTF_8);

    System.out.println("=== Case C: PutObject with NO SSE headers (baseline) ===");
    boolean created = false;
    try {
      var result = rawClient.putObject(
          PutObjectRequest.newBuilder()
              .bucket(BUCKET)
              .key(key)
              .body(BinaryData.fromBytes(content))
              .build(),
          OperationOptions.defaults());
      created = true;
      System.out.println("  PUT RESULT: 200 OK, etag=" + result.eTag());
      printEncryptionHeaders(key);
    } catch (Exception e) {
      System.out.println("  PUT RESULT: rejected — " + e.getClass().getSimpleName()
          + ": " + e.getMessage());
    } finally {
      if (created) {
        safeDelete(key);
      }
    }
  }

  // ============================ helpers ============================

  /** HEADs the object and prints the encryption-related response headers. */
  private void printEncryptionHeaders(String key) {
    try {
      HeadObjectResult head = rawClient.headObject(
          HeadObjectRequest.newBuilder().bucket(BUCKET).key(key).build(),
          OperationOptions.defaults());
      System.out.println("  object        = oss://" + BUCKET + "/" + key);
      System.out.println("  HEAD x-oss-server-side-encryption        = "
          + head.serverSideEncryption());
      System.out.println("  HEAD x-oss-server-side-encryption-key-id = "
          + head.serverSideEncryptionKeyId());
      // Dump ALL response headers so we can see everything OSS stamped on the object,
      // not just the encryption pair.
      Map<String, String> headers = head.headers();
      System.out.println("  --- all HEAD response headers (" + headers.size() + ") ---");
      headers.forEach((k, v) -> System.out.println("    " + k + " = " + v));
      // Dump user metadata (x-oss-meta-*) if any.
      Map<String, String> userMeta = head.metadata();
      System.out.println("  --- user metadata (" + (userMeta == null ? 0 : userMeta.size())
          + ") ---");
      if (userMeta != null) {
        userMeta.forEach((k, v) -> System.out.println("    meta: " + k + " = " + v));
      }
    } catch (Exception e) {
      System.out.println("  HEAD failed: " + e.getClass().getSimpleName()
          + ": " + e.getMessage());
    }
  }

  private String keyFor(String label) {
    return "kms-managed-smoke/" + runId + "/" + label;
  }

  private void safeDelete(String key) {
    // Set ALI_SMOKE_KEEP_OBJECTS=1 to skip cleanup and leave the created objects in the bucket
    // for manual inspection (no debugger / breakpoint needed). Remember to delete them afterward.
    String keep = System.getenv("ALI_SMOKE_KEEP_OBJECTS");
    if (keep != null && !keep.isEmpty()) {
      System.out.println("  KEEP: leaving object for inspection -> oss://" + BUCKET + "/" + key);
      return;
    }
    try {
      rawClient.deleteObject(
          DeleteObjectRequest.newBuilder().bucket(BUCKET).key(key).build(),
          OperationOptions.defaults());
    } catch (Exception ignored) {
      // best-effort
    }
  }
}
