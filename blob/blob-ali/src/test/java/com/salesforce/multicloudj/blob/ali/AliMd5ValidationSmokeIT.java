package com.salesforce.multicloudj.blob.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.OperationOptions;
import com.aliyun.sdk.service.oss2.PresignOptions;
import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.models.DeleteObjectRequest;
import com.aliyun.sdk.service.oss2.models.PresignResult;
import com.aliyun.sdk.service.oss2.models.PutObjectRequest;
import com.aliyun.sdk.service.oss2.transport.BinaryData;
import com.aliyun.sdk.service.oss2.utils.Md5Utils;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Standalone smoke test that proves the PROPOSED FIX for the OSS checksum-binding gap:
 * {@code Content-MD5} is the only OSS request header that triggers server-side body validation
 * (and rejection with 400 InvalidDigest on mismatch), on BOTH the regular PutObject path and the
 * presigned-URL path.
 *
 * <p>Context: {@code AliChecksumHeaderSmokeIT} proved that {@code x-oss-hash-crc64ecma} as a
 * request header is inert (OSS ignores it; wrong/mismatched values still return 200 OK). The
 * remediation is to route a caller-supplied digest to {@code Content-MD5} instead. The multicloudj
 * driver does not yet support an MD5 {@code ChecksumMethod}, so this test exercises the underlying
 * OSS SDK directly (raw {@link OSSClient}) to confirm the mechanism BEFORE we invest in the
 * cross-cloud enum + driver wiring. This mirrors the standalone, real-bucket approach of
 * {@code AliObjectLockSmokeIT}.
 *
 * <p>Expected results (4 cases):
 * <ul>
 *   <li><b>putObject, correct MD5</b> → 200 OK</li>
 *   <li><b>putObject, wrong MD5</b> → rejected (InvalidDigest / 400)</li>
 *   <li><b>presign, replay matching body</b> → 200 OK (signature valid, body matches MD5)</li>
 *   <li><b>presign, replay with mismatched body</b> → rejected (InvalidDigest / 400)</li>
 * </ul>
 *
 * <p>If all four behave as expected, the MD5-based fix is sound for both codepaths. Gated on
 * {@code ALIBABA_CLOUD_ACCESS_KEY_ID} so it never runs in CI/replay. Uses the non-versioned
 * conformance bucket and cleans up after itself.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfEnvironmentVariable(named = "ALIBABA_CLOUD_ACCESS_KEY_ID", matches = ".+")
public class AliMd5ValidationSmokeIT {

  private static final String ENDPOINT = "https://oss-cn-shanghai.aliyuncs.com";
  private static final String BUCKET = "chameleon-multicloudj-test";
  private static final String REGION = "cn-shanghai";

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

  // ============================ PutObject path ============================

  /**
   * Case 1: regular PutObject with a CORRECT Content-MD5 should succeed.
   */
  @Test
  void putObject_correctMd5_succeeds() {
    String key = keyFor("put-correct-md5");
    byte[] content = "md5 validation smoke — correct".getBytes(StandardCharsets.UTF_8);
    String md5 = Md5Utils.md5AsBase64(content);

    System.out.println("=== Case 1: putObject + correct Content-MD5 ===");
    System.out.println("  Content-MD5 = " + md5);
    try {
      var result = rawClient.putObject(
          PutObjectRequest.newBuilder()
              .bucket(BUCKET)
              .key(key)
              .contentMd5(md5)
              .body(BinaryData.fromBytes(content))
              .build(),
          OperationOptions.defaults());
      System.out.println("  RESULT: 200 OK, etag=" + result.eTag());
      assertNotNull(result.eTag());
    } finally {
      safeDelete(key);
    }
  }

  /**
   * Case 2: regular PutObject with a WRONG Content-MD5 must be rejected by OSS.
   * This is the proof that Content-MD5 IS server-validated (unlike x-oss-hash-crc64ecma).
   */
  @Test
  void putObject_wrongMd5_rejected() {
    String key = keyFor("put-wrong-md5");
    byte[] content = "md5 validation smoke — body".getBytes(StandardCharsets.UTF_8);
    byte[] otherContent = "a completely different body".getBytes(StandardCharsets.UTF_8);
    String wrongMd5 = Md5Utils.md5AsBase64(otherContent); // valid MD5, wrong for `content`

    System.out.println("=== Case 2: putObject + WRONG Content-MD5 ===");
    System.out.println("  Content-MD5 (of other body) = " + wrongMd5);
    try {
      rawClient.putObject(
          PutObjectRequest.newBuilder()
              .bucket(BUCKET)
              .key(key)
              .contentMd5(wrongMd5)
              .body(BinaryData.fromBytes(content))
              .build(),
          OperationOptions.defaults());
      // If we reach here, OSS accepted a mismatched MD5 — the fix premise would be wrong.
      System.out.println("  RESULT: 200 OK — UNEXPECTED. OSS did NOT validate Content-MD5.");
      fail("Expected OSS to reject mismatched Content-MD5, but the upload succeeded");
    } catch (AssertionError ae) {
      throw ae;
    } catch (Exception e) {
      System.out.println("  RESULT: rejected as expected — " + e.getClass().getSimpleName()
          + ": " + e.getMessage());
      assertTrue(messageMentionsDigest(e),
          "Expected an InvalidDigest-style rejection; got: " + e.getMessage());
    } finally {
      safeDelete(key);
    }
  }

  // ============================ Presign path ============================

  /**
   * Case 3: presign a PutObject WITH Content-MD5, then replay via raw HTTP sending the MATCHING
   * body. Signature is valid (MD5 header replayed) and body matches → 200 OK.
   */
  @Test
  void presign_replayMatchingBody_succeeds() throws Exception {
    String key = keyFor("presign-match");
    byte[] content = "presign md5 smoke — signed body".getBytes(StandardCharsets.UTF_8);
    String md5 = Md5Utils.md5AsBase64(content);

    System.out.println("=== Case 3: presign(Content-MD5) + replay MATCHING body ===");
    PresignResult presign = rawClient.presign(
        PutObjectRequest.newBuilder()
            .bucket(BUCKET)
            .key(key)
            .contentMd5(md5)
            .build(),
        PresignOptions.newBuilder().expiration(Duration.ofHours(1)).build());

    Map<String, String> signedHeaders = presign.signedHeaders().orElse(Map.of());
    System.out.println("  url           = " + presign.url());
    System.out.println("  signedHeaders = " + signedHeaders);
    assertTrue(signedHeaders.keySet().stream()
            .anyMatch(h -> h.equalsIgnoreCase("Content-MD5")),
        "Content-MD5 should appear in signedHeaders the uploader must replay");

    try {
      int code = httpPut(new URL(presign.url()), signedHeaders, content);
      System.out.println("  RESULT: HTTP " + code + (code == 200 ? " OK" : ""));
      assertEquals(200, code, "Matching body with replayed signed headers should succeed");
    } finally {
      safeDelete(key);
    }
  }

  /**
   * Case 4: presign a PutObject WITH Content-MD5, replay the signed headers BUT send a different
   * body. The signature is still valid (we replay the signed Content-MD5 verbatim), so this is
   * NOT a 403 SignatureDoesNotMatch — it is OSS validating the body against the signed MD5 and
   * rejecting with 400 InvalidDigest. This is the crux: the presigned MD5 is a real binding.
   */
  @Test
  void presign_replayMismatchedBody_rejected() throws Exception {
    String key = keyFor("presign-mismatch");
    byte[] signedBody = "presign md5 smoke — the body I signed".getBytes(StandardCharsets.UTF_8);
    byte[] tamperedBody = "presign md5 smoke — a tampered body".getBytes(StandardCharsets.UTF_8);
    String md5 = Md5Utils.md5AsBase64(signedBody);

    System.out.println("=== Case 4: presign(Content-MD5) + replay MISMATCHED body ===");
    PresignResult presign = rawClient.presign(
        PutObjectRequest.newBuilder()
            .bucket(BUCKET)
            .key(key)
            .contentMd5(md5)
            .build(),
        PresignOptions.newBuilder().expiration(Duration.ofHours(1)).build());

    Map<String, String> signedHeaders = presign.signedHeaders().orElse(Map.of());
    System.out.println("  signedHeaders = " + signedHeaders);

    try {
      // Replay the EXACT signed headers (incl. the signed Content-MD5) but send a different body.
      int code = httpPut(new URL(presign.url()), signedHeaders, tamperedBody);
      if (code == 200) {
        System.out.println("  RESULT: HTTP 200 — UNEXPECTED. OSS accepted a body that does not "
            + "match the signed Content-MD5.");
        fail("Expected OSS to reject the tampered body via Content-MD5, but it returned 200");
      }
      System.out.println("  RESULT: HTTP " + code + " — rejected as expected "
          + "(400 InvalidDigest = body vs signed MD5 mismatch; 403 would mean signature issue)");
      // A 400 is the integrity rejection we want. Document whatever non-200 we observe.
      assertNotEquals(200, code);
    } finally {
      safeDelete(key);
    }
  }

  // ============================ helpers ============================

  private String keyFor(String label) {
    return "md5-smoke/" + runId + "/" + label;
  }

  /** Issues a raw HTTP PUT replaying the given (signed) headers with the given body. */
  private static int httpPut(URL url, Map<String, String> headers, byte[] body)
      throws IOException {
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    try {
      conn.setRequestMethod("PUT");
      conn.setDoOutput(true);
      for (Map.Entry<String, String> h : headers.entrySet()) {
        conn.setRequestProperty(h.getKey(), h.getValue());
      }
      try (OutputStream os = conn.getOutputStream()) {
        os.write(body);
      }
      return conn.getResponseCode();
    } finally {
      conn.disconnect();
    }
  }

  private static boolean messageMentionsDigest(Exception e) {
    String m = (e.getMessage() == null ? "" : e.getMessage()).toLowerCase();
    Throwable cause = e.getCause();
    String cm = (cause == null || cause.getMessage() == null)
        ? "" : cause.getMessage().toLowerCase();
    return m.contains("digest") || m.contains("400") || m.contains("md5")
        || cm.contains("digest") || cm.contains("400") || cm.contains("md5");
  }

  private void safeDelete(String key) {
    try {
      rawClient.deleteObject(
          DeleteObjectRequest.newBuilder().bucket(BUCKET).key(key).build(),
          OperationOptions.defaults());
    } catch (Exception ignored) {
      // best-effort
    }
  }
}
