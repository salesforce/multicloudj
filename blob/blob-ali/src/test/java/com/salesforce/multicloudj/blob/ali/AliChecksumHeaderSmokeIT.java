package com.salesforce.multicloudj.blob.ali;

import com.aliyun.sdk.service.oss2.hash.CRC64;
import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.ChecksumMethod;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.ali.AliConstants;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Standalone smoke test that probes OSS's behavior toward the {@code x-oss-hash-crc64ecma}
 * request header. The driver currently routes any non-SHA256 user-supplied checksum into this
 * header (see {@code AliTransformer.toPutObjectRequest} and {@code toPresignedPutObjectRequest}),
 * including values computed with {@link ChecksumMethod#CRC32C}. CRC32C and CRC64 produce
 * different bytes for the same content, so this test asks empirically:
 *
 * <ol>
 *   <li>If the bytes match (real CRC64), does the upload succeed? (sanity)</li>
 *   <li>If the bytes are obviously wrong, does OSS reject with 400 InvalidDigest, or silently
 *       accept? Determines whether the header is validated server-side at all.</li>
 *   <li>If the bytes are a CRC32C-of-the-content placed under the CRC64 header — exactly the
 *       shape produced when a caller passes {@code ChecksumMethod.CRC32C} — does OSS notice?</li>
 * </ol>
 *
 * <p>Outcome decides the bug severity:
 * <ul>
 *   <li>Cases 2 + 3 return <b>400</b> → OSS validates the header. The CRC32C bug manifests as a
 *       loud failure on every Ali upload that supplies {@code ChecksumMethod.CRC32C}.</li>
 *   <li>Cases 2 + 3 return <b>200</b> → OSS ignores wrong values in this header on PutObject.
 *       The CRC32C bug is a silent-integrity-lie: the driver claims a checksum was bound when
 *       in fact none was. This is the worse outcome.</li>
 * </ul>
 *
 * <p>Gated on {@code ALIBABA_CLOUD_ACCESS_KEY_ID} so it never runs in CI/replay. Uses the
 * non-versioned conformance bucket {@code chameleon-multicloudj-test}; cleans up after itself.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfEnvironmentVariable(named = "ALIBABA_CLOUD_ACCESS_KEY_ID", matches = ".+")
public class AliChecksumHeaderSmokeIT {

  private static final String ENDPOINT = "https://oss-cn-shanghai.aliyuncs.com";
  private static final String BUCKET = "chameleon-multicloudj-test";
  private static final String REGION = "cn-shanghai";

  private BucketClient client;
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

    client =
        BucketClient.builder(AliConstants.PROVIDER_ID)
            .withBucket(BUCKET)
            .withRegion(REGION)
            .withEndpoint(URI.create(ENDPOINT))
            .withCredentialsOverrider(credentialsOverrider)
            .build();

    runId = UUID.randomUUID().toString();
  }

  @AfterAll
  void teardown() throws Exception {
    if (client != null) {
      client.close();
    }
  }

  /**
   * Case 1 (sanity): a real CRC64 of the content, labelled with the (currently unrouted) CRC64
   * algorithm intent. The driver routes this into {@code x-oss-hash-crc64ecma}. Should succeed.
   */
  @Test
  void case1_realCrc64_succeeds() {
    String key = keyFor("real-crc64");
    byte[] content = "smoke checksum test content".getBytes(StandardCharsets.UTF_8);
    String realCrc64 = computeCrc64(content);

    System.out.println("=== Case 1: real CRC64 in x-oss-hash-crc64ecma ===");
    System.out.println("  bytes = real CRC64 = " + realCrc64);

    try {
      // ChecksumMethod.CRC32C is the only non-SHA256 enum value. Driver default routes it
      // into x-oss-hash-crc64ecma regardless of the algorithm name. We supply a real CRC64 here
      // so this case isolates "header value matches the body" from "algorithm label mismatch".
      UploadResponse response = uploadWithChecksum(key, content, realCrc64, ChecksumMethod.CRC32C);
      System.out.println("  RESULT: 200 OK, etag=" + response.getETag());
      Assertions.assertNotNull(response.getKey());
    } finally {
      safeDelete(key);
    }
  }

  /**
   * Case 2 (validation probe): an obviously wrong CRC64 value. If OSS validates this header on
   * PutObject, it returns 400 InvalidDigest; if not, it accepts. This case alone settles the
   * "is the header validated at all" question.
   */
  @Test
  void case2_obviouslyWrongCrc64_revealsValidation() {
    String key = keyFor("wrong-crc64");
    byte[] content = "smoke checksum test content".getBytes(StandardCharsets.UTF_8);
    // Pick a value that cannot possibly be the CRC64 of the content.
    String fakeChecksum = "12345";

    System.out.println("=== Case 2: obviously wrong CRC64 in x-oss-hash-crc64ecma ===");
    System.out.println("  bytes = literal '12345' (definitely not CRC64 of body)");

    try {
      UploadResponse response =
          uploadWithChecksum(key, content, fakeChecksum, ChecksumMethod.CRC32C);
      System.out.println("  RESULT: 200 OK — OSS DID NOT validate the header");
      System.out.println("  → driver is silently shipping unvalidated 'integrity' headers");
      Assertions.assertNotNull(response.getKey()); // Document the silent-accept outcome.
    } catch (Exception e) {
      System.out.println("  RESULT: rejected — OSS DOES validate the header");
      System.out.println("  exception: " + e.getClass().getSimpleName() + ": " + e.getMessage());
      // Validation-rejection is also a valid (and louder) outcome; do not fail the test.
    } finally {
      safeDelete(key);
    }
  }

  /**
   * Case 3 (the actual bug shape): a real CRC32C of the content placed in the CRC64 header.
   * This is exactly what the driver produces when a caller passes
   * {@code ChecksumMethod.CRC32C} with a CRC32C-of-content value.
   */
  @Test
  void case3_crc32cValueInCrc64Header_revealsBugSeverity() {
    String key = keyFor("crc32c-as-crc64");
    byte[] content = "smoke checksum test content".getBytes(StandardCharsets.UTF_8);
    String realCrc32c = computeCrc32cBase64(content);
    String realCrc64 = computeCrc64(content);

    System.out.println("=== Case 3: real CRC32C bytes in x-oss-hash-crc64ecma ===");
    System.out.println("  bytes = real CRC32C   = " + realCrc32c);
    System.out.println("  (the right CRC64 is) = " + realCrc64);
    System.out.println("  these MUST differ for the test to be meaningful: "
        + !realCrc32c.equals(realCrc64));

    try {
      UploadResponse response =
          uploadWithChecksum(key, content, realCrc32c, ChecksumMethod.CRC32C);
      System.out.println("  RESULT: 200 OK — OSS accepted CRC32C bytes in CRC64 header");
      System.out.println("  → SILENT BUG: driver claims integrity binding it does not have");
      Assertions.assertNotNull(response.getKey());
    } catch (Exception e) {
      System.out.println("  RESULT: rejected — OSS noticed the value mismatch");
      System.out.println("  → LOUD BUG: every Ali upload with ChecksumMethod.CRC32C will fail");
      System.out.println("  exception: " + e.getClass().getSimpleName() + ": " + e.getMessage());
    } finally {
      safeDelete(key);
    }
  }

  // ---- helpers ----

  private String keyFor(String label) {
    return "checksum-smoke/" + runId + "/" + label;
  }

  private UploadResponse uploadWithChecksum(
      String key, byte[] content, String checksumValue, ChecksumMethod algorithm) {
    UploadRequest request =
        new UploadRequest.Builder()
            .withKey(key)
            .withContentLength(content.length)
            .withChecksumValue(checksumValue)
            .withChecksumAlgorithm(algorithm)
            .build();
    return client.upload(request, new ByteArrayInputStream(content));
  }

  private void safeDelete(String key) {
    try {
      client.delete(key, null);
    } catch (Exception ignored) {
      // best-effort
    }
  }

  /**
   * Computes CRC64-ECMA of {@code content} using the Ali SDK's own CRC64 implementation, and
   * returns the unsigned-decimal-string representation that the multicloudj UploadResponse
   * surfaces back from a successful upload (matches what {@code AliBlobStoreIT.computeChecksum}
   * already produces).
   */
  private static String computeCrc64(byte[] content) {
    CRC64 crc64 = new CRC64();
    crc64.update(content, content.length);
    return Long.toUnsignedString(crc64.getValue());
  }

  /**
   * Computes CRC32C of {@code content} and returns it as a base64-encoded 4-byte big-endian
   * value — the exact format {@code AbstractBlobStoreIT.computeChecksum()} (the default,
   * non-Ali harness path) uses for CRC32C, and the format any cross-cloud caller passing
   * {@code ChecksumMethod.CRC32C} would supply.
   */
  private static String computeCrc32cBase64(byte[] content) {
    CRC32C crc32c = new CRC32C();
    crc32c.update(content);
    long v = crc32c.getValue();
    byte[] out = new byte[4];
    out[0] = (byte) (v >> 24);
    out[1] = (byte) (v >> 16);
    out[2] = (byte) (v >> 8);
    out[3] = (byte) v;
    return Base64.getEncoder().encodeToString(out);
  }
}
