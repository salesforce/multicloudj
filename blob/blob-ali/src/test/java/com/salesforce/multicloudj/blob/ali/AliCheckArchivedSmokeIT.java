package com.salesforce.multicloudj.blob.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.OperationOptions;
import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.exceptions.ServiceException;
import com.aliyun.sdk.service.oss2.models.DeleteMarkerEntry;
import com.aliyun.sdk.service.oss2.models.DeleteObjectRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectResult;
import com.aliyun.sdk.service.oss2.models.ListObjectVersionsRequest;
import com.aliyun.sdk.service.oss2.models.ListObjectVersionsResult;
import com.aliyun.sdk.service.oss2.models.ObjectVersion;
import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.ali.AliConstants;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Standalone direct test that empirically verifies what Alibaba OSS returns when a versioned
 * object is deleted and a normal GET is attempted on the (now non-current) key. This validates the
 * core assumption behind the Ali <code>checkArchived</code> implementation: that OSS signals the
 * delete-marker scenario in a way comparable to S3's <code>x-amz-delete-marker</code> header.
 *
 * <p>This hits the real OSS service directly (gated on {@code ALIBABA_CLOUD_ACCESS_KEY_ID}, so it
 * never runs in CI/replay). It uses the <b>raw {@link OSSClient}</b> for the GET attempt and the
 * follow-up <code>ListObjectVersions</code> so the observable wire-level response is captured
 * unmodified by the multicloudj transformer.
 *
 * <p><b>Requires:</b> {@link #BUCKET} must be a versioned bucket.
 *
 * <p>What this test confirms (the open questions from the checkArchived research):
 * <ol>
 *   <li>The exception class / status code OSS returns on a deleted-key GET (expected: 404).
 *   <li>Whether a delete-marker signal is reachable on the response — via
 *       {@link ServiceException#headers()} (the analog of S3's <code>x-amz-delete-marker</code>)
 *       and/or {@link GetObjectResult#deleteMarker()}.
 *   <li>That a follow-up <code>ListObjectVersions</code> returns the prior object version's
 *       {@code versionId}, and that downloading by that id recovers the original bytes — which is
 *       what the cross-cloud {@code testDownload_checkArchived} conformance test asserts.
 * </ol>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfEnvironmentVariable(named = "ALIBABA_CLOUD_ACCESS_KEY_ID", matches = ".+")
public class AliCheckArchivedSmokeIT {

  private static final String ENDPOINT = "https://oss-cn-shanghai.aliyuncs.com";
  // Must be a versioned bucket.
  private static final String BUCKET = "chameleon-multicloudj-test-versioned";
  private static final String REGION = "cn-shanghai";

  private BucketClient client;
  // Raw OSS client used for the GET attempt + ListObjectVersions, plus best-effort cleanup
  // of any prior versions / delete markers left on the test key.
  private OSSClient rawClient;
  private String key;
  private String originalVersionId;
  private String deleteMarkerVersionId;

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

    key = "checkarchived-smoke/" + UUID.randomUUID() + "/object";
  }

  @AfterAll
  void teardown() throws Exception {
    // Best-effort cleanup of every version + the delete marker.
    if (rawClient != null) {
      if (originalVersionId != null) {
        try {
          rawClient.deleteObject(
              DeleteObjectRequest.newBuilder()
                  .bucket(BUCKET)
                  .key(key)
                  .versionId(originalVersionId)
                  .build(),
              OperationOptions.defaults());
        } catch (Exception ignored) {
          // ignore
        }
      }
      if (deleteMarkerVersionId != null) {
        try {
          rawClient.deleteObject(
              DeleteObjectRequest.newBuilder()
                  .bucket(BUCKET)
                  .key(key)
                  .versionId(deleteMarkerVersionId)
                  .build(),
              OperationOptions.defaults());
        } catch (Exception ignored) {
          // ignore
        }
      }
      rawClient.close();
    }
    if (client != null) {
      client.close();
    }
  }

  @Test
  void deletedVersionedObject_get404AndPriorVersionRecoverable() throws Exception {
    byte[] content = "checkArchived smoke test".getBytes(StandardCharsets.UTF_8);

    // 1. Upload an object via the multicloudj client and capture its version id.
    UploadResponse uploadResponse;
    try (ByteArrayInputStream in = new ByteArrayInputStream(content)) {
      uploadResponse = client.upload(
          new UploadRequest.Builder()
              .withKey(key)
              .withContentLength(content.length)
              .build(),
          in);
    }
    originalVersionId = uploadResponse.getVersionId();
    assertNotNull(originalVersionId, "Versioned bucket should return a versionId on upload");
    System.out.printf("  uploaded %s (originalVersionId=%s)%n", key, originalVersionId);

    // 2. Delete without versionId. On a versioned bucket this creates a delete marker; the prior
    //    object version remains retrievable by id. multicloudj's delete() does not return the
    //    delete-marker version id, so we'll discover it via the version listing in step 4.
    client.delete(key, null);
    System.out.println("  delete(key, null) issued (expected: delete marker created)");

    // 3. Attempt a normal GET via the RAW OSS client and observe the exact response.
    //    This is the wire behavior our checkArchived guard will react to.
    Exception ex = assertThrows(Exception.class,
        () -> rawClient.getObject(
            GetObjectRequest.newBuilder().bucket(BUCKET).key(key).build(),
            OperationOptions.defaults()),
        "GET of a deleted versioned object should fail");
    System.out.printf("  raw GET on deleted key threw: %s%n", ex.getClass().getName());
    System.out.printf("  message: %s%n", ex.getMessage());

    // Walk the cause chain to find the underlying OSS ServiceException so we can inspect
    // the HTTP status code and the response headers (for delete-marker detection).
    ServiceException service = findServiceException(ex);
    assertNotNull(service, "Expected an OSS ServiceException somewhere in the cause chain");
    System.out.printf("  ServiceException: statusCode=%d, errorCode=%s%n",
        service.statusCode(), service.errorCode());
    System.out.println("  ServiceException headers:");
    Map<String, String> headers = service.headers();
    if (headers != null) {
      headers.forEach((k, v) -> System.out.printf("    %s: %s%n", k, v));
    }
    assertEquals(404, service.statusCode(),
        "Expected HTTP 404 for GET on deleted versioned object");
    // Delete-marker signal — print whether it's present (case-insensitive). This is the assumption
    // we'll wire the production guard against.
    boolean deleteMarkerHeaderPresent = headers != null && headers.entrySet().stream()
        .anyMatch(e -> e.getKey().toLowerCase().contains("delete-marker")
            && "true".equalsIgnoreCase(e.getValue()));
    System.out.printf("  x-oss-delete-marker header present and true? %s%n",
        deleteMarkerHeaderPresent);

    // 4. ListObjectVersions on the prefix. We expect: 1 ObjectVersion (the original), plus
    //    1 DeleteMarkerEntry (the marker created in step 2). Capture both for the by-version
    //    download in step 5 and for teardown.
    ListObjectVersionsResult listResult = rawClient.listObjectVersions(
        ListObjectVersionsRequest.newBuilder()
            .bucket(BUCKET)
            .prefix(key)
            .maxKeys(10L)
            .build(),
        OperationOptions.defaults());
    List<ObjectVersion> versions = listResult.versions();
    List<DeleteMarkerEntry> markers = listResult.deleteMarkers();
    System.out.printf("  versions returned: %d, deleteMarkers returned: %d%n",
        versions != null ? versions.size() : 0,
        markers != null ? markers.size() : 0);
    assertNotNull(versions);
    assertTrue(versions.stream().anyMatch(v -> originalVersionId.equals(v.versionId())),
        "ListObjectVersions should include the original version id");
    if (markers != null && !markers.isEmpty()) {
      deleteMarkerVersionId = markers.get(0).versionId();
      System.out.printf("  deleteMarkerVersionId=%s (isLatest=%s)%n",
          deleteMarkerVersionId, markers.get(0).isLatest());
    }

    // 5. Download by the prior version's id via the multicloudj client and assert the original
    //    bytes are recovered. This is exactly what the cross-cloud
    //    testDownload_checkArchived conformance test will assert once enabled for Ali.
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    com.salesforce.multicloudj.blob.driver.DownloadResponse dr = client.download(
        com.salesforce.multicloudj.blob.driver.DownloadRequest.builder()
            .withKey(key)
            .withVersionId(originalVersionId)
            .build(),
        out);
    assertNotNull(dr);
    org.junit.jupiter.api.Assertions.assertArrayEquals(content, out.toByteArray(),
        "Downloading by the prior version id should recover the original bytes");
    System.out.println("  by-version download recovered original bytes — "
        + "checkArchived contract is achievable on OSS");
  }

  private static ServiceException findServiceException(Throwable t) {
    Throwable cur = t;
    while (cur != null) {
      if (cur instanceof ServiceException) {
        return (ServiceException) cur;
      }
      cur = cur.getCause();
    }
    return null;
  }
}
