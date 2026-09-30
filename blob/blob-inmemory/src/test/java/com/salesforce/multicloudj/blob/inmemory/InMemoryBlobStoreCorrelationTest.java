package com.salesforce.multicloudj.blob.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.common.observability.OperationContext;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests correlation-id stamping on InMemoryBlobStore. This closes a pre-existing coverage gap —
 * blob-inmemory stamps all three SDK logging keys (correlation-id, service-id, tenant-id) during
 * upload but had zero correlation-id tests before this feature.
 */
class InMemoryBlobStoreCorrelationTest {

  private InMemoryBlobStore store;

  @BeforeEach
  void setUp() {
    InMemoryBlobStore.clearStorage();
    InMemoryBlobStore.createBucket("test-bucket");
    store = new InMemoryBlobStore.Builder()
        .withBucket("test-bucket")
        .withRegion("local")
        .build();
  }

  @Test
  void upload_withCorrelationIdInContext_stampsDefaultKey() {
    OperationContext ctx = OperationContext.builder()
        .correlationId("req-default-123")
        .build();
    byte[] content = "test-data".getBytes();
    UploadRequest request = UploadRequest.builder()
        .withKey("test-key")
        .withMetadata(Map.of("user-key", "user-value"))
        .withOperationContext(ctx)
        .build();

    store.upload(request, new ByteArrayInputStream(content));

    BlobMetadata metadata = store.getMetadata("test-key", null);
    assertNotNull(metadata, "metadata should be retrievable after upload");
    assertEquals(
        "req-default-123",
        metadata.getMetadata().get(InMemoryBlobStore.CORRELATION_ID_METADATA_KEY),
        "default correlation-id key should be stamped when no custom key is supplied");
  }

  @Test
  void upload_withCustomCorrelationIdKey_stampsCustomKeyAndDefaultAbsent() {
    OperationContext ctx = OperationContext.builder()
        .correlationId("req-custom-456")
        .correlationIdKey("x-my-request-id")
        .build();
    byte[] content = "test-data".getBytes();
    UploadRequest request = UploadRequest.builder()
        .withKey("test-key-custom")
        .withMetadata(Map.of("user-key", "user-value"))
        .withOperationContext(ctx)
        .build();

    store.upload(request, new ByteArrayInputStream(content));

    BlobMetadata metadata = store.getMetadata("test-key-custom", null);
    Map<String, String> metadataMap = metadata.getMetadata();
    assertEquals(
        "req-custom-456",
        metadataMap.get("x-my-request-id"),
        "custom correlation-id key should be stamped with the context value");
    assertFalse(
        metadataMap.containsKey(InMemoryBlobStore.CORRELATION_ID_METADATA_KEY),
        "default correlation-id key must be absent when a custom key is used"
            + " (REPLACE semantics)");
  }

  @Test
  void upload_withCustomCorrelationIdKey_userSuppliedMetadataUnderCustomKeyWins() {
    OperationContext ctx = OperationContext.builder()
        .correlationId("sdk-generated")
        .correlationIdKey("x-my-request-id")
        .build();
    Map<String, String> userMetadata = new HashMap<>();
    userMetadata.put("x-my-request-id", "user-supplied-custom");
    userMetadata.put("other-key", "other-value");
    byte[] content = "test-data".getBytes();
    UploadRequest request = UploadRequest.builder()
        .withKey("test-key-user-wins")
        .withMetadata(userMetadata)
        .withOperationContext(ctx)
        .build();

    store.upload(request, new ByteArrayInputStream(content));

    BlobMetadata metadata = store.getMetadata("test-key-user-wins", null);
    assertEquals(
        "user-supplied-custom",
        metadata.getMetadata().get("x-my-request-id"),
        "application's explicit metadata value under the custom key must take"
            + " precedence over the SDK's");
  }

  @Test
  void upload_withDefaultKey_userSuppliedMetadataUnderDefaultKeyWins() {
    OperationContext ctx = OperationContext.builder()
        .correlationId("sdk-generated-default")
        .build();
    Map<String, String> userMetadata = new HashMap<>();
    userMetadata.put(InMemoryBlobStore.CORRELATION_ID_METADATA_KEY, "user-supplied-default");
    userMetadata.put("other-key", "other-value");
    byte[] content = "test-data".getBytes();
    UploadRequest request = UploadRequest.builder()
        .withKey("test-key-default-user-wins")
        .withMetadata(userMetadata)
        .withOperationContext(ctx)
        .build();

    store.upload(request, new ByteArrayInputStream(content));

    BlobMetadata metadata = store.getMetadata("test-key-default-user-wins", null);
    assertEquals(
        "user-supplied-default",
        metadata.getMetadata().get(InMemoryBlobStore.CORRELATION_ID_METADATA_KEY),
        "application's explicit metadata value under the default key must take"
            + " precedence over the SDK's");
  }

  @Test
  void upload_withoutOperationContext_noCorrelationIdStamped() {
    byte[] content = "test-data".getBytes();
    UploadRequest request = UploadRequest.builder()
        .withKey("test-key-no-context")
        .withMetadata(Map.of("user-key", "user-value"))
        .build();

    store.upload(request, new ByteArrayInputStream(content));

    BlobMetadata metadata = store.getMetadata("test-key-no-context", null);
    Map<String, String> metadataMap = metadata.getMetadata();
    assertFalse(
        metadataMap.containsKey(InMemoryBlobStore.CORRELATION_ID_METADATA_KEY),
        "correlation-id should not be stamped when no OperationContext is supplied");
    assertTrue(
        metadataMap.containsKey("user-key"),
        "user-supplied metadata should remain intact");
  }

  @Test
  void upload_withBlankCorrelationId_noCorrelationIdStamped() {
    OperationContext ctx = OperationContext.builder()
        .correlationId("")
        .build();
    byte[] content = "test-data".getBytes();
    UploadRequest request = UploadRequest.builder()
        .withKey("test-key-blank-correlation")
        .withMetadata(Map.of("user-key", "user-value"))
        .withOperationContext(ctx)
        .build();

    store.upload(request, new ByteArrayInputStream(content));

    BlobMetadata metadata = store.getMetadata("test-key-blank-correlation", null);
    Map<String, String> metadataMap = metadata.getMetadata();
    assertFalse(
        metadataMap.containsKey(InMemoryBlobStore.CORRELATION_ID_METADATA_KEY),
        "correlation-id should not be stamped when the context value is blank");
  }

  @Test
  void upload_withServiceAndTenantId_allThreeKeysStamped() {
    OperationContext ctx = OperationContext.builder()
        .correlationId("req-abc-123")
        .serviceId("my-service")
        .tenantId("my-tenant")
        .build();
    byte[] content = "test-data".getBytes();
    UploadRequest request = UploadRequest.builder()
        .withKey("test-key-all-three")
        .withMetadata(Map.of("user-key", "user-value"))
        .withOperationContext(ctx)
        .build();

    store.upload(request, new ByteArrayInputStream(content));

    BlobMetadata metadata = store.getMetadata("test-key-all-three", null);
    Map<String, String> metadataMap = metadata.getMetadata();
    assertEquals(
        "req-abc-123",
        metadataMap.get(InMemoryBlobStore.CORRELATION_ID_METADATA_KEY),
        "correlation-id should be stamped");
    assertEquals(
        "my-service",
        metadataMap.get(InMemoryBlobStore.SERVICE_ID_METADATA_KEY),
        "service-id should be stamped");
    assertEquals(
        "my-tenant",
        metadataMap.get(InMemoryBlobStore.TENANT_ID_METADATA_KEY),
        "tenant-id should be stamped");
  }

  @Test
  void upload_withCustomKeyAndServiceTenant_customCorrelationKeyAndDefaultServiceTenant() {
    OperationContext ctx = OperationContext.builder()
        .correlationId("req-custom-789")
        .correlationIdKey("x-request-id")
        .serviceId("my-service")
        .tenantId("my-tenant")
        .build();
    byte[] content = "test-data".getBytes();
    UploadRequest request = UploadRequest.builder()
        .withKey("test-key-mixed")
        .withMetadata(Map.of("user-key", "user-value"))
        .withOperationContext(ctx)
        .build();

    store.upload(request, new ByteArrayInputStream(content));

    BlobMetadata metadata = store.getMetadata("test-key-mixed", null);
    Map<String, String> metadataMap = metadata.getMetadata();
    assertEquals(
        "req-custom-789",
        metadataMap.get("x-request-id"),
        "custom correlation-id key should be stamped");
    assertFalse(
        metadataMap.containsKey(InMemoryBlobStore.CORRELATION_ID_METADATA_KEY),
        "default correlation-id key should be absent");
    assertEquals(
        "my-service",
        metadataMap.get(InMemoryBlobStore.SERVICE_ID_METADATA_KEY),
        "service-id should use default key (key symmetry was declined)");
    assertEquals(
        "my-tenant",
        metadataMap.get(InMemoryBlobStore.TENANT_ID_METADATA_KEY),
        "tenant-id should use default key (key symmetry was declined)");
  }

  @Test
  void initiateMultipartUpload_withCustomCorrelationIdKey_stampsCustomKeyAndDefaultAbsent() {
    OperationContext ctx = OperationContext.builder()
        .correlationId("req-mpu-123")
        .correlationIdKey("x-request-id")
        .build();
    MultipartUploadRequest mpuRequest = new MultipartUploadRequest.Builder()
        .withKey("test-key-mpu")
        .withMetadata(Map.of("user-key", "user-value"))
        .withOperationContext(ctx)
        .build();

    MultipartUpload mpu = store.initiateMultipartUpload(mpuRequest);

    Map<String, String> metadataMap = mpu.getMetadata();
    assertEquals("user-value", metadataMap.get("user-key"));
    assertEquals(
        "req-mpu-123",
        metadataMap.get("x-request-id"),
        "multipart upload should stamp the correlation id under the custom key");
    assertFalse(
        metadataMap.containsKey(InMemoryBlobStore.CORRELATION_ID_METADATA_KEY),
        "default correlation-id key should be absent on the multipart path");
  }
}
