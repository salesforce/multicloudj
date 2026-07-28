package com.salesforce.multicloudj.blob.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.observability.OperationContext;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for custom correlation ID metadata key feature in InMemoryBlobStore. Verifies that callers
 * can customize the metadata key name under which the correlation ID value is stamped onto uploaded
 * objects.
 */
public class InMemoryCorrelationIdMetadataKeyTest {

  private static final String TEST_BUCKET = "test-correlation-key-bucket";
  private static final String REGION = "us-west-2";
  private static final String DEFAULT_CORRELATION_KEY = "sdk-logging-correlation-id";

  private InMemoryBlobStore blobStore;

  @BeforeEach
  public void setUp() {
    InMemoryBlobStore.createBucket(TEST_BUCKET);
    blobStore = new InMemoryBlobStore.Builder().withBucket(TEST_BUCKET).withRegion(REGION).build();
  }

  @AfterEach
  public void tearDown() {
    InMemoryBlobStore.clearStorage();
  }

  @Test
  public void testCustomCorrelationIdKey_usesCustomKey() throws Exception {
    String key = "test-object-custom-key";
    String customKey = "x-custom-corr";
    String correlationIdValue = "req-abc-123";
    byte[] content = "test content".getBytes(StandardCharsets.UTF_8);

    OperationContext context =
        OperationContext.builder()
            .correlationId(correlationIdValue)
            .correlationIdMetadataKey(customKey)
            .build();

    UploadRequest uploadRequest =
        new UploadRequest.Builder()
            .withKey(key)
            .withContentLength(content.length)
            .withOperationContext(context)
            .build();

    try (InputStream inputStream = new ByteArrayInputStream(content)) {
      UploadResponse uploadResponse = blobStore.upload(uploadRequest, inputStream);
      assertNotNull(uploadResponse, "Upload response should not be null");
    }

    BlobMetadata metadata = blobStore.getMetadata(key, null);
    assertNotNull(metadata, "Metadata should not be null");

    Map<String, String> storedMetadata = metadata.getMetadata();
    assertTrue(
        storedMetadata.containsKey(customKey),
        "Metadata should contain custom correlation key: " + customKey);
    assertEquals(
        correlationIdValue,
        storedMetadata.get(customKey),
        "Custom correlation key should have the correct value");
    assertFalse(
        storedMetadata.containsKey(DEFAULT_CORRELATION_KEY),
        "Metadata should NOT contain default correlation key when custom key is used");
  }

  @Test
  public void testCustomCorrelationIdKey_notOverwrittenWhenAppSuppliesIt() throws Exception {
    String key = "test-object-user-supplied-custom-key";
    String customKey = "x-custom-corr";
    String userSuppliedValue = "user-supplied";
    String sdkGeneratedValue = "sdk-generated";
    byte[] content = "test content".getBytes(StandardCharsets.UTF_8);

    Map<String, String> userMetadata = new HashMap<>();
    userMetadata.put(customKey, userSuppliedValue);

    OperationContext context =
        OperationContext.builder()
            .correlationId(sdkGeneratedValue)
            .correlationIdMetadataKey(customKey)
            .build();

    UploadRequest uploadRequest =
        new UploadRequest.Builder()
            .withKey(key)
            .withContentLength(content.length)
            .withMetadata(userMetadata)
            .withOperationContext(context)
            .build();

    try (InputStream inputStream = new ByteArrayInputStream(content)) {
      UploadResponse uploadResponse = blobStore.upload(uploadRequest, inputStream);
      assertNotNull(uploadResponse, "Upload response should not be null");
    }

    BlobMetadata metadata = blobStore.getMetadata(key, null);
    assertNotNull(metadata, "Metadata should not be null");

    Map<String, String> storedMetadata = metadata.getMetadata();
    assertTrue(
        storedMetadata.containsKey(customKey),
        "Metadata should contain custom correlation key: " + customKey);
    assertEquals(
        userSuppliedValue,
        storedMetadata.get(customKey),
        "Custom correlation key should retain user-supplied value, not be overwritten by SDK");
  }

  @Test
  public void testDefaultCorrelationIdKey_stillWorksWhenNoCustomKeyProvided() throws Exception {
    String key = "test-object-default-key";
    String correlationIdValue = "req-xyz-789";
    byte[] content = "test content".getBytes(StandardCharsets.UTF_8);

    OperationContext context =
        OperationContext.builder().correlationId(correlationIdValue).build();

    UploadRequest uploadRequest =
        new UploadRequest.Builder()
            .withKey(key)
            .withContentLength(content.length)
            .withOperationContext(context)
            .build();

    try (InputStream inputStream = new ByteArrayInputStream(content)) {
      UploadResponse uploadResponse = blobStore.upload(uploadRequest, inputStream);
      assertNotNull(uploadResponse, "Upload response should not be null");
    }

    BlobMetadata metadata = blobStore.getMetadata(key, null);
    assertNotNull(metadata, "Metadata should not be null");

    Map<String, String> storedMetadata = metadata.getMetadata();
    assertTrue(
        storedMetadata.containsKey(DEFAULT_CORRELATION_KEY),
        "Metadata should contain default correlation key when no custom key is provided");
    assertEquals(
        correlationIdValue,
        storedMetadata.get(DEFAULT_CORRELATION_KEY),
        "Default correlation key should have the correct value");
  }
}
