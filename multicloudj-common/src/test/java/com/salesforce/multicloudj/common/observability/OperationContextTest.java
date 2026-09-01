package com.salesforce.multicloudj.common.observability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import org.junit.jupiter.api.Test;

class OperationContextTest {

  @Test
  void builder_setsCorrelationId() {
    OperationContext ctx = OperationContext.builder().correlationId("req-123").build();
    assertEquals("req-123", ctx.getCorrelationId());
  }

  @Test
  void builder_correlationIdMayBeNull() {
    OperationContext ctx = OperationContext.builder().build();
    assertNull(ctx.getCorrelationId());
  }

  @Test
  void toBuilder_allowsImmutableUpdate() {
    OperationContext original = OperationContext.builder().correlationId("orig").build();
    OperationContext updated = original.toBuilder().correlationId("updated").build();

    assertEquals("orig", original.getCorrelationId());
    assertEquals("updated", updated.getCorrelationId());
  }

  @Test
  void valueSemantics_equalsAndHashCode() {
    OperationContext a = OperationContext.builder().correlationId("x").build();
    OperationContext b = OperationContext.builder().correlationId("x").build();
    OperationContext c = OperationContext.builder().correlationId("y").build();

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, c);
  }

  @Test
  void builder_setsTenantId() {
    OperationContext ctx = OperationContext.builder().tenantId("tenant-42").build();
    assertEquals("tenant-42", ctx.getTenantId());
    assertNull(ctx.getCorrelationId());
  }

  @Test
  void builder_setsBothCorrelationAndTenant() {
    OperationContext ctx =
        OperationContext.builder().correlationId("req-1").tenantId("tenant-42").build();
    assertEquals("req-1", ctx.getCorrelationId());
    assertEquals("tenant-42", ctx.getTenantId());
  }

  @Test
  void toBuilder_preservesTenantId() {
    OperationContext original =
        OperationContext.builder().correlationId("req-1").tenantId("tenant-42").build();
    OperationContext updated = original.toBuilder().correlationId("req-2").build();

    assertEquals("req-2", updated.getCorrelationId());
    assertEquals("tenant-42", updated.getTenantId());
  }

  @Test
  void valueSemantics_tenantIdParticipatesInEquals() {
    OperationContext a = OperationContext.builder().correlationId("x").tenantId("t1").build();
    OperationContext b = OperationContext.builder().correlationId("x").tenantId("t1").build();
    OperationContext c = OperationContext.builder().correlationId("x").tenantId("t2").build();

    assertEquals(a, b);
    assertNotEquals(a, c);
  }

  @Test
  void builder_setsServiceId() {
    OperationContext ctx = OperationContext.builder().serviceId("svc-1").build();
    assertEquals("svc-1", ctx.getServiceId());
    assertNull(ctx.getCorrelationId());
    assertNull(ctx.getTenantId());
  }

  @Test
  void builder_setsAllThreeIds() {
    OperationContext ctx =
        OperationContext.builder()
            .correlationId("req-1")
            .tenantId("tenant-42")
            .serviceId("svc-1")
            .build();
    assertEquals("req-1", ctx.getCorrelationId());
    assertEquals("tenant-42", ctx.getTenantId());
    assertEquals("svc-1", ctx.getServiceId());
  }

  @Test
  void toBuilder_preservesServiceId() {
    OperationContext original =
        OperationContext.builder().correlationId("req-1").serviceId("svc-1").build();
    OperationContext updated = original.toBuilder().correlationId("req-2").build();

    assertEquals("req-2", updated.getCorrelationId());
    assertEquals("svc-1", updated.getServiceId());
  }

  @Test
  void valueSemantics_serviceIdParticipatesInEquals() {
    OperationContext a = OperationContext.builder().correlationId("x").serviceId("s1").build();
    OperationContext b = OperationContext.builder().correlationId("x").serviceId("s1").build();
    OperationContext c = OperationContext.builder().correlationId("x").serviceId("s2").build();

    assertEquals(a, b);
    assertNotEquals(a, c);
  }

  // ====== correlationIdKey field tests ======

  @Test
  void builder_correlationIdKeyMayBeNull() {
    OperationContext ctx = OperationContext.builder().correlationId("req-1").build();
    assertNull(ctx.getCorrelationIdKey(), "correlationIdKey should default to null");
  }

  @Test
  void builder_setsCorrelationIdKey() {
    OperationContext ctx =
        OperationContext.builder().correlationId("req-1").correlationIdKey("x-request-id").build();
    assertEquals("x-request-id", ctx.getCorrelationIdKey());
  }

  @Test
  void builder_correlationIdKeyMayBeBlank() {
    OperationContext ctx =
        OperationContext.builder().correlationId("req-1").correlationIdKey("").build();
    assertEquals("", ctx.getCorrelationIdKey(), "blank correlationIdKey is valid");
  }

  @Test
  void builder_correlationIdKeyMayBeWhitespace() {
    OperationContext ctx =
        OperationContext.builder().correlationId("req-1").correlationIdKey("  ").build();
    assertEquals("  ", ctx.getCorrelationIdKey(), "whitespace-only correlationIdKey is valid");
  }

  @Test
  void toBuilder_roundTripsCorrelationIdKey() {
    OperationContext original =
        OperationContext.builder().correlationId("req-1").correlationIdKey("trace-id").build();
    OperationContext updated = original.toBuilder().correlationId("req-2").build();

    assertEquals("req-2", updated.getCorrelationId());
    assertEquals("trace-id", updated.getCorrelationIdKey(), "toBuilder should preserve custom key");
  }

  @Test
  void valueSemantics_correlationIdKeyParticipatesInEquals() {
    OperationContext a =
        OperationContext.builder().correlationId("x").correlationIdKey("key1").build();
    OperationContext b =
        OperationContext.builder().correlationId("x").correlationIdKey("key1").build();
    OperationContext c =
        OperationContext.builder().correlationId("x").correlationIdKey("key2").build();

    assertEquals(a, b);
    assertNotEquals(a, c);
  }

  // ====== Resolver tests ======

  @Test
  void getEffectiveCorrelationIdMetadataKey_returnsDefaultWhenKeyIsNull() {
    OperationContext ctx = OperationContext.builder().correlationId("req-1").build();
    assertEquals(
        SdkLoggingMetadataKeys.CORRELATION_ID,
        ctx.getEffectiveCorrelationIdMetadataKey(),
        "Should return metadata default when correlationIdKey is null");
  }

  @Test
  void getEffectiveCorrelationIdMetadataKey_returnsDefaultWhenKeyIsBlank() {
    OperationContext ctx =
        OperationContext.builder().correlationId("req-1").correlationIdKey("").build();
    assertEquals(
        SdkLoggingMetadataKeys.CORRELATION_ID,
        ctx.getEffectiveCorrelationIdMetadataKey(),
        "Should return metadata default when correlationIdKey is blank");
  }

  @Test
  void getEffectiveCorrelationIdMetadataKey_returnsDefaultWhenKeyIsWhitespace() {
    OperationContext ctx =
        OperationContext.builder().correlationId("req-1").correlationIdKey("  ").build();
    assertEquals(
        SdkLoggingMetadataKeys.CORRELATION_ID,
        ctx.getEffectiveCorrelationIdMetadataKey(),
        "Should return metadata default when correlationIdKey is whitespace");
  }

  @Test
  void getEffectiveCorrelationIdMetadataKey_returnsCustomKey() {
    OperationContext ctx =
        OperationContext.builder().correlationId("req-1").correlationIdKey("x-request-id").build();
    assertEquals(
        "x-request-id",
        ctx.getEffectiveCorrelationIdMetadataKey(),
        "Should return custom key when supplied");
  }

  @Test
  void getEffectiveCorrelationIdAttributeKey_returnsDefaultWhenKeyIsNull() {
    OperationContext ctx = OperationContext.builder().correlationId("req-1").build();
    assertEquals(
        MultiCloudJLogger.ATTR_CORRELATION_ID,
        ctx.getEffectiveCorrelationIdAttributeKey(),
        "Should return attribute default when correlationIdKey is null");
  }

  @Test
  void getEffectiveCorrelationIdAttributeKey_returnsDefaultWhenKeyIsBlank() {
    OperationContext ctx =
        OperationContext.builder().correlationId("req-1").correlationIdKey("").build();
    assertEquals(
        MultiCloudJLogger.ATTR_CORRELATION_ID,
        ctx.getEffectiveCorrelationIdAttributeKey(),
        "Should return attribute default when correlationIdKey is blank");
  }

  @Test
  void getEffectiveCorrelationIdAttributeKey_returnsDefaultWhenKeyIsWhitespace() {
    OperationContext ctx =
        OperationContext.builder().correlationId("req-1").correlationIdKey("  ").build();
    assertEquals(
        MultiCloudJLogger.ATTR_CORRELATION_ID,
        ctx.getEffectiveCorrelationIdAttributeKey(),
        "Should return attribute default when correlationIdKey is whitespace");
  }

  @Test
  void getEffectiveCorrelationIdAttributeKey_returnsCustomKey() {
    OperationContext ctx =
        OperationContext.builder().correlationId("req-1").correlationIdKey("trace-id").build();
    assertEquals(
        "trace-id",
        ctx.getEffectiveCorrelationIdAttributeKey(),
        "Should return custom key when supplied");
  }

  @Test
  void resolvers_twoDefaultsDiffer() {
    OperationContext ctx = OperationContext.builder().correlationId("req-1").build();
    String metadataDefault = ctx.getEffectiveCorrelationIdMetadataKey();
    String attributeDefault = ctx.getEffectiveCorrelationIdAttributeKey();

    assertNotEquals(
        metadataDefault,
        attributeDefault,
        "Metadata default and attribute default must differ (dual-default invariant)");
  }

  @Test
  void existingBehavior_contextWithoutCorrelationIdKeyUnchanged() {
    // Verify that a context built the old way (no correlationIdKey) behaves exactly as before
    OperationContext ctx =
        OperationContext.builder()
            .correlationId("req-1")
            .tenantId("tenant-42")
            .serviceId("svc-1")
            .build();

    assertEquals("req-1", ctx.getCorrelationId());
    assertEquals("tenant-42", ctx.getTenantId());
    assertEquals("svc-1", ctx.getServiceId());
    assertNull(ctx.getCorrelationIdKey());
    assertEquals(SdkLoggingMetadataKeys.CORRELATION_ID, ctx.getEffectiveCorrelationIdMetadataKey());
    assertEquals(
        MultiCloudJLogger.ATTR_CORRELATION_ID, ctx.getEffectiveCorrelationIdAttributeKey());
  }

  // ====== Validation tests ======

  @Test
  void validation_rejectsUppercase() {
    InvalidArgumentException ex =
        assertThrows(
            InvalidArgumentException.class,
            () ->
                OperationContext.builder()
                    .correlationId("req-1")
                    .correlationIdKey("X-Request-Id")
                    .build(),
            "Should reject uppercase correlationIdKey");
    assertTrue(
        ex.getMessage().contains("lowercase"),
        "Error message should mention lowercase requirement: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("x-request-id"),
        "Error message should suggest lowercase form: " + ex.getMessage());
  }

  @Test
  void validation_rejectsLeadingHyphen() {
    InvalidArgumentException ex =
        assertThrows(
            InvalidArgumentException.class,
            () ->
                OperationContext.builder()
                    .correlationId("req-1")
                    .correlationIdKey("-request-id")
                    .build(),
            "Should reject leading hyphen");
    assertTrue(
        ex.getMessage().contains("alphanumeric"),
        "Error message should mention alphanumeric start requirement: " + ex.getMessage());
  }

  @Test
  void validation_rejectsLeadingUnderscore() {
    InvalidArgumentException ex =
        assertThrows(
            InvalidArgumentException.class,
            () ->
                OperationContext.builder()
                    .correlationId("req-1")
                    .correlationIdKey("_request_id")
                    .build(),
            "Should reject leading underscore");
    assertTrue(
        ex.getMessage().contains("alphanumeric"),
        "Error message should mention alphanumeric start requirement: " + ex.getMessage());
  }

  @Test
  void validation_rejectsDot() {
    InvalidArgumentException ex =
        assertThrows(
            InvalidArgumentException.class,
            () ->
                OperationContext.builder()
                    .correlationId("req-1")
                    .correlationIdKey("request.id")
                    .build(),
            "Should reject dot character");
    assertTrue(
        ex.getMessage().contains("invalid characters"),
        "Error message should mention invalid characters: " + ex.getMessage());
  }

  @Test
  void validation_rejectsSpace() {
    InvalidArgumentException ex =
        assertThrows(
            InvalidArgumentException.class,
            () ->
                OperationContext.builder()
                    .correlationId("req-1")
                    .correlationIdKey("request id")
                    .build(),
            "Should reject space character");
    assertTrue(
        ex.getMessage().contains("invalid characters"),
        "Error message should mention invalid characters: " + ex.getMessage());
  }

  @Test
  void validation_rejectsColon() {
    InvalidArgumentException ex =
        assertThrows(
            InvalidArgumentException.class,
            () ->
                OperationContext.builder()
                    .correlationId("req-1")
                    .correlationIdKey("request:id")
                    .build(),
            "Should reject colon character");
    assertTrue(
        ex.getMessage().contains("invalid characters"),
        "Error message should mention invalid characters: " + ex.getMessage());
  }

  @Test
  void validation_rejectsOver128Chars() {
    String longKey = "a".repeat(129);
    InvalidArgumentException ex =
        assertThrows(
            InvalidArgumentException.class,
            () ->
                OperationContext.builder().correlationId("req-1").correlationIdKey(longKey).build(),
            "Should reject key longer than 128 characters");
    assertTrue(
        ex.getMessage().contains("128"),
        "Error message should mention 128-char limit: " + ex.getMessage());
  }

  @Test
  void validation_allows128Chars() {
    String maxKey = "a".repeat(128);
    OperationContext ctx =
        OperationContext.builder().correlationId("req-1").correlationIdKey(maxKey).build();
    assertEquals(maxKey, ctx.getCorrelationIdKey(), "128-char key should be accepted");
  }

  @Test
  void validation_rejectsReservedKeys() {
    String[] reservedKeys = {
      "trace_id",
      "span_id",
      "sdk_service",
      "sdk_provider",
      "tenant_id",
      "service_id",
      "sdk-logging-service-id",
      "sdk-logging-tenant-id"
    };
    for (String reservedKey : reservedKeys) {
      InvalidArgumentException ex =
          assertThrows(
              InvalidArgumentException.class,
              () ->
                  OperationContext.builder()
                      .correlationId("req-1")
                      .correlationIdKey(reservedKey)
                      .build(),
              "Should reject reserved key: " + reservedKey);
      assertTrue(
          ex.getMessage().contains("reserved"),
          "Error message should mention reserved collision: " + ex.getMessage());
      assertTrue(
          ex.getMessage().contains(reservedKey),
          "Error message should mention the rejected key: " + ex.getMessage());
    }
  }

  @Test
  void validation_rejectsBucketSpanAttributeKey() {
    InvalidArgumentException ex =
        assertThrows(
            InvalidArgumentException.class,
            () ->
                OperationContext.builder()
                    .correlationId("req-1")
                    .correlationIdKey("bucket")
                    .build(),
            "Should reject the SDK-managed bucket span attribute key");

    assertTrue(
        ex.getMessage().contains("reserved"),
        "Error message should mention reserved collision: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("bucket"),
        "Error message should mention the rejected key: " + ex.getMessage());
  }

  @Test
  void validation_allowsCorrelationIdDefault_correlation_id() {
    // Explicit restatement of the attribute default should be allowed
    OperationContext ctx =
        OperationContext.builder()
            .correlationId("req-1")
            .correlationIdKey("correlation_id")
            .build();
    assertEquals(
        "correlation_id",
        ctx.getCorrelationIdKey(),
        "correlation_id (attribute default) should be allowed");
  }

  @Test
  void validation_allowsCorrelationIdDefault_sdkLoggingCorrelationId() {
    // Explicit restatement of the metadata default should be allowed
    OperationContext ctx =
        OperationContext.builder()
            .correlationId("req-1")
            .correlationIdKey("sdk-logging-correlation-id")
            .build();
    assertEquals(
        "sdk-logging-correlation-id",
        ctx.getCorrelationIdKey(),
        "sdk-logging-correlation-id (metadata default) should be allowed");
  }

  @Test
  void toBuilder_revalidatesCorrelationIdKey() {
    // Build a valid context, then try to update it with an invalid key via toBuilder
    OperationContext original =
        OperationContext.builder().correlationId("req-1").correlationIdKey("valid-key").build();

    InvalidArgumentException ex =
        assertThrows(
            InvalidArgumentException.class,
            () -> original.toBuilder().correlationIdKey("INVALID").build(),
            "toBuilder should re-validate correlationIdKey");
    assertTrue(
        ex.getMessage().contains("lowercase"),
        "Error message should mention lowercase requirement: " + ex.getMessage());
  }

  @Test
  void validation_exceptionIsInvalidArgumentException() {
    InvalidArgumentException ex =
        assertThrows(
            InvalidArgumentException.class,
            () ->
                OperationContext.builder()
                    .correlationId("req-1")
                    .correlationIdKey("BAD")
                    .build());

    assertEquals(
        InvalidArgumentException.class,
        ex.getClass(),
        "Should throw InvalidArgumentException specifically");
  }
}
