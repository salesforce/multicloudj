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

  @Test
  void builder_setsCorrelationIdMetadataKey() {
    OperationContext ctx =
        OperationContext.builder().correlationIdMetadataKey("my-corr-key").build();
    assertEquals("my-corr-key", ctx.getCorrelationIdMetadataKey());
  }

  @Test
  void resolveCorrelationIdMetadataKey_usesCustomKeyWhenSupplied() {
    OperationContext ctx =
        OperationContext.builder().correlationIdMetadataKey("my-corr-key").build();
    assertEquals("my-corr-key", ctx.resolveCorrelationIdMetadataKey());
  }

  @Test
  void resolveCorrelationIdMetadataKey_fallsBackToDefaultWhenNull() {
    OperationContext ctx = OperationContext.builder().build();
    assertNull(ctx.getCorrelationIdMetadataKey());
    assertEquals(SdkLoggingMetadataKeys.CORRELATION_ID, ctx.resolveCorrelationIdMetadataKey());
  }

  @Test
  void resolveCorrelationIdMetadataKey_fallsBackToDefaultWhenBlank() {
    OperationContext ctx = OperationContext.builder().correlationIdMetadataKey("   ").build();
    assertEquals(SdkLoggingMetadataKeys.CORRELATION_ID, ctx.resolveCorrelationIdMetadataKey());
  }

  @Test
  void toBuilder_preservesCorrelationIdMetadataKey() {
    OperationContext original =
        OperationContext.builder()
            .correlationId("req-1")
            .correlationIdMetadataKey("my-corr-key")
            .build();
    OperationContext updated = original.toBuilder().correlationId("req-2").build();

    assertEquals("req-2", updated.getCorrelationId());
    assertEquals("my-corr-key", updated.getCorrelationIdMetadataKey());
  }

  @Test
  void valueSemantics_correlationIdMetadataKeyParticipatesInEquals() {
    OperationContext a =
        OperationContext.builder().correlationId("x").correlationIdMetadataKey("k1").build();
    OperationContext b =
        OperationContext.builder().correlationId("x").correlationIdMetadataKey("k1").build();
    OperationContext c =
        OperationContext.builder().correlationId("x").correlationIdMetadataKey("k2").build();

    assertEquals(a, b);
    assertNotEquals(a, c);
  }

  @Test
  void resolveCorrelationIdMetadataKey_rejectsReservedServiceIdKey() {
    OperationContext ctx =
        OperationContext.builder()
            .correlationId("req-1")
            .correlationIdMetadataKey(SdkLoggingMetadataKeys.SERVICE_ID)
            .build();
    InvalidArgumentException ex =
        assertThrows(InvalidArgumentException.class, ctx::resolveCorrelationIdMetadataKey);
    assertTrue(ex.getMessage().contains(SdkLoggingMetadataKeys.SERVICE_ID));
  }

  @Test
  void resolveCorrelationIdMetadataKey_rejectsReservedTenantIdKey() {
    OperationContext ctx =
        OperationContext.builder()
            .correlationId("req-1")
            .correlationIdMetadataKey(SdkLoggingMetadataKeys.TENANT_ID)
            .build();
    assertThrows(InvalidArgumentException.class, ctx::resolveCorrelationIdMetadataKey);
  }

  @Test
  void resolveCorrelationIdMetadataKey_allowsDefaultKeyExplicitly() {
    // The correlation-id default key is not reserved against itself; setting it explicitly is fine.
    OperationContext ctx =
        OperationContext.builder()
            .correlationIdMetadataKey(SdkLoggingMetadataKeys.CORRELATION_ID)
            .build();
    assertEquals(SdkLoggingMetadataKeys.CORRELATION_ID, ctx.resolveCorrelationIdMetadataKey());
  }

  @Test
  void resolveCorrelationIdMetadataKey_rejectsKeyWithIllegalCharacters() {
    // Colons, spaces and non-ASCII are not portable across provider metadata-key rules.
    for (String bad : new String[] {"has space", "has:colon", "café", "a/b", "x=y"}) {
      OperationContext ctx =
          OperationContext.builder().correlationIdMetadataKey(bad).build();
      assertThrows(
          InvalidArgumentException.class,
          ctx::resolveCorrelationIdMetadataKey,
          "expected rejection of key: " + bad);
    }
  }

  @Test
  void resolveCorrelationIdMetadataKey_allowsValidKeyShapes() {
    for (String ok : new String[] {"x-custom-corr", "my_corr_key", "Trace-Id-123", "abc"}) {
      OperationContext ctx =
          OperationContext.builder().correlationIdMetadataKey(ok).build();
      assertEquals(ok, ctx.resolveCorrelationIdMetadataKey(), "expected acceptance of key: " + ok);
    }
  }
}
