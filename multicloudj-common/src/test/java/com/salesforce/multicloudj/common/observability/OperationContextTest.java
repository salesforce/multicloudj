package com.salesforce.multicloudj.common.observability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
}
