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
}
