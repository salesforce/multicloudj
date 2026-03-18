package com.salesforce.multicloudj.iam.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class EffectTest {

  @Test
  public void testAllowEffect() {
    assertEquals("Allow", Effect.ALLOW.getValue());
    assertEquals("Allow", Effect.ALLOW.toString());
  }

  @Test
  public void testDenyEffect() {
    assertEquals("Deny", Effect.DENY.getValue());
    assertEquals("Deny", Effect.DENY.toString());
  }

  @Test
  public void testEnumValues() {
    Effect[] effects = Effect.values();
    assertEquals(2, effects.length);
  }
}
