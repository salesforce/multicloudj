package com.salesforce.multicloudj.pubsub.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class AliAckIDTest {

  @Test
  void wrapsReceiptHandle() {
    AliAckID ackId = new AliAckID("handle-123");
    assertEquals("handle-123", ackId.getReceiptHandle());
    assertEquals("handle-123", ackId.toString());
  }

  @Test
  void rejectsNullHandle() {
    assertThrows(IllegalArgumentException.class, () -> new AliAckID(null));
  }

  @Test
  void rejectsBlankHandle() {
    assertThrows(IllegalArgumentException.class, () -> new AliAckID("   "));
  }

  @Test
  void equalityIsByHandle() {
    assertEquals(new AliAckID("h1"), new AliAckID("h1"));
    assertEquals(new AliAckID("h1").hashCode(), new AliAckID("h1").hashCode());
    assertNotEquals(new AliAckID("h1"), new AliAckID("h2"));
  }
}
