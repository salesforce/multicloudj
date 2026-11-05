package com.salesforce.multicloudj.common.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ItemFailureTest {

  private static final SubstrateSdkException DENIED = new UnAuthorizedException("denied");

  @Test
  void exposesItsFields() {
    ItemFailure failure = new ItemFailure("a", DENIED, Map.of("version", "v1"));

    assertEquals("a", failure.getItemId());
    assertSame(DENIED, failure.getException());
    assertEquals(Map.of("version", "v1"), failure.getAttributes());
  }

  @Test
  void attributesDefaultToEmpty() {
    assertTrue(new ItemFailure("a", DENIED).getAttributes().isEmpty());
    assertTrue(new ItemFailure("a", DENIED, null).getAttributes().isEmpty());
  }

  @Test
  void requiresItemIdAndException() {
    assertThrows(NullPointerException.class, () -> new ItemFailure(null, DENIED));
    assertThrows(NullPointerException.class, () -> new ItemFailure("a", null));
  }

  @Test
  void attributesAreCopiedAndUnmodifiable() {
    Map<String, String> source = new HashMap<>(Map.of("version", "v1"));
    ItemFailure failure = new ItemFailure("a", DENIED, source);
    source.put("other", "x");

    assertEquals(Map.of("version", "v1"), failure.getAttributes());
    assertThrows(UnsupportedOperationException.class, () -> failure.getAttributes().put("k", "v"));
    assertThrows(
        UnsupportedOperationException.class,
        () -> new ItemFailure("a", DENIED).getAttributes().put("k", "v"));
  }

  @Test
  void retryabilityDelegatesToTheException() {
    assertFalse(new ItemFailure("a", DENIED).isRetryable());
    assertTrue(new ItemFailure("a", new ResourceExhaustedException("slow down")).isRetryable());
    assertFalse(
        new ItemFailure("a", new ResourceExhaustedException("slow down", null, false))
            .isRetryable());
  }

  @Test
  void equalityCoversIdAttributesAndExceptionClassAndMessage() {
    ItemFailure base = new ItemFailure("a", DENIED, Map.of("version", "v1"));

    ItemFailure same =
        new ItemFailure("a", new UnAuthorizedException("denied"), Map.of("version", "v1"));
    assertEquals(base, same);
    assertEquals(base.hashCode(), same.hashCode());
    assertNotEquals(base, new ItemFailure("b", DENIED, Map.of("version", "v1")));
    assertNotEquals(base, new ItemFailure("a", DENIED, Map.of("version", "v2")));
    assertNotEquals(base, new ItemFailure("a", DENIED));
    assertNotEquals(
        base, new ItemFailure("a", new UnAuthorizedException("other"), Map.of("version", "v1")));
    assertNotEquals(
        base,
        new ItemFailure("a", new InvalidArgumentException("denied"), Map.of("version", "v1")));
  }

  @Test
  void toStringOmitsTheExceptionMessage() {
    String text =
        new ItemFailure("a", new UnAuthorizedException("secret detail"), Map.of("version", "v1"))
            .toString();

    assertTrue(text.contains("a"));
    assertTrue(text.contains("version=v1"));
    assertTrue(text.contains("UnAuthorizedException"));
    assertFalse(text.contains("secret detail"));
  }

  @Test
  void survivesSerializationRoundTrip() throws Exception {
    ItemFailure original =
        new ItemFailure("a", new ResourceExhaustedException("slow"), Map.of("version", "v1"));

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(original);
    }
    ItemFailure copy;
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      copy = (ItemFailure) in.readObject();
    }

    assertEquals(original, copy);
    assertTrue(copy.isRetryable());
  }
}
