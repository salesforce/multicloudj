package com.salesforce.multicloudj.blob.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.multicloudj.blob.driver.BlobBatchDeleteException.Failure;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BlobBatchDeleteExceptionTest {

  private static Failure failure(String key, String code, boolean retryable) {
    return new Failure(key, null, code, "provider detail", retryable);
  }

  @Test
  void exposesFailuresAndKeysInOrder() {
    Failure first = new Failure("a", "v1", "AccessDenied", "denied", false);
    Failure second = new Failure("b", null, null, null, false);

    BlobBatchDeleteException e = new BlobBatchDeleteException(List.of(first, second));

    assertEquals(List.of(first, second), e.getFailures());
    assertEquals(List.of("a", "b"), e.getFailedKeys());
    assertEquals("v1", first.getVersionId());
    assertEquals("AccessDenied", first.getCode());
    assertEquals("denied", first.getMessage());
    assertNull(second.getVersionId());
    assertNull(second.getCode());
    assertNull(second.getMessage());
  }

  @Test
  void messageListsKeysWithCodesAndOmitsProviderMessages() {
    BlobBatchDeleteException e =
        new BlobBatchDeleteException(
            List.of(failure("a", "AccessDenied", false), new Failure("b", null, null, "x", false)));

    assertEquals("Failed to delete 2 object(s): a (AccessDenied), b", e.getMessage());
    assertFalse(e.getMessage().contains("provider detail"));
  }

  @Test
  void messageListsAtMostTenKeysAndCountsTheRest() {
    List<Failure> failures = new ArrayList<>();
    for (int i = 1; i <= 13; i++) {
      failures.add(failure("k" + i, null, false));
    }

    BlobBatchDeleteException e = new BlobBatchDeleteException(failures);

    assertEquals(13, e.getFailures().size());
    assertTrue(e.getMessage().contains("k10"));
    assertFalse(e.getMessage().contains("k11"));
    assertTrue(e.getMessage().endsWith(" and 3 more"));
  }

  @Test
  void messageOmitsRemainderWhenExactlyTenFail() {
    List<Failure> failures = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      failures.add(failure("k" + i, null, false));
    }

    assertFalse(new BlobBatchDeleteException(failures).getMessage().contains("more"));
  }

  @Test
  void retryableOnlyWhenEveryFailureIsRetryable() {
    assertTrue(
        new BlobBatchDeleteException(List.of(failure("a", "x", true), failure("b", "y", true)))
            .isRetryable());
    assertFalse(
        new BlobBatchDeleteException(List.of(failure("a", "x", true), failure("b", "y", false)))
            .isRetryable());
    assertFalse(new BlobBatchDeleteException(List.of(failure("a", "x", false))).isRetryable());
  }

  @Test
  void mergeCombinesFailuresInOrder() {
    BlobBatchDeleteException first =
        new BlobBatchDeleteException(List.of(failure("a", "x", true), failure("b", "x", true)));
    BlobBatchDeleteException second =
        new BlobBatchDeleteException(List.of(failure("c", "y", false)));

    BlobBatchDeleteException merged = BlobBatchDeleteException.merge(List.of(first, second));

    assertEquals(List.of("a", "b", "c"), merged.getFailedKeys());
    assertFalse(merged.isRetryable());
  }

  @Test
  void rejectsEmptyOrNullFailures() {
    assertThrows(
        IllegalArgumentException.class, () -> new BlobBatchDeleteException(List.of()));
    assertThrows(IllegalArgumentException.class, () -> new BlobBatchDeleteException(null));
    assertThrows(
        IllegalArgumentException.class, () -> BlobBatchDeleteException.merge(List.of()));
  }

  @Test
  void failureRequiresKey() {
    assertThrows(NullPointerException.class, () -> new Failure(null, null, null, null, false));
  }

  @Test
  void failureListsAreUnmodifiableAndIsolatedFromTheSource() {
    List<Failure> source = new ArrayList<>(List.of(failure("a", null, false)));
    BlobBatchDeleteException e = new BlobBatchDeleteException(source);
    source.add(failure("b", null, false));

    assertEquals(List.of("a"), e.getFailedKeys());
    assertThrows(
        UnsupportedOperationException.class, () -> e.getFailures().add(failure("c", null, false)));
    assertThrows(UnsupportedOperationException.class, () -> e.getFailedKeys().add("c"));
  }

  @Test
  void failureEqualityCoversEveryField() {
    Failure base = new Failure("a", "v1", "AccessDenied", "denied", false);

    assertEquals(base, new Failure("a", "v1", "AccessDenied", "denied", false));
    assertEquals(
        base.hashCode(), new Failure("a", "v1", "AccessDenied", "denied", false).hashCode());
    assertNotEquals(base, new Failure("b", "v1", "AccessDenied", "denied", false));
    assertNotEquals(base, new Failure("a", "v2", "AccessDenied", "denied", false));
    assertNotEquals(base, new Failure("a", "v1", "Other", "denied", false));
    assertNotEquals(base, new Failure("a", "v1", "AccessDenied", "other", false));
    assertNotEquals(base, new Failure("a", "v1", "AccessDenied", "denied", true));
  }

  @Test
  void failureToStringOmitsProviderMessage() {
    String text = new Failure("a", "v1", "AccessDenied", "secret detail", false).toString();

    assertTrue(text.contains("a"));
    assertTrue(text.contains("AccessDenied"));
    assertFalse(text.contains("secret detail"));
  }

  @Test
  void survivesSerializationRoundTrip() throws Exception {
    BlobBatchDeleteException original =
        new BlobBatchDeleteException(
            List.of(new Failure("a", "v1", "AccessDenied", "denied", true)));

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(original);
    }
    BlobBatchDeleteException copy;
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      copy = (BlobBatchDeleteException) in.readObject();
    }

    assertEquals(original.getFailures(), copy.getFailures());
    assertEquals(original.getMessage(), copy.getMessage());
    assertEquals(original.isRetryable(), copy.isRetryable());
  }
}
