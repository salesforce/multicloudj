package com.salesforce.multicloudj.common.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BatchOperationExceptionTest {

  private static ItemFailure denied(String id) {
    return new ItemFailure(id, new UnAuthorizedException("provider detail"));
  }

  private static ItemFailure transientFailure(String id) {
    return new ItemFailure(id, new ResourceExhaustedException("provider detail"));
  }

  @Test
  void exposesFailuresAndItemIdsInOrder() {
    ItemFailure first = denied("a");
    ItemFailure second = transientFailure("b");

    BatchOperationException e = new BatchOperationException(List.of(first, second));

    assertEquals(List.of(first, second), e.getFailures());
    assertEquals(List.of("a", "b"), e.getFailedItemIds());
  }

  @Test
  void messageHasOnlyTheFailureCount() {
    BatchOperationException e =
        new BatchOperationException(List.of(denied("a"), transientFailure("b")));

    assertEquals("Batch operation failed for 2 item(s)", e.getMessage());
    assertFalse(e.getMessage().contains("provider detail"));
    assertFalse(e.getMessage().contains("UnAuthorizedException"));
  }

  @Test
  void messageOmitsItemIdsWhileAllFailuresRemainAvailableForLargeBatches() {
    List<ItemFailure> failures = new ArrayList<>();
    List<String> ids = new ArrayList<>();
    for (int i = 1; i <= 13; i++) {
      failures.add(denied("key-" + i));
      ids.add("key-" + i);
    }

    BatchOperationException e = new BatchOperationException(failures);

    assertEquals("Batch operation failed for 13 item(s)", e.getMessage());
    assertFalse(e.getMessage().contains("key-"));
    assertEquals(13, e.getFailures().size());
    assertEquals(ids, e.getFailedItemIds());
  }

  @Test
  void retryableOnlyWhenEveryFailureIsRetryable() {
    assertTrue(
        new BatchOperationException(List.of(transientFailure("a"), transientFailure("b")))
            .isRetryable());
    assertFalse(
        new BatchOperationException(List.of(transientFailure("a"), denied("b"))).isRetryable());
    assertFalse(new BatchOperationException(List.of(denied("a"))).isRetryable());
  }

  @Test
  void mergeCombinesFailuresInOrder() {
    BatchOperationException first =
        new BatchOperationException(List.of(transientFailure("a"), transientFailure("b")));
    BatchOperationException second = new BatchOperationException(List.of(denied("c")));

    BatchOperationException merged = BatchOperationException.merge(List.of(first, second));

    assertEquals(List.of("a", "b", "c"), merged.getFailedItemIds());
    assertFalse(merged.isRetryable());
  }

  @Test
  void rejectsEmptyOrNullInput() {
    assertThrows(IllegalArgumentException.class, () -> new BatchOperationException(List.of()));
    assertThrows(IllegalArgumentException.class, () -> new BatchOperationException(null));
    assertThrows(IllegalArgumentException.class, () -> BatchOperationException.merge(List.of()));
    assertThrows(IllegalArgumentException.class, () -> BatchOperationException.merge(null));
  }

  @Test
  void failureListsAreUnmodifiableAndIsolatedFromTheSource() {
    List<ItemFailure> source = new ArrayList<>(List.of(denied("a")));
    BatchOperationException e = new BatchOperationException(source);
    source.add(denied("b"));

    assertEquals(List.of("a"), e.getFailedItemIds());
    assertThrows(UnsupportedOperationException.class, () -> e.getFailures().add(denied("c")));
    assertThrows(UnsupportedOperationException.class, () -> e.getFailedItemIds().add("c"));
  }

  @Test
  void survivesSerializationRoundTrip() throws Exception {
    BatchOperationException original = new BatchOperationException(List.of(transientFailure("a")));

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(original);
    }
    BatchOperationException copy;
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      copy = (BatchOperationException) in.readObject();
    }

    assertEquals(original.getFailures(), copy.getFailures());
    assertEquals(original.getMessage(), copy.getMessage());
    assertEquals(original.isRetryable(), copy.isRetryable());
  }
}
