package com.salesforce.multicloudj.common.exceptions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;

/**
 * Thrown when a batch operation completed but one or more of its items failed.
 *
 * <p>Every item not listed in {@link #getFailures()} succeeded. Each {@link ItemFailure} carries
 * the exception for its own item, so a caller can retry just the items whose failure is
 * retryable.
 *
 * <p>The exception is retryable only if every failure is retryable.
 */
public class BatchOperationException extends SubstrateSdkException {

  private static final long serialVersionUID = 1L;

  /** The failed items in failure order; unmodifiable. */
  @Getter private final List<ItemFailure> failures;

  /**
   * @param failures the per-item failures; must not be null or empty
   * @throws IllegalArgumentException if {@code failures} is null or empty
   */
  public BatchOperationException(Collection<ItemFailure> failures) {
    super(buildMessage(failures), allRetryable(failures));
    this.failures = Collections.unmodifiableList(new ArrayList<>(failures));
  }

  /**
   * Combines the failures of several batch exceptions into a single exception.
   *
   * @param exceptions the exceptions to combine; must not be null or empty
   * @return an exception holding every failure of the given exceptions, in order
   */
  public static BatchOperationException merge(Collection<BatchOperationException> exceptions) {
    if (exceptions == null || exceptions.isEmpty()) {
      throw new IllegalArgumentException("exceptions must not be empty");
    }
    List<ItemFailure> merged = new ArrayList<>();
    for (BatchOperationException exception : exceptions) {
      merged.addAll(exception.getFailures());
    }
    return new BatchOperationException(merged);
  }

  /**
   * @return the ids of the failed items, in failure order
   */
  public List<String> getFailedItemIds() {
    return failures.stream().map(ItemFailure::getItemId).collect(Collectors.toUnmodifiableList());
  }

  private static boolean allRetryable(Collection<ItemFailure> failures) {
    requireNonEmpty(failures);
    return failures.stream().allMatch(ItemFailure::isRetryable);
  }

  private static String buildMessage(Collection<ItemFailure> failures) {
    requireNonEmpty(failures);
    return "Batch operation failed for " + failures.size() + " item(s)";
  }

  private static void requireNonEmpty(Collection<ItemFailure> failures) {
    if (failures == null || failures.isEmpty()) {
      throw new IllegalArgumentException("failures must not be empty");
    }
  }
}
