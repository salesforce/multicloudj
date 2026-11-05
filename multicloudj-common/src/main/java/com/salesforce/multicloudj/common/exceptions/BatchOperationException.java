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
 * <p>The exception is retryable only if it holds at least one failure and every failure is
 * retryable.
 */
public class BatchOperationException extends SubstrateSdkException {

  private static final long serialVersionUID = 1L;

  /** The failed items in failure order; unmodifiable. */
  @Getter private final List<ItemFailure> failures;

  /**
   * @param failures the per-item failures; a null collection is treated as empty
   */
  public BatchOperationException(Collection<ItemFailure> failures) {
    this(copyOf(failures));
  }

  private BatchOperationException(List<ItemFailure> failures) {
    super(
        "Batch operation failed for " + failures.size() + " item(s)",
        !failures.isEmpty() && failures.stream().allMatch(ItemFailure::isRetryable));
    this.failures = failures;
  }

  /**
   * @return the ids of the failed items, in failure order
   */
  public List<String> getFailedItemIds() {
    return failures.stream().map(ItemFailure::getItemId).collect(Collectors.toUnmodifiableList());
  }

  private static List<ItemFailure> copyOf(Collection<ItemFailure> failures) {
    return failures == null
        ? Collections.emptyList()
        : Collections.unmodifiableList(new ArrayList<>(failures));
  }
}
