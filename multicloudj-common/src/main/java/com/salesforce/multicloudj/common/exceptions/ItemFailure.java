package com.salesforce.multicloudj.common.exceptions;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;

/**
 * The failure of a single item within a batch operation.
 *
 * <p>Each failure carries its own {@link SubstrateSdkException}, so a caller can tell why the item
 * failed and, through {@link #isRetryable()}, whether retrying that item may succeed.
 */
public final class ItemFailure implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Identifies the failed item; never null. */
  @Getter private final String itemId;

  /** The reason the item failed; never null. */
  @Getter private final SubstrateSdkException exception;

  /** Additional values that, with the item id, identify the item; unmodifiable and never null. */
  @Getter private final Map<String, String> attributes;

  /**
   * Creates a failure with no additional identifying attributes.
   *
   * @param itemId identifies the failed item; must not be null
   * @param exception the reason the item failed; must not be null
   */
  public ItemFailure(String itemId, SubstrateSdkException exception) {
    this(itemId, exception, null);
  }

  /**
   * @param itemId identifies the failed item; must not be null
   * @param exception the reason the item failed; must not be null
   * @param attributes additional values that, together with the item id, identify the failed item;
   *     may be null, which is treated as empty. The map is copied.
   */
  public ItemFailure(
      String itemId, SubstrateSdkException exception, Map<String, String> attributes) {
    this.itemId = Objects.requireNonNull(itemId, "itemId");
    this.exception = Objects.requireNonNull(exception, "exception");
    this.attributes =
        attributes == null || attributes.isEmpty()
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
  }

  /**
   * @return true if retrying this item may succeed
   */
  public boolean isRetryable() {
    return exception.isRetryable();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ItemFailure)) {
      return false;
    }
    ItemFailure other = (ItemFailure) o;
    return itemId.equals(other.itemId)
        && attributes.equals(other.attributes)
        && exception.getClass().equals(other.exception.getClass())
        && Objects.equals(exception.getMessage(), other.exception.getMessage());
  }

  @Override
  public int hashCode() {
    return Objects.hash(itemId, attributes, exception.getClass().getName(), exception.getMessage());
  }

  /** Omits the exception message, which may echo request details. */
  @Override
  public String toString() {
    return "ItemFailure{itemId="
        + itemId
        + ", attributes="
        + attributes
        + ", exception="
        + exception.getClass().getSimpleName()
        + "}";
  }
}
