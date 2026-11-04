package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Thrown when a batch delete completed but one or more objects could not be deleted.
 *
 * <p>Objects that were already absent are treated as deleted and are never reported here. Every
 * object not listed in {@link #getFailures()} was deleted.
 */
public class BlobBatchDeleteException extends SubstrateSdkException {

  private static final int MAX_KEYS_IN_MESSAGE = 10;

  private final List<Failure> failures;

  /**
   * @param failures the per-object failures; must not be empty or contain null
   */
  public BlobBatchDeleteException(Collection<Failure> failures) {
    super(buildMessage(failures), allRetryable(failures));
    this.failures = Collections.unmodifiableList(new ArrayList<>(failures));
  }

  /**
   * Combines the failures of several batch delete exceptions into a single exception.
   *
   * @param exceptions the exceptions to combine; must not be empty
   * @return an exception holding every failure of the given exceptions, in order
   */
  public static BlobBatchDeleteException merge(Collection<BlobBatchDeleteException> exceptions) {
    List<Failure> merged = new ArrayList<>();
    for (BlobBatchDeleteException exception : exceptions) {
      merged.addAll(exception.getFailures());
    }
    return new BlobBatchDeleteException(merged);
  }

  /**
   * @return an unmodifiable list of the objects that could not be deleted
   */
  public List<Failure> getFailures() {
    return failures;
  }

  /**
   * @return the keys of the objects that could not be deleted, in failure order
   */
  public List<String> getFailedKeys() {
    return failures.stream().map(Failure::getKey).collect(Collectors.toUnmodifiableList());
  }

  private static boolean allRetryable(Collection<Failure> failures) {
    requireNonEmpty(failures);
    return failures.stream().allMatch(Failure::isRetryable);
  }

  private static String buildMessage(Collection<Failure> failures) {
    requireNonEmpty(failures);
    String listed =
        failures.stream()
            .limit(MAX_KEYS_IN_MESSAGE)
            .map(f -> f.getCode() == null ? f.getKey() : f.getKey() + " (" + f.getCode() + ")")
            .collect(Collectors.joining(", "));
    int remaining = failures.size() - MAX_KEYS_IN_MESSAGE;
    return "Failed to delete "
        + failures.size()
        + " object(s): "
        + listed
        + (remaining > 0 ? " and " + remaining + " more" : "");
  }

  private static void requireNonEmpty(Collection<Failure> failures) {
    if (failures == null || failures.isEmpty()) {
      throw new IllegalArgumentException("failures must not be empty");
    }
  }

  /** A single object that could not be deleted. */
  public static final class Failure implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String key;
    private final String versionId;
    private final String code;
    private final String message;
    private final boolean retryable;

    /**
     * @param key the object key; required
     * @param versionId the object version, or null if the failure applies to the key as a whole
     * @param code the provider error code, or null if the provider does not report one
     * @param message the provider error message, or null if the provider does not report one
     * @param retryable whether the provider classifies this failure as transient
     */
    public Failure(
        String key, String versionId, String code, String message, boolean retryable) {
      this.key = Objects.requireNonNull(key, "key");
      this.versionId = versionId;
      this.code = code;
      this.message = message;
      this.retryable = retryable;
    }

    public String getKey() {
      return key;
    }

    public String getVersionId() {
      return versionId;
    }

    public String getCode() {
      return code;
    }

    public String getMessage() {
      return message;
    }

    public boolean isRetryable() {
      return retryable;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Failure)) {
        return false;
      }
      Failure other = (Failure) o;
      return retryable == other.retryable
          && key.equals(other.key)
          && Objects.equals(versionId, other.versionId)
          && Objects.equals(code, other.code)
          && Objects.equals(message, other.message);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, versionId, code, message, retryable);
    }

    /** Omits the provider message, which may echo request details. */
    @Override
    public String toString() {
      return "Failure{key="
          + key
          + ", versionId="
          + versionId
          + ", code="
          + code
          + ", retryable="
          + retryable
          + "}";
    }
  }
}
