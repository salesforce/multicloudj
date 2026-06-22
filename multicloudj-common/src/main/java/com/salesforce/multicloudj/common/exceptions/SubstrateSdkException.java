package com.salesforce.multicloudj.common.exceptions;

/**
 * Base exception for all multicloudj SDK exceptions.
 *
 * <p>Carries a retryability flag so callers can decide whether the failed operation may succeed if
 * retried. The flag is derived from one of two sources, in priority order:
 *
 * <ol>
 *   <li>An explicit hint propagated by the provider implementation (e.g. AWS {@code
 *       isThrottlingException()}, GCP {@code ApiException.isRetryable()}). When present, the hint
 *       overrides the default.
 *   <li>The exception type's default. {@link ResourceExhaustedException}, {@link
 *       DeadlineExceededException} and {@link UnknownException} default to retryable; all other
 *       subclasses default to non-retryable.
 * </ol>
 */
public class SubstrateSdkException extends RuntimeException {

  private final boolean retryable;

  public SubstrateSdkException() {
    super();
    this.retryable = false;
  }

  public SubstrateSdkException(String message, Throwable cause) {
    super(message, cause);
    this.retryable = false;
  }

  public SubstrateSdkException(String message) {
    super(message);
    this.retryable = false;
  }

  public SubstrateSdkException(Throwable cause) {
    super(cause);
    this.retryable = false;
  }

  protected SubstrateSdkException(boolean retryable) {
    super();
    this.retryable = retryable;
  }

  protected SubstrateSdkException(String message, boolean retryable) {
    super(message);
    this.retryable = retryable;
  }

  protected SubstrateSdkException(Throwable cause, boolean retryable) {
    super(cause);
    this.retryable = retryable;
  }

  protected SubstrateSdkException(String message, Throwable cause, boolean retryable) {
    super(message, cause);
    this.retryable = retryable;
  }

  /**
   * @return true if the failed operation is potentially retryable
   */
  public boolean isRetryable() {
    return retryable;
  }
}
