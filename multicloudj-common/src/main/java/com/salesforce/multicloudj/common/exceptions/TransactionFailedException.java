package com.salesforce.multicloudj.common.exceptions;

/**
 * Exception thrown when a transaction fails due to conflicts, cancellations, or other
 * transaction-related issues.
 *
 * <p>This exception is used across different cloud providers to represent transaction failures:
 */
public class TransactionFailedException extends SubstrateSdkException {

  private static final boolean DEFAULT_RETRYABLE = false;

  public TransactionFailedException() {
    super(DEFAULT_RETRYABLE);
  }

  public TransactionFailedException(String message, Throwable cause) {
    super(message, cause, DEFAULT_RETRYABLE);
  }

  public TransactionFailedException(String message) {
    super(message, DEFAULT_RETRYABLE);
  }

  public TransactionFailedException(Throwable cause) {
    super(cause, DEFAULT_RETRYABLE);
  }

  public TransactionFailedException(Throwable cause, boolean retryable) {
    super(cause, retryable);
  }

  public TransactionFailedException(String message, Throwable cause, boolean retryable) {
    super(message, cause, retryable);
  }
}
