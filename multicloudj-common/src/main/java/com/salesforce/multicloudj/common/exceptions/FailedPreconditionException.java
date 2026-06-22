package com.salesforce.multicloudj.common.exceptions;

public class FailedPreconditionException extends SubstrateSdkException {

  private static final boolean DEFAULT_RETRYABLE = false;

  public FailedPreconditionException() {
    super(DEFAULT_RETRYABLE);
  }

  public FailedPreconditionException(String message, Throwable cause) {
    super(message, cause, DEFAULT_RETRYABLE);
  }

  public FailedPreconditionException(String message) {
    super(message, DEFAULT_RETRYABLE);
  }

  public FailedPreconditionException(Throwable cause) {
    super(cause, DEFAULT_RETRYABLE);
  }

  public FailedPreconditionException(Throwable cause, boolean retryable) {
    super(cause, retryable);
  }

  public FailedPreconditionException(String message, Throwable cause, boolean retryable) {
    super(message, cause, retryable);
  }
}
