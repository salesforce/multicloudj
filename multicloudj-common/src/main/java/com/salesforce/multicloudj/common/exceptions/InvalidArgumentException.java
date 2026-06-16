package com.salesforce.multicloudj.common.exceptions;

public class InvalidArgumentException extends SubstrateSdkException {

  private static final boolean DEFAULT_RETRYABLE = false;

  public InvalidArgumentException() {
    super(DEFAULT_RETRYABLE);
  }

  public InvalidArgumentException(String message, Throwable cause) {
    super(message, cause, DEFAULT_RETRYABLE);
  }

  public InvalidArgumentException(String message) {
    super(message, DEFAULT_RETRYABLE);
  }

  public InvalidArgumentException(Throwable cause) {
    super(cause, DEFAULT_RETRYABLE);
  }

  public InvalidArgumentException(Throwable cause, boolean retryable) {
    super(cause, retryable);
  }

  public InvalidArgumentException(String message, Throwable cause, boolean retryable) {
    super(message, cause, retryable);
  }
}
