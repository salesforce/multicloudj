package com.salesforce.multicloudj.common.exceptions;

public class UnSupportedOperationException extends SubstrateSdkException {

  private static final boolean DEFAULT_RETRYABLE = false;

  public UnSupportedOperationException() {
    super(DEFAULT_RETRYABLE);
  }

  public UnSupportedOperationException(String message, Throwable cause) {
    super(message, cause, DEFAULT_RETRYABLE);
  }

  public UnSupportedOperationException(String message) {
    super(message, DEFAULT_RETRYABLE);
  }

  public UnSupportedOperationException(Throwable cause) {
    super(cause, DEFAULT_RETRYABLE);
  }

  public UnSupportedOperationException(Throwable cause, boolean retryable) {
    super(cause, retryable);
  }

  public UnSupportedOperationException(String message, Throwable cause, boolean retryable) {
    super(message, cause, retryable);
  }
}
