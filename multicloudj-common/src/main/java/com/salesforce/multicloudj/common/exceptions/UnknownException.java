package com.salesforce.multicloudj.common.exceptions;

public class UnknownException extends SubstrateSdkException {

  private static final boolean DEFAULT_RETRYABLE = true;

  public UnknownException() {
    super(DEFAULT_RETRYABLE);
  }

  public UnknownException(String message, Throwable cause) {
    super(message, cause, DEFAULT_RETRYABLE);
  }

  public UnknownException(String message) {
    super(message, DEFAULT_RETRYABLE);
  }

  public UnknownException(Throwable cause) {
    super(cause, DEFAULT_RETRYABLE);
  }

  public UnknownException(Throwable cause, boolean retryable) {
    super(cause, retryable);
  }

  public UnknownException(String message, Throwable cause, boolean retryable) {
    super(message, cause, retryable);
  }
}
