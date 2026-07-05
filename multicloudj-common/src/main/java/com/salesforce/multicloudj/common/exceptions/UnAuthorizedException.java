package com.salesforce.multicloudj.common.exceptions;

public class UnAuthorizedException extends SubstrateSdkException {

  private static final boolean DEFAULT_RETRYABLE = false;

  public UnAuthorizedException() {
    super(DEFAULT_RETRYABLE);
  }

  public UnAuthorizedException(String message, Throwable cause) {
    super(message, cause, DEFAULT_RETRYABLE);
  }

  public UnAuthorizedException(String message) {
    super(message, DEFAULT_RETRYABLE);
  }

  public UnAuthorizedException(Throwable cause) {
    super(cause, DEFAULT_RETRYABLE);
  }

  public UnAuthorizedException(Throwable cause, boolean retryable) {
    super(cause, retryable);
  }

  public UnAuthorizedException(String message, Throwable cause, boolean retryable) {
    super(message, cause, retryable);
  }
}
