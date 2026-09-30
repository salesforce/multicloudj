package com.salesforce.multicloudj.common.exceptions;

public class ResourceExhaustedException extends SubstrateSdkException {

  private static final boolean DEFAULT_RETRYABLE = true;

  public ResourceExhaustedException() {
    super(DEFAULT_RETRYABLE);
  }

  public ResourceExhaustedException(String message, Throwable cause) {
    super(message, cause, DEFAULT_RETRYABLE);
  }

  public ResourceExhaustedException(String message) {
    super(message, DEFAULT_RETRYABLE);
  }

  public ResourceExhaustedException(Throwable cause) {
    super(cause, DEFAULT_RETRYABLE);
  }

  public ResourceExhaustedException(Throwable cause, boolean retryable) {
    super(cause, retryable);
  }

  public ResourceExhaustedException(String message, Throwable cause, boolean retryable) {
    super(message, cause, retryable);
  }
}
