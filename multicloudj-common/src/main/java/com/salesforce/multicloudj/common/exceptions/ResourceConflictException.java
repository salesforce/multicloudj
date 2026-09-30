package com.salesforce.multicloudj.common.exceptions;

public class ResourceConflictException extends SubstrateSdkException {

  private static final boolean DEFAULT_RETRYABLE = false;

  public ResourceConflictException() {
    super(DEFAULT_RETRYABLE);
  }

  public ResourceConflictException(String message, Throwable cause) {
    super(message, cause, DEFAULT_RETRYABLE);
  }

  public ResourceConflictException(String message) {
    super(message, DEFAULT_RETRYABLE);
  }

  public ResourceConflictException(Throwable cause) {
    super(cause, DEFAULT_RETRYABLE);
  }

  public ResourceConflictException(Throwable cause, boolean retryable) {
    super(cause, retryable);
  }

  public ResourceConflictException(String message, Throwable cause, boolean retryable) {
    super(message, cause, retryable);
  }
}
