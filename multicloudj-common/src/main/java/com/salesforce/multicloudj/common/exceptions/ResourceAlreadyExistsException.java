package com.salesforce.multicloudj.common.exceptions;

public class ResourceAlreadyExistsException extends SubstrateSdkException {

  private static final boolean DEFAULT_RETRYABLE = false;

  public ResourceAlreadyExistsException() {
    super(DEFAULT_RETRYABLE);
  }

  public ResourceAlreadyExistsException(String message, Throwable cause) {
    super(message, cause, DEFAULT_RETRYABLE);
  }

  public ResourceAlreadyExistsException(String message) {
    super(message, DEFAULT_RETRYABLE);
  }

  public ResourceAlreadyExistsException(Throwable cause) {
    super(cause, DEFAULT_RETRYABLE);
  }

  public ResourceAlreadyExistsException(Throwable cause, boolean retryable) {
    super(cause, retryable);
  }

  public ResourceAlreadyExistsException(String message, Throwable cause, boolean retryable) {
    super(message, cause, retryable);
  }
}
