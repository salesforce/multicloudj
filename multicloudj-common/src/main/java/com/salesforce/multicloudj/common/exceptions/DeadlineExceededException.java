package com.salesforce.multicloudj.common.exceptions;

public class DeadlineExceededException extends SubstrateSdkException {

  private static final boolean DEFAULT_RETRYABLE = true;

  public DeadlineExceededException() {
    super(DEFAULT_RETRYABLE);
  }

  public DeadlineExceededException(String message, Throwable cause) {
    super(message, cause, DEFAULT_RETRYABLE);
  }

  public DeadlineExceededException(String message) {
    super(message, DEFAULT_RETRYABLE);
  }

  public DeadlineExceededException(Throwable cause) {
    super(cause, DEFAULT_RETRYABLE);
  }

  public DeadlineExceededException(Throwable cause, boolean retryable) {
    super(cause, retryable);
  }

  public DeadlineExceededException(String message, Throwable cause, boolean retryable) {
    super(message, cause, retryable);
  }
}
