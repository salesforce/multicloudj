package com.salesforce.multicloudj.common.exceptions;

import lombok.Getter;

@Getter
public class ResourceNotFoundException extends SubstrateSdkException {

  private static final boolean DEFAULT_RETRYABLE = false;

  private final ArchiveInfo archiveInfo;

  public ResourceNotFoundException() {
    super(DEFAULT_RETRYABLE);
    this.archiveInfo = null;
  }

  public ResourceNotFoundException(String message, Throwable cause) {
    super(message, cause, DEFAULT_RETRYABLE);
    this.archiveInfo = null;
  }

  public ResourceNotFoundException(String message) {
    super(message, DEFAULT_RETRYABLE);
    this.archiveInfo = null;
  }

  public ResourceNotFoundException(Throwable cause) {
    super(cause, DEFAULT_RETRYABLE);
    this.archiveInfo = null;
  }

  public ResourceNotFoundException(String message, Throwable cause, ArchiveInfo archiveInfo) {
    super(message, cause, DEFAULT_RETRYABLE);
    this.archiveInfo = archiveInfo;
  }

  public ResourceNotFoundException(Throwable cause, boolean retryable) {
    super(cause, retryable);
    this.archiveInfo = null;
  }

  public ResourceNotFoundException(String message, Throwable cause, boolean retryable) {
    super(message, cause, retryable);
    this.archiveInfo = null;
  }
}
