package com.salesforce.multicloudj.common.exceptions;

import lombok.Getter;

@Getter
public class ResourceNotFoundException extends SubstrateSdkException {

  private final ArchiveInfo archiveInfo;

  public ResourceNotFoundException() {
    super();
    this.archiveInfo = null;
  }

  public ResourceNotFoundException(String message, Throwable cause) {
    super(message, cause);
    this.archiveInfo = null;
  }

  public ResourceNotFoundException(String message) {
    super(message);
    this.archiveInfo = null;
  }

  public ResourceNotFoundException(Throwable cause) {
    super(cause);
    this.archiveInfo = null;
  }

  public ResourceNotFoundException(String message, Throwable cause, ArchiveInfo archiveInfo) {
    super(message, cause);
    this.archiveInfo = archiveInfo;
  }

}
