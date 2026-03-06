package com.salesforce.multicloudj.blob.driver;

import java.nio.file.Path;
import lombok.Builder;
import lombok.Getter;

/** An object representing a failed blob download attempt */
@Builder
@Getter
public class FailedBlobDownload {
  private final Path destination;
  private final Throwable exception;
}
