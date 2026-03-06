package com.salesforce.multicloudj.blob.driver;

import java.nio.file.Path;
import lombok.Builder;
import lombok.Getter;

/** An object representing a failed blob upload attempt */
@Builder
@Getter
public class FailedBlobUpload {
  private final Path source;
  private final Throwable exception;
}
