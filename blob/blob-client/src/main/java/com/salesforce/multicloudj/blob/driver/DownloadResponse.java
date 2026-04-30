package com.salesforce.multicloudj.blob.driver;

import java.io.InputStream;
import lombok.Builder;
import lombok.Getter;

/** Wrapper object for download result metadata */
@Builder
@Getter
public class DownloadResponse {
  private final String key;
  private final BlobMetadata metadata;
  private final InputStream inputStream;

  /**
   * The correlation ID associated with this download. Echoes the application-supplied value when
   * provided via {@code OperationContext}, or the SDK-generated UUID when not.
   */
  private final String correlationId;
}
