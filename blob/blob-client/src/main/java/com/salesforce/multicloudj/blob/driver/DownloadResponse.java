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
}
