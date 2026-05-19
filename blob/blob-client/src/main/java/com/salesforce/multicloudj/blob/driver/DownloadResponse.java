package com.salesforce.multicloudj.blob.driver;

import java.io.InputStream;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Wrapper object for download result metadata */
@Builder
@Getter
@EqualsAndHashCode
@ToString
public class DownloadResponse {
  private final String key;
  private final BlobMetadata metadata;
  private final InputStream inputStream;

  // BucketClient rebuilds this response to stamp the correlationId; excluding the field from
  // equals/hashCode keeps the rebuilt copy equal to the underlying provider response (e.g. for
  // mock-based tests), and excluding it from toString avoids leaking observability metadata.
  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  private final String correlationId;
}
