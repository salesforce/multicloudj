package com.salesforce.multicloudj.blob.driver;

import java.net.URL;
import java.time.Instant;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

/**
 * Response from a presign operation, containing the presigned URL, the headers the substrate
 * signed into the URL (which the uploader must replay verbatim), and the expiration time.
 */
@Builder
@Getter
public class PresignedUrlResponse {

  /** The presigned URL. */
  private final URL url;

  /**
   * Headers the uploader must replay verbatim. Each header was bound into the URL signature, so
   * omitting or modifying any of them causes the substrate to reject the request. Never null;
   * empty map when no constraint headers were signed (e.g. download URLs).
   */
  private final Map<String, String> signedHeaders;

  /** When the URL stops being valid. Null only if the substrate doesn't expose it. */
  private final Instant expiration;
}
