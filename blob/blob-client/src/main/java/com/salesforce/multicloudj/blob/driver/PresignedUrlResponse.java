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
   * Headers the uploader must replay verbatim. On most substrates, each header was bound into the
   * URL signature — omitting or modifying any of them causes the substrate to reject the request.
   * <b>Substrate caveat:</b> not all substrates can sign all constraint headers (e.g. GCS cannot
   * sign Content-Length). Only headers that appear in this map were actually signed; callers should
   * not assume constraints beyond what is reported here are enforced. Never null; empty map when no
   * constraint headers were signed (e.g. download URLs).
   */
  @Builder.Default
  private final Map<String, String> signedHeaders = Map.of();

  /** When the URL stops being valid. Null only if the substrate doesn't expose it. */
  private final Instant expiration;
}
