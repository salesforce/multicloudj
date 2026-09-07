package com.salesforce.multicloudj.sts.gcp;

import java.time.Instant;
import lombok.ToString;
import lombok.Value;

/**
 * Immutable value held by {@link GcpStsTokenCache}: a token string paired with the instant it
 * expires. The expiry drives the cache's per-entry eviction so a token is never served past its
 * lifetime. A {@code null} {@link #getExpiresAt()} means the expiry could not be determined; the
 * cache treats such an entry as immediately expired (returned to the current caller, never reused).
 */
@Value
public class CachedToken {

  /**
   * The token string to return to callers. Excluded from {@code toString} because it is a secret
   * credential that must never be rendered into logs or diagnostics.
   */
  @ToString.Exclude
  String tokenValue;

  /** The instant the token expires, or {@code null} if unknown. */
  Instant expiresAt;
}
