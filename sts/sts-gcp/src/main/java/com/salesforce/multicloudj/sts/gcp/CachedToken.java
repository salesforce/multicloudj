package com.salesforce.multicloudj.sts.gcp;

import java.time.Instant;

/**
 * Immutable value held by {@link GcpStsTokenCache}: a token string paired with the instant it
 * expires. The expiry drives the cache's per-entry eviction so a token is never served past its
 * lifetime. A {@code null} {@link #getExpiresAt()} means the expiry could not be determined; the
 * cache treats such an entry as immediately expired (returned to the current caller, never reused).
 */
public final class CachedToken {
  private final String tokenValue;
  private final Instant expiresAt;

  /**
   * Creates a cached token.
   *
   * @param tokenValue the token string to return to callers
   * @param expiresAt the instant the token expires, or {@code null} if unknown
   */
  public CachedToken(String tokenValue, Instant expiresAt) {
    this.tokenValue = tokenValue;
    this.expiresAt = expiresAt;
  }

  public String getTokenValue() {
    return tokenValue;
  }

  public Instant getExpiresAt() {
    return expiresAt;
  }
}
