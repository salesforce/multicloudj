package com.salesforce.multicloudj.sts.gcp;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Ticker;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Provider-local, expiry-aware cache for the tokens produced by the GCP STS paths. It exists
 * because {@code StsCredentials} carries no expiry, so caching cannot live at the portable seam;
 * only here can the native token expiry ({@code AccessToken.getExpirationTime()}, a JWT
 * {@code exp}, or a token-exchange {@code expires_in}) be observed and honored.
 *
 * <p>The cache is a behavior-neutral optimization: a hit is indistinguishable from a fresh fetch,
 * and a token is never served within {@code expirySkew} of its expiry. Concurrent misses for the
 * same key collapse to a single network fetch (single-flight). Failed loads are not cached —
 * exceptions propagate and nothing is stored.
 *
 * <p>Enabled by default with bounded, safe internal defaults; there is no public configuration
 * surface. The hidden system property {@code multicloudj.gcp.sts.cache.enabled=false} disables it
 * as an operational escape hatch.
 */
public class GcpStsTokenCache {

  /** System property to disable the cache entirely (operational escape hatch). */
  static final String ENABLED_PROPERTY = "multicloudj.gcp.sts.cache.enabled";

  private static final long DEFAULT_MAX_SIZE = 1000L;
  private static final Duration DEFAULT_EXPIRY_SKEW = Duration.ofSeconds(60);

  private final Clock clock;
  private final Cache<TokenCacheKey, CachedToken> cache;

  /** Loads a fresh token when the cache misses. */
  @FunctionalInterface
  public interface TokenLoader {
    CachedToken load() throws IOException;
  }

  /** Production constructor: enabled unless disabled by system property, with default sizing. */
  public GcpStsTokenCache() {
    this(isEnabledByProperty(), DEFAULT_MAX_SIZE, DEFAULT_EXPIRY_SKEW, Clock.systemUTC(),
        Ticker.systemTicker());
  }

  /**
   * Test/internal constructor allowing an injected {@link Clock} and {@link Ticker} so expiry is
   * deterministic without real waits.
   */
  GcpStsTokenCache(boolean enabled, long maximumSize, Duration expirySkew, Clock clock,
      Ticker ticker) {
    this.clock = clock;
    if (enabled) {
      this.cache =
          Caffeine.newBuilder()
              .maximumSize(maximumSize)
              .ticker(ticker)
              .expireAfter(new TokenExpiry(clock, expirySkew))
              .build();
    } else {
      this.cache = null;
    }
  }

  private static boolean isEnabledByProperty() {
    String value = System.getProperty(ENABLED_PROPERTY);
    return value == null || !value.equalsIgnoreCase("false");
  }

  /**
   * Returns a valid token for {@code key}, loading and caching it on a miss. On a hit the loader is
   * not invoked and no network I/O occurs. When the cache is disabled the loader is always invoked.
   *
   * @param key fully-qualifying cache key for the requested token
   * @param loader supplier that performs the real (network) fetch on a miss
   * @return the token string
   * @throws IOException if the loader fails to fetch a token
   */
  public String getToken(TokenCacheKey key, TokenLoader loader) throws IOException {
    if (cache == null) {
      return loader.load().getTokenValue();
    }
    try {
      CachedToken token =
          cache.get(
              key,
              k -> {
                try {
                  return loader.load();
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
      return token.getTokenValue();
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  /** Current wall-clock instant from the injected clock; used to convert relative expiries. */
  public Instant now() {
    return clock.instant();
  }

  /**
   * Per-entry expiry: a relative TTL of {@code (expiresAt - skew) - now}, aged by Caffeine's
   * ticker. A relative TTL (rather than an absolute ticker deadline) avoids mixing the monotonic
   * ticker with wall-clock expiries. A {@code null} expiry yields TTL 0 — the value is returned to
   * the current caller but is never reused.
   */
  private static final class TokenExpiry implements Expiry<TokenCacheKey, CachedToken> {
    private final Clock clock;
    private final Duration skew;

    TokenExpiry(Clock clock, Duration skew) {
      this.clock = clock;
      this.skew = skew;
    }

    @Override
    public long expireAfterCreate(TokenCacheKey key, CachedToken value, long currentTime) {
      Instant expiresAt = value.getExpiresAt();
      if (expiresAt == null) {
        return 0L;
      }
      long ttlNanos = Duration.between(clock.instant(), expiresAt.minus(skew)).toNanos();
      return Math.max(0L, ttlNanos);
    }

    @Override
    public long expireAfterUpdate(
        TokenCacheKey key, CachedToken value, long currentTime, long currentDuration) {
      return currentDuration;
    }

    @Override
    public long expireAfterRead(
        TokenCacheKey key, CachedToken value, long currentTime, long currentDuration) {
      return currentDuration;
    }
  }
}
