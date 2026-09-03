package com.salesforce.multicloudj.sts.gcp;

import com.github.benmanes.caffeine.cache.Ticker;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link GcpStsTokenCache}. Time is driven by a manual {@link Clock} and
 * {@link Ticker} that advance in lockstep, so expiry is deterministic without real waits.
 */
class GcpStsTokenCacheTest {

  private static final Duration SKEW = Duration.ofSeconds(60);
  private static final long MAX_SIZE = 1000L;

  /** A clock and ticker backed by a single mutable nanosecond counter, advanced by tests. */
  private static final class ManualTime {
    private final Instant base = Instant.parse("2026-01-01T00:00:00Z");
    private long elapsedNanos = 0L;

    Clock clock() {
      return new Clock() {
        @Override
        public Instant instant() {
          return base.plusNanos(elapsedNanos);
        }

        @Override
        public ZoneId getZone() {
          return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
          return this;
        }
      };
    }

    Ticker ticker() {
      return () -> elapsedNanos;
    }

    void advance(Duration duration) {
      elapsedNanos += duration.toNanos();
    }
  }

  private GcpStsTokenCache newCache(boolean enabled, ManualTime time) {
    return new GcpStsTokenCache(enabled, MAX_SIZE, SKEW, time.clock(), time.ticker());
  }

  @Test
  void hitWithinTtl_loadsOnce() throws IOException {
    ManualTime time = new ManualTime();
    GcpStsTokenCache cache = newCache(true, time);
    AtomicInteger loads = new AtomicInteger();
    TokenCacheKey key = TokenCacheKey.forAccessToken("scope");
    GcpStsTokenCache.TokenLoader loader =
        () -> {
          loads.incrementAndGet();
          return new CachedToken("token", time.clock().instant().plus(Duration.ofMinutes(10)));
        };

    String first = cache.getToken(key, loader);
    time.advance(Duration.ofMinutes(5));
    String second = cache.getToken(key, loader);

    Assertions.assertEquals("token", first);
    Assertions.assertEquals("token", second);
    Assertions.assertEquals(1, loads.get(), "second call within TTL must not reload");
  }

  @Test
  void expiresAfterLifetimeMinusSkew() throws IOException {
    ManualTime time = new ManualTime();
    GcpStsTokenCache cache = newCache(true, time);
    AtomicInteger loads = new AtomicInteger();
    TokenCacheKey key = TokenCacheKey.forAccessToken("scope");
    // 5-minute lifetime, 60s skew => effective TTL of 4 minutes.
    GcpStsTokenCache.TokenLoader loader =
        () ->
            new CachedToken(
                "token-" + loads.incrementAndGet(),
                time.clock().instant().plus(Duration.ofMinutes(5)));

    Assertions.assertEquals("token-1", cache.getToken(key, loader));

    // Just before the skew-adjusted deadline: still cached.
    time.advance(Duration.ofSeconds(239));
    Assertions.assertEquals("token-1", cache.getToken(key, loader));
    Assertions.assertEquals(1, loads.get());

    // Past the skew-adjusted deadline: reloaded.
    time.advance(Duration.ofSeconds(2));
    Assertions.assertEquals("token-2", cache.getToken(key, loader));
    Assertions.assertEquals(2, loads.get());
  }

  @Test
  void keysAreIsolatedAcrossPaths() throws IOException {
    ManualTime time = new ManualTime();
    GcpStsTokenCache cache = newCache(true, time);
    AtomicInteger loads = new AtomicInteger();
    GcpStsTokenCache.TokenLoader loader =
        () ->
            new CachedToken(
                "token-" + loads.incrementAndGet(),
                time.clock().instant().plus(Duration.ofMinutes(10)));

    List<TokenCacheKey> keys =
        List.of(
            TokenCacheKey.forAssumeRole("role", 3600, "scopeHash"),
            TokenCacheKey.forAccessToken("scope"),
            TokenCacheKey.forCallerIdentity("aud"),
            TokenCacheKey.forWebIdentity("audience", "tokenHash"));

    for (TokenCacheKey key : keys) {
      cache.getToken(key, loader);
    }
    // Re-fetching each key must hit the cache, not reload.
    for (TokenCacheKey key : keys) {
      cache.getToken(key, loader);
    }

    Assertions.assertEquals(4, loads.get(), "each distinct path key loads exactly once");
  }

  @Test
  void distinctKeyInputsDoNotCollide() throws IOException {
    ManualTime time = new ManualTime();
    GcpStsTokenCache cache = newCache(true, time);
    AtomicInteger loads = new AtomicInteger();
    GcpStsTokenCache.TokenLoader loader =
        () ->
            new CachedToken(
                "token-" + loads.incrementAndGet(),
                time.clock().instant().plus(Duration.ofMinutes(10)));

    cache.getToken(TokenCacheKey.forAssumeRole("roleA", 3600, "hash"), loader);
    cache.getToken(TokenCacheKey.forAssumeRole("roleB", 3600, "hash"), loader);
    cache.getToken(TokenCacheKey.forAssumeRole("roleA", 7200, "hash"), loader);
    cache.getToken(TokenCacheKey.forAssumeRole("roleA", 3600, "otherHash"), loader);

    Assertions.assertEquals(4, loads.get(), "role, lifetime, and boundary each vary the key");
  }

  @Test
  void concurrentMissesCollapseToSingleLoad() throws Exception {
    ManualTime time = new ManualTime();
    GcpStsTokenCache cache = newCache(true, time);
    AtomicInteger loads = new AtomicInteger();
    TokenCacheKey key = TokenCacheKey.forAccessToken("scope");
    CountDownLatch release = new CountDownLatch(1);
    GcpStsTokenCache.TokenLoader loader =
        () -> {
          loads.incrementAndGet();
          try {
            release.await(5, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return new CachedToken("token", time.clock().instant().plus(Duration.ofMinutes(10)));
        };

    int threads = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      List<Future<String>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        futures.add(pool.submit(() -> cache.getToken(key, loader)));
      }
      release.countDown();
      for (Future<String> future : futures) {
        Assertions.assertEquals("token", future.get(5, TimeUnit.SECONDS));
      }
    } finally {
      pool.shutdownNow();
    }

    Assertions.assertEquals(1, loads.get(), "concurrent identical misses must load once");
  }

  @Test
  void failedLoadsAreNotCached() throws IOException {
    ManualTime time = new ManualTime();
    GcpStsTokenCache cache = newCache(true, time);
    TokenCacheKey key = TokenCacheKey.forAccessToken("scope");

    GcpStsTokenCache.TokenLoader failing =
        () -> {
          throw new IOException("boom");
        };
    Assertions.assertThrows(IOException.class, () -> cache.getToken(key, failing));

    // A subsequent successful load for the same key must proceed (nothing was cached).
    String value =
        cache.getToken(
            key,
            () -> new CachedToken("token", time.clock().instant().plus(Duration.ofMinutes(10))));
    Assertions.assertEquals("token", value);
  }

  @Test
  void unknownExpiryIsNotReused() throws IOException {
    ManualTime time = new ManualTime();
    GcpStsTokenCache cache = newCache(true, time);
    AtomicInteger loads = new AtomicInteger();
    TokenCacheKey key = TokenCacheKey.forWebIdentity("audience", "tokenHash");
    GcpStsTokenCache.TokenLoader loader =
        () -> new CachedToken("token-" + loads.incrementAndGet(), null);

    Assertions.assertEquals("token-1", cache.getToken(key, loader));
    // null expiry => TTL 0 => the entry is never reused.
    Assertions.assertEquals("token-2", cache.getToken(key, loader));
    Assertions.assertEquals(2, loads.get());
  }

  @Test
  void disabledCacheAlwaysInvokesLoader() throws IOException {
    ManualTime time = new ManualTime();
    GcpStsTokenCache cache = newCache(false, time);
    AtomicInteger loads = new AtomicInteger();
    TokenCacheKey key = TokenCacheKey.forAccessToken("scope");
    GcpStsTokenCache.TokenLoader loader =
        () ->
            new CachedToken(
                "token-" + loads.incrementAndGet(),
                time.clock().instant().plus(Duration.ofMinutes(10)));

    cache.getToken(key, loader);
    cache.getToken(key, loader);

    Assertions.assertEquals(2, loads.get(), "disabled cache never caches");
  }

  @Test
  void nowReflectsInjectedClock() {
    ManualTime time = new ManualTime();
    GcpStsTokenCache cache = newCache(true, time);
    Instant before = cache.now();
    time.advance(Duration.ofSeconds(30));
    Assertions.assertEquals(before.plusSeconds(30), cache.now());
  }
}
