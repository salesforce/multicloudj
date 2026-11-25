package com.salesforce.multicloudj.sts.model;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Keeps caller-supplied session credentials fresh for a supplier set on a {@link
 * CredentialsType#SESSION} override.
 *
 * <p>Credentials are fetched lazily: the supplier is not called until the first {@link
 * #getCredentials()}. Each fetch produces an immutable snapshot holding the credentials, the time
 * after which they should be refreshed, and the time at which they must no longer be served. The
 * snapshot is replaced as a whole, so readers never observe a partly updated state.
 *
 * <p>Timing is derived from what the supplier returns:
 *
 * <ul>
 *   <li>Credentials that carry an expiration ({@link StsCredentials#getExpiration()}): refresh
 *       after the smaller of the refresh interval and 80% of the remaining lifetime; the
 *       credentials are never served at or after their expiration.
 *   <li>Credentials without an expiration: refresh after the refresh interval; the credentials are
 *       never served at or after twice the refresh interval. An unknown expiration does not mean
 *       the credentials never expire.
 * </ul>
 *
 * <p>Thread safety: instances are safe for concurrent use. When a refresh is due and the previous
 * credentials are still servable, exactly one caller refreshes while the others immediately receive
 * the previous credentials. When there are no servable credentials, callers wait for a single
 * fetch.
 *
 * <p>Failure handling: if a refresh fails while the previous credentials are still servable, those
 * credentials keep being served and the supplier is not called again until the smaller of the
 * refresh interval and ten seconds has passed. When there are no servable credentials, either
 * because the first fetch has not succeeded or because the previous credentials are past their
 * cutoff, a failed fetch is thrown as an {@link UnAuthorizedException} wrapping the supplier's
 * failure and is remembered for that same period. Calls made during that period throw a new {@link
 * UnAuthorizedException} caused by the remembered failure without calling the supplier, so a
 * failing supplier is not called once per caller. A transient failure of the first fetch is
 * therefore visible for up to that period. The first call after the period calls the supplier
 * again, and a success clears the remembered failure. A supplier result that is {@code null},
 * already expired, or lacks a non-blank access key id, access key secret or security token counts
 * as a failed fetch and is never cached.
 *
 * <p>Supplier contract: the supplier may be called from any thread, may perform blocking I/O, and
 * should be safe to call repeatedly. At most one call is in flight at a time. It must return a
 * non-blank access key id, access key secret and security token, even for clouds that authenticate
 * with the security token alone.
 *
 * <p>The supplier has no timeout. A supplier that blocks indefinitely stalls every caller once no
 * servable credentials remain. It must not call back into a client built from the same overrider:
 * the lock is reentrant, so such a call recurses instead of waiting. Any {@link Exception} the
 * supplier throws counts as a failed fetch. An {@link Error} propagates unchanged and is not
 * remembered as a failure. The lock is released either way.
 *
 * <p>Times are read from the system UTC clock, which is not monotonic. A backwards clock jump
 * extends how long credentials without an expiration are served.
 */
public final class CredentialsRefresher {

  /** Upper bound on how long to wait before retrying a failed refresh. */
  static final Duration MAX_RETRY_SPACING = Duration.ofSeconds(10);

  private static final int REFRESH_LIFETIME_PERCENT = 80;
  private static final int CUTOFF_INTERVAL_MULTIPLE = 2;

  private final Supplier<StsCredentials> supplier;
  private final Duration refreshInterval;
  private final Clock clock;
  private final ReentrantLock refreshLock = new ReentrantLock();
  private volatile Snapshot snapshot;
  private volatile Failure lastFailure;

  public CredentialsRefresher(Supplier<StsCredentials> supplier, Duration refreshInterval) {
    this(supplier, refreshInterval, Clock.systemUTC());
  }

  CredentialsRefresher(Supplier<StsCredentials> supplier, Duration refreshInterval, Clock clock) {
    if (supplier == null) {
      throw new InvalidArgumentException("supplier must not be null");
    }
    if (refreshInterval == null || refreshInterval.isZero() || refreshInterval.isNegative()) {
      throw new InvalidArgumentException("refreshInterval must be provided and positive");
    }
    try {
      // The cutoff for credentials without an expiration is derived from the interval, so an
      // interval that cannot be represented must fail here and not on a later credentials request.
      clock.instant().plus(refreshInterval.multipliedBy(CUTOFF_INTERVAL_MULTIPLE));
    } catch (ArithmeticException | DateTimeException e) {
      throw new InvalidArgumentException("refreshInterval is too large: " + refreshInterval, e);
    }
    this.supplier = supplier;
    this.refreshInterval = refreshInterval;
    this.clock = clock;
  }

  /**
   * Returns credentials that are valid to use now, calling the supplier when a refresh is due.
   *
   * @throws UnAuthorizedException if no servable credentials exist and the supplier cannot provide
   *     usable ones
   */
  public StsCredentials getCredentials() {
    Snapshot current = snapshot;
    Instant now = clock.instant();
    if (current != null && now.isBefore(current.refreshAt)) {
      return current.credentials;
    }
    if (current != null && now.isBefore(current.servableUntil)) {
      if (refreshLock.tryLock()) {
        try {
          return refreshWhileLocked();
        } finally {
          refreshLock.unlock();
        }
      }
      return current.credentials;
    }
    throwIfBackingOff(now);
    refreshLock.lock();
    try {
      return refreshWhileLocked();
    } finally {
      refreshLock.unlock();
    }
  }

  private StsCredentials refreshWhileLocked() {
    Snapshot current = snapshot;
    Instant now = clock.instant();
    if (current != null && now.isBefore(current.refreshAt)) {
      return current.credentials;
    }
    boolean previousServable = current != null && now.isBefore(current.servableUntil);
    if (!previousServable) {
      throwIfBackingOff(now);
    }
    try {
      Snapshot fresh = fetch(now);
      snapshot = fresh;
      lastFailure = null;
      return fresh.credentials;
    } catch (UnAuthorizedException e) {
      // The supplier may have run for a long time, so whether the previous credentials are still
      // servable is decided again with the clock as it is now.
      Instant failedAt = clock.instant();
      if (!previousServable || !failedAt.isBefore(current.servableUntil)) {
        lastFailure = new Failure(e, failedAt.plus(retrySpacing()));
        throw e;
      }
      snapshot = current.retryAt(failedAt.plus(retrySpacing()));
      return current.credentials;
    }
  }

  private void throwIfBackingOff(Instant now) {
    Failure failure = lastFailure;
    if (failure != null && now.isBefore(failure.retryAt)) {
      throw new UnAuthorizedException(
          "Refreshable session credentials are unavailable: the last attempt to obtain them failed"
              + " and the supplier will not be called again before "
              + failure.retryAt,
          failure.cause);
    }
  }

  private Snapshot fetch(Instant fetchedAt) {
    StsCredentials credentials;
    try {
      credentials = supplier.get();
    } catch (Exception e) {
      throw new UnAuthorizedException(
          "Failed to obtain refreshable session credentials from the supplier", e);
    }
    if (credentials == null) {
      throw new UnAuthorizedException(
          "The refreshable session credentials supplier returned null");
    }
    requireNotBlank(credentials.getAccessKeyId(), "access key id");
    requireNotBlank(credentials.getAccessKeySecret(), "access key secret");
    requireNotBlank(credentials.getSecurityToken(), "security token");
    Instant expiration = credentials.getExpiration();
    if (expiration != null) {
      // Checked against the clock after the supplier returned: a slow supplier may hand back
      // credentials that expired while it was running.
      if (!expiration.isAfter(clock.instant())) {
        throw new UnAuthorizedException(
            "The refreshable session credentials supplier returned credentials that expired at "
                + expiration);
      }
      Duration lifetime = Duration.between(fetchedAt, expiration);
      Duration untilRefresh =
          lifetime.multipliedBy(REFRESH_LIFETIME_PERCENT).dividedBy(100);
      if (refreshInterval.compareTo(untilRefresh) < 0) {
        untilRefresh = refreshInterval;
      }
      return new Snapshot(credentials, fetchedAt.plus(untilRefresh), expiration);
    }
    return new Snapshot(
        credentials,
        fetchedAt.plus(refreshInterval),
        fetchedAt.plus(refreshInterval.multipliedBy(CUTOFF_INTERVAL_MULTIPLE)));
  }

  private static void requireNotBlank(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new UnAuthorizedException(
          "The refreshable session credentials supplier returned credentials without an " + field);
    }
  }

  private Duration retrySpacing() {
    return refreshInterval.compareTo(MAX_RETRY_SPACING) < 0 ? refreshInterval : MAX_RETRY_SPACING;
  }

  /** A failed fetch made while no credentials were servable, and when to try the supplier again. */
  private static final class Failure {
    private final UnAuthorizedException cause;
    private final Instant retryAt;

    private Failure(UnAuthorizedException cause, Instant retryAt) {
      this.cause = cause;
      this.retryAt = retryAt;
    }
  }

  private static final class Snapshot {
    private final StsCredentials credentials;
    private final Instant refreshAt;
    private final Instant servableUntil;

    private Snapshot(StsCredentials credentials, Instant refreshAt, Instant servableUntil) {
      this.credentials = credentials;
      this.refreshAt = refreshAt;
      this.servableUntil = servableUntil;
    }

    /** Same credentials, next refresh attempt at {@code retryAt} but never past the cutoff. */
    private Snapshot retryAt(Instant retryAt) {
      Instant next = retryAt.isAfter(servableUntil) ? servableUntil : retryAt;
      return new Snapshot(credentials, next, servableUntil);
    }
  }
}
