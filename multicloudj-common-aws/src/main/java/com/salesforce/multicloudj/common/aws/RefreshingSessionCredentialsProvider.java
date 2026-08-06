package com.salesforce.multicloudj.common.aws;

import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.utils.SdkAutoCloseable;
import software.amazon.awssdk.utils.cache.CachedSupplier;
import software.amazon.awssdk.utils.cache.RefreshResult;

/**
 * An {@link AwsCredentialsProvider} that renews caller-supplied session credentials in place, so
 * that a client kept for the lifetime of a JVM keeps working after the credentials it was built
 * with expire.
 *
 * <p>Credentials are held in a {@link CachedSupplier}, the same primitive the AWS SDK's own
 * refreshable credentials providers are built on. It gives two renewal tiers: at the prefetch time
 * a single caller renews while every other caller keeps using the credentials already cached, and
 * at the stale time all callers block on one renewal. Renewal is serialised on a lock that callers
 * wait five seconds for and then proceed without, so a supplier that can take longer than five
 * seconds may be invoked concurrently. Suppliers should return well inside that window.
 *
 * <p>Renewal is scheduled from {@link StsCredentials#getExpiration()}. When the supplier declares
 * no expiration there is nothing to schedule against, so credentials are renewed every {@link
 * #DEFAULT_REFRESH_INTERVAL} instead. Callers that can declare an expiration should do so, because
 * interval-based renewal cannot guarantee that a renewal happens before the credentials lapse.
 *
 * <p>Suppliers are invoked no more often than {@link #DEFAULT_MINIMUM_REFRESH_INTERVAL}, which
 * bounds the load placed on a supplier that keeps returning credentials that have already expired
 * or that expire sooner than the renewal windows above.
 *
 * <p>{@link #invalidate()} covers credentials the service rejects before the expiration the
 * supplier reported. Nothing invokes it on its own; it is driven by {@link
 * ExpiredCredentialsInterceptor}, which only the blob clients register. Every other service built
 * on this provider gets expiration-driven renewal but no failure-driven invalidation.
 *
 * <p>This class is thread-safe.
 */
public class RefreshingSessionCredentialsProvider
    implements AwsCredentialsProvider, SdkAutoCloseable {

  /** How long before expiry all callers block on a renewal. */
  public static final Duration DEFAULT_STALE_TIME = Duration.ofMinutes(1);

  /** How long before expiry a single caller renews on behalf of the others. */
  public static final Duration DEFAULT_PREFETCH_TIME = Duration.ofMinutes(5);

  /** How often credentials are renewed when the supplier declares no expiration. */
  public static final Duration DEFAULT_REFRESH_INTERVAL = Duration.ofMinutes(15);

  /** The shortest interval permitted between two supplier invocations. */
  public static final Duration DEFAULT_MINIMUM_REFRESH_INTERVAL = Duration.ofSeconds(1);

  private static final String CACHED_VALUE_NAME = "multicloudj-session-credentials";

  private final Supplier<StsCredentials> credentialsSupplier;
  private final Duration staleTime;
  private final Duration prefetchTime;
  private final Duration refreshInterval;
  private final Duration minimumRefreshInterval;
  private final AtomicReference<CachedSupplier<AwsSessionCredentials>> cache;
  private final AtomicReference<Instant> lastInvalidation = new AtomicReference<>(Instant.MIN);

  /**
   * Creates a provider over {@code credentialsSupplier} using the default renewal windows.
   *
   * @param credentialsSupplier callback returning currently valid session credentials
   */
  public RefreshingSessionCredentialsProvider(Supplier<StsCredentials> credentialsSupplier) {
    this(builder().credentialsSupplier(credentialsSupplier));
  }

  private RefreshingSessionCredentialsProvider(Builder builder) {
    this.credentialsSupplier =
        Objects.requireNonNull(builder.credentialsSupplier, "credentialsSupplier must not be null");
    this.staleTime = builder.staleTime;
    this.prefetchTime = builder.prefetchTime;
    this.refreshInterval = builder.refreshInterval;
    this.minimumRefreshInterval = builder.minimumRefreshInterval;
    this.cache = new AtomicReference<>(newCache());
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return cache.get().get();
  }

  /**
   * Discards the cached credentials so that the next {@link #resolveCredentials()} invokes the
   * supplier again.
   *
   * <p>Renewal windows are derived from what the supplier reports, so credentials that the service
   * rejects can still look fresh to this provider. This is the escape hatch for that case.
   *
   * <p>Discarded credentials are unconditionally stale, which bypasses the renewal windows
   * entirely. Invalidation is therefore honoured at most once per {@code minimumRefreshInterval};
   * without that bound, a credential source that keeps handing back credentials the service
   * rejects would be invoked once per failed request.
   */
  public void invalidate() {
    Instant now = Instant.now();
    Instant previous = lastInvalidation.get();
    if (Duration.between(previous, now).compareTo(minimumRefreshInterval) < 0
        || !lastInvalidation.compareAndSet(previous, now)) {
      return;
    }
    CachedSupplier<AwsSessionCredentials> discarded = cache.get();
    CachedSupplier<AwsSessionCredentials> replacement = newCache();
    if (cache.compareAndSet(discarded, replacement)) {
      discarded.close();
    } else {
      replacement.close();
    }
  }

  /**
   * Releases the resources held by the cached credentials. With the default configuration renewal
   * happens on the calling thread and there is nothing to release, so this neither stops renewal
   * nor makes the provider unusable: a later {@link #resolveCredentials()} invokes the supplier
   * again.
   */
  @Override
  public void close() {
    cache.get().close();
  }

  @Override
  public String toString() {
    return "RefreshingSessionCredentialsProvider";
  }

  private CachedSupplier<AwsSessionCredentials> newCache() {
    return CachedSupplier.builder(this::refresh).cachedValueName(CACHED_VALUE_NAME).build();
  }

  private RefreshResult<AwsSessionCredentials> refresh() {
    StsCredentials credentials = credentialsSupplier.get();
    if (credentials == null) {
      throw new IllegalStateException(
          "Session credentials supplier returned null; it must return valid session credentials.");
    }

    AwsSessionCredentials.Builder resolved =
        AwsSessionCredentials.builder()
            .accessKeyId(credentials.getAccessKeyId())
            .secretAccessKey(credentials.getAccessKeySecret())
            .sessionToken(credentials.getSecurityToken());

    Instant expiration = credentials.getExpiration();
    if (expiration != null) {
      resolved.expirationTime(expiration);
    }

    Instant now = Instant.now();
    Instant earliestRenewal = now.plus(minimumRefreshInterval);
    Instant staleAt;
    Instant prefetchAt;
    if (expiration == null) {
      staleAt = now.plus(refreshInterval);
      prefetchAt = staleAt;
    } else {
      staleAt = expiration.minus(staleTime);
      prefetchAt = expiration.minus(prefetchTime);
      if (staleAt.isBefore(earliestRenewal)) {
        staleAt = earliestRenewal;
      }
      if (prefetchAt.isBefore(earliestRenewal)) {
        // Renewal windows that fall in the past would make every caller renew; deferring the
        // prefetch to the stale boundary keeps the supplier invocation rate bounded.
        prefetchAt = staleAt;
      }
    }

    return RefreshResult.builder(resolved.build())
        .staleTime(staleAt)
        .prefetchTime(prefetchAt)
        .build();
  }

  public static class Builder {
    private Supplier<StsCredentials> credentialsSupplier;
    private Duration staleTime = DEFAULT_STALE_TIME;
    private Duration prefetchTime = DEFAULT_PREFETCH_TIME;
    private Duration refreshInterval = DEFAULT_REFRESH_INTERVAL;
    private Duration minimumRefreshInterval = DEFAULT_MINIMUM_REFRESH_INTERVAL;

    public Builder credentialsSupplier(Supplier<StsCredentials> credentialsSupplier) {
      this.credentialsSupplier = credentialsSupplier;
      return this;
    }

    public Builder staleTime(Duration staleTime) {
      this.staleTime = staleTime;
      return this;
    }

    public Builder prefetchTime(Duration prefetchTime) {
      this.prefetchTime = prefetchTime;
      return this;
    }

    public Builder refreshInterval(Duration refreshInterval) {
      this.refreshInterval = refreshInterval;
      return this;
    }

    public Builder minimumRefreshInterval(Duration minimumRefreshInterval) {
      this.minimumRefreshInterval = minimumRefreshInterval;
      return this;
    }

    public RefreshingSessionCredentialsProvider build() {
      requireNonNegative(staleTime, "staleTime");
      requireNonNegative(prefetchTime, "prefetchTime");
      requireNonNegative(refreshInterval, "refreshInterval");
      requireNonNegative(minimumRefreshInterval, "minimumRefreshInterval");
      // A prefetch window inside the stale window puts the prefetch instant after the stale
      // instant, which retires the single-caller renewal tier without saying so.
      if (prefetchTime.compareTo(staleTime) < 0) {
        throw new IllegalArgumentException(
            "prefetchTime must not be shorter than staleTime, but prefetchTime="
                + prefetchTime
                + " and staleTime="
                + staleTime);
      }
      return new RefreshingSessionCredentialsProvider(this);
    }

    private static void requireNonNegative(Duration value, String name) {
      if (value == null) {
        throw new IllegalArgumentException(name + " must not be null");
      }
      if (value.isNegative()) {
        throw new IllegalArgumentException(name + " must not be negative, but was " + value);
      }
    }
  }
}
