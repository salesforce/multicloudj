package com.salesforce.multicloudj.common.circuitbreaker;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType;
import java.time.Clock;
import java.time.Duration;

/**
 * Substrate-agnostic configuration for a {@link CircuitBreakerExecutor}. This is the only
 * circuit-breaker type a caller ever constructs; the underlying resilience4j configuration is an
 * implementation detail produced by {@link #toResilience4jConfig(Clock)}.
 *
 * <p>The breaker uses a time-based sliding window and counts only <em>retryable</em>
 * {@link SubstrateSdkException}s as failures: a retryable failure signals the dependency itself is
 * unhealthy, whereas a non-retryable failure (e.g. an invalid argument) is a caller error that must
 * not trip the breaker.
 *
 * <p>The defaults mirror resilience4j's own defaults adapted to a time-based window and are
 * deliberately conservative. Recommended production values for a high-throughput STS workload — for
 * reference, not defaults — are roughly: {@code failureRateThreshold=30}, {@code
 * slowCallRateThreshold=10}, {@code slowCallDurationThreshold=120s}, {@code
 * minimumNumberOfCalls=100}, {@code slidingWindowSize=600s}, {@code waitDurationInOpenState=1s},
 * {@code permittedNumberOfCallsInHalfOpenState=200}. Tune these to your call volume and latency
 * profile.
 */
public class CircuitBreakerConfig {

  private final float failureRateThreshold;
  private final float slowCallRateThreshold;
  private final Duration slowCallDurationThreshold;
  private final int slidingWindowSize;
  private final int minimumNumberOfCalls;
  private final Duration waitDurationInOpenState;
  private final int permittedNumberOfCallsInHalfOpenState;

  private CircuitBreakerConfig(Builder builder) {
    this.failureRateThreshold = builder.failureRateThreshold;
    this.slowCallRateThreshold = builder.slowCallRateThreshold;
    this.slowCallDurationThreshold = builder.slowCallDurationThreshold;
    this.slidingWindowSize = builder.slidingWindowSize;
    this.minimumNumberOfCalls = builder.minimumNumberOfCalls;
    this.waitDurationInOpenState = builder.waitDurationInOpenState;
    this.permittedNumberOfCallsInHalfOpenState = builder.permittedNumberOfCallsInHalfOpenState;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Translates this configuration into a resilience4j {@code CircuitBreakerConfig}. The supplied
   * {@link Clock} drives the time-based sliding window and open-state timing, so tests can inject a
   * controllable clock. Package-private: only {@link CircuitBreakerExecutor} needs it.
   */
  io.github.resilience4j.circuitbreaker.CircuitBreakerConfig toResilience4jConfig(Clock clock) {
    return io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.custom()
        .failureRateThreshold(failureRateThreshold)
        .slowCallRateThreshold(slowCallRateThreshold)
        .slowCallDurationThreshold(slowCallDurationThreshold)
        .slidingWindow(slidingWindowSize, minimumNumberOfCalls, SlidingWindowType.TIME_BASED)
        .waitDurationInOpenState(waitDurationInOpenState)
        .permittedNumberOfCallsInHalfOpenState(permittedNumberOfCallsInHalfOpenState)
        // Only retryable failures (unhealthy dependency) count toward opening the breaker; caller
        // errors are treated as successful calls so they never trip it.
        .recordException(
            t ->
                t instanceof SubstrateSdkException && ((SubstrateSdkException) t).isRetryable())
        .clock(clock)
        .build();
  }

  /** Builder for {@link CircuitBreakerConfig}. */
  public static class Builder {
    private float failureRateThreshold = 50f;
    private float slowCallRateThreshold = 100f;
    private Duration slowCallDurationThreshold = Duration.ofSeconds(60);
    private int slidingWindowSize = 60;
    private int minimumNumberOfCalls = 100;
    private Duration waitDurationInOpenState = Duration.ofSeconds(60);
    private int permittedNumberOfCallsInHalfOpenState = 10;

    /** Percentage (0–100) of recorded failures at or above which the breaker opens. */
    public Builder withFailureRateThreshold(float failureRateThreshold) {
      this.failureRateThreshold = failureRateThreshold;
      return this;
    }

    /** Percentage (0–100) of slow calls at or above which the breaker opens. */
    public Builder withSlowCallRateThreshold(float slowCallRateThreshold) {
      this.slowCallRateThreshold = slowCallRateThreshold;
      return this;
    }

    /** A call taking at least this long is counted as slow. */
    public Builder withSlowCallDurationThreshold(Duration slowCallDurationThreshold) {
      this.slowCallDurationThreshold = slowCallDurationThreshold;
      return this;
    }

    /** Size of the time-based sliding window, in seconds. */
    public Builder withSlidingWindowSize(int slidingWindowSize) {
      this.slidingWindowSize = slidingWindowSize;
      return this;
    }

    /** Minimum number of recorded calls before the failure/slow rate is evaluated. */
    public Builder withMinimumNumberOfCalls(int minimumNumberOfCalls) {
      this.minimumNumberOfCalls = minimumNumberOfCalls;
      return this;
    }

    /** How long the breaker stays open before transitioning to half-open. */
    public Builder withWaitDurationInOpenState(Duration waitDurationInOpenState) {
      this.waitDurationInOpenState = waitDurationInOpenState;
      return this;
    }

    /** Number of trial calls permitted while the breaker is half-open. */
    public Builder withPermittedNumberOfCallsInHalfOpenState(
        int permittedNumberOfCallsInHalfOpenState) {
      this.permittedNumberOfCallsInHalfOpenState = permittedNumberOfCallsInHalfOpenState;
      return this;
    }

    public CircuitBreakerConfig build() {
      return new CircuitBreakerConfig(this);
    }
  }
}
