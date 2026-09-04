package com.salesforce.multicloudj.common.circuitbreaker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.multicloudj.common.exceptions.CircuitBreakerOpenException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * Drives the breaker through its full lifecycle using an injected {@link Clock}, so every
 * open/half-open/close transition is deterministic and there are no sleeps.
 */
class CircuitBreakerExecutorTest {

  /** Retryable failure — the only kind the breaker is configured to count. */
  private static RuntimeException retryable() {
    return new ResourceExhaustedException("dependency unhealthy");
  }

  /** Non-retryable caller error — must never trip the breaker. */
  private static RuntimeException nonRetryable() {
    return new InvalidArgumentException("bad input");
  }

  /**
   * Small, deterministic config: 5 calls minimum, opens at a 50% failure rate, a 60s window keeps
   * every call in the same time bucket, 10s open duration, 3 trial calls while half-open.
   */
  private static CircuitBreakerConfig config() {
    return CircuitBreakerConfig.builder()
        .withFailureRateThreshold(50f)
        .withSlowCallRateThreshold(100f)
        .withSlowCallDurationThreshold(Duration.ofHours(1))
        .withSlidingWindowSize(60)
        .withMinimumNumberOfCalls(5)
        .withWaitDurationInOpenState(Duration.ofSeconds(10))
        .withPermittedNumberOfCallsInHalfOpenState(3)
        .build();
  }

  private static void driveFailures(CircuitBreakerExecutor executor, int count, Supplier<?> op) {
    for (int i = 0; i < count; i++) {
      assertThrows(RuntimeException.class, () -> executor.execute(op));
    }
  }

  @Test
  void closedBreaker_returnsOperationResult() {
    CircuitBreakerExecutor executor = new CircuitBreakerExecutor("t", config(), new MutableClock());
    assertEquals("ok", executor.execute(() -> "ok"));
  }

  @Test
  void retryableFailures_openTheBreaker() {
    CircuitBreakerExecutor executor = new CircuitBreakerExecutor("t", config(), new MutableClock());

    driveFailures(
        executor,
        5,
        () -> {
          throw retryable();
        });

    // Breaker is now open: the next call is rejected without being attempted.
    assertThrows(
        CircuitBreakerOpenException.class,
        () -> executor.execute(() -> "should not run"));
  }

  @Test
  void openBreaker_doesNotInvokeOperation() {
    CircuitBreakerExecutor executor = new CircuitBreakerExecutor("t", config(), new MutableClock());
    AtomicInteger invocations = new AtomicInteger();

    driveFailures(
        executor,
        5,
        () -> {
          invocations.incrementAndGet();
          throw retryable();
        });
    assertEquals(5, invocations.get());

    assertThrows(
        CircuitBreakerOpenException.class,
        () -> executor.execute(() -> invocations.incrementAndGet()));
    // Still 5: the guarded supplier never ran while the breaker was open.
    assertEquals(5, invocations.get());
  }

  @Test
  void nonRetryableFailures_doNotOpenBreaker() {
    CircuitBreakerExecutor executor = new CircuitBreakerExecutor("t", config(), new MutableClock());
    AtomicInteger invocations = new AtomicInteger();

    // Far more than minimumNumberOfCalls, all non-retryable: the breaker treats them as successful
    // calls and stays closed, so every call reaches the operation and the original exception
    // propagates unchanged.
    for (int i = 0; i < 20; i++) {
      InvalidArgumentException thrown =
          assertThrows(
              InvalidArgumentException.class,
              () ->
                  executor.execute(
                      () -> {
                        invocations.incrementAndGet();
                        throw nonRetryable();
                      }));
      assertFalse(thrown.isRetryable());
    }
    assertEquals(20, invocations.get());
  }

  @Test
  void openException_isNonRetryable() {
    CircuitBreakerExecutor executor = new CircuitBreakerExecutor("t", config(), new MutableClock());
    driveFailures(
        executor,
        5,
        () -> {
          throw retryable();
        });

    CircuitBreakerOpenException open =
        assertThrows(
            CircuitBreakerOpenException.class, () -> executor.execute(() -> "should not run"));
    assertFalse(open.isRetryable());
  }

  @Test
  void halfOpen_recoversAndClosesAfterSuccesses() {
    MutableClock clock = new MutableClock();
    CircuitBreakerExecutor executor = new CircuitBreakerExecutor("t", config(), clock);
    driveFailures(
        executor,
        5,
        () -> {
          throw retryable();
        });

    // Wait past the open duration; the next calls are permitted as half-open trials.
    clock.advance(Duration.ofSeconds(11));

    // permittedNumberOfCallsInHalfOpenState = 3 successes close the breaker.
    for (int i = 0; i < 3; i++) {
      assertEquals("ok", executor.execute(() -> "ok"));
    }

    // Closed again: normal calls flow through.
    assertEquals("recovered", executor.execute(() -> "recovered"));
  }

  @Test
  void halfOpen_reopensOnFailure() {
    MutableClock clock = new MutableClock();
    CircuitBreakerExecutor executor = new CircuitBreakerExecutor("t", config(), clock);
    driveFailures(
        executor,
        5,
        () -> {
          throw retryable();
        });

    clock.advance(Duration.ofSeconds(11));

    // Three half-open trial calls, all failing → 100% failure rate reopens the breaker.
    driveFailures(
        executor,
        3,
        () -> {
          throw retryable();
        });

    assertThrows(
        CircuitBreakerOpenException.class, () -> executor.execute(() -> "should not run"));
  }

  /** A {@link Clock} whose instant only moves when the test advances it. */
  private static final class MutableClock extends Clock {
    private Instant instant = Instant.parse("2024-01-01T00:00:00Z");

    void advance(Duration duration) {
      this.instant = this.instant.plus(duration);
    }

    @Override
    public Instant instant() {
      return instant;
    }

    @Override
    public long millis() {
      return instant.toEpochMilli();
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }
  }
}
