package com.salesforce.multicloudj.common.circuitbreaker;

import com.salesforce.multicloudj.common.exceptions.CircuitBreakerOpenException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.time.Clock;
import java.util.function.Supplier;

/**
 * Wraps a single standalone resilience4j {@link CircuitBreaker} and runs operations through it.
 * There is no shared registry: each executor owns exactly one breaker, so its state is isolated to
 * the client instance that created it.
 *
 * <p>When the breaker is open, resilience4j rejects the call with a
 * {@link CallNotPermittedException} which is translated to the substrate-agnostic
 * {@link CircuitBreakerOpenException}. Any exception
 * the operation itself throws propagates unchanged; the breaker records it as a failure only when
 * the configured predicate matches (see {@link CircuitBreakerConfig}).
 */
public class CircuitBreakerExecutor {

  private final CircuitBreaker circuitBreaker;

  /**
   * Creates an executor backed by a new breaker built from {@code config}, timed by the system UTC
   * clock.
   *
   * @param name the breaker's name, used in resilience4j events and diagnostics
   * @param config the substrate-agnostic breaker configuration
   */
  public CircuitBreakerExecutor(String name, CircuitBreakerConfig config) {
    this(name, config, Clock.systemUTC());
  }

  /**
   * Creates an executor backed by a new breaker built from {@code config}, timed by the supplied
   * {@link Clock}. Tests inject a controllable clock to drive the sliding window and open-state
   * timing deterministically.
   */
  public CircuitBreakerExecutor(String name, CircuitBreakerConfig config, Clock clock) {
    this.circuitBreaker = CircuitBreaker.of(name, config.toResilience4jConfig(clock));
  }

  /**
   * Runs {@code operation} through the breaker.
   *
   * @param operation the operation to execute
   * @param <T> the operation's result type
   * @return the operation's result
   * @throws CircuitBreakerOpenException if the breaker is open and the call was rejected
   */
  public <T> T execute(Supplier<T> operation) {
    try {
      return circuitBreaker.executeSupplier(operation);
    } catch (CallNotPermittedException e) {
      throw new CircuitBreakerOpenException(
          "Circuit breaker '" + circuitBreaker.getName() + "' is open; call not permitted", e);
    }
  }
}
