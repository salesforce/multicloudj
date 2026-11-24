package com.salesforce.multicloudj.common.retries;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

/**
 * Runs an operation with the attempts, backoff and total timeout described by a {@link
 * RetryConfig}. It is independent of any HTTP library: the caller supplies a {@link RetryPolicy}
 * that decides which failures and results are retryable.
 *
 * <p>Without a RetryConfig, each operation runs exactly once. A RetryConfig that leaves maxAttempts
 * unset allows {@value #DEFAULT_MAX_ATTEMPTS} attempts, and one that leaves the mode unset backs
 * off exponentially from {@value #DEFAULT_INITIAL_DELAY_MILLIS} ms, doubling up to {@value
 * #DEFAULT_MAX_DELAY_MILLIS} ms. An optional jitter function adjusts exponential delays; fixed
 * delays are used as configured.
 *
 * <p>totalTimeout is checked between attempts: no retry starts once the elapsed time plus the next
 * delay reaches it, but an attempt already in flight is not interrupted. When retries run out, the
 * last result is returned or the last {@link IOException} is thrown, so the caller handles it the
 * same way as an operation that was never retried. An interrupted thread stops retrying with an
 * {@link InterruptedIOException} whose cause is the last failure, if there was one.
 */
public final class RetryExecutor {

  public static final int DEFAULT_MAX_ATTEMPTS = 3;
  public static final long DEFAULT_INITIAL_DELAY_MILLIS = 100L;
  public static final long DEFAULT_MAX_DELAY_MILLIS = 20_000L;
  public static final double DEFAULT_MULTIPLIER = 2.0;

  /** A single attempt of the operation being retried. */
  @FunctionalInterface
  public interface Attempt<T> {
    T run() throws IOException;
  }

  /** Decides which outcomes of an attempt are retried. */
  public interface RetryPolicy<T> {
    /** Returns whether an attempt that failed with {@code error} may be retried. */
    boolean isRetryable(IOException error);

    /** Returns whether an attempt that completed with {@code result} should be retried. */
    default boolean isRetryable(T result) {
      return false;
    }

    /**
     * Returns the minimum delay in milliseconds the result asks for before the next attempt, such
     * as a server's Retry-After value. It lengthens the backoff delay and never shortens it. When
     * it exceeds the longest delay the configuration allows, the result is returned without
     * another attempt, since retrying sooner than asked is likely to fail the same way. That limit
     * is maxDelayMillis in EXPONENTIAL mode, the larger of fixedDelayMillis and {@value
     * RetryExecutor#DEFAULT_MAX_DELAY_MILLIS} ms in FIXED mode, and {@value
     * RetryExecutor#DEFAULT_MAX_DELAY_MILLIS} ms when the mode is unset.
     */
    default long minDelayMillis(T result) {
      return 0L;
    }

    /** Releases a retryable result that is discarded because another attempt follows. */
    default void discard(T result) throws IOException {}
  }

  /** Pauses between attempts; replaceable so tests do not sleep. */
  @FunctionalInterface
  public interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  private final RetryConfig retryConfig;
  private final Sleeper sleeper;
  private final LongSupplier clockMillis;
  private final LongUnaryOperator jitter;

  /**
   * Creates an executor for the given retry configuration with no jitter.
   *
   * @param retryConfig the retry configuration, or null to run each operation once
   */
  public RetryExecutor(RetryConfig retryConfig) {
    this(retryConfig, LongUnaryOperator.identity());
  }

  /**
   * Creates an executor for the given retry configuration.
   *
   * @param retryConfig the retry configuration, or null to run each operation once
   * @param jitter maps each exponential delay to the delay actually waited, such as {@link
   *     #equalJitter(long)}
   */
  public RetryExecutor(RetryConfig retryConfig, LongUnaryOperator jitter) {
    this(retryConfig, Thread::sleep, () -> System.nanoTime() / 1_000_000L, jitter);
  }

  /**
   * Creates an executor with an explicit sleeper and clock and no jitter.
   *
   * @param retryConfig the retry configuration, or null to run each operation once
   * @param sleeper pauses between attempts
   * @param clockMillis a monotonic clock in milliseconds used to enforce the total timeout
   */
  public RetryExecutor(RetryConfig retryConfig, Sleeper sleeper, LongSupplier clockMillis) {
    this(retryConfig, sleeper, clockMillis, LongUnaryOperator.identity());
  }

  /**
   * Creates an executor with an explicit sleeper, clock and jitter.
   *
   * @param retryConfig the retry configuration, or null to run each operation once
   * @param sleeper pauses between attempts
   * @param clockMillis a monotonic clock in milliseconds used to enforce the total timeout
   * @param jitter maps each exponential delay to the delay actually waited
   */
  public RetryExecutor(
      RetryConfig retryConfig,
      Sleeper sleeper,
      LongSupplier clockMillis,
      LongUnaryOperator jitter) {
    this.retryConfig = retryConfig;
    this.sleeper = sleeper;
    this.clockMillis = clockMillis;
    this.jitter = jitter;
  }

  /**
   * Returns a uniformly random delay between half of the given delay and the full delay,
   * inclusive. Spreading retries this way keeps clients that failed together, such as every
   * instance hitting the same endpoint outage, from retrying in lockstep.
   *
   * @param delayMillis the computed delay
   * @return the jittered delay
   */
  public static long equalJitter(long delayMillis) {
    long half = delayMillis / 2;
    return half + ThreadLocalRandom.current().nextLong(delayMillis - half + 1);
  }

  /**
   * Runs the attempt, retrying the outcomes the policy marks retryable.
   *
   * @param attempt the operation to run
   * @param policy decides which failures and results are retried
   * @return the first result that is not retried, or the last result once retries are exhausted
   * @throws IOException the last failure, if it is not retried or retries are exhausted
   */
  public <T> T execute(Attempt<T> attempt, RetryPolicy<T> policy) throws IOException {
    if (retryConfig == null) {
      return attempt.run();
    }
    int maxAttempts =
        retryConfig.getMaxAttempts() != null ? retryConfig.getMaxAttempts() : DEFAULT_MAX_ATTEMPTS;
    long startMillis = clockMillis.getAsLong();

    for (int attemptNumber = 1; ; attemptNumber++) {
      T result;
      try {
        result = attempt.run();
      } catch (IOException e) {
        if (!policy.isRetryable(e)) {
          throw e;
        }
        long delay = backoffDelay(attemptNumber);
        if (!canRetry(attemptNumber, maxAttempts, delay, startMillis)) {
          throw e;
        }
        pause(delay, e);
        continue;
      }

      if (!policy.isRetryable(result)) {
        return result;
      }
      long minDelay = policy.minDelayMillis(result);
      if (minDelay > minDelayLimitMillis()) {
        return result;
      }
      long delay = Math.max(backoffDelay(attemptNumber), minDelay);
      if (!canRetry(attemptNumber, maxAttempts, delay, startMillis)) {
        return result;
      }
      policy.discard(result);
      pause(delay, null);
    }
  }

  /**
   * Returns the base delay before the retry that follows the given failed attempt, before jitter.
   *
   * @param failedAttempt the attempt that failed, starting at 1
   * @return the delay in milliseconds
   */
  long computeDelay(int failedAttempt) {
    if (retryConfig.getMode() == RetryConfig.Mode.FIXED) {
      return retryConfig.getFixedDelayMillis();
    }
    long initialDelay = DEFAULT_INITIAL_DELAY_MILLIS;
    double multiplier = DEFAULT_MULTIPLIER;
    if (retryConfig.getMode() == RetryConfig.Mode.EXPONENTIAL) {
      initialDelay = retryConfig.getInitialDelayMillis();
      if (retryConfig.getMultiplier() > 0) {
        multiplier = retryConfig.getMultiplier();
      }
    }
    double delay = initialDelay * Math.pow(multiplier, failedAttempt - 1);
    return (long) Math.min(maxDelayMillis(), delay);
  }

  private long backoffDelay(int failedAttempt) {
    long delay = computeDelay(failedAttempt);
    return retryConfig.getMode() == RetryConfig.Mode.FIXED ? delay : jitter.applyAsLong(delay);
  }

  private long maxDelayMillis() {
    return retryConfig.getMode() == RetryConfig.Mode.EXPONENTIAL
        ? retryConfig.getMaxDelayMillis()
        : DEFAULT_MAX_DELAY_MILLIS;
  }

  /**
   * Returns the longest minimum delay a result may ask for and still be retried. A FIXED caller
   * already waits fixedDelayMillis between attempts, so a requested delay up to that value is
   * honored even when it exceeds the default cap.
   */
  private long minDelayLimitMillis() {
    if (retryConfig.getMode() == RetryConfig.Mode.FIXED) {
      return Math.max(retryConfig.getFixedDelayMillis(), DEFAULT_MAX_DELAY_MILLIS);
    }
    return maxDelayMillis();
  }

  private boolean canRetry(int attemptNumber, int maxAttempts, long delay, long startMillis) {
    if (attemptNumber >= maxAttempts) {
      return false;
    }
    Long totalTimeout = retryConfig.getTotalTimeout();
    if (totalTimeout == null) {
      return true;
    }
    // Compares remaining time instead of computing start + totalTimeout, which overflows for
    // very large timeouts such as Long.MAX_VALUE and would disable retries entirely.
    long elapsed = clockMillis.getAsLong() - startMillis;
    return delay < totalTimeout - elapsed;
  }

  private void pause(long millis, IOException lastError) throws InterruptedIOException {
    if (Thread.currentThread().isInterrupted()) {
      throw interrupted("Interrupted before retrying", lastError);
    }
    if (millis <= 0) {
      return;
    }
    try {
      sleeper.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw interrupted("Interrupted while waiting to retry", lastError);
    }
  }

  private static InterruptedIOException interrupted(String message, IOException lastError) {
    InterruptedIOException interrupted = new InterruptedIOException(message);
    interrupted.initCause(lastError);
    return interrupted;
  }
}
