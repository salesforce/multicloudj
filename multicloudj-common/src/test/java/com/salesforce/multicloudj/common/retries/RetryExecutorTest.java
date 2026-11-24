package com.salesforce.multicloudj.common.retries;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RetryExecutorTest {

  private static final RetryConfig FIXED_CONFIG =
      RetryConfig.builder()
          .mode(RetryConfig.Mode.FIXED)
          .maxAttempts(3)
          .fixedDelayMillis(50L)
          .build();

  /** Retries every I/O failure and every result other than "ok". */
  private final class TestPolicy implements RetryExecutor.RetryPolicy<String> {
    private final boolean retryErrors;
    private final long minDelayMillis;
    private final long discardMillis;
    private final List<String> discarded = new ArrayList<>();

    TestPolicy(boolean retryErrors, long minDelayMillis) {
      this(retryErrors, minDelayMillis, 0L);
    }

    /** Advances the clock by discardMillis on each discard, like a slow release. */
    TestPolicy(boolean retryErrors, long minDelayMillis, long discardMillis) {
      this.retryErrors = retryErrors;
      this.minDelayMillis = minDelayMillis;
      this.discardMillis = discardMillis;
    }

    @Override
    public boolean isRetryable(IOException error) {
      return retryErrors;
    }

    @Override
    public boolean isRetryable(String result) {
      return !"ok".equals(result);
    }

    @Override
    public long minDelayMillis(String result) {
      return minDelayMillis;
    }

    @Override
    public void discard(String result) {
      discarded.add(result);
      now.addAndGet(discardMillis);
    }
  }

  private AtomicLong now;
  private List<Long> sleeps;
  private AtomicInteger attempts;

  @BeforeEach
  void setUp() {
    now = new AtomicLong(1_000L);
    sleeps = new ArrayList<>();
    attempts = new AtomicInteger();
  }

  private RetryExecutor executor(RetryConfig retryConfig) {
    return new RetryExecutor(
        retryConfig,
        millis -> {
          sleeps.add(millis);
          now.addAndGet(millis);
        },
        now::get);
  }

  /** An attempt that returns the given outcomes in order; an IOException outcome is thrown. */
  private RetryExecutor.Attempt<String> attempt(Object... outcomes) {
    Deque<Object> remaining = new ArrayDeque<>(List.of(outcomes));
    return () -> {
      attempts.incrementAndGet();
      Object outcome = remaining.size() > 1 ? remaining.poll() : remaining.peek();
      if (outcome instanceof IOException) {
        throw (IOException) outcome;
      }
      return (String) outcome;
    };
  }

  @Test
  void testNullRetryConfig_RunsOnce() throws Exception {
    TestPolicy policy = new TestPolicy(true, 0L);

    assertEquals("busy", executor(null).execute(attempt("busy", "ok"), policy));
    assertEquals(1, attempts.get());
    assertTrue(sleeps.isEmpty());
  }

  @Test
  void testRetryableError_RetriesUntilSuccess() throws Exception {
    assertEquals(
        "ok",
        executor(FIXED_CONFIG)
            .execute(attempt(new IOException("reset"), "ok"), new TestPolicy(true, 0L)));
    assertEquals(2, attempts.get());
    assertEquals(List.of(50L), sleeps);
  }

  @Test
  void testNonRetryableError_ThrowsWithoutRetry() {
    IOException error = new IOException("denied");

    IOException thrown =
        assertThrows(
            IOException.class,
            () -> executor(FIXED_CONFIG).execute(attempt(error, "ok"), new TestPolicy(false, 0L)));
    assertSame(error, thrown);
    assertEquals(1, attempts.get());
  }

  @Test
  void testRetryableError_ThrowsLastErrorWhenAttemptsExhausted() {
    IOException last = new IOException("last");

    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                executor(FIXED_CONFIG)
                    .execute(
                        attempt(new IOException("first"), new IOException("second"), last),
                        new TestPolicy(true, 0L)));
    assertSame(last, thrown);
    assertEquals(3, attempts.get());
  }

  @Test
  void testRetryableResult_DiscardsAndRetriesUntilAccepted() throws Exception {
    TestPolicy policy = new TestPolicy(true, 0L);

    assertEquals("ok", executor(FIXED_CONFIG).execute(attempt("busy", "ok"), policy));
    assertEquals(List.of("busy"), policy.discarded);
    assertEquals(List.of(50L), sleeps);
  }

  @Test
  void testRetryableResult_ReturnsLastResultWhenAttemptsExhausted() throws Exception {
    TestPolicy policy = new TestPolicy(true, 0L);

    assertEquals(
        "busy-3",
        executor(FIXED_CONFIG).execute(attempt("busy-1", "busy-2", "busy-3"), policy));
    assertEquals(3, attempts.get());
    assertEquals(List.of("busy-1", "busy-2"), policy.discarded);
  }

  @Test
  void testUnsetMaxAttempts_UsesDefault() {
    RetryConfig config =
        RetryConfig.builder().mode(RetryConfig.Mode.FIXED).fixedDelayMillis(10L).build();

    assertThrows(
        IOException.class,
        () ->
            executor(config).execute(attempt(new IOException("reset")), new TestPolicy(true, 0L)));
    assertEquals(RetryExecutor.DEFAULT_MAX_ATTEMPTS, attempts.get());
  }

  @Test
  void testTotalTimeout_StopsRetryingBeforeDeadline() {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.FIXED)
            .maxAttempts(10)
            .fixedDelayMillis(100L)
            .totalTimeout(250L)
            .build();

    assertThrows(
        IOException.class,
        () ->
            executor(config).execute(attempt(new IOException("reset")), new TestPolicy(true, 0L)));
    // Retries at t=100 and t=200; a third retry at t=300 would pass the 250 ms deadline.
    assertEquals(3, attempts.get());
    assertEquals(List.of(100L, 100L), sleeps);
  }

  @Test
  void testTotalTimeout_ReachedWhileDiscarding_ThrowsWithoutAnotherAttempt() {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.FIXED)
            .maxAttempts(3)
            .fixedDelayMillis(50L)
            .totalTimeout(200L)
            .build();
    TestPolicy policy = new TestPolicy(true, 0L, 500L);

    assertThrows(
        InterruptedIOException.class,
        () -> executor(config).execute(attempt("busy", "ok"), policy));
    assertEquals(1, attempts.get());
    assertEquals(List.of("busy"), policy.discarded);
    assertTrue(sleeps.isEmpty());
  }

  @Test
  void testTotalTimeout_ReachedWhilePausing_ThrowsLastErrorWithoutAnotherAttempt() {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.FIXED)
            .maxAttempts(3)
            .fixedDelayMillis(50L)
            .totalTimeout(200L)
            .build();
    IOException error = new IOException("reset");
    RetryExecutor oversleeping =
        new RetryExecutor(
            config,
            millis -> {
              sleeps.add(millis);
              now.addAndGet(millis + 500L);
            },
            now::get);

    IOException thrown =
        assertThrows(
            IOException.class,
            () -> oversleeping.execute(attempt(error, "ok"), new TestPolicy(true, 0L)));
    assertSame(error, thrown);
    assertEquals(1, attempts.get());
    assertEquals(List.of(50L), sleeps);
  }

  @Test
  void testTotalTimeout_ReachedWhilePausingAfterDiscard_ThrowsWithoutAnotherAttempt() {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.FIXED)
            .maxAttempts(3)
            .fixedDelayMillis(50L)
            .totalTimeout(200L)
            .build();
    RetryExecutor oversleeping =
        new RetryExecutor(config, millis -> now.addAndGet(millis + 500L), now::get);

    assertThrows(
        InterruptedIOException.class,
        () -> oversleeping.execute(attempt("busy", "ok"), new TestPolicy(true, 0L)));
    assertEquals(1, attempts.get());
  }

  @Test
  void testMaximumTotalTimeout_StillRetries() throws Exception {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.FIXED)
            .maxAttempts(3)
            .fixedDelayMillis(50L)
            .totalTimeout(Long.MAX_VALUE)
            .build();

    String result =
        executor(config).execute(attempt("busy", "busy", "ok"), new TestPolicy(true, 0L));

    assertEquals("ok", result);
    assertEquals(3, attempts.get());
    assertEquals(List.of(50L, 50L), sleeps);
  }

  @Test
  void testMinDelay_LengthensBaseDelay() throws Exception {
    executor(FIXED_CONFIG).execute(attempt("busy", "ok"), new TestPolicy(true, 2_000L));

    assertEquals(List.of(2_000L), sleeps);
  }

  @Test
  void testMinDelay_NeverShortensBaseDelay() throws Exception {
    executor(FIXED_CONFIG).execute(attempt("busy", "ok"), new TestPolicy(true, 10L));

    assertEquals(List.of(50L), sleeps);
  }

  @Test
  void testMinDelayAboveMaxDelay_ReturnsResultWithoutRetry() throws Exception {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .maxAttempts(2)
            .initialDelayMillis(100L)
            .maxDelayMillis(1_500L)
            .build();
    TestPolicy policy = new TestPolicy(true, 60_000L);

    assertEquals("busy", executor(config).execute(attempt("busy", "ok"), policy));
    assertEquals(1, attempts.get());
    assertTrue(sleeps.isEmpty());
    assertTrue(policy.discarded.isEmpty());
  }

  @Test
  void testMinDelayEqualToMaxDelay_Retries() throws Exception {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .maxAttempts(2)
            .initialDelayMillis(100L)
            .maxDelayMillis(1_500L)
            .build();

    executor(config).execute(attempt("busy", "ok"), new TestPolicy(true, 1_500L));

    assertEquals(List.of(1_500L), sleeps);
  }

  @Test
  void testFixedMode_MinDelayWithinFixedDelayAboveDefaultCap_Retries() throws Exception {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.FIXED)
            .maxAttempts(2)
            .fixedDelayMillis(30_000L)
            .build();

    String result =
        executor(config).execute(attempt("busy", "ok"), new TestPolicy(true, 25_000L));

    assertEquals("ok", result);
    assertEquals(List.of(30_000L), sleeps);
  }

  @Test
  void testFixedMode_MinDelayAboveFixedDelayAndDefaultCap_ReturnsResultWithoutRetry()
      throws Exception {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.FIXED)
            .maxAttempts(2)
            .fixedDelayMillis(30_000L)
            .build();

    assertEquals(
        "busy", executor(config).execute(attempt("busy", "ok"), new TestPolicy(true, 30_001L)));
    assertEquals(1, attempts.get());
    assertTrue(sleeps.isEmpty());
  }

  @Test
  void testExponentialDelay_GrowsByMultiplierAndIsCapped() {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .initialDelayMillis(100L)
            .multiplier(3.0)
            .maxDelayMillis(1_000L)
            .build();
    RetryExecutor executor = executor(config);

    assertEquals(100L, executor.computeDelay(1));
    assertEquals(300L, executor.computeDelay(2));
    assertEquals(900L, executor.computeDelay(3));
    assertEquals(1_000L, executor.computeDelay(4));
  }

  @Test
  void testExponentialDelay_UnsetMultiplierDefaultsToDoubling() {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .initialDelayMillis(100L)
            .maxDelayMillis(10_000L)
            .build();
    RetryExecutor executor = executor(config);

    assertEquals(100L, executor.computeDelay(1));
    assertEquals(200L, executor.computeDelay(2));
    assertEquals(400L, executor.computeDelay(3));
  }

  @Test
  void testUnsetMode_UsesDefaultExponentialBackoff() {
    RetryExecutor executor = executor(RetryConfig.builder().maxAttempts(5).build());

    assertEquals(RetryExecutor.DEFAULT_INITIAL_DELAY_MILLIS, executor.computeDelay(1));
    assertEquals(RetryExecutor.DEFAULT_INITIAL_DELAY_MILLIS * 2, executor.computeDelay(2));
    assertEquals(RetryExecutor.DEFAULT_MAX_DELAY_MILLIS, executor.computeDelay(30));
  }

  @Test
  void testFixedDelay_IsConstant() {
    RetryExecutor executor = executor(FIXED_CONFIG);

    assertEquals(50L, executor.computeDelay(1));
    assertEquals(50L, executor.computeDelay(5));
  }

  @Test
  void testInterruptedWhileWaiting_ThrowsWithLastErrorAsCause() {
    IOException error = new IOException("reset");
    RetryExecutor executor =
        new RetryExecutor(
            FIXED_CONFIG,
            millis -> {
              throw new InterruptedException();
            },
            now::get);

    try {
      InterruptedIOException thrown =
          assertThrows(
              InterruptedIOException.class,
              () -> executor.execute(attempt(error), new TestPolicy(true, 0L)));
      assertSame(error, thrown.getCause());
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
    assertEquals(1, attempts.get());
  }

  @Test
  void testInterruptedBeforeRetry_ThrowsWithoutSleeping() {
    IOException error = new IOException("reset");

    Thread.currentThread().interrupt();
    try {
      InterruptedIOException thrown =
          assertThrows(
              InterruptedIOException.class,
              () -> executor(FIXED_CONFIG).execute(attempt(error), new TestPolicy(true, 0L)));
      assertSame(error, thrown.getCause());
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
    assertEquals(1, attempts.get());
    assertTrue(sleeps.isEmpty());
  }

  @Test
  void testZeroDelay_RetriesWithoutSleeping() throws Exception {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.FIXED)
            .maxAttempts(2)
            .fixedDelayMillis(0L)
            .build();

    String result =
        executor(config).execute(attempt(new IOException("reset"), "ok"), new TestPolicy(true, 0L));

    assertEquals("ok", result);
    assertEquals(2, attempts.get());
    assertTrue(sleeps.isEmpty());
  }

  @Test
  void testJitter_AppliesToExponentialDelays() throws Exception {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .maxAttempts(3)
            .initialDelayMillis(100L)
            .maxDelayMillis(1_000L)
            .build();
    RetryExecutor executor =
        new RetryExecutor(config, sleeps::add, now::get, delay -> delay / 2);

    executor.execute(attempt("busy", "busy", "ok"), new TestPolicy(true, 0L));

    assertEquals(List.of(50L, 100L), sleeps);
  }

  @Test
  void testJitter_DoesNotApplyToFixedDelays() throws Exception {
    RetryExecutor executor =
        new RetryExecutor(FIXED_CONFIG, sleeps::add, now::get, delay -> delay / 2);

    executor.execute(attempt(new IOException("reset"), "ok"), new TestPolicy(true, 0L));

    assertEquals(List.of(50L), sleeps);
  }

  @Test
  void testEqualJitter_StaysBetweenHalfAndFullDelay() {
    Set<Long> delays = new HashSet<>();
    for (int i = 0; i < 200; i++) {
      long delay = RetryExecutor.equalJitter(1_000L);
      assertTrue(delay >= 500L && delay <= 1_000L, "delay was " + delay);
      delays.add(delay);
    }
    assertTrue(delays.size() > 1, "delays were not jittered: " + delays);
    assertEquals(0L, RetryExecutor.equalJitter(0L));
  }
}
