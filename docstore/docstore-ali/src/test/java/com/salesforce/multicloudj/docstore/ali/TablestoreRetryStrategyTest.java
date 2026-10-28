package com.salesforce.multicloudj.docstore.ali;

import static com.salesforce.multicloudj.docstore.ali.TablestoreRetryStrategy.totalTimeoutMillis;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import org.junit.jupiter.api.Test;

class TablestoreRetryStrategyTest {

  private static final TableStoreException SERVER_BUSY =
      new TableStoreException("busy", null, "OTSServerBusy", "request-id", 503);

  private static long nextPause(RetryStrategy strategy) {
    return strategy.nextPause(OperationNames.OP_GET_ROW, SERVER_BUSY);
  }

  @Test
  void stopsRetryingAtMaxAttempts() {
    RetryStrategy strategy =
        new TablestoreRetryStrategy(
            RetryConfig.builder()
                .maxAttempts(3)
                .mode(RetryConfig.Mode.FIXED)
                .fixedDelayMillis(10)
                .build());

    assertEquals(10, nextPause(strategy));
    assertEquals(10, nextPause(strategy));
    assertEquals(0, nextPause(strategy));
    assertEquals(2, strategy.getRetries());
  }

  @Test
  void defaultsToThreeAttemptsWhenMaxAttemptsIsUnset() {
    RetryStrategy strategy =
        new TablestoreRetryStrategy(
            RetryConfig.builder().mode(RetryConfig.Mode.FIXED).fixedDelayMillis(10).build());

    for (int i = 1; i < TablestoreRetryStrategy.DEFAULT_MAX_ATTEMPTS; i++) {
      assertEquals(10, nextPause(strategy));
    }
    assertEquals(0, nextPause(strategy));
  }

  @Test
  void exponentialModeJittersWithinHalfToFullOfDoublingCeiling() {
    RetryStrategy strategy =
        new TablestoreRetryStrategy(
            RetryConfig.builder()
                .mode(RetryConfig.Mode.EXPONENTIAL)
                .maxAttempts(5)
                .initialDelayMillis(100)
                .multiplier(3.0)
                .maxDelayMillis(300)
                .build());

    assertBetween(50, 100, nextPause(strategy));
    assertBetween(100, 200, nextPause(strategy));
    assertBetween(150, 300, nextPause(strategy));
  }

  private static void assertBetween(long min, long max, long actual) {
    assertTrue(actual >= min && actual <= max, actual + " not in [" + min + ", " + max + "]");
  }

  @Test
  void keepsSdkPauseWhenModeIsUnset() {
    RetryStrategy strategy =
        new TablestoreRetryStrategy(RetryConfig.builder().maxAttempts(2).build());

    assertTrue(nextPause(strategy) > 0);
    assertEquals(0, nextPause(strategy));
  }

  @Test
  void doesNotRetryErrorsTheSdkDoesNotRetry() {
    RetryStrategy strategy =
        new TablestoreRetryStrategy(
            RetryConfig.builder().mode(RetryConfig.Mode.FIXED).fixedDelayMillis(10).build());
    TableStoreException invalidParameter =
        new TableStoreException("bad", null, "OTSParameterInvalid", "request-id", 400);

    assertEquals(0, strategy.nextPause(OperationNames.OP_GET_ROW, invalidParameter));
  }

  @Test
  void capsPauseAtTimeLeftBeforeTotalTimeout() {
    RetryStrategy strategy =
        new TablestoreRetryStrategy(
            RetryConfig.builder()
                .mode(RetryConfig.Mode.FIXED)
                .fixedDelayMillis(60_000)
                .totalTimeout(1000L)
                .build());

    long pause = nextPause(strategy);

    assertTrue(pause > 0 && pause <= 1000, "pause was " + pause);
  }

  @Test
  void unsetTotalTimeoutReplacesTheSdkDefaultDeadline() {
    assertEquals(Integer.MAX_VALUE, totalTimeoutMillis(RetryConfig.builder().build()));
    assertEquals(
        Integer.MAX_VALUE,
        totalTimeoutMillis(RetryConfig.builder().totalTimeout(Long.MAX_VALUE).build()));
    assertEquals(1000L, totalTimeoutMillis(RetryConfig.builder().totalTimeout(1000L).build()));
  }

  @Test
  void cloneStartsANewRequest() {
    RetryStrategy strategy =
        new TablestoreRetryStrategy(
            RetryConfig.builder()
                .maxAttempts(2)
                .mode(RetryConfig.Mode.FIXED)
                .fixedDelayMillis(10)
                .build());
    nextPause(strategy);

    RetryStrategy clone = strategy.clone();

    assertEquals(0, clone.getRetries());
    assertEquals(10, nextPause(clone));
  }
}
