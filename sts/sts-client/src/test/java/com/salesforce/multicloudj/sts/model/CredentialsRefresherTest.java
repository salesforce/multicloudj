package com.salesforce.multicloudj.sts.model;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CredentialsRefresherTest {

  private static final Duration INTERVAL = Duration.ofSeconds(60);
  private static final long WAIT_SECONDS = 10;

  private MutableClock clock;
  private ExecutorService executor;

  @BeforeEach
  void setUp() {
    clock = new MutableClock(Instant.parse("2030-01-01T00:00:00Z"));
    executor = Executors.newFixedThreadPool(8);
  }

  @AfterEach
  void tearDown() {
    executor.shutdownNow();
  }

  /** Supplier whose behavior for each call is scripted and whose calls are counted. */
  private static final class ScriptedSupplier implements Supplier<StsCredentials> {
    private final AtomicInteger calls = new AtomicInteger();
    private final List<Supplier<StsCredentials>> script = new ArrayList<>();

    ScriptedSupplier then(Supplier<StsCredentials> step) {
      script.add(step);
      return this;
    }

    ScriptedSupplier thenReturn(StsCredentials credentials) {
      return then(() -> credentials);
    }

    ScriptedSupplier thenFail(RuntimeException failure) {
      return then(
          () -> {
            throw failure;
          });
    }

    int calls() {
      return calls.get();
    }

    @Override
    public StsCredentials get() {
      int index = calls.getAndIncrement();
      Supplier<StsCredentials> step = script.get(Math.min(index, script.size() - 1));
      return step.get();
    }
  }

  private static StsCredentials plain(String id) {
    return new StsCredentials(id, "secret", "token");
  }

  private StsCredentials expiring(String id, Duration lifetimeFromNow) {
    return new StsCredentials(id, "secret", "token", clock.instant().plus(lifetimeFromNow));
  }

  private CredentialsRefresher refresherFor(Supplier<StsCredentials> supplier) {
    return new CredentialsRefresher(supplier, INTERVAL, clock);
  }

  @Test
  void doesNotCallSupplierAtConstruction() {
    ScriptedSupplier supplier = new ScriptedSupplier().thenReturn(plain("v1"));

    refresherFor(supplier);

    Assertions.assertEquals(0, supplier.calls());
  }

  @Test
  void firstCallFetchesAndCachesWithinInterval() {
    ScriptedSupplier supplier =
        new ScriptedSupplier().thenReturn(plain("v1")).thenReturn(plain("v2"));
    CredentialsRefresher refresher = refresherFor(supplier);

    StsCredentials first = refresher.getCredentials();
    clock.advance(INTERVAL.minusNanos(1));
    StsCredentials second = refresher.getCredentials();

    Assertions.assertEquals("v1", first.getAccessKeyId());
    Assertions.assertSame(first, second);
    Assertions.assertEquals(1, supplier.calls());
  }

  @Test
  void refreshesOnceTheIntervalHasElapsed() {
    ScriptedSupplier supplier =
        new ScriptedSupplier().thenReturn(plain("v1")).thenReturn(plain("v2"));
    CredentialsRefresher refresher = refresherFor(supplier);
    refresher.getCredentials();

    clock.advance(INTERVAL);
    StsCredentials refreshed = refresher.getCredentials();

    Assertions.assertEquals("v2", refreshed.getAccessKeyId());
    Assertions.assertEquals(2, supplier.calls());
  }

  @Test
  void concurrentFirstFetchCallsSupplierOnce() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .then(
                () -> {
                  entered.countDown();
                  await(release);
                  return plain("v1");
                });
    CredentialsRefresher refresher = refresherFor(supplier);

    List<Future<StsCredentials>> results = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      results.add(executor.submit(refresher::getCredentials));
    }
    Assertions.assertTrue(entered.await(WAIT_SECONDS, TimeUnit.SECONDS));
    release.countDown();

    for (Future<StsCredentials> result : results) {
      Assertions.assertEquals("v1", result.get(WAIT_SECONDS, TimeUnit.SECONDS).getAccessKeyId());
    }
    Assertions.assertEquals(1, supplier.calls());
  }

  @Test
  void onlyOneCallerRefreshesWhileOthersGetLastGoodCredentials() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .thenReturn(plain("v1"))
            .then(
                () -> {
                  entered.countDown();
                  await(release);
                  return plain("v2");
                });
    CredentialsRefresher refresher = refresherFor(supplier);
    refresher.getCredentials();
    clock.advance(INTERVAL);

    Future<StsCredentials> refreshing = executor.submit(refresher::getCredentials);
    Assertions.assertTrue(entered.await(WAIT_SECONDS, TimeUnit.SECONDS));

    List<Future<StsCredentials>> others = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      others.add(executor.submit(refresher::getCredentials));
    }
    for (Future<StsCredentials> other : others) {
      Assertions.assertEquals("v1", other.get(WAIT_SECONDS, TimeUnit.SECONDS).getAccessKeyId());
    }
    Assertions.assertFalse(refreshing.isDone());

    release.countDown();

    Assertions.assertEquals("v2", refreshing.get(WAIT_SECONDS, TimeUnit.SECONDS).getAccessKeyId());
    Assertions.assertEquals("v2", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(2, supplier.calls());
  }

  @Test
  void firstFetchFailurePropagatesAndLaterCallRecovers() {
    RuntimeException failure = new IllegalStateException("issuer unavailable");
    ScriptedSupplier supplier =
        new ScriptedSupplier().thenFail(failure).thenReturn(plain("v1"));
    CredentialsRefresher refresher = refresherFor(supplier);

    UnAuthorizedException thrown =
        Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);

    Assertions.assertSame(failure, thrown.getCause());
    clock.advance(CredentialsRefresher.MAX_RETRY_SPACING);
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(2, supplier.calls());
  }

  @Test
  void nullFromSupplierOnFirstFetchIsAFailureAndNotCached() {
    ScriptedSupplier supplier = new ScriptedSupplier().then(() -> null).thenReturn(plain("v1"));
    CredentialsRefresher refresher = refresherFor(supplier);

    Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);

    clock.advance(CredentialsRefresher.MAX_RETRY_SPACING);
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
  }

  @Test
  void refreshFailureBeforeCutoffServesLastGoodAndRespectsRetrySpacing() {
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .thenReturn(plain("v1"))
            .thenFail(new IllegalStateException("down"))
            .thenReturn(plain("v2"));
    CredentialsRefresher refresher = refresherFor(supplier);
    refresher.getCredentials();

    clock.advance(INTERVAL);
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(2, supplier.calls());

    clock.advance(CredentialsRefresher.MAX_RETRY_SPACING.minusNanos(1));
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(2, supplier.calls());

    clock.advance(Duration.ofNanos(1));
    Assertions.assertEquals("v2", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(3, supplier.calls());
  }

  @Test
  void retrySpacingNeverExceedsTheRefreshInterval() {
    Duration shortInterval = Duration.ofSeconds(3);
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .thenReturn(plain("v1"))
            .thenFail(new IllegalStateException("down"))
            .thenReturn(plain("v2"));
    CredentialsRefresher refresher = new CredentialsRefresher(supplier, shortInterval, clock);
    refresher.getCredentials();

    clock.advance(shortInterval);
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(2, supplier.calls());

    clock.advance(shortInterval.minusNanos(1));
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(2, supplier.calls());

    clock.advance(Duration.ofNanos(1));
    Assertions.assertEquals("v2", refresher.getCredentials().getAccessKeyId());
  }

  @Test
  void nullFromSupplierOnRefreshKeepsServingLastGood() {
    ScriptedSupplier supplier =
        new ScriptedSupplier().thenReturn(plain("v1")).then(() -> null);
    CredentialsRefresher refresher = refresherFor(supplier);
    refresher.getCredentials();

    clock.advance(INTERVAL);

    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
  }

  @Test
  void plainCredentialsAreServedUntilTwiceTheIntervalThenFailureIsThrown() {
    RuntimeException failure = new IllegalStateException("down");
    ScriptedSupplier supplier = new ScriptedSupplier().thenReturn(plain("v1")).thenFail(failure);
    CredentialsRefresher refresher = refresherFor(supplier);
    refresher.getCredentials();

    clock.advance(INTERVAL.multipliedBy(2).minusNanos(1));
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());

    clock.advance(Duration.ofNanos(1));
    UnAuthorizedException thrown =
        Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);
    Assertions.assertSame(failure, thrown.getCause());
  }

  @Test
  void credentialsWithAnUnknownExpirationUseTheIntervalOnly() {
    RuntimeException failure = new IllegalStateException("down");
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .then(() -> new StsCredentials("v1", "secret", "token", null))
            .thenFail(failure);
    CredentialsRefresher refresher = refresherFor(supplier);
    refresher.getCredentials();

    clock.advance(INTERVAL.minusNanos(1));
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(1, supplier.calls());

    clock.advance(Duration.ofNanos(1));
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(2, supplier.calls());

    clock.advance(INTERVAL);
    UnAuthorizedException thrown =
        Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);
    Assertions.assertSame(failure, thrown.getCause());
  }

  @Test
  void recoversAfterTheCutoffOnceTheSupplierWorksAgain() {
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .thenReturn(plain("v1"))
            .thenFail(new IllegalStateException("down"))
            .thenReturn(plain("v2"));
    CredentialsRefresher refresher = refresherFor(supplier);
    refresher.getCredentials();
    clock.advance(INTERVAL.multipliedBy(2));
    Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);

    clock.advance(CredentialsRefresher.MAX_RETRY_SPACING);
    Assertions.assertEquals("v2", refresher.getCredentials().getAccessKeyId());
  }

  @Test
  void expiryShorterThanTheIntervalRefreshesAtEightyPercentOfLifetime() {
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .then(() -> expiring("v1", Duration.ofSeconds(20)))
            .thenReturn(plain("v2"));
    CredentialsRefresher refresher = refresherFor(supplier);
    refresher.getCredentials();

    clock.advance(Duration.ofSeconds(16).minusNanos(1));
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(1, supplier.calls());

    clock.advance(Duration.ofNanos(1));
    Assertions.assertEquals("v2", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(2, supplier.calls());
  }

  @Test
  void expiryLongerThanTheIntervalRefreshesAtTheInterval() {
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .then(() -> expiring("v1", Duration.ofSeconds(600)))
            .thenReturn(plain("v2"));
    CredentialsRefresher refresher = refresherFor(supplier);
    refresher.getCredentials();

    clock.advance(INTERVAL.minusNanos(1));
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(1, supplier.calls());

    clock.advance(Duration.ofNanos(1));
    Assertions.assertEquals("v2", refresher.getCredentials().getAccessKeyId());
  }

  @Test
  void alreadyExpiredCredentialsAreRejectedAndNotCached() {
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .then(() -> expiring("expired", Duration.ZERO))
            .thenReturn(plain("v1"));
    CredentialsRefresher refresher = refresherFor(supplier);

    Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);

    clock.advance(CredentialsRefresher.MAX_RETRY_SPACING);
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
  }

  @Test
  void credentialsAreNeverServedAtOrPastTheirExpiration() {
    RuntimeException failure = new IllegalStateException("down");
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .then(() -> expiring("v1", Duration.ofSeconds(100)))
            .thenFail(failure);
    CredentialsRefresher refresher = refresherFor(supplier);
    refresher.getCredentials();

    clock.advance(Duration.ofSeconds(100).minusNanos(1));
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());

    clock.advance(Duration.ofNanos(1));
    UnAuthorizedException thrown =
        Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);
    Assertions.assertSame(failure, thrown.getCause());
  }

  @Test
  void firstFetchFailureIsRememberedForTheRetryWindowWithoutCallingTheSupplier() {
    RuntimeException failure = new IllegalStateException("issuer unavailable");
    ScriptedSupplier supplier =
        new ScriptedSupplier().thenFail(failure).thenReturn(plain("v1"));
    CredentialsRefresher refresher = refresherFor(supplier);
    UnAuthorizedException first =
        Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);

    clock.advance(CredentialsRefresher.MAX_RETRY_SPACING.minusNanos(1));
    UnAuthorizedException second =
        Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);

    Assertions.assertEquals(1, supplier.calls());
    Assertions.assertNotSame(first, second);
    Assertions.assertSame(first, second.getCause());
    Assertions.assertSame(failure, second.getCause().getCause());
    Assertions.assertTrue(
        second.getMessage().contains(clock.instant().plusNanos(1).toString()),
        "message should state when the supplier will be called again: " + second.getMessage());

    clock.advance(Duration.ofNanos(1));
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(2, supplier.calls());
  }

  @Test
  void successAfterTheWindowClearsTheRememberedFailure() {
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .thenFail(new IllegalStateException("down"))
            .thenReturn(plain("v1"))
            .thenReturn(plain("v2"));
    CredentialsRefresher refresher = refresherFor(supplier);
    Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);

    clock.advance(CredentialsRefresher.MAX_RETRY_SPACING);
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(2, supplier.calls());

    clock.advance(INTERVAL);
    Assertions.assertEquals("v2", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(3, supplier.calls());
  }

  @Test
  void failureAfterTheWindowStartsANewWindow() {
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .thenFail(new IllegalStateException("down"))
            .thenFail(new IllegalStateException("still down"))
            .thenReturn(plain("v1"));
    CredentialsRefresher refresher = refresherFor(supplier);
    Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);

    clock.advance(CredentialsRefresher.MAX_RETRY_SPACING);
    Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);
    Assertions.assertEquals(2, supplier.calls());

    clock.advance(CredentialsRefresher.MAX_RETRY_SPACING.minusNanos(1));
    Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);
    Assertions.assertEquals(2, supplier.calls());

    clock.advance(Duration.ofNanos(1));
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(3, supplier.calls());
  }

  @Test
  void failurePastTheCutoffIsRememberedForTheRetryWindow() {
    RuntimeException failure = new IllegalStateException("down");
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .thenReturn(plain("v1"))
            .thenFail(failure)
            .thenReturn(plain("v2"));
    CredentialsRefresher refresher = refresherFor(supplier);
    refresher.getCredentials();
    clock.advance(INTERVAL.multipliedBy(2));
    UnAuthorizedException first =
        Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);
    Assertions.assertSame(failure, first.getCause());
    Assertions.assertEquals(2, supplier.calls());

    clock.advance(CredentialsRefresher.MAX_RETRY_SPACING.minusNanos(1));
    UnAuthorizedException second =
        Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);
    Assertions.assertSame(first, second.getCause());
    Assertions.assertEquals(2, supplier.calls());

    clock.advance(Duration.ofNanos(1));
    Assertions.assertEquals("v2", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(3, supplier.calls());
  }

  @Test
  void rememberedFailureWindowNeverExceedsTheRefreshInterval() {
    Duration shortInterval = Duration.ofSeconds(3);
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .thenFail(new IllegalStateException("down"))
            .thenReturn(plain("v1"));
    CredentialsRefresher refresher = new CredentialsRefresher(supplier, shortInterval, clock);
    Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);

    clock.advance(shortInterval.minusNanos(1));
    Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);
    Assertions.assertEquals(1, supplier.calls());

    clock.advance(Duration.ofNanos(1));
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
  }

  @Test
  void failureWhileCredentialsAreServableIsNotRememberedPastTheCutoff() {
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .thenReturn(plain("v1"))
            .thenFail(new IllegalStateException("down"))
            .thenReturn(plain("v2"));
    CredentialsRefresher refresher = refresherFor(supplier);
    refresher.getCredentials();
    clock.advance(INTERVAL);
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(2, supplier.calls());

    clock.advance(INTERVAL);

    Assertions.assertEquals("v2", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(3, supplier.calls());
  }

  @Test
  void callersQueuedBehindAFailingFetchDoNotCallTheSupplierAgain() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    RuntimeException failure = new IllegalStateException("issuer unavailable");
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .then(
                () -> {
                  entered.countDown();
                  await(release);
                  throw failure;
                })
            .thenReturn(plain("v1"));
    CredentialsRefresher refresher = refresherFor(supplier);

    List<Future<StsCredentials>> results = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      results.add(executor.submit(refresher::getCredentials));
    }
    Assertions.assertTrue(entered.await(WAIT_SECONDS, TimeUnit.SECONDS));
    release.countDown();

    for (Future<StsCredentials> result : results) {
      ExecutionException thrown =
          Assertions.assertThrows(
              ExecutionException.class, () -> result.get(WAIT_SECONDS, TimeUnit.SECONDS));
      Assertions.assertInstanceOf(UnAuthorizedException.class, thrown.getCause());
    }
    Assertions.assertEquals(1, supplier.calls());
  }

  @Test
  void slowSupplierThatFailsPastTheCutoffDoesNotServeTheOldCredentials() {
    RuntimeException failure = new IllegalStateException("down");
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .thenReturn(plain("v1"))
            .then(
                () -> {
                  clock.advance(INTERVAL.multipliedBy(2));
                  throw failure;
                });
    CredentialsRefresher refresher = refresherFor(supplier);
    refresher.getCredentials();
    clock.advance(INTERVAL);

    UnAuthorizedException thrown =
        Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);

    Assertions.assertSame(failure, thrown.getCause());
    Assertions.assertEquals(2, supplier.calls());
    UnAuthorizedException backingOff =
        Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);
    Assertions.assertSame(thrown, backingOff.getCause());
    Assertions.assertEquals(2, supplier.calls());
  }

  @Test
  void callersQueuedBehindARefreshThatFailsPastTheCutoffDoNotCallTheSupplierAgain()
      throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    RuntimeException failure = new IllegalStateException("down");
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .thenReturn(plain("v1"))
            .then(
                () -> {
                  entered.countDown();
                  await(release);
                  throw failure;
                });
    CredentialsRefresher refresher = refresherFor(supplier);
    refresher.getCredentials();
    clock.advance(INTERVAL);
    List<Future<StsCredentials>> results = new ArrayList<>();
    results.add(executor.submit(refresher::getCredentials));
    Assertions.assertTrue(entered.await(WAIT_SECONDS, TimeUnit.SECONDS));
    clock.advance(INTERVAL);
    for (int i = 0; i < 5; i++) {
      results.add(executor.submit(refresher::getCredentials));
    }
    release.countDown();

    for (Future<StsCredentials> result : results) {
      ExecutionException thrown =
          Assertions.assertThrows(
              ExecutionException.class, () -> result.get(WAIT_SECONDS, TimeUnit.SECONDS));
      Assertions.assertInstanceOf(UnAuthorizedException.class, thrown.getCause());
    }
    Assertions.assertEquals(2, supplier.calls());
  }

  @Test
  void credentialsThatExpireWhileTheSupplierIsRunningAreRejectedAndNotCached() {
    Instant expiration = clock.instant().plusSeconds(10);
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .then(
                () -> {
                  clock.advance(Duration.ofSeconds(20));
                  return new StsCredentials("slow", "secret", "token", expiration);
                })
            .thenReturn(plain("v1"));
    CredentialsRefresher refresher = refresherFor(supplier);

    Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);

    Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);
    Assertions.assertEquals(1, supplier.calls());
    clock.advance(CredentialsRefresher.MAX_RETRY_SPACING);
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
  }

  @Test
  void credentialsWithoutAnAccessKeyIdOrSecretAreAFailedFetch() {
    StsCredentials[] unusable = {
      new StsCredentials(null, "secret-value", "token"),
      new StsCredentials("", "secret-value", "token"),
      new StsCredentials("   ", "secret-value", "token"),
      new StsCredentials("key-id-value", null, "token"),
      new StsCredentials("key-id-value", "", "token"),
      new StsCredentials("key-id-value", "   ", "token")
    };
    for (StsCredentials credentials : unusable) {
      ScriptedSupplier supplier =
          new ScriptedSupplier().thenReturn(credentials).thenReturn(plain("v1"));
      CredentialsRefresher refresher = refresherFor(supplier);

      UnAuthorizedException thrown =
          Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);

      Assertions.assertTrue(
          thrown.getMessage().contains("access key"), "unexpected message: " + thrown.getMessage());
      Assertions.assertFalse(thrown.getMessage().contains("secret-value"));
      Assertions.assertFalse(thrown.getMessage().contains("key-id-value"));
      clock.advance(CredentialsRefresher.MAX_RETRY_SPACING);
      Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    }
  }

  @Test
  void credentialsWithoutAnAccessKeyIdOrSecretDoNotReplaceTheLastGoodCredentials() {
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .thenReturn(plain("v1"))
            .thenReturn(new StsCredentials("", "secret", "token"))
            .thenReturn(new StsCredentials("v3", null, "token"))
            .thenReturn(plain("v4"));
    CredentialsRefresher refresher = refresherFor(supplier);
    refresher.getCredentials();

    clock.advance(INTERVAL);
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(2, supplier.calls());

    clock.advance(CredentialsRefresher.MAX_RETRY_SPACING);
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(3, supplier.calls());

    clock.advance(CredentialsRefresher.MAX_RETRY_SPACING);
    Assertions.assertEquals("v4", refresher.getCredentials().getAccessKeyId());
  }

  @Test
  void credentialsWithoutASecurityTokenAreAFailedFetch() {
    StsCredentials[] unusable = {
      new StsCredentials("key-id-value", "secret-value", null),
      new StsCredentials("key-id-value", "secret-value", ""),
      new StsCredentials("key-id-value", "secret-value", "   ")
    };
    for (StsCredentials credentials : unusable) {
      ScriptedSupplier supplier =
          new ScriptedSupplier().thenReturn(credentials).thenReturn(plain("v1"));
      CredentialsRefresher refresher = refresherFor(supplier);

      UnAuthorizedException thrown =
          Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);

      Assertions.assertTrue(
          thrown.getMessage().contains("security token"),
          "unexpected message: " + thrown.getMessage());
      Assertions.assertFalse(thrown.getMessage().contains("secret-value"));
      Assertions.assertFalse(thrown.getMessage().contains("key-id-value"));
      Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);
      Assertions.assertEquals(1, supplier.calls());
      clock.advance(CredentialsRefresher.MAX_RETRY_SPACING);
      Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    }
  }

  @Test
  void credentialsWithoutASecurityTokenDoNotReplaceTheLastGoodCredentials() {
    Duration interval = Duration.ofSeconds(300);
    Instant expiration = clock.instant().plusSeconds(1800);
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .thenReturn(new StsCredentials("v1", "secret", "token", expiration))
            .thenReturn(new StsCredentials("v2", "secret", "", expiration))
            .thenReturn(new StsCredentials("v3", "secret", "token", expiration));
    CredentialsRefresher refresher = new CredentialsRefresher(supplier, interval, clock);
    refresher.getCredentials();

    clock.advance(Duration.ofSeconds(301));
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(2, supplier.calls());

    clock.advance(Duration.ofSeconds(1));
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(2, supplier.calls());

    clock.advance(Duration.ofSeconds(10));
    Assertions.assertEquals("v3", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(3, supplier.calls());
  }

  @Test
  void aCheckedExceptionThrownSneakilyBySupplierIsAFailedFetch() {
    IOException failure = new IOException("issuer unreachable");
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .then(
                () -> {
                  CredentialsRefresherTest.<RuntimeException>sneakyThrow(failure);
                  return null;
                })
            .thenReturn(plain("v1"))
            .then(
                () -> {
                  CredentialsRefresherTest.<RuntimeException>sneakyThrow(failure);
                  return null;
                });
    CredentialsRefresher refresher = refresherFor(supplier);

    UnAuthorizedException thrown =
        Assertions.assertThrows(UnAuthorizedException.class, refresher::getCredentials);

    Assertions.assertSame(failure, thrown.getCause());
    clock.advance(CredentialsRefresher.MAX_RETRY_SPACING);
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    clock.advance(INTERVAL);
    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
    Assertions.assertEquals(3, supplier.calls());
  }

  @Test
  void anErrorThrownBySupplierPropagatesAndReleasesTheLock() throws Exception {
    Error failure = new Error("boom");
    ScriptedSupplier supplier =
        new ScriptedSupplier()
            .then(
                () -> {
                  throw failure;
                })
            .thenReturn(plain("v1"));
    CredentialsRefresher refresher = refresherFor(supplier);

    Error thrown = Assertions.assertThrows(Error.class, refresher::getCredentials);

    Assertions.assertSame(failure, thrown);
    Assertions.assertEquals(
        "v1",
        executor.submit(refresher::getCredentials).get(WAIT_SECONDS, TimeUnit.SECONDS)
            .getAccessKeyId());
    Assertions.assertEquals(2, supplier.calls());
  }

  @Test
  void rejectsMissingSupplierAndNonPositiveInterval() {
    Supplier<StsCredentials> supplier = () -> plain("v1");

    Assertions.assertThrows(
        InvalidArgumentException.class, () -> new CredentialsRefresher(null, INTERVAL));
    Assertions.assertThrows(
        InvalidArgumentException.class, () -> new CredentialsRefresher(supplier, null));
    Assertions.assertThrows(
        InvalidArgumentException.class, () -> new CredentialsRefresher(supplier, Duration.ZERO));
    Assertions.assertThrows(
        InvalidArgumentException.class,
        () -> new CredentialsRefresher(supplier, Duration.ofSeconds(-1)));
  }

  @Test
  void rejectsAnIntervalWhoseArithmeticOverflows() {
    Supplier<StsCredentials> supplier = () -> plain("v1");

    Assertions.assertThrows(
        InvalidArgumentException.class,
        () -> new CredentialsRefresher(supplier, Duration.ofSeconds(Long.MAX_VALUE), clock));
    InvalidArgumentException pastTheLastInstant =
        Assertions.assertThrows(
            InvalidArgumentException.class,
            () ->
                new CredentialsRefresher(
                    supplier, Duration.ofSeconds(Instant.MAX.getEpochSecond()), clock));
    Assertions.assertTrue(
        pastTheLastInstant.getMessage().contains("refreshInterval"),
        "unexpected message: " + pastTheLastInstant.getMessage());
  }

  @Test
  void acceptsALongButRepresentableInterval() {
    ScriptedSupplier supplier = new ScriptedSupplier().thenReturn(plain("v1"));
    CredentialsRefresher refresher =
        new CredentialsRefresher(supplier, Duration.ofDays(365), clock);

    Assertions.assertEquals("v1", refresher.getCredentials().getAccessKeyId());
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void sneakyThrow(Throwable throwable) throws T {
    throw (T) throwable;
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
        throw new IllegalStateException("timed out waiting for the test to release the supplier");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static final class MutableClock extends Clock {
    private volatile Instant now;

    MutableClock(Instant start) {
      this.now = start;
    }

    void advance(Duration duration) {
      now = now.plus(duration);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return now;
    }
  }
}
