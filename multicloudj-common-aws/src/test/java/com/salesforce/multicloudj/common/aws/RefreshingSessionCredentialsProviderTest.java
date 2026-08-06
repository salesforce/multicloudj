package com.salesforce.multicloudj.common.aws;

import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class RefreshingSessionCredentialsProviderTest {

  @Test
  public void testResolvedCredentialsCarryTheSuppliedValues() {
    Instant expiration = Instant.now().plus(Duration.ofHours(1));
    RefreshingSessionCredentialsProvider provider =
        new RefreshingSessionCredentialsProvider(
            () -> new StsCredentials("key", "secret", "token", expiration));

    AwsCredentials credentials = provider.resolveCredentials();

    Assertions.assertInstanceOf(AwsSessionCredentials.class, credentials);
    AwsSessionCredentials sessionCredentials = (AwsSessionCredentials) credentials;
    Assertions.assertEquals("key", sessionCredentials.accessKeyId());
    Assertions.assertEquals("secret", sessionCredentials.secretAccessKey());
    Assertions.assertEquals("token", sessionCredentials.sessionToken());
    Assertions.assertEquals(expiration, sessionCredentials.expirationTime().orElse(null));
  }

  @Test
  public void testSupplierIsNotInvokedUntilCredentialsAreResolved() {
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider =
        new RefreshingSessionCredentialsProvider(
            countingSupplier(invocations, Duration.ofHours(1)));

    Assertions.assertEquals(0, invocations.get());

    Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));
    Assertions.assertEquals(1, invocations.get());
  }

  @Test
  public void testRepeatedResolvesWithinFreshnessWindowInvokeSupplierOnce() {
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider =
        new RefreshingSessionCredentialsProvider(
            countingSupplier(invocations, Duration.ofHours(1)));

    for (int i = 0; i < 10; i++) {
      Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));
    }

    Assertions.assertEquals(1, invocations.get());
  }

  @Test
  public void testStaleCredentialsAreRenewedAndTheNewTokenIsReturned() throws Exception {
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider =
        RefreshingSessionCredentialsProvider.builder()
            .credentialsSupplier(countingSupplier(invocations, Duration.ofMillis(300)))
            .staleTime(Duration.ZERO)
            .prefetchTime(Duration.ZERO)
            .minimumRefreshInterval(Duration.ofMillis(1))
            .build();

    Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));
    Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));
    Assertions.assertEquals(1, invocations.get());

    Thread.sleep(400);

    Assertions.assertEquals("securityToken-2", sessionToken(provider.resolveCredentials()));
    Assertions.assertEquals(2, invocations.get());
  }

  @Test
  public void testConcurrentResolvesOnAStaleCacheInvokeSupplierOnce() throws Exception {
    int threads = 16;
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider =
        new RefreshingSessionCredentialsProvider(
            () -> {
              int invocation = invocations.incrementAndGet();
              try {
                Thread.sleep(100);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
              }
              return new StsCredentials(
                  "accessKeyId-" + invocation,
                  "accessKeySecret-" + invocation,
                  "securityToken-" + invocation,
                  Instant.now().plus(Duration.ofHours(1)));
            });

    CountDownLatch startGate = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      List<Future<String>> resolved = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        resolved.add(
            pool.submit(
                () -> {
                  startGate.await();
                  return sessionToken(provider.resolveCredentials());
                }));
      }
      startGate.countDown();

      for (Future<String> token : resolved) {
        Assertions.assertEquals("securityToken-1", token.get(30, TimeUnit.SECONDS));
      }
    } finally {
      pool.shutdownNow();
    }

    Assertions.assertEquals(1, invocations.get());
  }

  @Test
  public void testInvalidateForcesTheNextResolveToReinvokeTheSupplier() {
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider =
        new RefreshingSessionCredentialsProvider(
            countingSupplier(invocations, Duration.ofHours(1)));

    Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));
    Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));
    Assertions.assertEquals(1, invocations.get());

    provider.invalidate();

    Assertions.assertEquals("securityToken-2", sessionToken(provider.resolveCredentials()));
    Assertions.assertEquals(2, invocations.get());
  }

  @Test
  public void testRepeatedInvalidationWithinTheMinimumRefreshIntervalIsHonouredOnce() {
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider =
        RefreshingSessionCredentialsProvider.builder()
            .credentialsSupplier(countingSupplier(invocations, Duration.ofHours(1)))
            .minimumRefreshInterval(Duration.ofHours(1))
            .build();

    Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));

    provider.invalidate();
    Assertions.assertEquals("securityToken-2", sessionToken(provider.resolveCredentials()));

    for (int i = 0; i < 100; i++) {
      provider.invalidate();
      Assertions.assertEquals("securityToken-2", sessionToken(provider.resolveCredentials()));
    }
    Assertions.assertEquals(
        2,
        invocations.get(),
        "a credential source the service keeps rejecting must not be invoked once per failure");
  }

  @Test
  public void testInvalidationIsHonouredAgainOnceTheMinimumRefreshIntervalElapses()
      throws Exception {
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider =
        RefreshingSessionCredentialsProvider.builder()
            .credentialsSupplier(countingSupplier(invocations, Duration.ofHours(1)))
            .minimumRefreshInterval(Duration.ofMillis(1))
            .build();

    Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));

    provider.invalidate();
    Assertions.assertEquals("securityToken-2", sessionToken(provider.resolveCredentials()));

    Thread.sleep(50);

    provider.invalidate();
    Assertions.assertEquals("securityToken-3", sessionToken(provider.resolveCredentials()));
    Assertions.assertEquals(3, invocations.get());
  }

  @Test
  public void testConcurrentInvalidationIsHonouredOnce() throws Exception {
    int threads = 16;
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider =
        RefreshingSessionCredentialsProvider.builder()
            .credentialsSupplier(countingSupplier(invocations, Duration.ofHours(1)))
            .minimumRefreshInterval(Duration.ofHours(1))
            .build();

    Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));

    CountDownLatch startGate = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      List<Future<?>> invalidations = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        invalidations.add(
            pool.submit(
                () -> {
                  startGate.await();
                  provider.invalidate();
                  return null;
                }));
      }
      startGate.countDown();
      for (Future<?> invalidation : invalidations) {
        invalidation.get(30, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }

    Assertions.assertEquals("securityToken-2", sessionToken(provider.resolveCredentials()));
    Assertions.assertEquals(2, invocations.get());
  }

  @Test
  public void testNullExpirationCachesForTheRefreshInterval() {
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider =
        new RefreshingSessionCredentialsProvider(countingSupplier(invocations, null));

    AwsSessionCredentials credentials = (AwsSessionCredentials) provider.resolveCredentials();
    Assertions.assertEquals("securityToken-1", credentials.sessionToken());
    Assertions.assertTrue(credentials.expirationTime().isEmpty());

    for (int i = 0; i < 10; i++) {
      Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));
    }
    Assertions.assertEquals(1, invocations.get());
  }

  @Test
  public void testNullExpirationIsRenewedOnceTheRefreshIntervalElapses() throws Exception {
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider =
        RefreshingSessionCredentialsProvider.builder()
            .credentialsSupplier(countingSupplier(invocations, null))
            .refreshInterval(Duration.ofMillis(200))
            .build();

    Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));
    Assertions.assertEquals(1, invocations.get());

    Thread.sleep(300);

    Assertions.assertEquals("securityToken-2", sessionToken(provider.resolveCredentials()));
    Assertions.assertEquals(2, invocations.get());
  }

  @Test
  public void testAlreadyExpiredCredentialsMakeProgressWithoutReinvokingTheSupplierPerResolve()
      throws Exception {
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider =
        RefreshingSessionCredentialsProvider.builder()
            .credentialsSupplier(countingSupplier(invocations, Duration.ofMinutes(-1)))
            .minimumRefreshInterval(Duration.ofSeconds(2))
            .build();

    Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));

    for (int i = 0; i < 100; i++) {
      Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));
    }
    Assertions.assertEquals(
        1,
        invocations.get(),
        "renewal windows derived from a past expiration must not make every resolve renew");

    Thread.sleep(2_300);

    Assertions.assertEquals("securityToken-2", sessionToken(provider.resolveCredentials()));
    Assertions.assertEquals(2, invocations.get());
  }

  @Test
  public void testExpirationNearerThanTheStaleWindowStillCaches() {
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider =
        RefreshingSessionCredentialsProvider.builder()
            .credentialsSupplier(countingSupplier(invocations, Duration.ofSeconds(5)))
            .minimumRefreshInterval(Duration.ofSeconds(2))
            .build();

    for (int i = 0; i < 100; i++) {
      Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));
    }
    Assertions.assertEquals(1, invocations.get());
  }

  @Test
  public void testSupplierReturningNullThrowsAndLeavesTheProviderUsable() {
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider =
        new RefreshingSessionCredentialsProvider(
            () -> {
              if (invocations.incrementAndGet() == 1) {
                return null;
              }
              return new StsCredentials(
                  "key", "secret", "token", Instant.now().plus(Duration.ofHours(1)));
            });

    IllegalStateException failure =
        Assertions.assertThrows(IllegalStateException.class, provider::resolveCredentials);
    Assertions.assertTrue(
        failure.getMessage().contains("returned null"),
        "unexpected message: " + failure.getMessage());

    Assertions.assertEquals("token", sessionToken(provider.resolveCredentials()));
    Assertions.assertEquals(2, invocations.get());
  }

  @Test
  public void testSupplierThrowingPropagatesAndLeavesTheProviderUsable() {
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider =
        new RefreshingSessionCredentialsProvider(
            () -> {
              if (invocations.incrementAndGet() == 1) {
                throw new IllegalArgumentException("no credentials available");
              }
              return new StsCredentials(
                  "key", "secret", "token", Instant.now().plus(Duration.ofHours(1)));
            });

    IllegalArgumentException failure =
        Assertions.assertThrows(IllegalArgumentException.class, provider::resolveCredentials);
    Assertions.assertEquals("no credentials available", failure.getMessage());

    Assertions.assertEquals("token", sessionToken(provider.resolveCredentials()));
    Assertions.assertEquals(2, invocations.get());
  }

  @Test
  public void testCloseLeavesTheProviderUsable() {
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider =
        new RefreshingSessionCredentialsProvider(
            countingSupplier(invocations, Duration.ofHours(1)));

    provider.close();
    provider.close();

    Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));
    Assertions.assertEquals(1, invocations.get());
  }

  @Test
  public void testNullSupplierIsRejected() {
    Assertions.assertThrows(
        NullPointerException.class, () -> new RefreshingSessionCredentialsProvider(null));
  }

  @Test
  public void testNullDurationsAreRejected() {
    assertBuildRejects(builder -> builder.staleTime(null), "staleTime must not be null");
    assertBuildRejects(builder -> builder.prefetchTime(null), "prefetchTime must not be null");
    assertBuildRejects(
        builder -> builder.refreshInterval(null), "refreshInterval must not be null");
    assertBuildRejects(
        builder -> builder.minimumRefreshInterval(null), "minimumRefreshInterval must not be null");
  }

  @Test
  public void testNegativeDurationsAreRejected() {
    Duration negative = Duration.ofSeconds(-1);
    assertBuildRejects(builder -> builder.staleTime(negative), "staleTime must not be negative");
    assertBuildRejects(
        builder -> builder.prefetchTime(negative), "prefetchTime must not be negative");
    assertBuildRejects(
        builder -> builder.refreshInterval(negative), "refreshInterval must not be negative");
    assertBuildRejects(
        builder -> builder.minimumRefreshInterval(negative),
        "minimumRefreshInterval must not be negative");
  }

  @Test
  public void testPrefetchTimeShorterThanStaleTimeIsRejected() {
    assertBuildRejects(
        builder -> builder.staleTime(Duration.ofMinutes(5)).prefetchTime(Duration.ofMinutes(1)),
        "prefetchTime must not be shorter than staleTime");
  }

  @Test
  public void testPrefetchTimeEqualToStaleTimeIsAccepted() {
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider =
        RefreshingSessionCredentialsProvider.builder()
            .credentialsSupplier(countingSupplier(invocations, Duration.ofHours(1)))
            .staleTime(Duration.ofMinutes(5))
            .prefetchTime(Duration.ofMinutes(5))
            .build();

    Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));
  }

  private static void assertBuildRejects(
      UnaryOperator<RefreshingSessionCredentialsProvider.Builder> configuration,
      String expectedMessage) {
    RefreshingSessionCredentialsProvider.Builder builder =
        configuration.apply(
            RefreshingSessionCredentialsProvider.builder()
                .credentialsSupplier(() -> new StsCredentials("key", "secret", "token")));

    IllegalArgumentException failure =
        Assertions.assertThrows(IllegalArgumentException.class, builder::build);
    Assertions.assertTrue(
        failure.getMessage().startsWith(expectedMessage),
        "unexpected message: " + failure.getMessage());
  }

  private static Supplier<StsCredentials> countingSupplier(
      AtomicInteger invocations, Duration validity) {
    return () -> {
      int invocation = invocations.incrementAndGet();
      return new StsCredentials(
          "accessKeyId-" + invocation,
          "accessKeySecret-" + invocation,
          "securityToken-" + invocation,
          validity == null ? null : Instant.now().plus(validity));
    };
  }

  private static String sessionToken(AwsCredentials credentials) {
    return ((AwsSessionCredentials) credentials).sessionToken();
  }
}
