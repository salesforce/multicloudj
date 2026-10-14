package com.salesforce.multicloudj.sts.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.auth.Retryable;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.message.BasicHttpRequest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class GcpStsRetrierTest {
  private static final GcpStsRetrier.Sleeper NO_SLEEP = millis -> {};
  private static final LongUnaryOperator NO_JITTER = millis -> millis;

  private static HttpResponseException httpError(int status) {
    return new HttpResponseException.Builder(status, "status " + status, new HttpHeaders())
        .build();
  }

  /** Fails with the given errors in order, then returns "ok". */
  private static GcpStsRetrier.IoOperation<String> failingThenOk(
      AtomicInteger calls, IOException... errors) {
    return () -> {
      int call = calls.getAndIncrement();
      if (call < errors.length) {
        throw errors[call];
      }
      return "ok";
    };
  }

  @Test
  public void testUnsetModeUsesDefaultExponentialBackoff() throws Exception {
    List<Long> sleeps = new ArrayList<>();
    RetryConfig retryConfig = RetryConfig.builder().maxAttempts(10).build();
    GcpStsRetrier retrier =
        new GcpStsRetrier(retryConfig, sleeps::add, System::nanoTime, NO_JITTER);
    AtomicInteger calls = new AtomicInteger();
    IOException[] errors = new IOException[9];
    java.util.Arrays.fill(errors, httpError(503));

    assertEquals("ok", retrier.execute(failingThenOk(calls, errors)));

    assertEquals(List.of(100L, 200L, 400L, 800L, 1600L, 3200L, 6400L, 12800L, 20000L), sleeps);
  }

  @Test
  public void testInterruptedThreadStopsRetryWithLastErrorAsCause() {
    AtomicInteger calls = new AtomicInteger();
    IOException error = httpError(503);
    GcpStsRetrier retrier =
        new GcpStsRetrier(
            RetryConfig.builder().maxAttempts(5).build(), NO_SLEEP, System::nanoTime);

    Thread.currentThread().interrupt();
    try {
      java.io.InterruptedIOException thrown =
          assertThrows(
              java.io.InterruptedIOException.class,
              () -> retrier.execute(failingThenOk(calls, error, error, error)));
      assertSame(error, thrown.getCause());
      assertEquals(1, calls.get());
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void testDefaultsToThreeAttemptsWhenMaxAttemptsUnset() {
    AtomicInteger calls = new AtomicInteger();
    GcpStsRetrier retrier =
        new GcpStsRetrier(RetryConfig.builder().build(), NO_SLEEP, System::nanoTime);
    IOException last = httpError(503);

    IOException thrown =
        assertThrows(
            IOException.class,
            () -> retrier.execute(failingThenOk(calls, httpError(503), httpError(503), last)));

    assertSame(last, thrown);
    assertEquals(3, calls.get());
  }

  @Test
  public void testDoesNotRetryNonRetryableErrors() {
    AtomicInteger calls = new AtomicInteger();
    GcpStsRetrier retrier =
        new GcpStsRetrier(
            RetryConfig.builder().maxAttempts(5).build(), NO_SLEEP, System::nanoTime);

    assertThrows(IOException.class, () -> retrier.execute(failingThenOk(calls, httpError(403))));
    assertEquals(1, calls.get());
  }

  @Test
  public void testSleepsWithExponentialBackoff() throws IOException {
    List<Long> sleeps = new ArrayList<>();
    RetryConfig retryConfig =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .maxAttempts(5)
            .initialDelayMillis(100)
            .multiplier(3.0)
            .maxDelayMillis(500)
            .build();
    GcpStsRetrier retrier =
        new GcpStsRetrier(retryConfig, sleeps::add, System::nanoTime, NO_JITTER);

    retrier.execute(
        failingThenOk(
            new AtomicInteger(), httpError(503), httpError(503), httpError(503), httpError(503)));

    assertEquals(List.of(100L, 300L, 500L, 500L), sleeps);
  }

  @Test
  public void testExponentialBackoffAppliesEqualJitter() throws IOException {
    List<Long> sleeps = new ArrayList<>();
    RetryConfig retryConfig =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .maxAttempts(51)
            .initialDelayMillis(1000)
            .maxDelayMillis(1000)
            .build();
    GcpStsRetrier retrier = new GcpStsRetrier(retryConfig, sleeps::add, System::nanoTime);
    IOException[] errors = new IOException[50];
    java.util.Arrays.fill(errors, httpError(503));

    retrier.execute(failingThenOk(new AtomicInteger(), errors));

    assertTrue(sleeps.stream().allMatch(s -> s >= 500 && s <= 1000), "sleeps were " + sleeps);
    assertTrue(sleeps.stream().distinct().count() > 1, "sleeps were not jittered: " + sleeps);
  }

  @Test
  public void testSleepsWithFixedBackoff() throws IOException {
    List<Long> sleeps = new ArrayList<>();
    RetryConfig retryConfig =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.FIXED)
            .maxAttempts(3)
            .fixedDelayMillis(75)
            .build();
    GcpStsRetrier retrier = new GcpStsRetrier(retryConfig, sleeps::add, System::nanoTime);

    retrier.execute(failingThenOk(new AtomicInteger(), httpError(502), httpError(504)));

    assertEquals(List.of(75L, 75L), sleeps);
  }

  @Test
  public void testStopsWhenNextDelayWouldExceedTotalTimeout() {
    AtomicLong nowNanos = new AtomicLong();
    AtomicInteger calls = new AtomicInteger();
    RetryConfig retryConfig =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.FIXED)
            .maxAttempts(10)
            .fixedDelayMillis(400)
            .totalTimeout(1000L)
            .build();
    GcpStsRetrier retrier =
        new GcpStsRetrier(
            retryConfig, millis -> nowNanos.addAndGet(millis * 1_000_000L), nowNanos::get);

    assertThrows(
        IOException.class,
        () ->
            retrier.execute(
                failingThenOk(
                    calls, httpError(503), httpError(503), httpError(503), httpError(503))));

    // Attempts start at 0ms, 400ms and 800ms; another 400ms wait would end past the 1000ms total.
    assertEquals(3, calls.get());
  }

  @Test
  public void testInterruptedSleepStopsRetrying() {
    AtomicInteger calls = new AtomicInteger();
    GcpStsRetrier retrier =
        new GcpStsRetrier(
            RetryConfig.builder()
                .mode(RetryConfig.Mode.FIXED)
                .maxAttempts(3)
                .fixedDelayMillis(10)
                .build(),
            millis -> {
              throw new InterruptedException();
            },
            System::nanoTime);

    assertThrows(
        java.io.InterruptedIOException.class,
        () -> retrier.execute(failingThenOk(calls, httpError(503))));
    assertEquals(1, calls.get());
    assertTrue(Thread.interrupted());
  }

  @Test
  public void testIsRetryableClassification() {
    assertTrue(GcpStsRetrier.isRetryable(httpError(408)));
    assertTrue(GcpStsRetrier.isRetryable(httpError(429)));
    assertTrue(GcpStsRetrier.isRetryable(httpError(500)));
    assertTrue(GcpStsRetrier.isRetryable(new java.net.ConnectException()));
    assertTrue(GcpStsRetrier.isRetryable(new SocketTimeoutException()));
    assertTrue(GcpStsRetrier.isRetryable(new IOException("wrapped", httpError(503))));
    assertFalse(GcpStsRetrier.isRetryable(httpError(400)));
    assertFalse(GcpStsRetrier.isRetryable(httpError(404)));
    assertFalse(GcpStsRetrier.isRetryable(new IOException("bad credentials file")));
  }

  /** Stands in for google-auth's GoogleAuthException, whose constructors are not public. */
  private static final class AuthException extends IOException implements Retryable {
    private final boolean retryable;

    AuthException(boolean retryable, Throwable cause) {
      super(cause);
      this.retryable = retryable;
    }

    @Override
    public boolean isRetryable() {
      return retryable;
    }

    @Override
    public int getRetryCount() {
      return 0;
    }
  }

  @Test
  public void testIsRetryableClassifiesHttpStatusUnderNonRetryableAuthError() {
    assertTrue(GcpStsRetrier.isRetryable(new AuthException(false, httpError(502))));
    assertTrue(GcpStsRetrier.isRetryable(new AuthException(false, httpError(504))));
    assertTrue(GcpStsRetrier.isRetryable(new AuthException(true, httpError(400))));
    assertFalse(GcpStsRetrier.isRetryable(new AuthException(false, httpError(400))));
    assertFalse(GcpStsRetrier.isRetryable(new AuthException(false, null)));
  }

  @Test
  public void testGetAccessTokenRetriesTransientRefreshFailures() throws IOException {
    GoogleCredentials credentials = Mockito.mock(GoogleCredentials.class);
    Mockito.doThrow(httpError(503)).doNothing().when(credentials).refreshIfExpired();
    Mockito.when(credentials.getAccessToken())
        .thenReturn(new AccessToken("token-value", null));
    GcpStsRetrier retrier =
        new GcpStsRetrier(
            RetryConfig.builder().maxAttempts(2).build(), NO_SLEEP, System::nanoTime);
    GcpSts sts = new GcpSts(new GcpSts().builder(), credentials, null, retrier);

    StsCredentials result =
        sts.getAccessTokenFromProvider(GetAccessTokenRequest.newBuilder().build());

    assertEquals("token-value", result.getSecurityToken());
    Mockito.verify(credentials, Mockito.times(2)).refreshIfExpired();
  }

  @Test
  public void testGetAccessTokenWithoutRetryConfigFailsOnFirstError() throws IOException {
    GoogleCredentials credentials = Mockito.mock(GoogleCredentials.class);
    Mockito.doThrow(httpError(503)).when(credentials).refreshIfExpired();
    GcpSts sts = new GcpSts(new GcpSts().builder(), credentials);

    assertThrows(
        SubstrateSdkException.class,
        () -> sts.getAccessTokenFromProvider(GetAccessTokenRequest.newBuilder().build()));
    Mockito.verify(credentials, Mockito.times(1)).refreshIfExpired();
  }

  @Test
  public void testTokenExchangeRetriesServerErrors() {
    AtomicInteger requests = new AtomicInteger();
    MockHttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest() {
              @Override
              public LowLevelHttpResponse execute() {
                if (requests.getAndIncrement() == 0) {
                  return new MockLowLevelHttpResponse().setStatusCode(503);
                }
                return new MockLowLevelHttpResponse()
                    .setStatusCode(200)
                    .setContentType("application/json")
                    .setContent("{\"access_token\":\"exchanged-token\"}");
              }
            };
          }
        };
    GcpStsRetrier retrier =
        new GcpStsRetrier(
            RetryConfig.builder().maxAttempts(3).build(), NO_SLEEP, System::nanoTime);
    GcpSts sts = new GcpSts(new GcpSts().builder(), null, () -> transport, retrier);

    StsCredentials result =
        sts.getSTSCredentialsWithAssumeRoleWebIdentity(
            AssumeRoleWebIdentityRequest.builder()
                .role("//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p")
                .webIdentityToken("jwt")
                .build());

    assertEquals("exchanged-token", result.getSecurityToken());
    assertEquals(2, requests.get());
  }

  private static RequestConfig applyAttemptTimeout(int timeoutMillis, RequestConfig config) {
    HttpClientContext context = HttpClientContext.create();
    context.setRequestConfig(config);
    new GcpSts.AttemptTimeoutInterceptor(timeoutMillis)
        .process(new BasicHttpRequest("GET", "/"), context);
    return context.getRequestConfig();
  }

  @Test
  public void testAttemptTimeoutCapsPoolConnectAndReadTimeouts() {
    RequestConfig capped =
        applyAttemptTimeout(
            500,
            RequestConfig.custom()
                .setConnectionRequestTimeout(-1)
                .setConnectTimeout(20000)
                .setSocketTimeout(0)
                .setRedirectsEnabled(false)
                .build());

    assertEquals(500, capped.getConnectionRequestTimeout());
    assertEquals(500, capped.getConnectTimeout());
    assertEquals(500, capped.getSocketTimeout());
    assertFalse(capped.isRedirectsEnabled());
  }

  @Test
  public void testAttemptTimeoutKeepsShorterTimeouts() {
    RequestConfig capped =
        applyAttemptTimeout(
            500,
            RequestConfig.custom()
                .setConnectionRequestTimeout(100)
                .setConnectTimeout(200)
                .setSocketTimeout(300)
                .build());

    assertEquals(100, capped.getConnectionRequestTimeout());
    assertEquals(200, capped.getConnectTimeout());
    assertEquals(300, capped.getSocketTimeout());
  }
}
