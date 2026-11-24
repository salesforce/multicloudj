package com.salesforce.multicloudj.registry.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.common.retries.RetryConfig;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.DateUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

/** Unit tests for HttpRetryExecutor. */
class HttpRetryExecutorTest {

  private static final HttpGet REQUEST = new HttpGet("https://registry.example.com/v2/");
  private static final RetryConfig FIXED_CONFIG =
      RetryConfig.builder()
          .mode(RetryConfig.Mode.FIXED)
          .maxAttempts(3)
          .fixedDelayMillis(50L)
          .build();

  private CloseableHttpClient httpClient;
  private AtomicLong now;
  private List<Long> sleeps;

  @BeforeEach
  void setUp() {
    httpClient = mock(CloseableHttpClient.class);
    now = new AtomicLong(1_000_000L);
    sleeps = new ArrayList<>();
  }

  private HttpRetryExecutor executor(RetryConfig retryConfig) {
    return new HttpRetryExecutor(
        retryConfig,
        millis -> {
          sleeps.add(millis);
          now.addAndGet(millis);
        },
        now::get,
        now::get);
  }

  private static CloseableHttpResponse response(int statusCode) {
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(statusLine.getStatusCode()).thenReturn(statusCode);
    when(response.getStatusLine()).thenReturn(statusLine);
    return response;
  }

  @Test
  void testNullRetryConfig_SendsOnce() throws Exception {
    CloseableHttpResponse unavailable = response(HttpStatus.SC_SERVICE_UNAVAILABLE);
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(unavailable);

    assertSame(unavailable, executor(null).execute(httpClient, REQUEST));
    verify(httpClient, times(1)).execute(REQUEST);
    assertTrue(sleeps.isEmpty());
  }

  @Test
  void testNullRetryConfig_PropagatesIoExceptionWithoutRetry() throws Exception {
    when(httpClient.execute(any(HttpUriRequest.class))).thenThrow(new IOException("reset"));

    assertThrows(IOException.class, () -> executor(null).execute(httpClient, REQUEST));
    verify(httpClient, times(1)).execute(REQUEST);
  }

  @ParameterizedTest
  @ValueSource(ints = {429, 500, 502, 503, 504})
  void testRetryableStatus_RetriesUntilSuccess(int statusCode) throws Exception {
    CloseableHttpResponse failure = response(statusCode);
    CloseableHttpResponse ok = response(HttpStatus.SC_OK);
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(failure, ok);

    assertSame(ok, executor(FIXED_CONFIG).execute(httpClient, REQUEST));
    verify(httpClient, times(2)).execute(REQUEST);
    verify(failure).close();
    verify(failure, never()).getEntity();
    assertEquals(List.of(50L), sleeps);
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 401, 403, 404})
  void testNonRetryableStatus_ReturnsImmediately(int statusCode) throws Exception {
    CloseableHttpResponse failure = response(statusCode);
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(failure);

    assertSame(failure, executor(FIXED_CONFIG).execute(httpClient, REQUEST));
    verify(httpClient, times(1)).execute(REQUEST);
    verify(failure, never()).close();
  }

  @Test
  void testRetryableStatus_ReturnsLastResponseWhenAttemptsExhausted() throws Exception {
    CloseableHttpResponse first = response(HttpStatus.SC_SERVICE_UNAVAILABLE);
    CloseableHttpResponse second = response(HttpStatus.SC_SERVICE_UNAVAILABLE);
    CloseableHttpResponse last = response(HttpStatus.SC_SERVICE_UNAVAILABLE);
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(first, second, last);

    assertSame(last, executor(FIXED_CONFIG).execute(httpClient, REQUEST));
    verify(httpClient, times(3)).execute(REQUEST);
    verify(first).close();
    verify(second).close();
    verify(last, never()).close();
  }

  @Test
  void testRetryableStatus_CloseFailureDoesNotStopRetries() throws Exception {
    CloseableHttpResponse failure = response(HttpStatus.SC_SERVICE_UNAVAILABLE);
    doThrow(new IOException("close failed")).when(failure).close();
    CloseableHttpResponse ok = response(HttpStatus.SC_OK);
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(failure, ok);

    assertSame(ok, executor(FIXED_CONFIG).execute(httpClient, REQUEST));
    verify(httpClient, times(2)).execute(REQUEST);
  }

  @Test
  void testExponentialBackoff_AppliesEqualJitter() throws Exception {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .maxAttempts(51)
            .initialDelayMillis(1_000L)
            .maxDelayMillis(1_000L)
            .build();
    CloseableHttpResponse unavailable = response(HttpStatus.SC_BAD_GATEWAY);
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(unavailable);

    executor(config).execute(httpClient, REQUEST);

    assertEquals(50, sleeps.size());
    assertTrue(sleeps.stream().allMatch(s -> s >= 500L && s <= 1_000L), "sleeps were " + sleeps);
    assertTrue(sleeps.stream().distinct().count() > 1, "sleeps were not jittered: " + sleeps);
  }

  @Test
  void testIoException_RetriesThenSucceeds() throws Exception {
    CloseableHttpResponse ok = response(HttpStatus.SC_OK);
    when(httpClient.execute(any(HttpUriRequest.class)))
        .thenThrow(new IOException("reset"))
        .thenReturn(ok);

    assertSame(ok, executor(FIXED_CONFIG).execute(httpClient, REQUEST));
    verify(httpClient, times(2)).execute(REQUEST);
  }

  static Stream<IOException> permanentIoFailures() {
    return Stream.of(
        new UnknownHostException("no.such.registry"),
        new SSLHandshakeException("handshake failed"),
        new SSLPeerUnverifiedException("peer not verified"),
        new ClientProtocolException("circular redirect"));
  }

  @ParameterizedTest
  @MethodSource("permanentIoFailures")
  void testPermanentIoException_NotRetried(IOException failure) throws Exception {
    when(httpClient.execute(any(HttpUriRequest.class))).thenThrow(failure);

    IOException thrown =
        assertThrows(IOException.class, () -> executor(FIXED_CONFIG).execute(httpClient, REQUEST));
    assertSame(failure, thrown);
    verify(httpClient, times(1)).execute(REQUEST);
    assertTrue(sleeps.isEmpty());
  }

  @Test
  void testSocketTimeout_Retried() throws Exception {
    CloseableHttpResponse ok = response(HttpStatus.SC_OK);
    when(httpClient.execute(any(HttpUriRequest.class)))
        .thenThrow(new SocketTimeoutException("read timed out"))
        .thenReturn(ok);

    assertSame(ok, executor(FIXED_CONFIG).execute(httpClient, REQUEST));
    verify(httpClient, times(2)).execute(REQUEST);
  }

  @ParameterizedTest
  @ValueSource(strings = {"120", "9999999999999999"})
  void testRetryAfterBeyondMaxDelay_ReturnsResponseWithoutRetry(String retryAfter)
      throws Exception {
    CloseableHttpResponse throttled = response(HttpStatus.SC_TOO_MANY_REQUESTS);
    when(throttled.getFirstHeader(HttpHeaders.RETRY_AFTER))
        .thenReturn(new BasicHeader(HttpHeaders.RETRY_AFTER, retryAfter));
    CloseableHttpResponse ok = response(HttpStatus.SC_OK);
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(throttled, ok);

    assertSame(throttled, executor(FIXED_CONFIG).execute(httpClient, REQUEST));
    verify(httpClient, times(1)).execute(REQUEST);
    verify(throttled, never()).close();
    assertTrue(sleeps.isEmpty());
  }

  @Test
  void testRetryAfterSeconds_LengthensDelay() throws Exception {
    CloseableHttpResponse throttled = response(HttpStatus.SC_TOO_MANY_REQUESTS);
    when(throttled.getFirstHeader(HttpHeaders.RETRY_AFTER))
        .thenReturn(new BasicHeader(HttpHeaders.RETRY_AFTER, "2"));
    CloseableHttpResponse ok = response(HttpStatus.SC_OK);
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(throttled, ok);

    executor(FIXED_CONFIG).execute(httpClient, REQUEST);

    assertEquals(List.of(2000L), sleeps);
  }

  @Test
  void testRetryAfterHttpDate_LengthensDelay() throws Exception {
    now.set(1_700_000_000_000L);
    String date = DateUtils.formatDate(new Date(now.get() + 3000L));
    CloseableHttpResponse unavailable = response(HttpStatus.SC_SERVICE_UNAVAILABLE);
    when(unavailable.getFirstHeader(HttpHeaders.RETRY_AFTER))
        .thenReturn(new BasicHeader(HttpHeaders.RETRY_AFTER, date));
    CloseableHttpResponse ok = response(HttpStatus.SC_OK);
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(unavailable, ok);

    executor(FIXED_CONFIG).execute(httpClient, REQUEST);

    assertEquals(List.of(3000L), sleeps);
  }

  @Test
  void testRetryAfter_NeverShortensBaseDelay() throws Exception {
    CloseableHttpResponse throttled = response(HttpStatus.SC_TOO_MANY_REQUESTS);
    when(throttled.getFirstHeader(HttpHeaders.RETRY_AFTER))
        .thenReturn(new BasicHeader(HttpHeaders.RETRY_AFTER, "0"));
    CloseableHttpResponse ok = response(HttpStatus.SC_OK);
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(throttled, ok);

    executor(FIXED_CONFIG).execute(httpClient, REQUEST);

    assertEquals(List.of(50L), sleeps);
  }

  @Test
  void testRetryAfter_IgnoredForServerError() throws Exception {
    CloseableHttpResponse serverError = response(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    CloseableHttpResponse ok = response(HttpStatus.SC_OK);
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(serverError, ok);

    executor(FIXED_CONFIG).execute(httpClient, REQUEST);

    verify(serverError, never()).getFirstHeader(HttpHeaders.RETRY_AFTER);
    assertEquals(List.of(50L), sleeps);
  }

  @Test
  void testConfigureClient_NullRetryConfig_LeavesBuilderUntouched() {
    HttpClientBuilder builder = mock(HttpClientBuilder.class);

    assertSame(builder, HttpRetryExecutor.configureClient(builder, null));
    verifyNoInteractions(builder);
  }

  @Test
  void testConfigureClient_DisablesAutomaticRetries() {
    HttpClientBuilder builder = mock(HttpClientBuilder.class);

    HttpRetryExecutor.configureClient(builder, FIXED_CONFIG);

    verify(builder).disableAutomaticRetries();
    verify(builder, never()).setDefaultRequestConfig(any());
  }

  @Test
  void testConfigureClient_AppliesAttemptTimeout() {
    HttpClientBuilder builder = mock(HttpClientBuilder.class);
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.FIXED)
            .fixedDelayMillis(10L)
            .attemptTimeout(1500L)
            .build();

    HttpRetryExecutor.configureClient(builder, config);

    ArgumentCaptor<RequestConfig> captor = ArgumentCaptor.forClass(RequestConfig.class);
    verify(builder).setDefaultRequestConfig(captor.capture());
    assertEquals(1500, captor.getValue().getConnectionRequestTimeout());
    assertEquals(1500, captor.getValue().getConnectTimeout());
    assertEquals(1500, captor.getValue().getSocketTimeout());
  }
}
