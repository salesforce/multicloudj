package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.common.retries.RetryExecutor;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.DateUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * Executes registry HTTP requests with the retry behavior described by a {@link RetryConfig}.
 *
 * <p>Transient I/O failures and HTTP 429, 500, 502, 503 and 504 responses are retried. When the
 * retries run out, the last response is returned (or the last {@link IOException} is thrown) so
 * the caller maps it the same way as a request that was never retried. Only obtaining the
 * response is retried; a failure while the caller reads a returned body stream is not.
 *
 * <p>Exponential delays get equal jitter (between half and all of the computed delay) so clients
 * throttled or failed together do not retry in lockstep; fixed delays are used as configured. For
 * 429 and 503 responses, a {@code Retry-After} header lengthens the delay; one longer than the
 * retry configuration allows ends the retries and the response is returned to the caller.
 * Without a RetryConfig, this executor sends each request once and only the HTTP client's own
 * retry handling applies.
 */
class HttpRetryExecutor {

  private static final Set<Integer> RETRYABLE_STATUS_CODES =
      Set.of(
          HttpStatus.SC_TOO_MANY_REQUESTS,
          HttpStatus.SC_INTERNAL_SERVER_ERROR,
          HttpStatus.SC_BAD_GATEWAY,
          HttpStatus.SC_SERVICE_UNAVAILABLE,
          HttpStatus.SC_GATEWAY_TIMEOUT);

  private final RetryExecutor retryExecutor;
  private final RetryExecutor.RetryPolicy<CloseableHttpResponse> retryPolicy;
  private final LongSupplier wallClockMillis;

  /**
   * Creates an executor for the given retry configuration.
   *
   * @param retryConfig the retry configuration, or null to send each request once
   */
  HttpRetryExecutor(RetryConfig retryConfig) {
    this(
        retryConfig,
        Thread::sleep,
        () -> System.nanoTime() / 1_000_000L,
        System::currentTimeMillis);
  }

  HttpRetryExecutor(
      RetryConfig retryConfig,
      RetryExecutor.Sleeper sleeper,
      LongSupplier clockMillis,
      LongSupplier wallClockMillis) {
    this.retryExecutor =
        new RetryExecutor(retryConfig, sleeper, clockMillis, RetryExecutor::equalJitter);
    this.wallClockMillis = wallClockMillis;
    this.retryPolicy = new ResponseRetryPolicy();
  }

  /**
   * Configures an HTTP client being built so that a {@link RetryConfig} is the only retry policy
   * applied to its requests. Automatic retries of the HTTP client are disabled, since they resend
   * failed requests immediately and would multiply the configured attempts.
   * {@link RetryConfig#getAttemptTimeout()} is applied as the timeouts for leasing a connection
   * from the pool, connecting, and waiting for data. The socket timeout bounds inactivity between
   * packets rather than the whole response, so long blob downloads that keep receiving data are
   * not cut off.
   *
   * @param builder the HTTP client builder to configure
   * @param retryConfig the retry configuration, or null to keep the client defaults
   * @return the same builder
   */
  static HttpClientBuilder configureClient(HttpClientBuilder builder, RetryConfig retryConfig) {
    if (retryConfig == null) {
      return builder;
    }
    builder.disableAutomaticRetries();
    if (retryConfig.getAttemptTimeout() != null) {
      int timeout = (int) Math.min(Integer.MAX_VALUE, retryConfig.getAttemptTimeout());
      builder.setDefaultRequestConfig(
          RequestConfig.custom()
              .setConnectionRequestTimeout(timeout)
              .setConnectTimeout(timeout)
              .setSocketTimeout(timeout)
              .build());
    }
    return builder;
  }

  /**
   * Executes the request, retrying retryable failures according to the retry configuration.
   *
   * @param httpClient the client to send the request with
   * @param request the request to send
   * @return the first non-retryable response, or the last response once retries are exhausted
   * @throws IOException if the last attempt fails with an I/O error
   */
  CloseableHttpResponse execute(CloseableHttpClient httpClient, HttpUriRequest request)
      throws IOException {
    return retryExecutor.execute(() -> httpClient.execute(request), retryPolicy);
  }

  private final class ResponseRetryPolicy
      implements RetryExecutor.RetryPolicy<CloseableHttpResponse> {

    /**
     * Rejects failures that another attempt cannot fix: an unresolvable host, a TLS handshake or
     * certificate failure, and an HTTP protocol violation such as a redirect loop. Other I/O
     * failures, including connection resets and timeouts, are treated as transient.
     */
    @Override
    public boolean isRetryable(IOException error) {
      return !(error instanceof UnknownHostException
          || error instanceof SSLHandshakeException
          || error instanceof SSLPeerUnverifiedException
          || error instanceof ClientProtocolException);
    }

    @Override
    public boolean isRetryable(CloseableHttpResponse response) {
      return RETRYABLE_STATUS_CODES.contains(response.getStatusLine().getStatusCode());
    }

    /** Honors Retry-After only on responses where the server is asking clients to back off. */
    @Override
    public long minDelayMillis(CloseableHttpResponse response) {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode != HttpStatus.SC_TOO_MANY_REQUESTS
          && statusCode != HttpStatus.SC_SERVICE_UNAVAILABLE) {
        return 0L;
      }
      Long retryAfter = parseRetryAfterMillis(response.getFirstHeader(HttpHeaders.RETRY_AFTER));
      return retryAfter != null ? retryAfter : 0L;
    }

    /**
     * Closes the response without reading the rest of its body, which drops the connection rather
     * than returning it to the pool; draining could block on a slowly streamed body and use up
     * the total timeout. A failure to close is ignored so it does not hide the retryable status or
     * cancel the remaining attempts.
     */
    @Override
    public void discard(CloseableHttpResponse response) {
      try {
        response.close();
      } catch (IOException ignored) {
        // The next attempt opens its own connection.
      }
    }
  }

  /** Parses a Retry-After header given either as delta-seconds or as an HTTP-date. */
  private Long parseRetryAfterMillis(Header header) {
    if (header == null) {
      return null;
    }
    String value = header.getValue().trim();
    try {
      // toMillis saturates instead of overflowing, so an oversized value still means "wait long".
      return Math.max(0L, TimeUnit.SECONDS.toMillis(Long.parseLong(value)));
    } catch (NumberFormatException e) {
      Date date = DateUtils.parseDate(value);
      return date != null ? Math.max(0L, date.getTime() - wallClockMillis.getAsLong()) : null;
    }
  }
}
