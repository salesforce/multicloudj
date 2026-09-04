package com.salesforce.multicloudj.blob.ali;

import com.aliyun.sdk.service.oss2.transport.HttpClient;
import com.aliyun.sdk.service.oss2.transport.RequestContext;
import com.aliyun.sdk.service.oss2.transport.RequestMessage;
import com.aliyun.sdk.service.oss2.transport.ResponseMessage;
import com.salesforce.multicloudj.common.observability.ConnectionPoolMetrics;
import com.salesforce.multicloudj.common.observability.MetricsPublisher;
import java.util.concurrent.CompletableFuture;
import org.apache.hc.core5.pool.PoolStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link HttpClient} decorator that samples Alibaba OSS connection-pool utilization and forwards
 * it to a {@link MetricsPublisher}.
 *
 * <p>The Alibaba OSS SDK transports its requests over Apache HTTP client connection pools that
 * expose point-in-time statistics via {@code getTotalStats()} but provide no push callback. This
 * decorator wraps the SDK's own transport so that after each request the current pool statistics
 * are read and published as cloud-agnostic {@link
 * com.salesforce.multicloudj.common.observability.Metric}s, giving a per-request sampling cadence
 * for connection-pool observability. The synchronous pool is sampled on {@link #send} and the
 * asynchronous pool is sampled when {@link #sendAsync} completes.
 *
 * <p>The delegate and its pool statistics are supplied by the caller, which builds them through the
 * SDK's own client builder so that all default transport behavior (TLS trust configuration,
 * idle-connection reaping, timeouts) is preserved. A pool supplier may be {@code null} when the
 * corresponding transport path exposes no pooling manager; in that case sampling for that path
 * degrades to a no-op. Sampling failures are swallowed so metrics never disrupt a request.
 *
 * <p>The OSS client closes its transport only when the transport is {@link AutoCloseable}, so this
 * decorator implements {@link AutoCloseable} and forwards {@link #close()} to the delegate. Without
 * this, wrapping the transport would prevent the underlying connection pools (and their idle
 * reapers) from ever being released.
 */
class AliConnectionPoolMetricsHttpClient implements HttpClient, AutoCloseable {

  private static final Logger logger =
      LoggerFactory.getLogger(AliConnectionPoolMetricsHttpClient.class);

  /** Supplies point-in-time pool statistics for a transport path, or {@code null} when absent. */
  @FunctionalInterface
  interface PoolStatsSupplier {
    PoolStats get();
  }

  private final HttpClient delegate;
  private final MetricsPublisher metricsPublisher;
  private final PoolStatsSupplier syncPoolStats;
  private final PoolStatsSupplier asyncPoolStats;

  AliConnectionPoolMetricsHttpClient(
      HttpClient delegate,
      MetricsPublisher metricsPublisher,
      PoolStatsSupplier syncPoolStats,
      PoolStatsSupplier asyncPoolStats) {
    this.delegate = delegate;
    this.metricsPublisher = metricsPublisher;
    this.syncPoolStats = syncPoolStats;
    this.asyncPoolStats = asyncPoolStats;
  }

  @Override
  public ResponseMessage send(RequestMessage request, RequestContext context) {
    try {
      return delegate.send(request, context);
    } finally {
      publish(syncPoolStats);
    }
  }

  @Override
  public CompletableFuture<ResponseMessage> sendAsync(
      RequestMessage request, RequestContext context) {
    return delegate
        .sendAsync(request, context)
        .whenComplete((response, throwable) -> publish(asyncPoolStats));
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public void close() throws Exception {
    if (delegate instanceof AutoCloseable) {
      ((AutoCloseable) delegate).close();
    }
  }

  private void publish(PoolStatsSupplier supplier) {
    if (supplier == null) {
      return;
    }
    try {
      PoolStats stats = supplier.get();
      if (stats == null) {
        return;
      }
      metricsPublisher.publish(
          ConnectionPoolMetrics.from(
              stats.getMax(), stats.getLeased(), stats.getAvailable(), stats.getPending()));
    } catch (RuntimeException e) {
      logger.debug("Failed to sample Alibaba OSS connection pool metrics", e);
    }
  }
}
