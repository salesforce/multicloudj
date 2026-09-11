package com.salesforce.multicloudj.blob.gcp;

import com.salesforce.multicloudj.common.observability.ConnectionPoolMetrics;
import com.salesforce.multicloudj.common.observability.MetricsPublisher;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.pool.PoolStats;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Samples GCP HTTP connection-pool utilization and forwards it to a {@link MetricsPublisher}.
 *
 * <p>The GCP Cloud Storage client is configured with an Apache {@code
 * PoolingHttpClientConnectionManager}, which exposes point-in-time pool statistics via {@code
 * getTotalStats()} but provides no push callback. This interceptor is installed on the Apache HTTP
 * client so that after every response the current pool statistics are read and published as
 * cloud-agnostic {@link com.salesforce.multicloudj.common.observability.Metric}s, giving the
 * per-request sampling cadence needed for connection-pool observability.
 *
 * <p>The hook degrades safely: if no pool manager was supplied (for example a custom transport that
 * does not use pooling), or if reading statistics fails, the interceptor does nothing and the HTTP
 * response is unaffected.
 */
class GcpConnectionPoolMetricsInterceptor implements HttpResponseInterceptor {

  private static final Logger logger =
      LoggerFactory.getLogger(GcpConnectionPoolMetricsInterceptor.class);

  private final PoolStatsSupplier poolStatsSupplier;

  private final MetricsPublisher metricsPublisher;

  @FunctionalInterface
  interface PoolStatsSupplier {
    PoolStats get();
  }

  GcpConnectionPoolMetricsInterceptor(
      PoolStatsSupplier poolStatsSupplier, MetricsPublisher metricsPublisher) {
    this.poolStatsSupplier = poolStatsSupplier;
    this.metricsPublisher = metricsPublisher;
  }

  @Override
  public void process(HttpResponse response, HttpContext context) {
    if (metricsPublisher == null || poolStatsSupplier == null) {
      return;
    }
    try {
      PoolStats stats = poolStatsSupplier.get();
      if (stats == null) {
        return;
      }
      metricsPublisher.publish(
          ConnectionPoolMetrics.from(
              stats.getMax(), stats.getLeased(), stats.getAvailable(), stats.getPending()));
    } catch (RuntimeException e) {
      logger.debug("Failed to sample GCP connection pool metrics", e);
    }
  }
}
