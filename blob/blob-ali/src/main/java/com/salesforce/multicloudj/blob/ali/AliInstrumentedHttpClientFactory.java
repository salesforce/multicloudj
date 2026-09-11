package com.salesforce.multicloudj.blob.ali;

import com.aliyun.sdk.service.oss2.transport.HttpClient;
import com.aliyun.sdk.service.oss2.transport.apache5client.Apache5AsyncHttpClient;
import com.aliyun.sdk.service.oss2.transport.apache5client.Apache5HttpClient;
import com.salesforce.multicloudj.common.observability.MetricsPublisher;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;

/**
 * Wraps an Alibaba OSS transport client so its connection-pool utilization is sampled and published
 * as cloud-agnostic metrics.
 *
 * <p>The OSS SDK builds a distinct transport per client kind — a synchronous {@link
 * Apache5HttpClient} and an asynchronous {@link Apache5AsyncHttpClient}, each backed by its own
 * Apache connection pool. Rather than reconstruct those clients, this factory wraps the one the SDK
 * builder already produced, so all of the SDK's default transport behavior (TLS trust config,
 * idle-connection reaping, proxy, timeouts, pool sizing) is preserved untouched. It retains a
 * reference to the built client's connection-pool manager so its {@code getTotalStats()} can be
 * sampled after each request.
 *
 * <p>When a built client does not expose an Apache pooling manager (unexpected transport shape),
 * sampling degrades to a no-op.
 */
public final class AliInstrumentedHttpClientFactory {

  private AliInstrumentedHttpClientFactory() {}

  /**
   * Wraps a synchronous OSS transport so its connection pool is sampled on each {@code send}.
   *
   * @param metricsPublisher the publisher to receive connection-pool metrics; must not be {@code
   *     null}
   * @param client the SDK-built synchronous transport to instrument
   * @return an instrumented {@link HttpClient} suitable for {@code
   *     OSSClient.newBuilder().httpClient(...)}
   */
  public static HttpClient instrument(MetricsPublisher metricsPublisher, Apache5HttpClient client) {
    return new AliConnectionPoolMetricsHttpClient(
        client, metricsPublisher, poolStatsSupplier(client), null);
  }

  /**
   * Wraps an asynchronous OSS transport so its connection pool is sampled when {@code sendAsync}
   * completes.
   *
   * @param metricsPublisher the publisher to receive connection-pool metrics; must not be {@code
   *     null}
   * @param client the SDK-built asynchronous transport to instrument
   * @return an instrumented {@link HttpClient} suitable for {@code
   *     OSSAsyncClient.newBuilder().httpClient(...)}
   */
  public static HttpClient instrument(
      MetricsPublisher metricsPublisher, Apache5AsyncHttpClient client) {
    return new AliConnectionPoolMetricsHttpClient(
        client, metricsPublisher, null, poolStatsSupplier(client));
  }

  private static AliConnectionPoolMetricsHttpClient.PoolStatsSupplier poolStatsSupplier(
      Apache5HttpClient client) {
    Object manager = client.getConnectionManager();
    if (manager instanceof PoolingHttpClientConnectionManager) {
      PoolingHttpClientConnectionManager pool = (PoolingHttpClientConnectionManager) manager;
      return pool::getTotalStats;
    }
    return null;
  }

  private static AliConnectionPoolMetricsHttpClient.PoolStatsSupplier poolStatsSupplier(
      Apache5AsyncHttpClient client) {
    Object manager = client.getConnectionManager();
    if (manager instanceof PoolingAsyncClientConnectionManager) {
      PoolingAsyncClientConnectionManager pool = (PoolingAsyncClientConnectionManager) manager;
      return pool::getTotalStats;
    }
    return null;
  }
}
