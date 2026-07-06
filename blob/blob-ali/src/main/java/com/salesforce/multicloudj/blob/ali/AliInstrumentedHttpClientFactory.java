package com.salesforce.multicloudj.blob.ali;

import com.aliyun.sdk.service.oss2.transport.HttpClient;
import com.aliyun.sdk.service.oss2.transport.HttpClientOptions;
import com.aliyun.sdk.service.oss2.transport.apache5client.Apache5AsyncHttpClient;
import com.aliyun.sdk.service.oss2.transport.apache5client.Apache5HttpClient;
import com.aliyun.sdk.service.oss2.transport.apache5client.Apache5MixedHttpClient;
import com.salesforce.multicloudj.common.observability.MetricsPublisher;
import java.time.Duration;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;

/**
 * Builds an Alibaba OSS transport that is instrumented for connection-pool metrics.
 *
 * <p>The Alibaba OSS SDK's default transport is an {@link Apache5MixedHttpClient} composed of a
 * synchronous {@link Apache5HttpClient} and an asynchronous {@link Apache5AsyncHttpClient}, each
 * backed by an Apache connection pool. This factory reconstructs that same transport using the
 * SDK's own client builders — preserving the SDK's default TLS trust configuration and
 * idle-connection reaping — while retaining references to the two connection-pool managers so their
 * utilization can be sampled and published. The caller-supplied proxy host and read/write timeout
 * are mirrored onto the transport; other {@link HttpClientOptions} settings retain their SDK
 * defaults.
 *
 * <p>The resulting {@link HttpClient} is intended to be handed to the SDK via {@code
 * OSSClient.newBuilder().httpClient(...)}. When a pool manager is not exposed by a built client
 * (unexpected transport shape), sampling for that path degrades to a no-op.
 */
public final class AliInstrumentedHttpClientFactory {

  private AliInstrumentedHttpClientFactory() {}

  public static HttpClient create(
      MetricsPublisher metricsPublisher, String proxyHost, Duration readWriteTimeout) {
    HttpClientOptions.Builder optionsBuilder = HttpClientOptions.custom();
    if (proxyHost != null) {
      optionsBuilder.proxyHost(proxyHost);
    }
    if (readWriteTimeout != null) {
      optionsBuilder.readWriteTimeout(readWriteTimeout);
    }
    HttpClientOptions options = optionsBuilder.build();

    Apache5HttpClient syncClient = Apache5HttpClient.custom().options(options).build();
    Apache5AsyncHttpClient asyncClient = Apache5AsyncHttpClient.custom().options(options).build();

    AliConnectionPoolMetricsHttpClient.PoolStatsSupplier syncStats =
        poolStatsSupplier(syncClient);
    AliConnectionPoolMetricsHttpClient.PoolStatsSupplier asyncStats =
        poolStatsSupplier(asyncClient);

    Apache5MixedHttpClient delegate = new Apache5MixedHttpClient(syncClient, asyncClient);
    return new AliConnectionPoolMetricsHttpClient(
        delegate, metricsPublisher, syncStats, asyncStats);
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
