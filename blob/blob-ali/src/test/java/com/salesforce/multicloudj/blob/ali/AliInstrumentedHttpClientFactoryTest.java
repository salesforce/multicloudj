package com.salesforce.multicloudj.blob.ali;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.aliyun.sdk.service.oss2.transport.HttpClient;
import com.aliyun.sdk.service.oss2.transport.HttpClientOptions;
import com.aliyun.sdk.service.oss2.transport.apache5client.Apache5AsyncHttpClient;
import com.aliyun.sdk.service.oss2.transport.apache5client.Apache5AsyncHttpClientBuilder;
import com.aliyun.sdk.service.oss2.transport.apache5client.Apache5HttpClient;
import com.aliyun.sdk.service.oss2.transport.apache5client.Apache5HttpClientBuilder;
import com.salesforce.multicloudj.common.observability.Metric;
import com.salesforce.multicloudj.common.observability.MetricsPublisher;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the factory wraps the SDK's own Apache 5 transport clients so their connection-pool
 * statistics can be sampled. This guards the assumption that the SDK-built clients expose Apache
 * pooling managers whose {@code getTotalStats()} is readable.
 */
class AliInstrumentedHttpClientFactoryTest {

  private static final class NoopPublisher implements MetricsPublisher {
    @Override
    public void publish(List<Metric> metrics) {}
  }

  @Test
  void instrumentWrapsSyncTransport() {
    Apache5HttpClient sync =
        Apache5HttpClientBuilder.create().options(HttpClientOptions.custom().build()).build();

    HttpClient client = AliInstrumentedHttpClientFactory.instrument(new NoopPublisher(), sync);

    assertNotNull(client);
    assertTrue(client instanceof AliConnectionPoolMetricsHttpClient);
  }

  @Test
  void instrumentWrapsAsyncTransport() {
    Apache5AsyncHttpClient async =
        Apache5AsyncHttpClientBuilder.create().options(HttpClientOptions.custom().build()).build();

    HttpClient client = AliInstrumentedHttpClientFactory.instrument(new NoopPublisher(), async);

    assertNotNull(client);
    assertTrue(client instanceof AliConnectionPoolMetricsHttpClient);
  }
}
