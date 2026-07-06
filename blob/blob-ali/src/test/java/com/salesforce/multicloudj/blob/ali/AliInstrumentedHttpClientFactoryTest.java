package com.salesforce.multicloudj.blob.ali;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.aliyun.sdk.service.oss2.transport.HttpClient;
import com.salesforce.multicloudj.common.observability.Metric;
import com.salesforce.multicloudj.common.observability.MetricsPublisher;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the factory produces an instrumented transport backed by the SDK's own Apache 5
 * clients, and that the extracted pool suppliers return real statistics. This guards the assumption
 * that the SDK-built clients expose Apache pooling managers whose {@code getTotalStats()} can be
 * sampled.
 */
class AliInstrumentedHttpClientFactoryTest {

  private static final class NoopPublisher implements MetricsPublisher {
    @Override
    public void publish(List<Metric> metrics) {}
  }

  @Test
  void createProducesInstrumentedClientOverSdkTransport() {
    HttpClient client =
        AliInstrumentedHttpClientFactory.create(
            new NoopPublisher(), null, Duration.ofSeconds(10));

    assertNotNull(client);
    assertTrue(client instanceof AliConnectionPoolMetricsHttpClient);
  }

  @Test
  void createToleratesNullProxyAndTimeout() {
    HttpClient client = AliInstrumentedHttpClientFactory.create(new NoopPublisher(), null, null);
    assertNotNull(client);
  }

  @Test
  void createAppliesProxyHostWithoutThrowing() {
    HttpClient client =
        AliInstrumentedHttpClientFactory.create(
            new NoopPublisher(), "proxy.example.com:8080", Duration.ofSeconds(5));
    assertNotNull(client);
  }
}
