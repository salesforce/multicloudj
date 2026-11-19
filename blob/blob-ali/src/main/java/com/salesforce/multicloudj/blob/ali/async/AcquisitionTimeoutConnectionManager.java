package com.salesforce.multicloudj.blob.ali.async;

import com.aliyun.sdk.service.oss2.transport.HttpClientOptions;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hc.client5.http.HttpRoute;
import org.apache.hc.client5.http.SystemDefaultDnsResolver;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.DefaultSchemePortResolver;
import org.apache.hc.client5.http.impl.nio.DefaultAsyncClientConnectionOperator;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.nio.ManagedAsyncClientConnection;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.DefaultHostnameVerifier;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.pool.StrictConnPool;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

final class AcquisitionTimeoutConnectionManager extends PoolingAsyncClientConnectionManager {

  private final ScheduledExecutorService timeoutExecutor;
  private final ScheduledFuture<?> timeoutTask;

  AcquisitionTimeoutConnectionManager(HttpClientOptions options) {
    this(new StrictConnPool<>(options.maxConnections(), options.maxConnections()), options);
  }

  private AcquisitionTimeoutConnectionManager(
      StrictConnPool<HttpRoute, ManagedAsyncClientConnection> pool, HttpClientOptions options) {
    super(pool, new DefaultAsyncClientConnectionOperator(
        RegistryBuilder.<TlsStrategy>create()
            .register("https", ClientTlsStrategyBuilder.create()
                .setHostnameVerifier(new DefaultHostnameVerifier()).buildAsync()).build(),
        DefaultSchemePortResolver.INSTANCE, SystemDefaultDnsResolver.INSTANCE) {});
    setDefaultConnectionConfig(ConnectionConfig.custom()
        .setConnectTimeout(Timeout.of(options.connectTimeout()))
        .setSocketTimeout(Timeout.of(options.readWriteTimeout()))
        .setValidateAfterInactivity(TimeValue.ofSeconds(2))
        .build());
    timeoutExecutor = Executors.newSingleThreadScheduledExecutor(task -> {
      Thread thread = new Thread(task, "multicloudj-ali-acquisition-timeout");
      thread.setDaemon(true);
      return thread;
    });
    // StrictConnPool otherwise reports expired async leases only when pool activity resumes.
    timeoutTask = timeoutExecutor.scheduleWithFixedDelay(
        pool::validatePendingRequests, 10, 10, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    close(CloseMode.GRACEFUL);
  }

  @Override
  public void close(CloseMode closeMode) {
    timeoutTask.cancel(false);
    timeoutExecutor.shutdownNow();
    super.close(closeMode);
  }
}
