package com.salesforce.multicloudj.blob.ali.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.aliyun.sdk.service.oss2.transport.HttpClientOptions;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hc.client5.http.HttpRoute;
import org.apache.hc.client5.http.nio.AsyncConnectionEndpoint;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.util.DeadlineTimeoutException;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.junit.jupiter.api.Test;

class AcquisitionTimeoutConnectionManagerTest {

  private static final HttpRoute ROUTE = new HttpRoute(new HttpHost("https", "example.com", 443));

  @Test
  void timesOutPendingLeaseWithoutReleasingAnAcquiredEndpoint() throws Exception {
    try (AcquisitionTimeoutConnectionManager manager = new AcquisitionTimeoutConnectionManager(
        HttpClientOptions.custom().maxConnections(1).build())) {
      AsyncConnectionEndpoint held = lease(manager).get(1, TimeUnit.SECONDS);
      try {
        CompletableFuture<AsyncConnectionEndpoint> pending = lease(manager);
        ExecutionException failure = assertThrows(ExecutionException.class,
            () -> pending.get(2, TimeUnit.SECONDS));

        assertInstanceOf(DeadlineTimeoutException.class, failure.getCause());
        assertEquals(1, manager.getTotalStats().getLeased());
        assertEquals(1, manager.getMaxTotal());
      } finally {
        manager.release(held, null, TimeValue.ZERO_MILLISECONDS);
      }

      AsyncConnectionEndpoint recovered = lease(manager).get(1, TimeUnit.SECONDS);
      assertNotNull(recovered);
      manager.release(recovered, null, TimeValue.ZERO_MILLISECONDS);
      assertEquals(0, manager.getTotalStats().getLeased());
    }
  }

  private static CompletableFuture<AsyncConnectionEndpoint> lease(
      AcquisitionTimeoutConnectionManager manager) {
    CompletableFuture<AsyncConnectionEndpoint> result = new CompletableFuture<>();
    manager.lease("test", ROUTE, null, Timeout.ofMilliseconds(100), new FutureCallback<>() {
      @Override
      public void completed(AsyncConnectionEndpoint endpoint) {
        result.complete(endpoint);
      }

      @Override
      public void failed(Exception failure) {
        result.completeExceptionally(failure);
      }

      @Override
      public void cancelled() {
        result.cancel(false);
      }
    });
    return result;
  }
}
