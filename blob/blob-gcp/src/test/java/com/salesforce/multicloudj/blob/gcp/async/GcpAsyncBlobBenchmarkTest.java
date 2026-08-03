package com.salesforce.multicloudj.blob.gcp.async;

import com.salesforce.multicloudj.blob.async.client.AbstractAsyncBlobBenchmarkTest;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStore;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GcpAsyncBlobBenchmarkTest extends AbstractAsyncBlobBenchmarkTest {

  @Override
  protected String getProviderId() {
    return "gcp";
  }

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl extends BaseHarnessImpl {
    @Override
    protected AsyncBlobStore buildStore() {
      String bucket = requireEnv("BLOB_BENCHMARK_GCP_BUCKET");
      GcpAsyncBlobStore.Builder builder = GcpAsyncBlobStore.builder();
      builder.withBucket(bucket);
      // Controlled experiment toggle: -Dbench.gcp.tuned=true raises the Apache HTTP
      // per-route connection cap (default 20) and the TransferManager worker pool so we can
      // measure whether connection-pool saturation is the directory/small-op bottleneck.
      if (Boolean.getBoolean("bench.gcp.tuned")) {
        int maxConn = Integer.getInteger("bench.gcp.maxConnections", 200);
        int tmPool = Integer.getInteger("bench.gcp.tmPoolSize", 64);
        builder.withMaxConnections(maxConn);
        builder.withTransferManagerThreadPoolSize(tmPool);
      }
      return builder.build();
    }
  }
}
