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
      String region = requireEnv("BLOB_BENCHMARK_GCP_REGION");
      return GcpAsyncBlobStore.builder().withBucket(bucket).withRegion(region).build();
    }
  }
}
