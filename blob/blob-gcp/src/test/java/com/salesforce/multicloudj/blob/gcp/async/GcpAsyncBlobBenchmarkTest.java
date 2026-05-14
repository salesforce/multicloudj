package com.salesforce.multicloudj.blob.gcp.async;

import com.salesforce.multicloudj.blob.async.client.AbstractAsyncBlobBenchmarkTest;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStore;

public class GcpAsyncBlobBenchmarkTest extends AbstractAsyncBlobBenchmarkTest {

  private static final String BUCKET_NAME =
      System.getProperty("benchmark.gcp.bucket", "multicloudj-dir-benchmark-uswest1");
  private static final String REGION =
      System.getProperty("benchmark.gcp.region", "us-west1");

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  @Override
  protected String getProviderId() {
    return "gcp";
  }

  public static class HarnessImpl extends BaseHarnessImpl {
    @Override
    protected AsyncBlobStore buildStore() {
      return GcpAsyncBlobStore.builder().withBucket(BUCKET_NAME).withRegion(REGION).build();
    }
  }
}
