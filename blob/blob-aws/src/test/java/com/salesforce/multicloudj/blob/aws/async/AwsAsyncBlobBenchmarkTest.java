package com.salesforce.multicloudj.blob.aws.async;

import com.salesforce.multicloudj.blob.async.client.AbstractAsyncBlobBenchmarkTest;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStore;

public class AwsAsyncBlobBenchmarkTest extends AbstractAsyncBlobBenchmarkTest {

  private static final String BUCKET_NAME =
      System.getProperty("benchmark.aws.bucket", "multicloudj-dir-benchmark-uswest2");
  private static final String REGION =
      System.getProperty("benchmark.aws.region", "us-west-2");

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  @Override
  protected String getProviderId() {
    return "aws";
  }

  public static class HarnessImpl extends BaseHarnessImpl {
    @Override
    protected AsyncBlobStore buildStore() {
      return AwsAsyncBlobStore.builder().withBucket(BUCKET_NAME).withRegion(REGION).build();
    }
  }
}
