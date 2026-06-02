package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.blob.client.AbstractBlobBenchmarkTest;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AwsBlobBenchmarkTest extends AbstractBlobBenchmarkTest {

  @Override
  protected String getProviderId() {
    return "aws";
  }

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements Harness {
    AwsBlobStore blobStore;

    @Override
    public AbstractBlobStore createBlobStore() {
      String region = requireEnv("BLOB_BENCHMARK_AWS_REGION");
      String bucket = requireEnv("BLOB_BENCHMARK_AWS_BUCKET");
      blobStore = new AwsBlobStore.Builder().withBucket(bucket).withRegion(region).build();
      return blobStore;
    }

    @Override
    public String getBucketName() {
      return requireEnv("BLOB_BENCHMARK_AWS_BUCKET");
    }

    @Override
    public void close() {
      if (blobStore != null) {
        blobStore.close();
      }
    }
  }
}
