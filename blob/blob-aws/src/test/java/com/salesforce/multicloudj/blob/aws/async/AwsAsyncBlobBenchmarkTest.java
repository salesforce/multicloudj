package com.salesforce.multicloudj.blob.aws.async;

import com.salesforce.multicloudj.blob.async.client.AbstractAsyncBlobBenchmarkTest;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsAsyncBlobBenchmarkTest extends AbstractAsyncBlobBenchmarkTest {

  private static final Logger logger = LoggerFactory.getLogger(AwsAsyncBlobBenchmarkTest.class);

  private static final String BUCKET_NAME = "multicloudj-dir-benchmark-uswest2";
  private static final String REGION = "us-west-2";

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  @Override
  protected String getProviderId() {
    return "aws";
  }

  public static class HarnessImpl implements Harness {
    private AsyncBlobStore store;

    @Override
    public AsyncBlobStore createAsyncBlobStore() {
      logger.info(
          "Creating AWS async blob store with bucket: {}, region: {}", BUCKET_NAME, REGION);
      try {
        store = AwsAsyncBlobStore.builder().withBucket(BUCKET_NAME).withRegion(REGION).build();
        return store;
      } catch (Exception e) {
        logger.error("Failed to create AWS async blob store", e);
        throw new RuntimeException("Failed to create AWS async blob store", e);
      }
    }

    @Override
    public void close() {
      if (store != null) {
        try {
          store.close();
        } catch (Exception e) {
          logger.warn("Failed to close AWS async blob store", e);
        }
      }
    }
  }
}
