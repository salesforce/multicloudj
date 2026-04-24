package com.salesforce.multicloudj.blob.gcp.async;

import com.salesforce.multicloudj.blob.async.client.AbstractAsyncBlobBenchmarkTest;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcpAsyncBlobBenchmarkTest extends AbstractAsyncBlobBenchmarkTest {

  private static final Logger logger = LoggerFactory.getLogger(GcpAsyncBlobBenchmarkTest.class);

  private static final String BUCKET_NAME = "multicloudj-dir-benchmark-uswest1";
  private static final String REGION = "us-west1";

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  @Override
  protected String getProviderId() {
    return "gcp";
  }

  public static class HarnessImpl implements Harness {
    private AsyncBlobStore store;

    @Override
    public AsyncBlobStore createAsyncBlobStore() {
      logger.info(
          "Creating GCP async blob store with bucket: {}, region: {}", BUCKET_NAME, REGION);
      try {
        store =
            GcpAsyncBlobStore.builder().withBucket(BUCKET_NAME).withRegion(REGION).build();
        return store;
      } catch (Exception e) {
        logger.error("Failed to create GCP async blob store", e);
        throw new RuntimeException("Failed to create GCP async blob store", e);
      }
    }

    @Override
    public void close() {
      if (store != null) {
        try {
          store.close();
        } catch (Exception e) {
          logger.warn("Failed to close GCP async blob store", e);
        }
      }
    }
  }
}
