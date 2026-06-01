package com.salesforce.multicloudj.blob.gcp;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.salesforce.multicloudj.blob.client.AbstractBlobBenchmarkTest;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GcpBlobBenchmarkTest extends AbstractBlobBenchmarkTest {

  private static final Logger logger = LoggerFactory.getLogger(GcpBlobBenchmarkTest.class);

  @Override
  protected String getProviderId() {
    return "gcp";
  }

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements Harness {
    Storage storage;

    @Override
    public AbstractBlobStore createBlobStore() {
      String projectId = requireEnv("BLOB_BENCHMARK_GCP_PROJECT_ID");
      String bucket = requireEnv("BLOB_BENCHMARK_GCP_BUCKET");

      logger.info(
          "Creating GCP Cloud Storage blob store with project: {}, bucket: {}",
          projectId, bucket);

      try {
        storage = StorageOptions.newBuilder()
            .setProjectId(projectId)
            .build()
            .getService();

        logger.info("Successfully created Storage client");

        AbstractBlobStore blobStore =
            new GcpBlobStore.Builder().withStorage(storage).withBucket(bucket).build();

        logger.info("Successfully created GCP Cloud Storage blob store");
        return blobStore;

      } catch (Exception e) {
        logger.error("Failed to create GCP Cloud Storage blob store", e);
        storage = null;
        throw new RuntimeException("Failed to create GCP Cloud Storage blob store", e);
      }
    }

    @Override
    public String getBucketName() {
      return requireEnv("BLOB_BENCHMARK_GCP_BUCKET");
    }

    @Override
    public void close() throws Exception {
      if (storage != null) {
        storage = null;
      }
    }
  }
}
