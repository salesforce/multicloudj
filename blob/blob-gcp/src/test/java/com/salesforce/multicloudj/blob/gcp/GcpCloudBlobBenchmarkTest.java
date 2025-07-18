package com.salesforce.multicloudj.blob.gcp;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.salesforce.multicloudj.blob.client.AbstractBlobBenchmarkTest;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.jupiter.api.Disabled;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GcpCloudBlobBenchmarkTest extends AbstractBlobBenchmarkTest {

    private static final Logger logger = LoggerFactory.getLogger(GcpCloudBlobBenchmarkTest.class);
    private static final String projectId = System.getProperty("gcp.project.id",
            System.getenv().getOrDefault("GOOGLE_CLOUD_PROJECT", "substrate-sdk-gcp-poc1"));
    private static final String bucketName = System.getProperty("gcp.bucket.name", "gcp-benchmark-test");

    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    public static class HarnessImpl implements Harness {
        Storage storage;

        @Override
        public AbstractBlobStore<?> createBlobStore() {
            logger.info("Creating GCP Cloud Storage blob store with project: {}, bucket: {}",
                    projectId, bucketName);

            try {
                Storage client = StorageOptions.getDefaultInstance().getService();
                if (storage == null) {
                    storage = client;
                }

                logger.info("Successfully created Storage client");

                AbstractBlobStore<?> blobStore = new GcpBlobStore.Builder()
                        .withStorage(client)
                        .withBucket(bucketName)
                        .build();

                logger.info("Successfully created GCP Cloud Storage blob store");
                return blobStore;

            } catch (Exception e) {
                logger.error("Failed to create GCP Cloud Storage blob store", e);

                if (storage != null) {
                    try {
                        logger.debug("Storage client will be garbage collected");
                    } catch (Exception cleanupException) {
                        logger.warn("Cleanup warning", cleanupException);
                    }
                    storage = null;
                }

                throw new RuntimeException("Failed to create GCP Cloud Storage blob store", e);
            }
        }

        @Override
        public String getBucketName() {
            return bucketName;
        }

        @Override
        public void close() throws Exception {
            if (storage != null) {
                logger.debug("Storage client resources released");
                storage = null;
            }
        }
    }
}