package com.salesforce.multicloudj.blob.inmemory;

import com.salesforce.multicloudj.blob.client.AbstractBlobStoreIT;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import org.junit.jupiter.api.AfterEach;

public class InMemoryBlobStoreIT extends AbstractBlobStoreIT {

    private static final String bucketName = "test-bucket";
    private static final String versionedBucketName = "test-bucket-versioned";
    private static final String nonExistentBucketName = "non-existent-bucket";
    private static final String region = "us-west-2";

    @AfterEach
    public void cleanup() {
        // Clear the in-memory storage after each test
        InMemoryBlobStore.clearStorage();
        InMemoryBlobClient.clearBuckets();
    }

    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    public static class HarnessImpl implements Harness {

        @Override
        public AbstractBlobStore createBlobStore(boolean useValidBucket, boolean useValidCredentials, boolean useVersionedBucket) {
            // For in-memory, credentials don't matter
            String bucketNameToUse = useValidBucket ? (useVersionedBucket ? versionedBucketName : bucketName) : nonExistentBucketName;

            // Create the bucket if it should exist
            if (useValidBucket) {
                InMemoryBlobClient client = new InMemoryBlobClient.Builder()
                        .withRegion(region)
                        .build();
                client.createBucket(bucketNameToUse);
            }

            return new InMemoryBlobStore.Builder()
                    .withBucket(bucketNameToUse)
                    .withRegion(region)
                    .build();
        }

        @Override
        public String getEndpoint() {
            return "http://localhost:8080";
        }

        @Override
        public String getProviderId() {
            return "inmemory";
        }

        @Override
        public String getMetadataHeader(String key) {
            return "x-inmemory-meta-" + key;
        }

        @Override
        public String getTaggingHeader() {
            return "x-inmemory-tagging";
        }

        @Override
        public int getPort() {
            return 8080;
        }

        @Override
        public String getKmsKeyId() {
            // In-memory doesn't support KMS encryption
            return null;
        }

        @Override
        public void close() {
            // Nothing to close for in-memory implementation
        }
    }
}
