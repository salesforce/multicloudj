package com.salesforce.multicloudj.blob.inmemory;

import com.salesforce.multicloudj.blob.client.AbstractBlobStoreIT;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.driver.ChecksumMethod;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

public class InMemoryBlobStoreIT extends AbstractBlobStoreIT {

  private static final String bucketName = "test-bucket";
  private static final String versionedBucketName = "test-bucket-versioned";
  private static final String nonExistentBucketName = "non-existent-bucket";
  private static final String region = "us-west-2";

  @AfterEach
  public void cleanup() {
    // Clear the in-memory storage after each test (includes buckets)
    InMemoryBlobStore.clearStorage();
  }

  @Test
  @Override
  public void testInvalidCredentials() {
    // This test doesn't apply to in memory blobstore.
    Assumptions.assumeTrue(true, "testInvalidCredentials");
  }

  @Test
  @Override
  public void testListBlobVersions_happy() {
    // In-memory blob store does not implement listBlobVersions.
    Assumptions.assumeTrue(false, "List object versions not supported by in-memory provider");
  }

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements Harness {

    @Override
    public AbstractBlobStore createBlobStore(
        boolean useValidBucket, boolean useValidCredentials, boolean useVersionedBucket) {
      // For in-memory, credentials don't matter
      String bucketNameToUse =
          useValidBucket
              ? (useVersionedBucket ? versionedBucketName : bucketName)
              : nonExistentBucketName;

      // Create the bucket if it should exist
      if (useValidBucket) {
        InMemoryBlobStore.createBucket(bucketNameToUse);
      }

      return new InMemoryBlobStore.Builder().withBucket(bucketNameToUse).withRegion(region).build();
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
    public boolean isDirectoryUploadSupported() {
      return false;
    }

    @Override
    public Set<ChecksumMethod> getSupportedChecksumAlgorithmsForUpload() {
      // The in-memory test double computes every algorithm locally, so it validates them all.
      return Set.of(
          ChecksumMethod.CRC32C,
          ChecksumMethod.SHA256,
          ChecksumMethod.CRC64,
          ChecksumMethod.MD5);
    }

    @Override
    public void close() {
      // Nothing to close for in-memory implementation
    }
  }
}
