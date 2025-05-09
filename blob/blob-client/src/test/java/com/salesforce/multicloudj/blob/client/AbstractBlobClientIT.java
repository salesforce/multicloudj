package com.salesforce.multicloudj.blob.client;

import com.salesforce.multicloudj.blob.driver.AbstractBlobClient;
import com.salesforce.multicloudj.blob.driver.BucketInfo;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractBlobClientIT {

    // Define the Harness interface
    public interface Harness extends AutoCloseable {

        // Method to create a blob driver
        AbstractBlobClient<?> createBlobClient(boolean useValidCredentials);

        // provide the BlobClient endpoint in provider
        String getEndpoint();

        // provide the provider ID
        String getProviderId();

        // Wiremock server need the https port, if
        // we make it constant at abstract class, we won't be able
        // to run tests in parallel. Each provider can provide the
        // randomly selected port number.
        int getPort();
    }

    protected abstract Harness createHarness();

    private Harness harness;

    /**
     * Initializes the WireMock server before all tests.
     */
    @BeforeAll
    public void initializeWireMockServer() {
        harness = createHarness();
        TestsUtil.startWireMockServer("src/test/resources", harness.getPort());
    }

    /**
     * Shuts down the WireMock server after all tests.
     */
    @AfterAll
    public void shutdownWireMockServer() throws Exception {

        TestsUtil.stopWireMockServer();
        harness.close();
    }

    /**
     * Initialize the harness and
     */
    @BeforeEach
    public void setupTestEnvironment() {
        TestsUtil.startWireMockRecording(harness.getEndpoint());
    }

    /**
     * Cleans up the test environment after each test.
     */
    @AfterEach
    public void cleanupTestEnvironment() {
        TestsUtil.stopWireMockRecording();
    }


    @Test
    public void testListBuckets() {

        // Create the blobClient driver for listing buckets
        AbstractBlobClient<?> blob = harness.createBlobClient(true);
        BlobClient blobClient = new BlobClient(blob);

        var resp = blobClient.listBuckets();

        List<BucketInfo> buckets = resp.getBucketInfoList();

        Assertions.assertNotNull(buckets);
        Assertions.assertFalse(buckets.isEmpty());

        for (BucketInfo bucket : buckets) {
            Assertions.assertNotNull(bucket.getName());
            Assertions.assertNotNull(bucket.getCreationDate());
        }
    }

    @Test
    public void testInvalidCredentials() {

        // Create the blobstore driver for a bucket that exists, but use invalid credentialsOverrider
        AbstractBlobClient<?> blob = harness.createBlobClient(false);
        BlobClient blobClient = new BlobClient(blob);

        com.salesforce.multicloudj.blob.driver.ListBucketsResponse resp = null;
        try {
            resp = blobClient.listBuckets();
        } catch (Throwable throwable) {
            Assertions.assertTrue(throwable instanceof InvalidArgumentException);
            Assertions.assertTrue(throwable.getMessage().contains("Access Key Id"));
        }
    }
}
