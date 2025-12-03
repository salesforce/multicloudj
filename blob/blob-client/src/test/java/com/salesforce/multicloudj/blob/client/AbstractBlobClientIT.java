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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeoutException;

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
    @Disabled("this test is disabled for now because the recorded file conflicts with valid credentials test")
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

    @Test
    public void testCreateBucket() {
        // Create the blobClient driver
        AbstractBlobClient<?> blob = harness.createBlobClient(true);
        BlobClient blobClient = new BlobClient(blob);

        // Generate a unique bucket name for testing
        String bucketName = "test-bucket-create-multicloudj";

        // Create the bucket
        blobClient.createBucket(bucketName);

        // Verify the bucket was created by listing buckets
        try {
            waitForBucketVisible(blobClient, bucketName,
                    Duration.ofSeconds(30),   // total timeout
                    Duration.ofSeconds(2));   // poll interval
            System.out.println("Bucket is now visible");
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    public void waitForBucketVisible(
            BlobClient blobClient,
            String bucketName,
            Duration timeout,
            Duration pollInterval
    ) throws InterruptedException, TimeoutException {

        Instant deadline = Instant.now().plus(timeout);

        while (Instant.now().isBefore(deadline)) {
            var resp = blobClient.listBuckets();
            List<BucketInfo> buckets = resp.getBucketInfoList();

            boolean bucketExists = buckets.stream()
                    .anyMatch(bucket -> bucket.getName().equals(bucketName));

            if (bucketExists) {
                // Found it – return successfully
                return;
            }

            // Not found yet – wait before next attempt
            Thread.sleep(pollInterval.toMillis());
        }

        throw new TimeoutException(
                "Bucket '" + bucketName + "' was not visible after " + timeout.toSeconds() + " seconds");
    }
}
