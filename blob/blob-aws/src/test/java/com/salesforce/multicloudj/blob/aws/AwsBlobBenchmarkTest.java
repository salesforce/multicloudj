package com.salesforce.multicloudj.blob.aws;
import com.salesforce.multicloudj.blob.client.AbstractBlobBenchmarkTest;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.common.aws.util.TestsUtilAws;
import com.salesforce.multicloudj.common.util.common.TestsUtil;

import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import java.net.URI;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.runner.RunnerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Disabled
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AwsBlobBenchmarkTest extends AbstractBlobBenchmarkTest {

    private static final Logger logger = LoggerFactory.getLogger(AwsBlobBenchmarkTest.class);
    private static final String endpoint = "https://s3.us-east-2.amazonaws.com";
    private static final String bucketName = "chameleon-jcloud-benchmarks";
    private static final String region = "us-east-2";

    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    public static class HarnessImpl implements Harness {
        SdkHttpClient httpClient;
        S3Client client;

        @Override
        public AbstractBlobStore<?> createBlobStore() {
            logger.info("Creating AWS blob store with endpoint: {}, bucket: {}, region: {}", 
                    endpoint, bucketName, region);
            
            try {
                URI endpointUri;
                try {
                    endpointUri = URI.create(endpoint);
                    logger.debug("Successfully parsed endpoint URI: {}", endpointUri);
                } catch (IllegalArgumentException e) {
                    logger.error("Invalid endpoint URI: {}", endpoint, e);
                    throw new RuntimeException("Failed to parse endpoint URI: " + endpoint, e);
                }
                logger.debug("Building S3 client with region: {}", Region.US_EAST_2);
                client = S3Client.builder()
                        .region(Region.US_EAST_2)
                        .endpointOverride(endpointUri)
                        .serviceConfiguration(S3Configuration.builder()
                                .pathStyleAccessEnabled(true)
                                .build())
                        .build();
                
                logger.info("Successfully created S3 client");

                logger.debug("Building AwsBlobStore with bucket: {}", bucketName);
                AwsBlobStore.Builder builder = new AwsBlobStore.Builder();
                builder.withS3Client(client)
                        .withEndpoint(endpointUri)
                        .withBucket(bucketName)
                        .withRegion(region);

                AbstractBlobStore<?> blobStore = builder.build();
                logger.info("Successfully created AWS blob store");
                
                return blobStore;
                
            } catch (Exception e) {
                logger.error("Failed to create AWS blob store", e);
                
                if (client != null) {
                    try {
                        client.close();
                        logger.debug("Cleaned up S3 client after failure");
                    } catch (Exception cleanupException) {
                        logger.warn("Failed to cleanup S3 client after blob store creation failure", cleanupException);
                    }
                    client = null;
                }
                
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new RuntimeException("Failed to create AWS blob store", e);
                }
            }
        }

        @Override
        public String getBucketName() {
            return bucketName;
        }

        @Override
        public void close() throws Exception {
            if (client != null) {
                client.close();
            }
            if (httpClient != null) {
                httpClient.close();
            }

        }
    }
}