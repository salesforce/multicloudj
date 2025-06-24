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

//@Disabled
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
            client = S3Client.builder()
                    .region(Region.US_EAST_2)
                    .endpointOverride(URI.create(endpoint))
                    .serviceConfiguration(S3Configuration.builder()
                            .pathStyleAccessEnabled(true)
                            .build())
                    .build();

            AwsBlobStore.Builder builder = new AwsBlobStore.Builder();
            builder.withS3Client(client)
                    .withEndpoint(URI.create(endpoint))
                    .withBucket(bucketName)
                    .withRegion(region);

            return builder.build();
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