package com.salesforce.multicloudj.blob.aws;
import com.salesforce.multicloudj.blob.client.AbstractBlobBenchmarkTest;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.common.aws.util.TestsUtilAws;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
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
    private static final String versionedBucketName = "chameleon-jcloud-versioned";
    private static final String nonExistentBucketName = "java-bucket-does-not-exist";
    private static final String region = "us-east-2";

    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    @Override
    @Test
    public void runBenchmarks() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + this.getClass().getName() + ".*") 
                .forks(1)
                .warmupIterations(3)
                .measurementIterations(5)
                .resultFormat(org.openjdk.jmh.results.format.ResultFormatType.JSON)
                .result("target/jmh-results.json")
                .build();

        new Runner(opt).run();
    }

    public static class HarnessImpl implements Harness {
        SdkHttpClient httpClient;
        S3Client client;

        @Override
        public AbstractBlobStore<?> createBlobStore(boolean useValidBucket, boolean useValidCredentials, boolean useVersionedBucket) {
            String accessKeyId = System.getenv().getOrDefault("AWS_ACCESS_KEY_ID", "FAKE_ACCESS_KEY");
            String secretAccessKey = System.getenv().getOrDefault("AWS_SECRET_ACCESS_KEY", "FAKE_SECRET_ACCESS_KEY");
            String sessionToken = System.getenv().getOrDefault("AWS_SESSION_TOKEN", "FAKE_SESSION_TOKEN");

            StsCredentials sessionCreds = new StsCredentials(accessKeyId, secretAccessKey, sessionToken);
            CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                    .withSessionCredentials(sessionCreds).build();
            
            String bucketNameToUse = useValidBucket ? 
                    (useVersionedBucket ? versionedBucketName : bucketName) : 
                    nonExistentBucketName;

            return createBlobStore(bucketNameToUse, credentialsOverrider);
        }

        private AbstractBlobStore<?> createBlobStore(final String bucketName, final CredentialsOverrider credentialsOverrider) {
            AwsSessionCredentials awsCredentials = AwsSessionCredentials.create(
                    credentialsOverrider.getSessionCredentials().getAccessKeyId(),
                    credentialsOverrider.getSessionCredentials().getAccessKeySecret(),
                    credentialsOverrider.getSessionCredentials().getSecurityToken());


            client = S3Client.builder()
                    .region(Region.US_EAST_2)
                    .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
                    .endpointOverride(URI.create(endpoint))
                    .serviceConfiguration(S3Configuration.builder()
                            .pathStyleAccessEnabled(true)
                            .build())
                    .build();

            AwsBlobStore.Builder builder = new AwsBlobStore.Builder();
            builder.withS3Client(client)
                    .withEndpoint(URI.create(endpoint))
                    .withBucket(bucketName)
                    .withRegion(region)
                    .withCredentialsOverrider(credentialsOverrider);

            return builder.build();
        }

        @Override
        public String getEndpoint() {
            return endpoint;
        }

        @Override
        public String getProviderId() {
            return "aws";
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

