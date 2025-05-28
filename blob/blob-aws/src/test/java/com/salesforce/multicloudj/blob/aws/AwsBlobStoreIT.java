package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.blob.client.AbstractBlobStoreIT;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.common.aws.util.TestsUtilAws;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import java.net.URI;
import java.util.concurrent.ThreadLocalRandom;

public class AwsBlobStoreIT extends AbstractBlobStoreIT {

    private static final String endpoint = "https://s3.us-west-2.amazonaws.com";
    private static final String bucketName = "chameleon-jcloud";
    private static final String versionedBucketName = "chameleon-jcloud-versioned";
    private static final String nonExistentBucketName = "java-bucket-does-not-exist";
    private static final String region = "us-west-2";

    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    public static class HarnessImpl implements Harness {
        int port = ThreadLocalRandom.current().nextInt(1000, 10000);

        SdkHttpClient httpClient;
        S3Client client;

        @Override
        public AbstractBlobStore<?> createBlobStore(boolean useValidBucket, boolean useValidCredentials, boolean useVersionedBucket){

            String accessKeyId = System.getenv().getOrDefault("ACCESS_KEY_ID", "FAKE_ACCESS_KEY");
            String secretAccessKey = System.getenv().getOrDefault("SECRET_ACCESS_KEY", "FAKE_SECRET_ACCESS_KEY");
            String sessionToken = System.getenv().getOrDefault("SESSION_TOKEN", "FAKE_SESSION_TOKEN");

            if(!useValidCredentials) {
                accessKeyId = "invalidAccessKey";
                secretAccessKey = "invalidSecretAccessKey";
                sessionToken = "invalidSessionToken";
            }

            StsCredentials sessionCreds = new StsCredentials(accessKeyId, secretAccessKey, sessionToken);
            CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                    .withSessionCredentials(sessionCreds).build();
            String bucketNameToUse = useValidBucket ? (useVersionedBucket ? versionedBucketName : bucketName) : nonExistentBucketName;

            return createBlobStore(bucketNameToUse, credentialsOverrider);
        }

        private AbstractBlobStore<?> createBlobStore(final String bucketName, final CredentialsOverrider credentialsOverrider){

            AwsSessionCredentials awsCredentials = AwsSessionCredentials.create(
                    credentialsOverrider.getSessionCredentials().getAccessKeyId(),
                    credentialsOverrider.getSessionCredentials().getAccessKeySecret(),
                    credentialsOverrider.getSessionCredentials().getSecurityToken());

            httpClient = TestsUtilAws.getProxyClient("https", port);
            client = S3Client.builder()
                    .region(Region.US_WEST_2)
                    .httpClient(httpClient)
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
        public String getMetadataHeader(String key) {
            return "x-amz-meta-" + key;
        }

        @Override
        public String getTaggingHeader() {
            return "x-amz-tagging";
        }

        @Override
        public int getPort() {
            return port;
        }

        @Override
        public void close() {
           client.close();
           httpClient.close();
        }
    }
}
