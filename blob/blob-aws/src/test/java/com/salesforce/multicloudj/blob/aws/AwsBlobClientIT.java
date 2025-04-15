package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.blob.client.AbstractBlobClientIT;
import com.salesforce.multicloudj.blob.driver.AbstractBlobClient;
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

public class AwsBlobClientIT extends AbstractBlobClientIT {

    private static final String endpoint = "https://s3.us-west-2.amazonaws.com";
    private static final String region = "us-west-2";

    @Override
    protected AbstractBlobClientIT.Harness createHarness() {
        return new HarnessImpl();
    }

    public static class HarnessImpl implements AbstractBlobClientIT.Harness {
        int port = ThreadLocalRandom.current().nextInt(1000, 10000);

        SdkHttpClient httpClient;
        S3Client client;

        @Override
        public AbstractBlobClient<?> createBlobClient(boolean useValidCredentials) {

            String accessKeyId = System.getenv().getOrDefault("ACCESS_KEY_ID", "FAKE_ACCESS_KEY");
            String secretAccessKey = System.getenv().getOrDefault("SECRET_ACCESS_KEY", "FAKE_SECRET_ACCESS_KEY");
            String sessionToken = System.getenv().getOrDefault("SESSION_TOKEN", "FAKE_SESSION_TOKEN");

            if (!useValidCredentials) {
                accessKeyId = "invalidAccessKey";
                secretAccessKey = "invalidSecretAccessKey";
                sessionToken = "invalidSessionToken";
            }

            StsCredentials sessionCreds = new StsCredentials(accessKeyId, secretAccessKey, sessionToken);
            CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                    .withSessionCredentials(sessionCreds).build();

            return createBlobClient(credentialsOverrider);
        }

        private AbstractBlobClient<?> createBlobClient(final CredentialsOverrider credentialsOverrider) {

            AwsSessionCredentials awsCreds = AwsSessionCredentials.create(
                    credentialsOverrider.getSessionCredentials().getAccessKeyId(),
                    credentialsOverrider.getSessionCredentials().getAccessKeySecret(),
                    credentialsOverrider.getSessionCredentials().getSecurityToken());

            httpClient = TestsUtilAws.getProxyClient("https", port);
            client = S3Client.builder()
                    .region(Region.US_WEST_2)
                    .httpClient(httpClient)
                    .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                    .endpointOverride(URI.create(endpoint))
                    .serviceConfiguration(S3Configuration.builder()
                            .pathStyleAccessEnabled(true)
                            .build())
                    .build();

            AwsBlobClient.Builder builder = new AwsBlobClient.Builder();
            builder.withEndpoint(URI.create(endpoint))
                    .withRegion(region)
                    .withCredentialsOverrider(credentialsOverrider);

            return builder.build(client);
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
