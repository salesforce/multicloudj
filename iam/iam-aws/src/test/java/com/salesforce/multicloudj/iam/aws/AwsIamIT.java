package com.salesforce.multicloudj.iam.aws;

import com.salesforce.multicloudj.common.aws.util.TestsUtilAws;
import com.salesforce.multicloudj.iam.client.AbstractIamIT;
import com.salesforce.multicloudj.iam.driver.AbstractIam;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.IamClientBuilder;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class AwsIamIT extends AbstractIamIT {
    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    public static class HarnessImpl implements AbstractIamIT.Harness {
        SdkHttpClient httpClient;
        IamClient client;
        int port = ThreadLocalRandom.current().nextInt(1000, 10000);

        @Override
        public AbstractIam createIamDriver() {
            httpClient = TestsUtilAws.getProxyClient("https", port);

            IamClientBuilder builder = IamClient.builder()
                    .httpClient(httpClient)
                    .region(Region.US_EAST_1)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsSessionCredentials.create(
                            System.getenv().getOrDefault("AWS_ACCESS_KEY_ID", "FAKE_ACCESS_KEY"),
                            System.getenv().getOrDefault("AWS_SECRET_ACCESS_KEY", "FAKE_SECRET_ACCESS_KEY"),
                            System.getenv().getOrDefault("AWS_SESSION_TOKEN", "FAKE_SESSION_TOKEN"))))
                    .endpointOverride(
                            URI.create("https://iam.amazonaws.com"));

            if (System.getProperty("record") != null) {
                builder.credentialsProvider(ProfileCredentialsProvider.create());
            }

            client = builder.build();
            return new AwsIam.Builder()
                    .withIamClient(client)
                    .withRegion("us-west-2")
                    .build();
        }

        @Override
        public String getIdentityName() {
            return "testSa";
        }

        @Override
        public String getTenantId() {
            return "654654370895";
        }

        @Override
        public String getRegion() {
            return "us-west-2";
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
        public List<String> getWiremockExtensions() {
            return List.of("com.salesforce.multicloudj.iam.aws.ReplaceAuthHeaderTransformer");
        }

        @Override
        public String getIamEndpoint() {
            return "https://iam.amazonaws.com";
        }

        @Override
        public String getTrustedPrincipal() {
            return "arn:aws:iam::654654370895:role/chameleon-jcloud-test";
        }

        @Override
        public String getTestIdentityName() {
            return "MultiCloudJTestRole";
        }

        @Override
        public String getPolicyVersion() {
            return "2012-10-17";
        }

        @Override
        public String getTestPolicyEffect() {
            return "Allow";
        }

        @Override
        public List<String> getTestPolicyActions() {
            return List.of("s3:GetObject", "s3:PutObject");
        }

        @Override
        public String getTestPolicyName() {
            return "TestPolicy";
        }

        @Override
        public String getTestRoleName() {
            return "testSa"; // IAM role name for attach/getInlinePolicyDetails.
        }

        /** Dummy S3 resource ARN for the test inline policy (valid format; used in record/replay IT). */
        private static final String TEST_POLICY_RESOURCE = "arn:aws:s3:::multicloudj-iam-it-test-bucket/*";

        @Override
        public String getTestPolicyResource() {
            return TEST_POLICY_RESOURCE;
        }

        @Override
        public void close() {
            if (client != null) {
                client.close();
            }
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }
}
