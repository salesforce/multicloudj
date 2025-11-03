package com.salesforce.multicloudj.pubsub.aws;

import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.aws.util.TestsUtilAws;
import com.salesforce.multicloudj.pubsub.client.AbstractPubsubIT;
import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Disabled;

public class AwsPubsubIT extends AbstractPubsubIT {

    private static final String SQS_ENDPOINT = "https://sqs.us-west-2.amazonaws.com";
    private static final String TEST_QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/654654370895/test-queue";

    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    // Disable some tests except send and receive
    @Override
    @Disabled("AWS IT tests disabled - only testing send and receive")
    public void testAckAfterReceive() throws Exception {
        super.testAckAfterReceive();
    }

    @Override
    @Disabled("AWS IT tests disabled - only testing send and receive")
    public void testNackAfterReceive() throws Exception {
        super.testNackAfterReceive();
    }

    @Override
    @Disabled("AWS IT tests disabled - only testing send and receive")
    public void testBatchAck() throws Exception {
        super.testBatchAck();
    }

    @Override
    @Disabled("AWS IT tests disabled - only testing send and receive")
    public void testBatchNack() throws Exception {
        super.testBatchNack();
    }

    @Override
    @Disabled("AWS IT tests disabled - only testing send and receive")
    public void testAckNullThrows() throws Exception {
        super.testAckNullThrows();
    }

    @Override
    @Disabled("AWS IT tests disabled - only testing send and receive")
    public void testDoubleAck() throws Exception {
        super.testDoubleAck();
    }

    public static class HarnessImpl implements Harness {
        private AwsTopic topic;
        private AwsSubscription subscription;
        private SqsClient sqsClient;
        private SdkHttpClient httpClient;
        private int port = ThreadLocalRandom.current().nextInt(1000, 10000);

        @Override
        public AbstractTopic createTopicDriver() {
            httpClient = TestsUtilAws.getProxyClient("https", port);
            
            String accessKey = System.getenv().getOrDefault("ACCESS_KEY_ID", "FAKE_ACCESS_KEY");
            String secretKey = System.getenv().getOrDefault("SECRET_ACCESS_KEY", "FAKE_SECRET_ACCESS_KEY");
            String sessionToken = System.getenv().getOrDefault("SESSION_TOKEN", "FAKE_SESSION_TOKEN");
            
            SqsClientBuilder sqsBuilder = SqsClient.builder()
                .httpClient(httpClient)
                .region(Region.US_WEST_2)  
                .credentialsProvider(StaticCredentialsProvider.create(AwsSessionCredentials.create(
                    accessKey, secretKey, sessionToken)))
                .endpointOverride(URI.create(SQS_ENDPOINT));
            
            sqsClient = sqsBuilder.build();
            
            AwsTopic.Builder topicBuilder = new AwsTopic.Builder();
            topicBuilder.withTopicName(TEST_QUEUE_URL);
            topicBuilder.withSqsClient(sqsClient);
            topic = new AwsTopic(topicBuilder);
            
            return topic;
        }

        @Override
        public AbstractSubscription createSubscriptionDriver() {
            AwsSubscription.Builder subscriptionBuilder = new AwsSubscription.Builder();
            subscriptionBuilder.withSubscriptionName(TEST_QUEUE_URL);
            subscriptionBuilder.withWaitTimeSeconds(1); // Use 1 second wait time for conformance tests
            subscriptionBuilder.withSqsClient(sqsClient);
            subscription = new AwsSubscription(subscriptionBuilder);
            
            return subscription;
        }

        @Override
        public String getPubsubEndpoint() {
            return SQS_ENDPOINT;
        }

        @Override
        public String getProviderId() {
            return AwsConstants.PROVIDER_ID;
        }

        @Override
        public int getPort() {
            return port;
        }

        @Override
        public List<String> getWiremockExtensions() {
            return List.of();
        }

        @Override
        public void close() throws Exception {
            if (topic != null) {
                topic.close();
            }
            if (subscription != null) {
                subscription.close();
            }
            if (sqsClient != null) {
                sqsClient.close();
            }
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }
}
