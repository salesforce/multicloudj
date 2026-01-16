package com.salesforce.multicloudj.pubsub.aws;

import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.aws.util.TestsUtilAws;
import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.pubsub.client.AbstractPubsubIT;
import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

public class AwsPubsubIT extends AbstractPubsubIT {

    private static final String SQS_ENDPOINT = "https://sqs.us-west-2.amazonaws.com";
    private static final String BASE_QUEUE_NAME = "test-queue";

    private HarnessImpl harnessImpl;
    private String queueName;

    @Override
    protected Harness createHarness() {
        harnessImpl = new HarnessImpl();
        return harnessImpl;
    }

    /**
     * Generate a unique queue name for each test.
     * Uses test method name so the same test always uses the same queue.
     * Topic creates queue, Subscription does not.
     */
    @BeforeEach
    public void setupTestQueue(TestInfo testInfo) {
        String testMethodName = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        queueName = BASE_QUEUE_NAME + "-" + testMethodName;
        if (harnessImpl != null) {
            harnessImpl.setQueueName(queueName);
        }
    }

    public static class HarnessImpl implements Harness {
        private AwsTopic topic;
        private AwsSubscription subscription;
        private SqsClient sqsClient;
        private SdkHttpClient httpClient;
        private int port = ThreadLocalRandom.current().nextInt(1000, 10000);
        private String queueName = BASE_QUEUE_NAME;
        private String cachedQueueUrl; // Cache queue URL to avoid calling GetQueueUrl multiple times for the same queue

        public void setQueueName(String queueName) {
            this.queueName = queueName;
            this.cachedQueueUrl = null; // Reset cache when queue name changes
        }

        private SqsClient createSqsClient() {
            if (sqsClient == null) {
                httpClient = TestsUtilAws.getProxyClient("https", port);
                String accessKey = System.getenv().getOrDefault("AWS_ACCESS_KEY_ID", "FAKE_ACCESS_KEY");
                String secretKey = System.getenv().getOrDefault("AWS_SECRET_ACCESS_KEY", "FAKE_SECRET_ACCESS_KEY");
                String sessionToken = System.getenv().getOrDefault("AWS_SESSION_TOKEN", "FAKE_SESSION_TOKEN");
                SqsClientBuilder sqsBuilder = SqsClient.builder()
                    .httpClient(httpClient)
                    .region(Region.US_WEST_2)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsSessionCredentials.create(
                        accessKey, secretKey, sessionToken)))
                    .endpointOverride(URI.create(SQS_ENDPOINT));
                sqsClient = sqsBuilder.build();
            }
            return sqsClient;
        }

        /**
         * Ensures the queue exists before build() is called.
         * In record mode, we create the queue if it doesn't exist (without calling GetQueueUrl).
         * In replay mode, we don't do anything - only build() will call GetQueueUrl once.
         * This ensures only one GetQueueUrl mapping is generated per test.
         */
        private void ensureQueueExists() {
            if (System.getProperty("record") != null) {
                // In record mode, try to create queue if it doesn't exist
                // We don't call GetQueueUrl here to avoid generating multiple mappings
                // build() will call GetQueueUrl once, which will handle both existing and new queues
                try {
                    // Try to create the queue - CreateQueue is idempotent if queue already exists
                    sqsClient.createQueue(CreateQueueRequest.builder()
                        .queueName(queueName)
                        .build());
                } catch (Exception e) {
                    // If creation fails, build() will handle it when calling GetQueueUrl
                    System.err.println("Warning: Failed to create queue: " + e.getMessage());
                }
            }
            // In replay mode, do nothing - build() will call GetQueueUrl once
        }

        @Override
        public AbstractTopic<?> createTopicDriver() {
            sqsClient = createSqsClient();
            ensureQueueExists();

            // If queue URL is not cached, get it now (this will be the only GetQueueUrl call for this queue)
            if (cachedQueueUrl == null) {
                GetQueueUrlResponse response = sqsClient.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build());
                cachedQueueUrl = response.queueUrl();
            }

            AwsTopic.Builder topicBuilder = new AwsTopic.Builder();
            System.out.println("createTopicDriver using queueName: " + queueName);
            topicBuilder.withTopicName(queueName);
            topicBuilder.withSqsClient(sqsClient);
            topicBuilder.withTopicUrl(cachedQueueUrl); // Use cached URL to avoid calling GetQueueUrl again
            topic = topicBuilder.build();

            return topic;
        }

        @Override
        public AbstractSubscription<?> createSubscriptionDriver() {
            sqsClient = createSqsClient();
            ensureQueueExists();

            // If queue URL is not cached, get it now (this will be the only GetQueueUrl call for this queue)
            if (cachedQueueUrl == null) {
                GetQueueUrlResponse response = sqsClient.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build());
                cachedQueueUrl = response.queueUrl();
            }

            AwsSubscription.Builder subscriptionBuilder = new AwsSubscription.Builder();
            System.out.println("createSubscriptionDriver using queueName: " + queueName);
            subscriptionBuilder.withSubscriptionName(queueName);
            subscriptionBuilder.withWaitTimeSeconds(1); // Use 1 second wait time for conformance tests
            subscriptionBuilder.withSqsClient(sqsClient);
            subscriptionBuilder.subscriptionUrl = cachedQueueUrl;

            subscriptionBuilder.build(); // This will use the cached URL, not call GetQueueUrl
            subscription = new AwsSubscription(subscriptionBuilder) {
                @Override
                protected Batcher.Options createReceiveBatcherOptions() {
                    return new Batcher.Options()
                        .setMaxHandlers(1)
                        .setMinBatchSize(1)
                        .setMaxBatchSize(1)
                        .setMaxBatchByteSize(0);
                }
            };

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

    // TODO: Implement testSendReceiveTwo() when WireMock supports multiple upstream endpoints
    // Currently disabled because WireMock only supports a single upstream endpoint per recording session.
    // SNS workflows require both SNS and SQS APIs, which can't be fully tested in one WireMock setup.
    // Routing SQS requests through a WireMock proxy configured with an SNS target results in
    // SQS APIs being forwarded to the SNS endpoint, which fails with UnknownOperationException.
}
