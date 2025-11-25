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
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

public class AwsPubsubIT extends AbstractPubsubIT {

    private static final String SQS_ENDPOINT = "https://sqs.us-west-2.amazonaws.com";
    private static final String ACCOUNT_ID = "654654370895";
    private static final String BASE_QUEUE_NAME = "test-queue";

    private HarnessImpl harnessImpl;
    private String currentQueueUrl;

    @Override
    protected Harness createHarness() {
        harnessImpl = new HarnessImpl();
        return harnessImpl;
    }

    /**
     * Generate a unique queue URL for each test.
     * Uses test method name so the same test always uses the same queue.
     * Topic creates queue, Subscription does not.
     */
    @BeforeEach
    public void setupTestQueue(TestInfo testInfo) {
        String testMethodName = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        String queueName = BASE_QUEUE_NAME + "-" + testMethodName;
        currentQueueUrl = String.format("https://sqs.us-west-2.amazonaws.com/%s/%s", ACCOUNT_ID, queueName);
        if (harnessImpl != null) {
            harnessImpl.setQueueUrl(currentQueueUrl);
        }
    }

    public static class HarnessImpl implements Harness {
        private AwsTopic topic;
        private AwsSubscription subscription;
        private SqsClient sqsClient;
        private SdkHttpClient httpClient;
        private int port = ThreadLocalRandom.current().nextInt(1000, 10000);
        private String queueUrl = String.format("https://sqs.us-west-2.amazonaws.com/%s/%s", ACCOUNT_ID, BASE_QUEUE_NAME);

        public void setQueueUrl(String queueUrl) {
            this.queueUrl = queueUrl;
        }

        @Override
        public AbstractTopic createTopicDriver() {
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

            // Topic creates queue if it doesn't exist (idempotent)
            // Extract queue name from queue URL: https://sqs.region.amazonaws.com/account/queue-name
            String queueName = queueUrl.substring(queueUrl.lastIndexOf('/') + 1);
            if (System.getProperty("record") != null) {
                // In record mode, create queue if it doesn't exist 
                try {
                    try {
                        sqsClient.getQueueUrl(GetQueueUrlRequest.builder()
                            .queueName(queueName)
                            .build());
                    } catch (QueueDoesNotExistException e) {
                        sqsClient.createQueue(CreateQueueRequest.builder()
                            .queueName(queueName)
                            .build());
                    }
                } catch (Exception e) {
                    System.err.println("Warning: Failed to create queue in createTopicDriver: " + e.getMessage());
                }
            }

            AwsTopic.Builder topicBuilder = new AwsTopic.Builder();
            System.out.println("createTopicDriver using queueUrl: " + queueUrl);
            topicBuilder.withTopicName(queueUrl);
            topicBuilder.withSqsClient(sqsClient);
            topic = new AwsTopic(topicBuilder);

            return topic;
        }

        @Override
        public AbstractSubscription createSubscriptionDriver() {
            // If queue doesn't exist, AWS will return error (e.g., AWS.SimpleQueueService.NonExistentQueue)
            AwsSubscription.Builder subscriptionBuilder = new AwsSubscription.Builder();
            System.out.println("createSubscriptionDriver using queueUrl: " + queueUrl);
            subscriptionBuilder.withSubscriptionName(queueUrl);
            subscriptionBuilder.withWaitTimeSeconds(1); // Use 1 second wait time for conformance tests
            subscriptionBuilder.withSqsClient(sqsClient);

            // Disable dynamic batch size adjustment by setting MaxHandlers=1 and MaxBatchSize=1
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
}
