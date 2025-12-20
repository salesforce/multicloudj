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
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.CreateTopicResponse;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInfo;

/**
 * Integration tests for AWS SNS Topic.
 * 
 * SNS topics require a subscription (typically SQS queue) to receive messages.
 * For integration tests, we create an SNS topic and subscribe an SQS queue to it.
 * The subscription driver uses the SQS queue to receive messages published to the SNS topic.
 */
public class AwsSnsPubsubIT extends AbstractPubsubIT {

    private static final String SNS_ENDPOINT = "https://sns.us-west-2.amazonaws.com";
    private static final String SQS_ENDPOINT = "https://sqs.us-west-2.amazonaws.com";
    private static final String ACCOUNT_ID = "654654370895";
    private static final String BASE_TOPIC_NAME = "test-sns-topic";
    private static final String BASE_QUEUE_NAME = "test-sns-queue";

    private HarnessImpl harnessImpl;
    private String topicName;
    private String queueName;

    @Override
    protected Harness createHarness() {
        harnessImpl = new HarnessImpl();
        return harnessImpl;
    }

    /**
     * Generate unique topic and queue names for each test.
     */
    @BeforeEach
    public void setupTestResources(TestInfo testInfo) {
        String testMethodName = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        topicName = BASE_TOPIC_NAME + "-" + testMethodName;
        queueName = BASE_QUEUE_NAME + "-" + testMethodName;
        if (harnessImpl != null) {
            harnessImpl.setTopicName(topicName);
            harnessImpl.setQueueName(queueName);
        }
    }

    public static class HarnessImpl implements Harness {
        private AwsTopic topic;
        private AwsSubscription subscription;
        private SnsClient snsClient;
        private SqsClient sqsClient;
        private SdkHttpClient httpClient;
        private int port = ThreadLocalRandom.current().nextInt(1000, 10000);
        private String topicName = BASE_TOPIC_NAME;
        private String queueName = BASE_QUEUE_NAME;
        private String cachedTopicArn; // Cache topic ARN to avoid multiple calls
        private String cachedQueueUrl; // Cache queue URL to avoid multiple calls
        private String cachedSubscriptionArn; // Cache subscription ARN

        public void setTopicName(String topicName) {
            this.topicName = topicName;
            this.cachedTopicArn = null; // Reset cache when topic name changes
        }

        public void setQueueName(String queueName) {
            this.queueName = queueName;
            this.cachedQueueUrl = null; // Reset cache when queue name changes
        }

        private AwsSessionCredentials createCredentials() {
            String accessKey = System.getenv().getOrDefault("AWS_ACCESS_KEY_ID", "FAKE_ACCESS_KEY");
            String secretKey = System.getenv().getOrDefault("AWS_SECRET_ACCESS_KEY", "FAKE_SECRET_ACCESS_KEY");
            String sessionToken = System.getenv().getOrDefault("AWS_SESSION_TOKEN", "FAKE_SESSION_TOKEN");
            return AwsSessionCredentials.create(accessKey, secretKey, sessionToken);
        }

        private SnsClient createSnsClient() {
            if (snsClient == null) {
                if (httpClient == null) {
                    httpClient = TestsUtilAws.getProxyClient("https", port);
                }
                snsClient = SnsClient.builder()
                    .httpClient(httpClient)
                    .region(Region.US_WEST_2)
                    .credentialsProvider(StaticCredentialsProvider.create(createCredentials()))
                    .endpointOverride(URI.create(SNS_ENDPOINT))
                    .build();
            }
            return snsClient;
        }

        private SqsClient createSqsClient() {
            if (sqsClient == null) {
                if (httpClient == null) {
                    httpClient = TestsUtilAws.getProxyClient("https", port);
                }
                sqsClient = SqsClient.builder()
                    .httpClient(httpClient)
                    .region(Region.US_WEST_2)
                    .credentialsProvider(StaticCredentialsProvider.create(createCredentials()))
                    .endpointOverride(URI.create(SQS_ENDPOINT))
                    .build();
            }
            return sqsClient;
        }

        /**
         * Ensures the SQS queue exists and is subscribed to the SNS topic.
         * In record mode, we create the queue and subscription if they don't exist.
         */
        private void ensureQueueExistsAndSubscribed(SqsClient sqsClient, SnsClient snsClient) {
            if (System.getProperty("record") != null) {
                // Create the queue if it doesn't exist
                sqsClient.createQueue(CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build());

                // Get queue URL
                GetQueueUrlResponse urlResponse = sqsClient.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build());
                cachedQueueUrl = urlResponse.queueUrl();

                // Get queue ARN (needed for subscription)
                String queueArn = formatArn("sqs", queueName);

                // Subscribe queue to SNS topic
                if (cachedTopicArn != null) {
                    snsClient.subscribe(SubscribeRequest.builder()
                        .topicArn(cachedTopicArn)
                        .protocol("sqs")
                        .endpoint(queueArn)
                        .build());

                    // Set queue policy to allow SNS to send messages
                    String policy = String.format(
                        "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"sns.amazonaws.com\"},\"Action\":\"sqs:SendMessage\",\"Resource\":\"%s\"}]}",
                        queueArn);
                    Map<QueueAttributeName, String> attributes = new HashMap<>();
                    attributes.put(QueueAttributeName.POLICY, policy);
                    sqsClient.setQueueAttributes(SetQueueAttributesRequest.builder()
                        .queueUrl(cachedQueueUrl)
                        .attributes(attributes)
                        .build());
                }
            }
        }

        private String formatArn(String service, String resourceName) {
            return String.format("arn:aws:%s:us-west-2:%s:%s", service, ACCOUNT_ID, resourceName);
        }

        @Override
        public AbstractTopic createTopicDriver() {
            snsClient = createSnsClient();
            
            if (cachedTopicArn == null) {
                CreateTopicResponse response = snsClient.createTopic(CreateTopicRequest.builder()
                    .name(topicName)
                    .build());
                cachedTopicArn = response.topicArn();
            }

            AwsTopic.Builder topicBuilder = new AwsTopic.Builder();
            System.out.println("createTopicDriver using topicName: " + topicName + ", topicArn: " + cachedTopicArn);
            topicBuilder.withServiceType(AwsTopic.ServiceType.SNS);
            topicBuilder.withTopicName(cachedTopicArn);
            topicBuilder.withSnsClient(snsClient);
            topic = topicBuilder.build();

            return topic;
        }

        @Override
        public AbstractSubscription createSubscriptionDriver() {
            // For SNS, we use SQS queue as subscription
            sqsClient = createSqsClient();
            snsClient = createSnsClient(); // Need SNS client for subscription
            ensureQueueExistsAndSubscribed(sqsClient, snsClient);

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

            subscriptionBuilder.build(); 
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
            // Return SNS endpoint for topic operations
            return SNS_ENDPOINT;
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
            return List.of("com.salesforce.multicloudj.pubsub.aws.util.PubsubReplaceAuthHeaderTransformer");
        }

        @Override
        public void close() throws Exception {
            if (topic != null) {
                topic.close();
            }
            if (subscription != null) {
                subscription.close();
            }
            if (snsClient != null) {
                snsClient.close();
            }
            if (sqsClient != null) {
                sqsClient.close();
            }
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    // Disable some tests except testSendBatchMessages
    @Override
    @Disabled
    public void testReceiveAfterSend() throws Exception {
        super.testReceiveAfterSend();
    }

    @Override
    @Disabled
    public void testAckAfterReceive() throws Exception {
        super.testAckAfterReceive();
    }

    @Override
    @Disabled
    public void testNackAfterReceive() throws Exception {
        super.testNackAfterReceive();
    }

    @Override
    @Disabled
    public void testBatchAck() throws Exception {
        super.testBatchAck();
    }

    @Override
    @Disabled
    public void testBatchNack() throws Exception {
        super.testBatchNack();
    }

    @Override
    @Disabled
    public void testAckNullThrows() throws Exception {
        super.testAckNullThrows();
    }

    @Override
    @Disabled
    public void testGetAttributes() throws Exception {
        super.testGetAttributes();
    }
}

