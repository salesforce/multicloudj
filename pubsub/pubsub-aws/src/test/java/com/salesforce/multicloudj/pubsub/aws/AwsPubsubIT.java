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
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;

public class AwsPubsubIT extends AbstractPubsubIT {

    private static final String SQS_ENDPOINT = "https://sqs.us-west-2.amazonaws.com";
    private static final String TEST_QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/654654370895/testQueue";
    private HarnessImpl harnessImpl;

    @Override
    protected Harness createHarness() {
        harnessImpl = new HarnessImpl();
        return harnessImpl;
    }

    /**
     * Clear the queue before each test to ensure test isolation.
     * This prevents messages from previous tests affecting the current test.
     */
    @BeforeEach
    public void clearQueueBeforeTest() {
        // Skip in replay mode - WireMock handles all requests, queue state doesn't matter
        boolean isRecording = System.getProperty("record") != null;
        if (!isRecording) {
            return;
        }
        
        if (harnessImpl != null && harnessImpl.sqsClient != null) {
            // First, try to receive and delete any remaining messages
            int maxAttempts = 10;
            for (int i = 0; i < maxAttempts; i++) {
                ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(TEST_QUEUE_URL)
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(0) 
                    .build();
                
                var response = harnessImpl.sqsClient.receiveMessage(receiveRequest);
                var messages = response.messages();
                
                if (messages.isEmpty()) {
                    break; // Queue is empty
                }
                
                // Delete all received messages
                List<DeleteMessageBatchRequestEntry> entries = new ArrayList<>();
                for (int j = 0; j < messages.size(); j++) {
                    entries.add(DeleteMessageBatchRequestEntry.builder()
                        .id(String.valueOf(j))
                        .receiptHandle(messages.get(j).receiptHandle())
                        .build());
                }
                
                DeleteMessageBatchRequest deleteRequest = DeleteMessageBatchRequest.builder()
                    .queueUrl(TEST_QUEUE_URL)
                    .entries(entries)
                    .build();
                
                harnessImpl.sqsClient.deleteMessageBatch(deleteRequest);
            }
        }
    }

    public static class HarnessImpl implements Harness {
        private AwsTopic topic;
        private AwsSubscription subscription;
        protected SqsClient sqsClient; 
        private SdkHttpClient httpClient;
        private int port = ThreadLocalRandom.current().nextInt(1000, 10000);

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

        @Override
        @SuppressWarnings("rawtypes")
        public AbstractTopic createTopicDriver() {
            SqsClient client = createSqsClient();
            
            AwsTopic.Builder topicBuilder = new AwsTopic.Builder();
            topicBuilder.withTopicName(TEST_QUEUE_URL);
            topicBuilder.withSqsClient(client);
            topic = new AwsTopic(topicBuilder);
            
            return topic;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public AbstractSubscription createSubscriptionDriver() {
            SqsClient client = createSqsClient();
            
            AwsSubscription.Builder subscriptionBuilder = new AwsSubscription.Builder();
            subscriptionBuilder.withSubscriptionName(TEST_QUEUE_URL);
            subscriptionBuilder.withWaitTimeSeconds(1); // Use 1 second wait time for conformance tests
            subscriptionBuilder.withSqsClient(client);
            
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

    @Disabled
    @Override
    public void testNackAfterReceive() throws Exception {
        // Disabled Temporarily
    }

    @Disabled
    @Override
    public void testBatchNack() throws Exception {
        // Disabled Temporarily
    }
}
