package com.salesforce.multicloudj.pubsub.aws;

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
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

public class AwsPubsubSqsIT extends AbstractPubsubIT {

    private static final String SQS_ENDPOINT = "https://sqs.us-west-2.amazonaws.com";
    private static final String BASE_QUEUE_NAME = "test-sqs";

    private HarnessImpl harnessImpl;
    private String queueName;

    @Override
    protected Harness createHarness() {
        harnessImpl = new HarnessImpl();
        return harnessImpl;
    }

    @BeforeEach
    public void setupTestResources(TestInfo testInfo) {
        String testMethodName = testInfo.getTestMethod()
            .map(m -> m.getName())
            .orElseThrow(() -> new IllegalStateException("Test method not found in TestInfo"));
        queueName = BASE_QUEUE_NAME + "-" + testMethodName;
        
        if (harnessImpl != null) {
            harnessImpl.setQueueName(queueName);
        }
    }

    public static class HarnessImpl implements Harness {
        private AbstractTopic topic;
        private AwsSubscription subscription;
        private SqsClient sqsClient;
        private SdkHttpClient httpClient;
        private int port = ThreadLocalRandom.current().nextInt(1000, 10000);
        private String queueName = BASE_QUEUE_NAME;
        private String cachedQueueUrl;

        public void setQueueName(String queueName) {
            this.queueName = queueName;
            this.cachedQueueUrl = null;
        }

        private AwsSessionCredentials createCredentials() {
            String accessKey = System.getenv().getOrDefault("AWS_ACCESS_KEY_ID", "FAKE_ACCESS_KEY");
            String secretKey = System.getenv().getOrDefault("AWS_SECRET_ACCESS_KEY", "FAKE_SECRET_ACCESS_KEY");
            String sessionToken = System.getenv().getOrDefault("AWS_SESSION_TOKEN", "FAKE_SESSION_TOKEN");
            return AwsSessionCredentials.create(accessKey, secretKey, sessionToken);
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

        private void ensureQueueExists() {
            sqsClient = createSqsClient();

            if (cachedQueueUrl == null) {
                // CreateQueue is idempotent: creates queue if doesn't exist, returns URL if exists
                CreateQueueResponse response = sqsClient.createQueue(CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build());
                cachedQueueUrl = response.queueUrl();
            }
        }

        @Override
        @SuppressWarnings("rawtypes")
        public AbstractTopic createTopicDriver() {
            sqsClient = createSqsClient();
            ensureQueueExists();

            AwsSqsTopic.Builder topicBuilder = new AwsSqsTopic.Builder();
            topicBuilder.withTopicName(cachedQueueUrl);
            topicBuilder.withSqsClient(sqsClient);
            topic = topicBuilder.build();

            return topic;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public AbstractSubscription createSubscriptionDriver() {
            sqsClient = createSqsClient();
            ensureQueueExists();

            AwsSubscription.Builder subscriptionBuilder = new AwsSubscription.Builder();
            subscriptionBuilder.withSubscriptionName(queueName);
            subscriptionBuilder.withWaitTimeSeconds(1);
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
            return SQS_ENDPOINT;
        }

        @Override
        public String getProviderId() {
            return AwsSqsTopic.PROVIDER_ID;
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
            if (sqsClient != null) {
                sqsClient.close();
            }
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }
}
