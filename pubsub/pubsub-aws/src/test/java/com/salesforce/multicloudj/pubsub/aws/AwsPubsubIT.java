package com.salesforce.multicloudj.pubsub.aws;

import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.aws.util.TestsUtilAws;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This class supports two modes:
 * 1. SQS mode: Direct SQS send/receive/ack/nack
 * 2. SNS mode: SNS send, SQS receive (SNS publishes to subscribed SQS queue)
 * 
 * Use @TestMode annotation on test methods to specify the mode.
 * Default is SQS mode if annotation is not present.
 */
public class AwsPubsubIT extends AbstractPubsubIT {
    
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface TestMode {
        Mode value();
    }

    private static final String SNS_ENDPOINT = "https://sns.us-west-2.amazonaws.com";
    private static final String SQS_ENDPOINT = "https://sqs.us-west-2.amazonaws.com";
    private static final String ACCOUNT_ID = "654654370895";
    private static final String BASE_QUEUE_NAME = "test-queue";
    private static final String BASE_TOPIC_NAME = "test-sns-topic";

    public enum Mode {
        SQS,  
        SNS  
    }

    private HarnessImpl harnessImpl;
    private String queueName;
    private String topicName;
    private Mode currentMode = Mode.SQS; // Default to SQS mode

    @Override
    protected Harness createHarness() {
        harnessImpl = new HarnessImpl(currentMode);
        return harnessImpl;
    }

    /**
     * Sets the test mode (SQS or SNS) for the current test.
     * This is called automatically from setupTestResources based on @TestMode annotation.
     */
    private void setMode(Mode mode) {
        this.currentMode = mode;
        if (harnessImpl != null) {
            harnessImpl.setMode(mode);
        }
    }

    /**
     * Generate unique queue/topic names for each test.
     * Sets the test mode based on @TestMode annotation on the test method.
     * Default mode is SQS if annotation is not present.
     */
    @BeforeEach
    public void setupTestResources(TestInfo testInfo) {
        String testMethodName = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        
        Mode testMode = testInfo.getTestMethod()
            .map(method -> {
                TestMode annotation = method.getAnnotation(TestMode.class);
                return annotation != null ? annotation.value() : Mode.SQS;
            })
            .orElse(Mode.SQS);
        
        queueName = BASE_QUEUE_NAME + "-" + testMethodName;
        
        setMode(testMode);
        
        if (harnessImpl != null) {
            harnessImpl.setQueueName(queueName);
            if (testMode == Mode.SNS) {
                topicName = BASE_TOPIC_NAME + "-" + testMethodName;
                harnessImpl.setTopicName(topicName);
            }
        }
    }
    
    @Override
    @BeforeEach
    public void setupTestEnvironment() {
        String endpoint = (currentMode == Mode.SNS) ? SNS_ENDPOINT : SQS_ENDPOINT;
        TestsUtil.startWireMockRecording(endpoint);
    }

    public static class HarnessImpl implements Harness {
        private AbstractTopic topic;
        private AwsSubscription subscription;
        private SnsClient snsClient;
        private SqsClient sqsClient;
        private SdkHttpClient httpClient;
        private int port = ThreadLocalRandom.current().nextInt(1000, 10000);
        private String queueName = BASE_QUEUE_NAME;
        private String topicName = BASE_TOPIC_NAME;
        private String cachedTopicArn; // Cache topic ARN to avoid multiple calls
        private String cachedQueueUrl; // Cache queue URL to avoid multiple calls
        private Mode mode;

        public HarnessImpl(Mode mode) {
            this.mode = mode;
        }

        public void setMode(Mode mode) {
            this.mode = mode;
        }

        public void setQueueName(String queueName) {
            this.queueName = queueName;
            this.cachedQueueUrl = null; // Reset cache when queue name changes
        }

        public void setTopicName(String topicName) {
            this.topicName = topicName;
            this.cachedTopicArn = null; // Reset cache when topic name changes
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
         * Ensures the queue exists.
         * For SNS mode, also subscribes the queue to the SNS topic.
         */
        private void ensureQueueExists() {
            if (System.getProperty("record") != null) {
                try {
                    sqsClient.createQueue(CreateQueueRequest.builder()
                        .queueName(queueName)
                        .build());
                } catch (Exception e) {
                }

                if (cachedQueueUrl == null) {
                    GetQueueUrlResponse urlResponse = sqsClient.getQueueUrl(GetQueueUrlRequest.builder()
                        .queueName(queueName)
                        .build());
                    cachedQueueUrl = urlResponse.queueUrl();
                }
            } else {
                // In replay mode, only get queue URL
                if (cachedQueueUrl == null) {
                    GetQueueUrlResponse urlResponse = sqsClient.getQueueUrl(GetQueueUrlRequest.builder()
                        .queueName(queueName)
                        .build());
                    cachedQueueUrl = urlResponse.queueUrl();
                }
            }
        }

        /**
         * For SNS mode: ensures the SQS queue exists and is subscribed to the SNS topic.
         */
        private void ensureQueueExistsAndSubscribed(SqsClient sqsClient, SnsClient snsClient) {
            ensureQueueExists();

            if (mode == Mode.SNS && System.getProperty("record") != null) {
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
        @SuppressWarnings("rawtypes")
        public AbstractTopic createTopicDriver() {
            if (mode == Mode.SNS) {
                // SNS mode: create SNS topic
                snsClient = createSnsClient();
                
                if (cachedTopicArn == null) {
                    CreateTopicResponse response = snsClient.createTopic(CreateTopicRequest.builder()
                        .name(topicName)
                        .build());
                    cachedTopicArn = response.topicArn();
                }

                AwsSnsTopic.Builder topicBuilder = new AwsSnsTopic.Builder();
                topicBuilder.withTopicName(cachedTopicArn);
                topicBuilder.withSnsClient(snsClient);
                topic = topicBuilder.build();
            } else {
                // SQS mode: use SQS queue as topic
                sqsClient = createSqsClient();
                ensureQueueExists();

                if (cachedQueueUrl == null) {
                    GetQueueUrlResponse response = sqsClient.getQueueUrl(GetQueueUrlRequest.builder()
                        .queueName(queueName)
                        .build());
                    cachedQueueUrl = response.queueUrl();
                }

                AwsSqsTopic.Builder topicBuilder = new AwsSqsTopic.Builder();
                topicBuilder.withTopicName(cachedQueueUrl); // Use full URL to avoid GetQueueUrl call
                topicBuilder.withSqsClient(sqsClient);
                topic = topicBuilder.build();
            }

            return topic;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public AbstractSubscription createSubscriptionDriver() {
            sqsClient = createSqsClient();
            
            if (mode == Mode.SNS) {
                snsClient = createSnsClient();
                ensureQueueExistsAndSubscribed(sqsClient, snsClient);
            } else {
                ensureQueueExists();
            }

            if (cachedQueueUrl == null) {
                throw new IllegalStateException("Queue URL should have been cached");
            }

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
            return mode == Mode.SNS ? SNS_ENDPOINT : SQS_ENDPOINT;
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

    /**
     * Test SNS send batch messages.
     */
    @Test
    @TestMode(Mode.SNS)
    public void testSnsSendBatchMessages() throws Exception {
        super.testSendBatchMessages();
    }
}


