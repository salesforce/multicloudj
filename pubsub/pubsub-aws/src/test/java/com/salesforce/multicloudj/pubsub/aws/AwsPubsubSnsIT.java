package com.salesforce.multicloudj.pubsub.aws;

import com.salesforce.multicloudj.common.aws.util.TestsUtilAws;
import com.salesforce.multicloudj.pubsub.client.AbstractPubsubIT;
import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInfo;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.CreateTopicResponse;

public class AwsPubsubSnsIT extends AbstractPubsubIT {

    private static final String SNS_ENDPOINT = "https://sns.us-west-2.amazonaws.com";
    private static final String BASE_TOPIC_NAME = "test-sns";

    private HarnessImpl harnessImpl;
    private String topicName;

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
        topicName = BASE_TOPIC_NAME + "-" + testMethodName;
        
        if (harnessImpl != null) {
            harnessImpl.setTopicName(topicName);
        }
    }

    public static class HarnessImpl implements Harness {
        private AbstractTopic topic;
        private SnsClient snsClient;
        private SdkHttpClient httpClient;
        private int port = ThreadLocalRandom.current().nextInt(1000, 10000);
        private String topicName = BASE_TOPIC_NAME;
        private String cachedTopicArn;

        public void setTopicName(String topicName) {
            this.topicName = topicName;
            this.cachedTopicArn = null;
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

        @Override
        @SuppressWarnings("rawtypes")
        public AbstractTopic createTopicDriver() {
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

            return topic;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public AbstractSubscription createSubscriptionDriver() {
            throw new UnsupportedOperationException(
                "SNS does not support receiving messages. Use AwsPubsubSqsIT for receive/ack/nack tests.");
        }

        @Override
        public String getPubsubEndpoint() {
            return SNS_ENDPOINT;
        }

        @Override
        public String getProviderId() {
            return AwsSnsTopic.PROVIDER_ID;
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
            if (snsClient != null) {
                snsClient.close();
            }
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    @Override
    @Disabled("SNS does not support receiving messages")
    public void testReceiveAfterSend() throws Exception {
    }

    @Override
    @Disabled("SNS does not support receiving messages")
    public void testAckAfterReceive() throws Exception {
    }

    @Override
    @Disabled("SNS does not support receiving messages")
    public void testNackAfterReceive() throws Exception {
    }

    @Override
    @Disabled("SNS does not support receiving messages")
    public void testBatchAck() throws Exception {
    }

    @Override
    @Disabled("SNS does not support receiving messages")
    public void testBatchNack() throws Exception {
    }

    @Override
    @Disabled("SNS does not support receiving messages")
    public void testAckNullThrows() throws Exception {
    }

    @Override
    @Disabled("SNS does not support receiving messages")
    public void testGetAttributes() throws Exception {
    }
}
