package com.salesforce.multicloudj.pubsub.gcp;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.salesforce.multicloudj.common.gcp.util.MockGoogleCredentialsFactory;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.gcp.util.TestsUtilGcp;
import com.salesforce.multicloudj.pubsub.client.AbstractPubsubIT;
import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class GcpPubsubIT extends AbstractPubsubIT {

    private static final String PROJECT_ID = "substrate-sdk-gcp-poc1";
    private static final String BASE_TOPIC_NAME = "topic-test";
    private static final String BASE_SUBSCRIPTION_NAME = "sub-test";

    private HarnessImpl harnessImpl;
    private String topicName;
    private String subscriptionName;

    @Override
    protected Harness createHarness() {
        harnessImpl = new HarnessImpl();
        return harnessImpl;
    }

    /**
     * Generate unique topic and subscription names for each test.
     */
    @BeforeEach
    public void setupTestResources(TestInfo testInfo) {
        String testMethodName = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        topicName = BASE_TOPIC_NAME + testMethodName.substring(4); // Remove "test" prefix
        subscriptionName = BASE_SUBSCRIPTION_NAME + testMethodName.substring(4); // Remove "test" prefix
        if (harnessImpl != null) {
            harnessImpl.setTopicName(topicName);
            harnessImpl.setSubscriptionName(subscriptionName);
        }
    }

    public static class HarnessImpl implements Harness {
        private static final String DEFAULT_TOPIC_NAME = "topic-test";
        private static final String DEFAULT_SUBSCRIPTION_NAME = "sub-test";
        
        private GcpTopic topic;
        private GcpSubscription subscription;
        private TopicAdminClient topicAdminClient;
        private SubscriptionAdminClient subscriptionAdminClient;
        private int port = ThreadLocalRandom.current().nextInt(1000, 10000);
        private String topicName = DEFAULT_TOPIC_NAME;
        private String subscriptionName = DEFAULT_SUBSCRIPTION_NAME;

        public void setTopicName(String topicName) {
            this.topicName = topicName;
        }

        public void setSubscriptionName(String subscriptionName) {
            this.subscriptionName = subscriptionName;
        }

        /**
         * Create a subscription with an index suffix.
         * This allows creating multiple subscriptions to the same topic.
         */
        @Override
        public AbstractSubscription createSubscriptionDriverWithIndex(int index) {
            boolean isRecordingEnabled = System.getProperty("record") != null;

            TransportChannelProvider channelProvider = TestsUtilGcp.getTransportChannelProvider(port);

            try {
                SubscriptionAdminSettings.Builder settingsBuilder = SubscriptionAdminSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider);

                if (!isRecordingEnabled) {
                    // Replay path - inject mock credentials
                    GoogleCredentials mockCreds = MockGoogleCredentialsFactory.createMockCredentials();
                    settingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(mockCreds));
                }

                SubscriptionAdminClient client = SubscriptionAdminClient.create(settingsBuilder.build());
                
                // add index suffix only if index > 0
                String subscriptionNameWithIndex = index > 0 ? subscriptionName + "-" + index : subscriptionName;
                String fullSubscriptionName = "projects/" + GcpPubsubIT.PROJECT_ID + "/subscriptions/" + subscriptionNameWithIndex;
                
                GcpSubscription.Builder subscriptionBuilder = new GcpSubscription.Builder()
                        .withSubscriptionName(fullSubscriptionName);
                GcpSubscription sub = new GcpSubscription(subscriptionBuilder, client);
                
                if (index == 0) {
                    subscription = sub;
                    subscriptionAdminClient = client;
                }
                
                return sub;
            } catch (IOException e) {
                Assertions.fail("Failed to create the SubscriptionAdminClient", e);
                return null;
            }
        }

        @Override
        public AbstractTopic createTopicDriver() {
            boolean isRecordingEnabled = System.getProperty("record") != null;

            TransportChannelProvider channelProvider = TestsUtilGcp.getTransportChannelProvider(port);

            try {
                TopicAdminSettings.Builder settingsBuilder = TopicAdminSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider);

                if (!isRecordingEnabled) {
                    // Replay path - inject mock credentials
                    GoogleCredentials mockCreds = MockGoogleCredentialsFactory.createMockCredentials();
                    settingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(mockCreds));
                }

                topicAdminClient = TopicAdminClient.create(settingsBuilder.build());
            } catch (IOException e) {
                Assertions.fail("Failed to create the TopicAdminClient", e);
            }

            GcpTopic.Builder topicBuilder = new GcpTopic.Builder();
            String fullTopicName = "projects/" + GcpPubsubIT.PROJECT_ID + "/topics/" + topicName;
            topicBuilder.withTopicName(fullTopicName);
            topic = new GcpTopic(topicBuilder, topicAdminClient);

            return topic;
        }

        @Override
        public AbstractSubscription createSubscriptionDriver() {
            return createSubscriptionDriverWithIndex(0);
        }

        @Override
        public String getPubsubEndpoint() {
            return "https://pubsub.googleapis.com";
        }

        @Override
        public String getProviderId() {
            return GcpConstants.PROVIDER_ID;
        }

        @Override
        public int getPort() {
            return port;
        }

        @Override
        public List<String> getWiremockExtensions() {
            return List.of("com.salesforce.multicloudj.pubsub.gcp.util.AckMatcherRelaxingTransformer");
        }

        @Override
        public void close() throws Exception {
            if (topic != null) {
                topic.close();
            }
            if (subscription != null) {
                subscription.close();
            }
            if (topicAdminClient != null) {
                topicAdminClient.close();
            }
            if (subscriptionAdminClient != null) {
                subscriptionAdminClient.close();
            }
        }

    }

}