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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class GcpPubsubIT extends AbstractPubsubIT {

    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    public static class HarnessImpl implements Harness {
        private GcpTopic topic;
        private GcpSubscription subscription;
        private TopicAdminClient topicAdminClient;
        private SubscriptionAdminClient subscriptionAdminClient;
        private int port = ThreadLocalRandom.current().nextInt(1000, 10000);

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
            topicBuilder.withTopicName("projects/substrate-sdk-gcp-poc1/topics/test-topic");
            topic = new GcpTopic(topicBuilder, topicAdminClient);

            return topic;
        }

        @Override
        public AbstractSubscription createSubscriptionDriver() {
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

                subscriptionAdminClient = SubscriptionAdminClient.create(settingsBuilder.build());
            } catch (IOException e) {
                Assertions.fail("Failed to create the SubscriptionAdminClient", e);
            }

            GcpSubscription.Builder subscriptionBuilder = new GcpSubscription.Builder()
                    .withSubscriptionName("projects/substrate-sdk-gcp-poc1/subscriptions/test-subscription")
                    .withReceiveTimeoutSeconds(120); // Use 120 seconds timeout for conformance tests
            subscription = new GcpSubscription(subscriptionBuilder, subscriptionAdminClient);

            return subscription;
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