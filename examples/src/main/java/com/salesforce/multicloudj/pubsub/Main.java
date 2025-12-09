package com.salesforce.multicloudj.pubsub;

import com.salesforce.multicloudj.pubsub.client.GetAttributeResult;
import com.salesforce.multicloudj.pubsub.client.SubscriptionClient;
import com.salesforce.multicloudj.pubsub.client.TopicClient;
import com.salesforce.multicloudj.pubsub.driver.AckID;
import com.salesforce.multicloudj.pubsub.driver.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Main {

    static String provider = "gcp";

    public static void main(String[] args) {
        publishMessage();
        publishMessageWithMetadata();
        publishBatchMessages();
        receiveMessage();
        acknowledgeMessage();
        acknowledgeMessagesBatch();
        negativeAcknowledgeMessage();
        getSubscriptionAttributes();
        sendReceiveMultipleMessages();
    }

    /**
     * Publishes a simple message to a topic.
     */
    public static void publishMessage() {
        // Create a TopicClient instance based on the provider
        TopicClient topicClient = getTopicClient(provider);

        // Create a message with body content
        Message message = Message.builder()
                .withBody("Hello from MultiCloudJ PubSub!")
                .build();

        // Send the message to the topic
        topicClient.send(message);

        // Log the success
        getLogger().info("Message published successfully");
    }

    /**
     * Publishes a message with metadata to a topic.
     */
    public static void publishMessageWithMetadata() {
        // Get the TopicClient instance
        TopicClient topicClient = getTopicClient(provider);

        // Create a message with body and metadata
        Message message = Message.builder()
                .withBody("Message with metadata")
                .withMetadata("source", "demo")
                .withMetadata("timestamp", String.valueOf(System.currentTimeMillis()))
                .withMetadata("messageId", "msg-123")
                .build();

        // Send the message
        topicClient.send(message);

        getLogger().info("Message with metadata published successfully");
    }

    /**
     * Publishes multiple messages for batch operations.
     */
    public static void publishBatchMessages() {
        // Get the TopicClient instance
        TopicClient topicClient = getTopicClient(provider);

        // Publish 5 messages for batch acknowledgment
        for (int i = 1; i <= 5; i++) {
            Message message = Message.builder()
                    .withBody("Batch message #" + i)
                    .withMetadata("batchId", "batch-1")
                    .withMetadata("messageNumber", String.valueOf(i))
                    .build();

            topicClient.send(message);
            getLogger().info("Published batch message #{}", i);
        }

        getLogger().info("Published 5 messages for batch acknowledgment");
    }

    /**
     * Publishes multiple messages for batch operations.
     */
    public static void sendReceiveMultipleMessages() {
        // Get the TopicClient instance
        TopicClient topicClient = getTopicClient(provider);
        SubscriptionClient subscriptionClient = getSubscriptionClient(provider);

        // Publish 5 messages for batch acknowledgment
        for (int i = 1; i <= 5; i++) {
            Message message = Message.builder()
                    .withBody("Batch message #" + i)
                    .withMetadata("batchId", "batch-1")
                    .withMetadata("messageNumber", String.valueOf(i))
                    .build();

            topicClient.send(message);
            Message m = subscriptionClient.receive();
            getLogger().info("Received message #{}", m.getAckID());
        }

        getLogger().info("Published 5 messages for batch acknowledgment");
    }

    /**
     * Receives a message from a subscription.
     */
    public static void receiveMessage() {
        // Get the SubscriptionClient instance
        SubscriptionClient subscriptionClient = getSubscriptionClient(provider);

        // Receive a message from the subscription
        Message message = subscriptionClient.receive();

        // Process the received message
        if (message != null) {
            String body = new String(message.getBody());
            getLogger().info("Received message: {}", body);

            if (message.getMetadata() != null) {
                getLogger().info("Message metadata: {}", message.getMetadata());
            }

            if (message.getAckID() != null) {
                getLogger().info("Message AckID: {}", message.getAckID());
            }
        }
    }

    /**
     * Acknowledges a single message.
     */
    public static void acknowledgeMessage() {
        // Get the SubscriptionClient instance
        SubscriptionClient subscriptionClient = getSubscriptionClient(provider);

        // Receive a message
        Message message = subscriptionClient.receive();

        if (message != null && message.getAckID() != null) {
            // Acknowledge the message
            subscriptionClient.sendAck(message.getAckID());
            getLogger().info("Message acknowledged successfully");
        }
    }

    /**
     * Acknowledges multiple messages in a batch.
     */
    public static void acknowledgeMessagesBatch() {
        // Get the SubscriptionClient instance
        SubscriptionClient subscriptionClient = getSubscriptionClient(provider);

        // Receive multiple messages and collect their AckIDs
        List<AckID> ackIDs = new ArrayList<>();
        int messageCount = 0;
        int maxMessages = 5;

        while (messageCount < maxMessages) {
            Message message = subscriptionClient.receive();
            if (message != null && message.getAckID() != null) {
                ackIDs.add(message.getAckID());
                messageCount++;
            }
        }

        if (!ackIDs.isEmpty()) {
            // Acknowledge all messages in batch
            CompletableFuture<Void> ackFuture = subscriptionClient.sendAcks(ackIDs);

            // Wait for batch acknowledgment to complete
            ackFuture.join();

            getLogger().info("Acknowledged {} messages in batch", ackIDs.size());
        }
    }

    /**
     * Negatively acknowledges a message (nack).
     */
    public static void negativeAcknowledgeMessage() {
        // Get the SubscriptionClient instance
        SubscriptionClient subscriptionClient = getSubscriptionClient(provider);

        // Check if nacking is supported
        if (!subscriptionClient.canNack()) {
            getLogger().info("Negative acknowledgment is not supported by this provider");
            return;
        }

        // First, publish a message to nack
        TopicClient topicClient = getTopicClient(provider);
        Message messageToNack = Message.builder()
                .withBody("Message to be nacked and redelivered")
                .withMetadata("purpose", "nack-test")
                .build();
        topicClient.send(messageToNack);
        getLogger().info("Published message for nack test");

        // Wait a bit for message to be available
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Receive the message
        Message message = subscriptionClient.receive();

        if (message != null && message.getAckID() != null) {
            String messageBody = new String(message.getBody());
            getLogger().info("Received message: {}", messageBody);

            // Negatively acknowledge the message
            subscriptionClient.sendNack(message.getAckID());
            getLogger().info("Message negatively acknowledged");
        }
    }

    /**
     * Gets subscription attributes.
     */
    public static void getSubscriptionAttributes() {
        // Get the SubscriptionClient instance
        SubscriptionClient subscriptionClient = getSubscriptionClient(provider);

        // Get subscription attributes
        GetAttributeResult attributes = subscriptionClient.getAttributes();

        getLogger().info("Subscription name: {}", attributes.getName());
        getLogger().info("Topic: {}", attributes.getTopic());
    }

    private static TopicClient getTopicClient(String provider) {
        return TopicClient.builder(provider)
                .withTopicName("projects/substrate-sdk-gcp-poc1/topics/test-topic")
//                .withRegion("us-west-2")
                .build();
    }

    private static SubscriptionClient getSubscriptionClient(String provider) {
        return SubscriptionClient.builder(provider)
                .withSubscriptionName("projects/substrate-sdk-gcp-poc1/subscriptions/test-subscription")
//                .withRegion("us-west-2")
                .build();
    }

    private static Logger getLogger() {
        return LoggerFactory.getLogger("Main");
    }
}
