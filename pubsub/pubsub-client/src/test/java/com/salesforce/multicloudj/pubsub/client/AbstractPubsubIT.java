package com.salesforce.multicloudj.pubsub.client;

import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.pubsub.driver.Message;
import com.salesforce.multicloudj.pubsub.driver.AckID;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractPubsubIT {

    public interface Harness extends AutoCloseable {

        AbstractTopic createTopicDriver();

        default AbstractSubscription createSubscriptionDriver() {
            return createSubscriptionDriverWithIndex(0);
        }

        /**
         * Create a subscription driver with an index suffix.
         * This allows creating multiple subscriptions to the same topic.
         */
        default AbstractSubscription createSubscriptionDriverWithIndex(int index) {
            // Default implementation: just create a subscription driver
            // Providers that need index-based naming should override this method
            return createSubscriptionDriver();
        }

        String getPubsubEndpoint();

        String getProviderId();

        int getPort();

        List<String> getWiremockExtensions();
    }

    protected abstract Harness createHarness();

    private Harness harness;

    /**
     * Initializes the WireMock server before all tests.
     */
    @BeforeAll
    public void initializeWireMockServer() {
        harness = createHarness();
        String rootDir = "src/test/resources";
        List<String> extensions = harness.getWiremockExtensions();
        TestsUtil.startWireMockServer(rootDir, harness.getPort(),
                extensions.toArray(new String[0]));
    }

    /**
     * Shuts down the WireMock server after all tests.
     */
    @AfterAll
    public void shutdownWireMockServer() throws Exception {
        TestsUtil.stopWireMockServer();
        harness.close();
    }

    /**
     * Initialize the harness and start recording
     */
    @BeforeEach
    public void setupTestEnvironment() {
        TestsUtil.startWireMockRecording(harness.getPubsubEndpoint());
    }

    /**
     * Cleans up the test environment after each test.
     */
    @AfterEach
    public void cleanupTestEnvironment() {
        TestsUtil.stopWireMockRecording();
    }

    @Test
    public void testSendBatchMessages() throws Exception {
        try (AbstractTopic topic = harness.createTopicDriver()) {
            List<Message> messages = List.of(
                    Message.builder().withBody("Message 1".getBytes()).withMetadata(Map.of("batch-id", "1")).build(),
                    Message.builder().withBody("Message 2".getBytes()).withMetadata(Map.of("batch-id", "2")).build(),
                    Message.builder().withBody("Message 3".getBytes()).withMetadata(Map.of("batch-id", "3")).build()
            );


            for (Message message : messages) {
                topic.send(message);
            }

            for (Message message : messages) {
                Assertions.assertNotNull(message, "Message should not be null");
                Assertions.assertNotNull(message.getBody(), "Message body should not be null");
                Assertions.assertNotNull(message.getMetadata(), "Message metadata should not be null");
            }

        }
    }

    @Test
    @Timeout(30) // Integration test that calls receive() - fail fast if recordings are missing
    public void testReceiveAfterSend() throws Exception {
        try (AbstractTopic topic = harness.createTopicDriver();
             AbstractSubscription subscription = harness.createSubscriptionDriver()) {

            Message toSend = Message.builder()
                    .withBody("it-receive-test".getBytes())
                    .withMetadata(Map.of("case", "receive"))
                    .build();
            topic.send(toSend);

            Message received = null;
            for (int i = 0; i < 50; i++) {
                received = subscription.receive();
                if (received != null) {
                    break;
                }
                TimeUnit.MILLISECONDS.sleep(200);
            }

            Assertions.assertNotNull(received, "Should receive a message within timeout");
            Assertions.assertNotNull(received.getBody(), "Received message body should not be null");
            Assertions.assertNotNull(received.getAckID(), "Received message should have AckID");
        }
    }

    @Test
    @Timeout(30) // Integration test that calls receive() - fail fast if recordings are missing
    public void testAckAfterReceive() throws Exception {
        try (AbstractTopic topic = harness.createTopicDriver();
             AbstractSubscription subscription = harness.createSubscriptionDriver()) {

            Message toSend = Message.builder()
                    .withBody("it-ack-test".getBytes())
                    .withMetadata(Map.of("case", "ack"))
                    .build();
            topic.send(toSend);

            Message received = null;
            for (int i = 0; i < 50; i++) {
                received = subscription.receive();
                if (received != null) break;
                TimeUnit.MILLISECONDS.sleep(200);
            }

            Assertions.assertNotNull(received, "Should receive a message to ack");
            Assertions.assertNotNull(received.getAckID(), "AckID must not be null");

            subscription.sendAck(received.getAckID());
        }
    }

    @Test
    @Timeout(30) // Integration test that calls receive() - fail fast if recordings are missing
    public void testNackAfterReceive() throws Exception {
        try (AbstractTopic topic = harness.createTopicDriver();
             AbstractSubscription subscription = harness.createSubscriptionDriver()) {

            Message toSend = Message.builder()
                    .withBody("it-nack-test".getBytes())
                    .withMetadata(Map.of("case", "nack"))
                    .build();
            topic.send(toSend);

            Message received = null;
            for (int i = 0; i < 50; i++) {
                received = subscription.receive();
                if (received != null) break;
                TimeUnit.MILLISECONDS.sleep(200);
            }

            Assertions.assertNotNull(received, "Should receive a message to nack");
            Assertions.assertNotNull(received.getAckID(), "AckID must not be null");

            subscription.sendNack(received.getAckID());
        }
    }

    @Test
    public void testBatchAck() throws Exception {
        try (AbstractTopic topic = harness.createTopicDriver();
             AbstractSubscription subscription = harness.createSubscriptionDriver()) {

            List<Message> toSend = List.of(
                    Message.builder().withBody("batch-ack-1".getBytes()).withMetadata(Map.of("batch", "ack")).build(),
                    Message.builder().withBody("batch-ack-2".getBytes()).withMetadata(Map.of("batch", "ack")).build(),
                    Message.builder().withBody("batch-ack-3".getBytes()).withMetadata(Map.of("batch", "ack")).build()
            );
            for (Message m : toSend) topic.send(m);

            TimeUnit.MILLISECONDS.sleep(500);

            List<AckID> ackIDs = new java.util.ArrayList<>();
            boolean isRecording = System.getProperty("record") != null;
            long timeoutSeconds = isRecording ? 120 : 60; // Increased timeout for integration tests
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);

            while (ackIDs.size() < toSend.size() && System.nanoTime() < deadline) {
                try {
                    Message r = subscription.receive();
                    if (r != null && r.getAckID() != null) {
                        ackIDs.add(r.getAckID());
                    } else {
                        TimeUnit.MILLISECONDS.sleep(100);
                    }
                } catch (Exception e) {
                    TimeUnit.MILLISECONDS.sleep(100);
                }
            }

            Assertions.assertEquals(toSend.size(), ackIDs.size(),
                    "Should collect all AckIDs. Expected: " + toSend.size() + ", Got: " + ackIDs.size());
            subscription.sendAcks(ackIDs).join();
        }
    }
    
    @Test
    @Timeout(120) // Integration test with batch operations - allow time for message delivery
    public void testBatchNack() throws Exception {
        try (AbstractTopic topic = harness.createTopicDriver();
             AbstractSubscription subscription = harness.createSubscriptionDriver()) {

            List<Message> toSend = List.of(
                    Message.builder().withBody("batch-nack-1".getBytes()).withMetadata(Map.of("batch", "nack")).build(),
                    Message.builder().withBody("batch-nack-2".getBytes()).withMetadata(Map.of("batch", "nack")).build(),
                    Message.builder().withBody("batch-nack-3".getBytes()).withMetadata(Map.of("batch", "nack")).build()
            );
            for (Message m : toSend) topic.send(m);

            TimeUnit.MILLISECONDS.sleep(500);

            List<Message> received = receiveMessages(subscription, toSend.size());
            List<AckID> ackIDs = received.stream()
                    .map(Message::getAckID)
                    .collect(java.util.stream.Collectors.toList());

            Assertions.assertEquals(toSend.size(), ackIDs.size(),
                    "Should collect all AckIDs. Expected: " + toSend.size() + ", Got: " + ackIDs.size());
            subscription.sendNacks(ackIDs).join();
        }
    }

    @Test
    public void testAckNullThrows() throws Exception {
        try (AbstractSubscription subscription = harness.createSubscriptionDriver()) {
            Assertions.assertThrows(InvalidArgumentException.class, () -> subscription.sendAck(null));
        }
    }

    @Test
    @Disabled
    public void testDoubleAck() throws Exception {
        try (AbstractTopic topic = harness.createTopicDriver();
             AbstractSubscription subscription = harness.createSubscriptionDriver()) {

            List<Message> messages = List.of(
                    Message.builder().withBody("0".getBytes()).build(),
                    Message.builder().withBody("1".getBytes()).build(),
                    Message.builder().withBody("2".getBytes()).build()
            );

            for (Message message : messages) {
                topic.send(message);
            }

            // Receive all messages
            List<Message> receivedMessages = new ArrayList<>();
            for (int i = 0; i < messages.size(); i++) {
                Message received = subscription.receive();
                Assertions.assertNotNull(received, "Should receive message " + (i + 1));
                Assertions.assertNotNull(received.getAckID(), "Received message should have AckID");
                receivedMessages.add(received);
            }

            Assertions.assertEquals(messages.size(), receivedMessages.size(),
                    "Should receive all " + messages.size() + " messages");

            // Ack the first two messages
            List<AckID> firstTwoAcks = List.of(
                    receivedMessages.get(0).getAckID(),
                    receivedMessages.get(1).getAckID()
            );
            subscription.sendAcks(firstTwoAcks).join();

            // Ack them again, this should succeed even though we've acked them before
            subscription.sendAcks(firstTwoAcks).join();

            // Test double ack on individual messages 
            subscription.sendAck(receivedMessages.get(0).getAckID());
            subscription.sendAck(receivedMessages.get(0).getAckID());
        }
    }

    @Test
    public void testGetAttributes() throws Exception {
        try (AbstractSubscription subscription = harness.createSubscriptionDriver()) {
            GetAttributeResult attributes = subscription.getAttributes();

            // Verify that attributes are returned
            Assertions.assertNotNull(attributes, "Attributes should not be null");

            // Verify essential attributes that should be present across all providers
            Assertions.assertNotNull(attributes.getName(), "Name should not be null");
            Assertions.assertFalse(attributes.getName().isEmpty(), "Name should not be empty");

            Assertions.assertNotNull(attributes.getTopic(), "Topic should not be null");
            Assertions.assertFalse(attributes.getTopic().isEmpty(), "Topic should not be empty");

            // Verify that we have the essential attributes
            Assertions.assertNotNull(attributes.getName(), "Should have name attribute");
            Assertions.assertNotNull(attributes.getTopic(), "Should have topic attribute");
        }
    }

    @Test
    @Disabled
    public void testMultipleSendReceiveWithoutBatch() throws Exception {
        try (AbstractTopic topic = harness.createTopicDriver();
             AbstractSubscription subscription = harness.createSubscriptionDriver()) {

            int numMessages = 5;
            List<Message> sentMessages = new ArrayList<>();
            
            // Send messages one by one (not in batch)
            for (int i = 0; i < numMessages; i++) {
                Message message = Message.builder()
                        .withBody(("non-batch-msg-" + i).getBytes())
                        .withMetadata(Map.of("index", String.valueOf(i)))
                        .build();
                topic.send(message);
                sentMessages.add(message);
            }

            // Receive and ack messages one by one (not in batch)
            List<Message> receivedMessages = new ArrayList<>();
            for (int i = 0; i < numMessages; i++) {
                Message received = subscription.receive();
                Assertions.assertNotNull(received, "Should receive message " + (i + 1));
                Assertions.assertNotNull(received.getAckID(), "Received message should have AckID");
                receivedMessages.add(received);
                // Ack immediately after receiving (not in batch)
                subscription.sendAck(received.getAckID());
            }

            Assertions.assertEquals(numMessages, receivedMessages.size(),
                    "Should receive all " + numMessages + " messages");

            // Verify all messages were received
            for (int i = 0; i < receivedMessages.size(); i++) {
                Message received = receivedMessages.get(i);
                Assertions.assertNotNull(received, "Received message " + i + " should not be null");
                Assertions.assertNotNull(received.getBody(), "Received message " + i + " body should not be null");
                Assertions.assertNotNull(received.getAckID(), "Received message " + i + " should have AckID");
            }
        }
    }

    /**
     * Receive from two subscriptions to the same topic.
     * Verify both get all the messages.
     */
    @Test
    @Timeout(120) // Integration test with multiple subscriptions - allow time for message delivery
    public void testSendReceiveTwo() throws Exception {
        // Create two subscriptions to the same topic
        AbstractSubscription subscription1 = harness.createSubscriptionDriverWithIndex(1);
        AbstractSubscription subscription2 = harness.createSubscriptionDriverWithIndex(2);

        try (AbstractTopic topic = harness.createTopicDriver();
             AbstractSubscription sub1 = subscription1;
             AbstractSubscription sub2 = subscription2) {

            // Send 3 messages to the topic
            List<Message> messagesToSend = List.of(
                    Message.builder().withBody("fanout-msg1".getBytes()).withMetadata(Map.of("id", "1")).build(),
                    Message.builder().withBody("fanout-msg2".getBytes()).withMetadata(Map.of("id", "2")).build(),
                    Message.builder().withBody("fanout-msg3".getBytes()).withMetadata(Map.of("id", "3")).build()
            );

            for (Message message : messagesToSend) {
                topic.send(message);
            }

            TimeUnit.MILLISECONDS.sleep(500);

            // Receive messages from both subscriptions
            List<Message> received1 = receiveMessages(sub1, messagesToSend.size());
            List<Message> received2 = receiveMessages(sub2, messagesToSend.size());

            // Verify both subscriptions received all messages
            Assertions.assertEquals(messagesToSend.size(), received1.size(),
                    "Subscription 1 should receive all " + messagesToSend.size() + " messages. Got: " + received1.size());
            Assertions.assertEquals(messagesToSend.size(), received2.size(),
                    "Subscription 2 should receive all " + messagesToSend.size() + " messages. Got: " + received2.size());

            // Verify messages match for both subscriptions
            verifyMessages(received1, messagesToSend, "Subscription 1");
            verifyMessages(received2, messagesToSend, "Subscription 2");

            // Ack all messages from both subscriptions
            ackMessages(sub1, received1);
            ackMessages(sub2, received2);
        }
    }

    /**
     * Helper function: Receives messages from a subscription until the expected count is reached.
     */
    private List<Message> receiveMessages(AbstractSubscription subscription, int expectedCount) throws InterruptedException {
        boolean isRecording = System.getProperty("record") != null;
        long timeoutSeconds = isRecording ? 120 : 60;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);

        List<Message> received = new ArrayList<>();
        try {
            while (received.size() < expectedCount && System.nanoTime() < deadline) {
                Message r = subscription.receive();
                // receive() either returns a Message or throws an exception
                received.add(r);
            }
        } catch (Exception e) {
            if (!isRecording) {
                // replay mode: An exception is thrown even when the timeout has not occurred.
                String wireMockError = TestsUtil.getWireMockUnmatchedRequestsError();
                String errorMsg = String.format(
                    "Failed to receive messages: Got exception after receiving %d/%d messages.%n%n" +
                    "WireMock Error Details:%n%s",
                    received.size(), expectedCount, wireMockError != null ? wireMockError : "No unmatched requests");
                throw new AssertionError(errorMsg, e);
            }
            // record mode: throw the exception directly
            throw e;
        }
        // If the loop exits but received count is less than expected, it indicates a timeout occurred.
        if (received.size() < expectedCount) {
            if (isRecording) {
                String errorMsg = String.format(
                    "Timeout waiting for messages: Received %d/%d messages.",
                    received.size(), expectedCount);
                throw new AssertionError(errorMsg);
            } else {
                // In replay mode, timeout might indicate mapping mismatch
                String wireMockError = TestsUtil.getWireMockUnmatchedRequestsError();
                String errorMsg = String.format(
                    "Timeout waiting for messages: Received %d/%d messages.%n%n" +
                    "WireMock Error Details:%n%s",
                    received.size(), expectedCount, wireMockError != null ? wireMockError : "No unmatched requests");
                throw new AssertionError(errorMsg);
            }
        }
        return received;
    }

    /**
     * Helper function: Verifies that received messages match the expected messages.
     */
    private void verifyMessages(List<Message> received, List<Message> expected, String subscriptionName) {
        if (received.size() != expected.size()) {
            Assertions.fail(String.format("%s: got %d messages, expected %d", 
                    subscriptionName, received.size(), expected.size()));
        }
        Map<String, Message> gotByBody = new HashMap<>();
        for (Message msg : received) {
            gotByBody.put(new String(msg.getBody()), msg);
        }
        for (Message exp : expected) {
            String body = new String(exp.getBody());
            Message got = gotByBody.get(body);
            if (got == null) {
                Assertions.fail(subscriptionName + ": missing message: " + body);
            }
            if (!Arrays.equals(exp.getBody(), got.getBody())) {
                Assertions.fail(subscriptionName + ": body mismatch for " + body);
            }
            if (exp.getMetadata() != null) {
                for (Map.Entry<String, String> entry : exp.getMetadata().entrySet()) {
                    String expValue = entry.getValue();
                    String gotValue = got.getMetadata() != null ? got.getMetadata().get(entry.getKey()) : null;
                    if (!expValue.equals(gotValue)) {
                        Assertions.fail(String.format("%s: metadata[%s] mismatch for %s: expected %s, got %s",
                                subscriptionName, entry.getKey(), body, expValue, gotValue));
                    }
                }
            }
        }
    }


    /**
     * Helper function: Acknowledges all messages in the given list.
     */
    private void ackMessages(AbstractSubscription subscription, List<Message> messages) {
        if (!messages.isEmpty()) {
            List<AckID> ackIDs = new ArrayList<>();
            for (Message msg : messages) {
                ackIDs.add(msg.getAckID());
            }
            subscription.sendAcks(ackIDs).join();
        }
    }
}
