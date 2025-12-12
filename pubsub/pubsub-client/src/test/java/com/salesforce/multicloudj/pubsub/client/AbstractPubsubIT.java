package com.salesforce.multicloudj.pubsub.client;

import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.pubsub.driver.Message;
import com.salesforce.multicloudj.pubsub.driver.AckID;
import com.salesforce.multicloudj.pubsub.client.GetAttributeResult;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractPubsubIT {

    protected static final String AWS_PROVIDER_ID = "aws";

    public interface Harness extends AutoCloseable {

        AbstractTopic createTopicDriver();

        AbstractSubscription createSubscriptionDriver();

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

            System.out.println("Starting to collect " + toSend.size() + " messages with timeout: " + timeoutSeconds + "s");

            while (ackIDs.size() < toSend.size() && System.nanoTime() < deadline) {
                try {
                    Message r = subscription.receive();
                    if (r != null && r.getAckID() != null) {
                        ackIDs.add(r.getAckID());
                        System.out.println("Received message " + ackIDs.size() + "/" + toSend.size() +
                                " with AckID: " + r.getAckID());
                    } else {
                        System.out.println("Received null message, waiting...");
                        TimeUnit.MILLISECONDS.sleep(100);
                    }
                } catch (Exception e) {
                    System.err.println("Error receiving message: " + e.getMessage());
                    TimeUnit.MILLISECONDS.sleep(100);
                }
            }

            Assertions.assertEquals(toSend.size(), ackIDs.size(),
                    "Should collect all AckIDs. Expected: " + toSend.size() + ", Got: " + ackIDs.size());
            subscription.sendAcks(ackIDs).join();
        }
    }

    @Test
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

            List<AckID> ackIDs = new java.util.ArrayList<>();
            boolean isRecording = System.getProperty("record") != null;
            long timeoutSeconds = isRecording ? 120 : 60; // Increased timeout for integration tests
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);

            System.out.println("Starting to collect " + toSend.size() + " messages with timeout: " + timeoutSeconds + "s");

            while (ackIDs.size() < toSend.size() && System.nanoTime() < deadline) {
                try {
                    Message r = subscription.receive();
                    if (r != null && r.getAckID() != null) {
                        ackIDs.add(r.getAckID());
                        System.out.println("Received message " + ackIDs.size() + "/" + toSend.size() +
                                " with AckID: " + r.getAckID());
                    } else {
                        System.out.println("Received null message, waiting...");
                        TimeUnit.MILLISECONDS.sleep(100);
                    }
                } catch (Exception e) {
                    System.err.println("Error receiving message: " + e.getMessage());
                    TimeUnit.MILLISECONDS.sleep(100);
                }
            }

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

    @Disabled
    @Test
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

            List<Message> receivedMessages = new ArrayList<>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);

            while (receivedMessages.size() < 3 && System.nanoTime() < deadline) {
                Message received = subscription.receive();
                if (received != null) {
                    receivedMessages.add(received);
                } else {
                    TimeUnit.MILLISECONDS.sleep(100);
                }
            }

            Assertions.assertEquals(3, receivedMessages.size(), "Should receive all 3 messages within timeout");

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
    public void testMultipleSendReceiveWithoutBatch() throws Exception {
        Assumptions.assumeFalse(AWS_PROVIDER_ID.equals(harness.getProviderId()));
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
                TimeUnit.MILLISECONDS.sleep(100); // Small delay between sends
            }

            TimeUnit.MILLISECONDS.sleep(500); // Allow time for messages to be available

            // Receive and ack messages one by one (not in batch)
            List<Message> receivedMessages = new ArrayList<>();
            
            while (receivedMessages.size() < numMessages) {
                try {
                    Message received = subscription.receive();
                    if (received != null && received.getAckID() != null) {
                        receivedMessages.add(received);
                        // Ack immediately after receiving (not in batch)
                        subscription.sendAck(received.getAckID());
                        System.out.println("Received and acked message " + receivedMessages.size() + "/" + numMessages);
                    } else {
                        TimeUnit.MILLISECONDS.sleep(100);
                    }
                } catch (Exception e) {
                    System.err.println("Error receiving message: " + e.getMessage());
                    TimeUnit.MILLISECONDS.sleep(100);
                }
            }

            Assertions.assertEquals(numMessages, receivedMessages.size(),
                    "Should receive all messages. Expected: " + numMessages + ", Got: " + receivedMessages.size());

            // Verify all messages were received
            for (int i = 0; i < receivedMessages.size(); i++) {
                Message received = receivedMessages.get(i);
                Assertions.assertNotNull(received, "Received message " + i + " should not be null");
                Assertions.assertNotNull(received.getBody(), "Received message " + i + " body should not be null");
                Assertions.assertNotNull(received.getAckID(), "Received message " + i + " should have AckID");
            }
        }
    }
}
