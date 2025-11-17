package com.salesforce.multicloudj.pubsub.driver;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AbstractTopicTest {

    private TestableAbstractTopic topic;
    private static final String PROVIDER_ID = "test-provider";
    private static final String TOPIC_NAME = "test-topic";
    private static final String REGION = "test-region";
    
    @BeforeEach
    void setUp() {
        topic = new TestableAbstractTopic(PROVIDER_ID, TOPIC_NAME, REGION);
    }

    @Test
    void testConstructorWithDefaultTimeout() {
        assertEquals(PROVIDER_ID, topic.getProviderId());
        assertEquals(TOPIC_NAME, topic.topicName);
        assertEquals(REGION, topic.region);
    }

    @Test
    void testBuilderPattern() {
        TestableAbstractTopic tempTopic = new TestableAbstractTopic(PROVIDER_ID, TOPIC_NAME, REGION);
        TestableAbstractTopic builtTopic = tempTopic.builder()
            .withTopicName(TOPIC_NAME)
            .withRegion(REGION)
            .withEndpoint(java.net.URI.create("https://custom-endpoint.example.com"))
            .withProxyEndpoint(java.net.URI.create("https://proxy.example.com:8080"))
            .withCredentialsOverrider(null)
            .build();
        
        assertEquals(TOPIC_NAME, builtTopic.topicName);
        assertEquals(REGION, builtTopic.region);
        assertEquals(java.net.URI.create("https://custom-endpoint.example.com"), builtTopic.endpoint);
        assertEquals(java.net.URI.create("https://proxy.example.com:8080"), builtTopic.proxyEndpoint);
        assertNull(builtTopic.credentialsOverrider);
    }

    @Test
    void testBuilderWithoutCustomTimeout() {
        TestableAbstractTopic tempTopic = new TestableAbstractTopic(PROVIDER_ID, TOPIC_NAME, REGION);
        TestableAbstractTopic builtTopic = tempTopic.builder()
            .withTopicName(TOPIC_NAME)
            .withRegion(REGION)
            .build();
        
        assertEquals(TOPIC_NAME, builtTopic.topicName);
    }

    @Test
    void testSendValidMessage() throws Exception {
        Message message = Message.builder().withBody("test-data").build();
        topic.send(message);
        
        // Verify doSendBatch was called with the message
        assertEquals(1, topic.sentBatches.size());
        assertEquals(1, topic.sentBatches.get(0).size());
        assertEquals(message, topic.sentBatches.get(0).get(0));
    }

    @Test
    void testSendNullMessage() {
        assertThrows(InvalidArgumentException.class, () -> topic.send(null));
    }

    @Test
    void testSendEmptyMessage() {
        Message emptyMessage = Message.builder().withBody(new byte[0]).build();
        // Empty messages are actually valid - they just have no body content
        topic.send(emptyMessage);
        assertEquals(1, topic.sentBatches.size());
    }

    @Test
    void testSendWithProviderException() {
        topic.shouldThrowRuntimeException = true;
        Message message = Message.builder().withBody("test-data").build();
        
        RuntimeException exception = assertThrows(RuntimeException.class, () -> topic.send(message));
        assertEquals("Provider error", exception.getMessage());
    }

    @Test
    void testSendWithCheckedProviderException() {
        topic.shouldThrowCheckedException = true;
        Message message = Message.builder().withBody("test-data").build();
        
        // Our test implementation wraps the checked exception in RuntimeException
        RuntimeException exception = assertThrows(RuntimeException.class, () -> topic.send(message));
        assertEquals("java.lang.Exception: Checked provider error", exception.getMessage());
        assertInstanceOf(Exception.class, exception.getCause());
    }

    @Test 
    void testSendWithInterruption() throws Exception {
        topic.shouldInterrupt = true;
        Message message = Message.builder().withBody("test-data").build();
        
        // The test implementation throws RuntimeException when interrupted
        RuntimeException exception = assertThrows(RuntimeException.class, () -> topic.send(message));
        assertEquals("Interrupted", exception.getMessage());
    }

    @Test
    void testGetExceptionMethod() {
        RuntimeException error = new RuntimeException("test");
        assertNull(topic.getException(error)); // Default implementation returns null
    }

    @Test
    void testConcurrentSends() throws Exception {
        int numThreads = 10;
        int messagesPerThread = 100;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicReference<Exception> errorRef = new AtomicReference<>();

        for (int i = 0; i < numThreads; i++) {
            int threadId = i;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < messagesPerThread; j++) {
                        Message message = Message.builder().withBody("thread-" + threadId + "-msg-" + j).build();
                        topic.send(message);
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    errorRef.set(e);
                } finally {
                    completeLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown(); // Start all threads
        assertTrue(completeLatch.await(8, TimeUnit.SECONDS)); // Wait for completion
        
        assertNull(errorRef.get(), "Should not have any errors");
        assertEquals(numThreads * messagesPerThread, successCount.get());
        assertTrue(topic.sentBatches.size() > 0, "Should have sent some batches");
    }

    @Test 
    void testCloseMethod() throws Exception {
        assertDoesNotThrow(() -> topic.close());
    }

    private static class TestableAbstractTopic extends AbstractTopic<TestableAbstractTopic> {
        
        // Test control flags
        boolean shouldThrowRuntimeException = false;
        boolean shouldThrowCheckedException = false;
        boolean shouldInterrupt = false;
        
        // Track method calls
        List<List<Message>> sentBatches = new ArrayList<>();

        
        public TestableAbstractTopic(String providerId, String topicName, String region) {
            super(providerId, topicName, region, null, null, null);
        }

        @Override
        protected void doSendBatch(List<Message> messages) {
            if (shouldInterrupt) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted");
            }
            
            if (shouldThrowRuntimeException) {
                throw new RuntimeException("Provider error");
            }
            
            if (shouldThrowCheckedException) {
                throw new RuntimeException(new Exception("Checked provider error"));
            }
            
            sentBatches.add(new ArrayList<>(messages));
        }



        @Override
        public Class<? extends SubstrateSdkException> getException(Throwable t) {
            return null;
        }

        @Override
        public Builder builder() {
            return new Builder();
        }

        public static class Builder extends AbstractTopic.Builder<TestableAbstractTopic> {
            @Override
            public TestableAbstractTopic build() {
                // Set provider ID for tests
                this.providerId = PROVIDER_ID;
                return new TestableAbstractTopic(this);
            }
        }
        
        public TestableAbstractTopic(Builder builder) {
            super(builder);
        }
    }
}