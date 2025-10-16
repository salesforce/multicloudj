package com.salesforce.multicloudj.pubsub.driver;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AbstractSubscriptionTest {

    // Helper test builder for testing builder methods
    private static class TestBuilder extends AbstractSubscription.Builder<TestSubscription> {
        @Override
        public TestSubscription build() {
            throw new UnsupportedOperationException("Test builder - build not implemented");
        }
    }

    private static class TestAckID implements AckID {
        private final String id;

        TestAckID(String id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return id;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            TestAckID testAckID = (TestAckID) obj;
            return id != null ? id.equals(testAckID.id) : testAckID.id == null;
        }

        @Override
        public int hashCode() {
            return id != null ? id.hashCode() : 0;
        }
    }

    // Mock external message source that simulates a real pub/sub service
    private static class MockMessageSource {
        private final Queue<Message> availableMessages = new ArrayDeque<>();
        private boolean shouldThrow = false;
        private Exception exceptionToThrow = null;

        void addMessages(List<Message> messages) {
            availableMessages.addAll(messages);
        }

        void setThrowException(Exception e) {
            this.shouldThrow = true;
            this.exceptionToThrow = e;
        }

        synchronized List<Message> fetchBatch(int batchSize) {
            if (shouldThrow && exceptionToThrow != null) {
                if (exceptionToThrow instanceof RuntimeException) {
                    throw (RuntimeException) exceptionToThrow;
                } else {
                    throw new RuntimeException(exceptionToThrow);
                }
            }

            List<Message> batch = new ArrayList<>();
            for (int i = 0; i < batchSize && !availableMessages.isEmpty(); i++) {
                Message msg = availableMessages.poll();
                if (msg != null) {  // Only add non-null messages
                    batch.add(msg);
                }
            }
            return batch;
        }
    }

    private static class TestSubscription extends AbstractSubscription<TestSubscription> {
        private final MockMessageSource messageSource;

        TestSubscription() {
            super("test", "sub", "region", null);
            this.messageSource = new MockMessageSource();
        }

        TestSubscription(MockMessageSource messageSource) {
            super("test", "sub", "region", null);
            this.messageSource = messageSource;
        }

        TestSubscription(MockMessageSource messageSource, TestBuilder builder) {
            super(builder);
            this.messageSource = messageSource;
        }

        @Override
        protected void doSendAcks(List<AckID> ackIDs) { }

        @Override
        protected void doSendNacks(List<AckID> ackIDs) { }

        @Override
        protected Batcher.Options createAckBatcherOptions() {
            return new Batcher.Options()
                    .setMaxHandlers(2)
                    .setMinBatchSize(1)
                    .setMaxBatchSize(1000)
                    .setMaxBatchByteSize(0);
        }

        @Override
        public boolean canNack() { return false; }

        @Override
        public Map<String, String> getAttributes() { return Collections.emptyMap(); }

        @Override
        public boolean isRetryable(Throwable error) {
            // Only retry very specific transient exceptions to avoid infinite loops
            // Most exceptions in test scenarios should fail fast
            if (error instanceof java.net.SocketTimeoutException ||
                error instanceof java.net.ConnectException ||
                error instanceof java.io.IOException && error.getMessage().contains("timeout")) {
                return true;
            }
            return false;
        }

        @Override
        public Class<? extends SubstrateSdkException> getException(Throwable t) {
            return SubstrateSdkException.class;
        }

        @Override
        protected List<Message> doReceiveBatch(int batchSize) {
            // This simulates fetching from an external service like AWS SQS or GCP Pub/Sub
            return messageSource.fetchBatch(batchSize);
        }
    }

    @Test
    @Timeout(10) // Calls receive() which could timeout
    void testReceiveMethodSignature() {
        // Test that receive() method exists and has correct signature
        MockMessageSource source = new MockMessageSource();
        source.addMessages(List.of(
            Message.builder().withBody("test".getBytes()).build()
        ));
        TestSubscription sub = new TestSubscription(source);

        // Verify method returns a Message (not List<Message>)
        Message result = sub.receive();
        assertNotNull(result);
        assertEquals("test", new String(result.getBody()));
    }

    @Test
    void testReceiveWithProviderException() {
        MockMessageSource source = new MockMessageSource();
        source.setThrowException(new RuntimeException("Provider error"));
        TestSubscription sub = new TestSubscription(source);
        assertThrows(RuntimeException.class, () -> sub.receive());
    }

    @Test
    void testReceiveWithCheckedProviderException() {
        MockMessageSource source = new MockMessageSource();
        source.setThrowException(new Exception("Checked exception"));
        TestSubscription sub = new TestSubscription(source);
        assertThrows(RuntimeException.class, () -> sub.receive());
    }

    @Test
    void testConstructorWithDefaultParameters() {
        MockMessageSource source = new MockMessageSource();
        TestSubscription sub = new TestSubscription(source);
        assertEquals("test", sub.getProviderId());
    }



    @Test
    void testCloseMethod() throws Exception {
        MockMessageSource source = new MockMessageSource();
        TestSubscription sub = new TestSubscription(source);
        // Just verify close method can be called without exception
        assertDoesNotThrow(() -> sub.close());
    }

    @Test
    void testAbstractMethodsAreImplemented() {
        MockMessageSource source = new MockMessageSource();
        TestSubscription sub = new TestSubscription(source);

        // Test that methods exist and can be called
        assertDoesNotThrow(() -> sub.sendAcks(Collections.emptyList()));
        assertThrows(InvalidArgumentException.class, () -> sub.sendNack(null));
        assertDoesNotThrow(() -> sub.sendNacks(Collections.emptyList()));
        assertFalse(sub.canNack());
        assertNotNull(sub.getAttributes());
        assertFalse(sub.isRetryable(new RuntimeException()));
        assertNotNull(sub.getException(new RuntimeException()));
    }

    @Test
    void testSendAckWithNullAckID() {
        MockMessageSource source = new MockMessageSource();
        TestSubscription sub = new TestSubscription(source);

        // sendAck should throw exception for null AckID
        assertThrows(InvalidArgumentException.class, () -> sub.sendAck(null));
    }

    @Test
    void testSendAcksWithNullAckID() {
        MockMessageSource source = new MockMessageSource();
        TestSubscription sub = new TestSubscription(source);

        // sendAcks should throw exception for null AckID in list
        List<AckID> ackIDs = new ArrayList<>();
        ackIDs.add(null);
        
        assertThrows(InvalidArgumentException.class, () -> sub.sendAcks(ackIDs));
    }

    @Test
    void testSendAcksWithEmptyList() {
        MockMessageSource source = new MockMessageSource();
        TestSubscription sub = new TestSubscription(source);

        // sendAcks should return completed future for empty list
        CompletableFuture<Void> future = sub.sendAcks(Collections.emptyList());
        assertTrue(future.isDone());
        assertDoesNotThrow(() -> future.get());
    }

    @Test
    void testSendAckWithValidAckID() {
        MockMessageSource source = new MockMessageSource();
        TestSubscription sub = new TestSubscription(source);

        // Create a mock AckID
        AckID mockAckID = new TestAckID("test-ack-id");

        // sendAck should not throw exception for valid AckID
        assertDoesNotThrow(() -> sub.sendAck(mockAckID));
    }

    @Test
    void testSendAcksWithValidAckIDs() {
        MockMessageSource source = new MockMessageSource();
        TestSubscription sub = new TestSubscription(source);

        // Create mock AckIDs
        List<AckID> ackIDs = List.of(
            new TestAckID("test-ack-id-1"),
            new TestAckID("test-ack-id-2")
        );

        // sendAcks should return completed future for valid AckIDs
        CompletableFuture<Void> future = sub.sendAcks(ackIDs);
        assertTrue(future.isDone());
        assertDoesNotThrow(() -> future.get());
    }

    @Test
    void testExecutionExceptionWithNullCause() {
        MockMessageSource source = new MockMessageSource();
        TestSubscription sub = new TestSubscription(source) {
            @Override
            protected List<Message> doReceiveBatch(int batchSize) {
                throw new RuntimeException(new ExecutionException("test", null));
            }
        };

        // Should handle ExecutionException with null cause gracefully
        assertThrows(RuntimeException.class, () -> sub.receive());
    }

    @Test
    void testInterruptedExceptionHandling() throws InterruptedException {
        // Test that subscription handles interruption gracefully
        MockMessageSource source = new MockMessageSource();
        source.setThrowException(new InterruptedException("Test interruption"));
        TestSubscription sub = new TestSubscription(source);

        // Should handle InterruptedException and wrap in SubstrateSdkException
        assertThrows(SubstrateSdkException.class, () -> sub.receive());
    }

    @Test
    void testPermanentErrorStateHandling() {
        MockMessageSource source = new MockMessageSource();
        TestSubscription sub = new TestSubscription(source);

        // Inject a permanent error by causing doReceiveBatch to throw
        source.setThrowException(new RuntimeException("Permanent error"));

        // First call should trigger the error and set permanent error state
        assertThrows(RuntimeException.class, () -> sub.receive());

        // Note: Testing subsequent calls would require more complex setup to avoid blocking
        // The permanent error state is set correctly by the first call
    }


    @Test
    void testBuilderMethods() {
        // Test that builder pattern works (even though we can't fully test concrete builders)
        TestBuilder builder = new TestBuilder();

        builder.withSubscriptionName("test-sub")
               .withRegion("us-west-2")
               .withEndpoint(URI.create("https://custom-endpoint.example.com"))
               .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
               .withCredentialsOverrider(null); // Use default credentials

        // Verify builder stores values
        assertEquals("test-sub", builder.subscriptionName);
        assertEquals("us-west-2", builder.region);
        assertEquals(URI.create("https://custom-endpoint.example.com"), builder.endpoint);
        assertEquals(URI.create("https://proxy.example.com:8080"), builder.proxyEndpoint);
    }


    @Test
    void testPrefetchErrorHandling() throws Exception {
        // Test error handling in doPrefetch
        MockMessageSource source = new MockMessageSource();

        TestSubscription sub = new TestSubscription(source) {
            private volatile boolean firstCall = true;

            @Override
            protected List<Message> doReceiveBatch(int batchSize) {
                if (firstCall) {
                    firstCall = false;
                    // Throw a non-retryable exception during prefetch
                    throw new IllegalArgumentException("Non-retryable prefetch error");
                }
                return source.fetchBatch(batchSize);
            }

            @Override
            public boolean isRetryable(Throwable error) {
                // Make IllegalArgumentException non-retryable to trigger permanentError setting
                return !(error instanceof IllegalArgumentException);
            }
        };

        // First call should trigger prefetch error, which sets permanent error state and throws SubstrateSdkException
        assertThrows(SubstrateSdkException.class, () -> sub.receive());

        // Subsequent calls should also throw due to permanent error state
        assertThrows(SubstrateSdkException.class, () -> sub.receive());

        sub.close();
    }

    @Test
    void testAdvancedShutdownLogic() throws Exception {
        MockMessageSource source = new MockMessageSource();
        source.addMessages(List.of(
            Message.builder().withBody("test".getBytes()).build()
        ));

        TestSubscription sub = new TestSubscription(source);

        // First receive a message to ensure the background pool is initialized
        Message msg = sub.receive();
        assertNotNull(msg);
        assertEquals("test", new String(msg.getBody()));

        assertDoesNotThrow(() -> sub.close());

        // Verify that after close, the subscription cannot be used
        assertThrows(SubstrateSdkException.class, () -> sub.receive());
    }


    @Test
    void testSendNackWithValidAckID() {
        TestSubscription testSubscription = new TestSubscription();
        AckID ackID = new TestAckID("test-ack-id");
        
        assertDoesNotThrow(() -> testSubscription.sendNack(ackID));
    }
    
    @Test
    void testSendNackWithNullAckID() {
        TestSubscription testSubscription = new TestSubscription();
        
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
            () -> testSubscription.sendNack(null));
        assertEquals("AckID cannot be null", exception.getMessage());
    }
    
    @Test
    void testSendNacksWithValidAckIDs() {
        TestSubscription testSubscription = new TestSubscription();
        List<AckID> ackIDs = Arrays.asList(
            new TestAckID("test-ack-id-1"),
            new TestAckID("test-ack-id-2")
        );
        
        assertDoesNotThrow(() -> testSubscription.sendNacks(ackIDs));
    }
    
    @Test
    void testSendNacksWithNullList() {
        TestSubscription testSubscription = new TestSubscription();
        
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
            () -> testSubscription.sendNacks(null));
        assertEquals("AckIDs list cannot be null", exception.getMessage());
    }
    
    @Test
    void testSendNacksWithEmptyList() {
        TestSubscription testSubscription = new TestSubscription();
        List<AckID> emptyList = Collections.emptyList();
        
        CompletableFuture<Void> future = testSubscription.sendNacks(emptyList);
        assertTrue(future.isDone());
        assertDoesNotThrow(() -> future.get());
    }
    
    @Test
    void testSendNacksWithNullAckIDInList() {
        TestSubscription testSubscription = new TestSubscription();
        List<AckID> ackIDs = Arrays.asList(
            new TestAckID("test-ack-id-1"),
            null,
            new TestAckID("test-ack-id-2")
        );
        
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
            () -> testSubscription.sendNacks(ackIDs));
        assertEquals("AckID cannot be null in batch negative acknowledgment", exception.getMessage());
    }
    
    @Test
    void testSendNacksWithSingleAckID() {
        TestSubscription testSubscription = new TestSubscription();
        List<AckID> ackIDs = Arrays.asList(new TestAckID("test-ack-id"));
        
        assertDoesNotThrow(() -> testSubscription.sendNacks(ackIDs));
    }
    
    @Test
    void testSendNacksWithLargeList() {
        TestSubscription testSubscription = new TestSubscription();
        List<AckID> ackIDs = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            ackIDs.add(new TestAckID("test-ack-id-" + i));
        }
        
        assertDoesNotThrow(() -> testSubscription.sendNacks(ackIDs));
    }
    
    @Test
    @Timeout(30) // Creates 10 threads with 100 ops each, uses latch.await() without timeout
    void testSendNacksConcurrentAccess() throws InterruptedException {
        TestSubscription testSubscription = new TestSubscription();
        int numThreads = 10;
        int acksPerThread = 100;
        CountDownLatch latch = new CountDownLatch(numThreads);
        List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            new Thread(() -> {
                try {
                    for (int j = 0; j < acksPerThread; j++) {
                        List<AckID> ackIDs = Arrays.asList(new TestAckID("thread-" + threadId + "-ack-" + j));
                        testSubscription.sendNacks(ackIDs);
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        
        latch.await();
        assertTrue(exceptions.isEmpty(), "No exceptions should occur during concurrent access");
    }
    
    @Test
    void testSendNackAndSendNacksConsistency() {
        TestSubscription testSubscription = new TestSubscription();
        AckID ackID = new TestAckID("test-ack-id");
        
        // Both methods should work with the same AckID
        assertDoesNotThrow(() -> testSubscription.sendNack(ackID));
        assertDoesNotThrow(() -> testSubscription.sendNacks(Arrays.asList(ackID)));
    }
    
    @Test
    void testSendNackAfterSubscriptionClose() throws Exception {
        TestSubscription testSubscription = new TestSubscription();
        AckID ackID = new TestAckID("test-ack-id");
        
        testSubscription.close();
        
        assertThrows(SubstrateSdkException.class, () -> testSubscription.sendNack(ackID));
    }
    
    @Test
    void testSendNacksAfterSubscriptionClose() throws Exception {
        TestSubscription testSubscription = new TestSubscription();
        List<AckID> ackIDs = Arrays.asList(new TestAckID("test-ack-id"));
        
        testSubscription.close();
        
        assertThrows(SubstrateSdkException.class, () -> testSubscription.sendNacks(ackIDs));
    }
    
    @Test
    void testSendNackWithDifferentAckIDTypes() {
        TestSubscription testSubscription = new TestSubscription();
        
        AckID ackID1 = new TestAckID("test-ack-id-1");
        AckID ackID2 = new TestAckID("test-ack-id-2");
        
        assertDoesNotThrow(() -> testSubscription.sendNack(ackID1));
        assertDoesNotThrow(() -> testSubscription.sendNack(ackID2));
    }
    
    @Test
    void testSendNacksWithMixedAckIDTypes() {
        TestSubscription testSubscription = new TestSubscription();
        List<AckID> ackIDs = Arrays.asList(
            new TestAckID("test-ack-id-1"),
            new TestAckID("test-ack-id-2")
        );
        
        assertDoesNotThrow(() -> testSubscription.sendNacks(ackIDs));
    }
    
    
    @Test
    void testSendNacksReturnValue() {
        TestSubscription testSubscription = new TestSubscription();
        List<AckID> ackIDs = Arrays.asList(new TestAckID("test-ack-id"));
        
        CompletableFuture<Void> future = testSubscription.sendNacks(ackIDs);
        
        // Future should be completed immediately
        assertTrue(future.isDone());
        assertDoesNotThrow(() -> future.get());
    }
    
    @Test
    void testSendNacksWithEmptyListReturnValue() {
        TestSubscription testSubscription = new TestSubscription();
        List<AckID> emptyList = Collections.emptyList();
        
        CompletableFuture<Void> future = testSubscription.sendNacks(emptyList);
        
        assertTrue(future.isDone());
        assertDoesNotThrow(() -> future.get());
    }
    
    @Test
    void testSendNackAndSendAckConsistency() {
        TestSubscription testSubscription = new TestSubscription();
        AckID ackID = new TestAckID("test-ack-id");
        
        // Both ack and nack should work with the same AckID
        assertDoesNotThrow(() -> testSubscription.sendAck(ackID));
        assertDoesNotThrow(() -> testSubscription.sendNack(ackID));
    }
    
    @Test
    void testSendNacksAndSendAcksConsistency() {
        TestSubscription testSubscription = new TestSubscription();
        List<AckID> ackIDs = Arrays.asList(new TestAckID("test-ack-id"));
        
        // Both acks and nacks should work with the same AckID list
        assertDoesNotThrow(() -> testSubscription.sendAcks(ackIDs));
        assertDoesNotThrow(() -> testSubscription.sendNacks(ackIDs));
    }
    
    
    
    @Test
    void testSendNackWithSameAckIDMultipleTimes() {
        TestSubscription testSubscription = new TestSubscription();
        AckID ackID = new TestAckID("test-ack-id");
        
        // Send the same AckID multiple times - should not throw exception
        assertDoesNotThrow(() -> testSubscription.sendNack(ackID));
        assertDoesNotThrow(() -> testSubscription.sendNack(ackID));
        assertDoesNotThrow(() -> testSubscription.sendNack(ackID));
    }
    
    @Test
    void testSendNacksWithSameAckIDMultipleTimes() {
        TestSubscription testSubscription = new TestSubscription();
        AckID ackID = new TestAckID("test-ack-id");
        List<AckID> ackIDs = Arrays.asList(ackID);
        
        // Send the same AckID multiple times - should not throw exception
        assertDoesNotThrow(() -> testSubscription.sendNacks(ackIDs));
        assertDoesNotThrow(() -> testSubscription.sendNacks(ackIDs));
        assertDoesNotThrow(() -> testSubscription.sendNacks(ackIDs));
    }
    
    @Test
    void testSendNackWithNullAckIDAfterValidAckID() {
        TestSubscription testSubscription = new TestSubscription();
        AckID validAckID = new TestAckID("valid-ack-id");
        
        // First send a valid nack
        assertDoesNotThrow(() -> testSubscription.sendNack(validAckID));
        
        // Then try to send null AckID - should still throw exception
        assertThrows(InvalidArgumentException.class, () -> testSubscription.sendNack(null));
    }
    
    @Test
    void testSendNacksWithNullAckIDAfterValidAckIDs() {
        TestSubscription testSubscription = new TestSubscription();
        List<AckID> validAckIDs = Arrays.asList(new TestAckID("valid-ack-id"));
        
        // First send valid nacks
        assertDoesNotThrow(() -> testSubscription.sendNacks(validAckIDs));
        
        // Then try to send null list - should still throw exception
        assertThrows(InvalidArgumentException.class, () -> testSubscription.sendNacks(null));
    }
    
    @Test
    void testSendNackAndSendNacksMixedUsage() {
        TestSubscription testSubscription = new TestSubscription();
        AckID singleAckID = new TestAckID("single-ack-id");
        List<AckID> multipleAckIDs = Arrays.asList(
            new TestAckID("multiple-ack-id-1"),
            new TestAckID("multiple-ack-id-2")
        );
        
        // Mix single and multiple nack calls
        assertDoesNotThrow(() -> testSubscription.sendNack(singleAckID));
        assertDoesNotThrow(() -> testSubscription.sendNacks(multipleAckIDs));
        assertDoesNotThrow(() -> testSubscription.sendNack(singleAckID));
        assertDoesNotThrow(() -> testSubscription.sendNacks(multipleAckIDs));
    }
    
}