package com.salesforce.multicloudj.pubsub.gcp;

import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.pubsub.driver.Message;
import com.salesforce.multicloudj.pubsub.driver.AckID;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PubsubMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;

import com.google.protobuf.ByteString;

@ExtendWith(MockitoExtension.class)
public class GcpSubscriptionTest {
    
    private static final String VALID_SUBSCRIPTION_NAME = "projects/test-project/subscriptions/test-subscription";
    @Mock
    private CredentialsOverrider mockCredentialsOverrider;
    
    private GcpSubscription subscription;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        subscription = new GcpSubscription.Builder()
            .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
            .withCredentialsOverrider(mockCredentialsOverrider)
            .withEndpoint(URI.create("https://pubsub.googleapis.com"))
            .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
            .build();
    }

    /**
     * Helper method to create a test subscription with common mock setup
     * @param nackLazy whether to enable nack lazy mode
     * @return configured GcpSubscription instance
     */
    private GcpSubscription createTestSubscription(boolean nackLazy) {
        return new GcpSubscription.Builder()
            .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
            .withCredentialsOverrider(mockCredentialsOverrider)
            .withEndpoint(URI.create("https://pubsub.googleapis.com"))
            .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
            .withNackLazy(nackLazy)
            .build();
    }

    /**
     * Helper method to create a test subscription with default settings (nackLazy = false)
     * @return configured GcpSubscription instance
     */
    private GcpSubscription createTestSubscription() {
        return createTestSubscription(false);
    }
    

    @Test
    void testSubscriptionNameValidation() {
        // Invalid subscription names should throw
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
            () -> new GcpSubscription.Builder()
                .withSubscriptionName("just-a-subscription")
                .build());
        assertTrue(exception.getMessage().contains("projects/{projectId}/subscriptions/{subscriptionId}"));
    }

    @Test
    void testConvertToMessage() {
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8("test message body"))
            .setMessageId("test-message-id")
            .putAttributes("key1", "value1")
            .putAttributes("key2", "value2")
            .build();
        
        Message result = subscription.convertToMessage(pubsubMessage, mock(com.google.cloud.pubsub.v1.AckReplyConsumer.class));
        
        assertNotNull(result);
        assertEquals("test message body", new String(result.getBody()));
        assertEquals("test-message-id", result.getLoggableID());
        assertNotNull(result.getAckID()); 
        assertInstanceOf(AckID.class, result.getAckID());
        assertEquals("test-message-id", result.getAckID().toString());
        assertEquals("value1", result.getMetadata().get("key1"));
        assertEquals("value2", result.getMetadata().get("key2"));
    }

    @Test
    void testCreateReceiveBatcherOptions() {
        Batcher.Options options = subscription.createReceiveBatcherOptions();
        assertEquals(10, options.getMaxHandlers());
        assertEquals(1, options.getMinBatchSize());
        assertEquals(1000, options.getMaxBatchSize());
        assertEquals(0, options.getMaxBatchByteSize()); 
    }

    @Test
    void testDoReceiveBatchWithEmptyQueue() {
        
        List<Message> messages = subscription.doReceiveBatch(5);
        assertNotNull(messages);
        assertTrue(messages.isEmpty());
    }

    @Test
    void testGetAttributes() {
        Map<String, String> attributes = subscription.getAttributes();
        assertNotNull(attributes);
        assertTrue(attributes.isEmpty());
    }

    @Test
    void testValidateAckIDTypeWithValidGcpAckID() {
        AckID validAckID = new GcpSubscription.GcpAckID("valid-ack-id");
        
        assertDoesNotThrow(() -> subscription.validateAckIDType(validAckID));
    }
    
    @Test
    void testValidateAckIDTypeWithNonGcpAckID() {
        AckID nonGcpAckID = new AckID() {
            @Override
            public String toString() {
                return "test-ack-id";
            }
        };
        
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
            () -> subscription.validateAckIDType(nonGcpAckID));
        assertTrue(exception.getMessage().contains("Expected GcpAckID"));
    }
    
    @Test
    void testGcpAckIDConstructorWithNullString() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, 
            () -> new GcpSubscription.GcpAckID(null));
        assertTrue(exception.getMessage().contains("AckID string cannot be null or empty"));
    }
    
    @Test
    void testGcpAckIDConstructorWithEmptyString() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, 
            () -> new GcpSubscription.GcpAckID(""));
        assertTrue(exception.getMessage().contains("AckID string cannot be null or empty"));
    }
    
    @Test
    void testGcpAckIDConstructorWithWhitespaceString() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, 
            () -> new GcpSubscription.GcpAckID("   "));
        assertTrue(exception.getMessage().contains("AckID string cannot be null or empty"));
    }
    
    @Test
    void testGcpAckIDConstructorWithValidString() {
        assertDoesNotThrow(() -> new GcpSubscription.GcpAckID("valid-ack-id"));
    }

    @Test
    void testValidateAckIDTypeWithNullAckID() {
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
            () -> subscription.validateAckIDType(null));
        assertTrue(exception.getMessage().contains("AckID cannot be null"));
    }





    @Test
    void testGetException() {
        assertSame(com.salesforce.multicloudj.common.exceptions.UnknownException.class, 
            subscription.getException(new RuntimeException("test")));
        assertSame(com.salesforce.multicloudj.common.exceptions.UnknownException.class, 
            subscription.getException(new IllegalArgumentException("test")));
        assertSame(com.salesforce.multicloudj.common.exceptions.UnknownException.class, 
            subscription.getException(null));
        
    
        try {
          
            assertNotNull(subscription.getException(new RuntimeException("test")));
        } catch (Exception e) {
            fail("getException should not throw an exception");
        }
    }

    @Test
    void testIsRetryable() {
        assertFalse(subscription.isRetryable(null));
        
        assertFalse(subscription.isRetryable(new RuntimeException("test")));
    }

    @Test
    void testClose() {
        assertDoesNotThrow(() -> subscription.close());
        
        
        GcpSubscription testSubscription2 = new GcpSubscription.Builder()
            .withSubscriptionName("projects/test-project/subscriptions/test-sub")
            .build();
        
        assertDoesNotThrow(() -> testSubscription2.close());
    }

    @Test
    void testDoReceiveBatchWithLargeBatchSize() {
        List<Message> messages = subscription.doReceiveBatch(1000); // Max batch size
        assertNotNull(messages);
        assertTrue(messages.isEmpty());
    }

    @Test
    void testDoReceiveBatchWithInvalidSubscriptionName() {
        // Test that doReceiveBatch handles invalid subscription names gracefully
        // This tests the error path in getOrCreateSubscriber
        // Use a valid format but invalid project/subscription that will fail during GCP operations
        GcpSubscription invalidSubscription = new GcpSubscription.Builder()
            .withSubscriptionName("projects/non-existent-project/subscriptions/non-existent-sub")
            .build();
        
        // Should return empty list when subscription creation fails during GCP operations
        List<Message> messages = invalidSubscription.doReceiveBatch(10);
        assertNotNull(messages);
        assertTrue(messages.isEmpty());
    }

    @Test
    void testDoReceiveBatchWithExceptionHandling() {
        // Test that doReceiveBatch handles exceptions gracefully
        // Create a subscription that will fail during subscriber creation due to invalid credentials
        CredentialsOverrider invalidCredentials = new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(new StsCredentials("invalid", "invalid", "invalid"))
            .build();
        
        GcpSubscription failingSubscription = new GcpSubscription.Builder()
            .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
            .withCredentialsOverrider(invalidCredentials)
            .build();
        
        // This should trigger the exception handling path and return empty list
        List<Message> messages = failingSubscription.doReceiveBatch(10);
        assertNotNull(messages);
        assertTrue(messages.isEmpty());
    }

    @Test
    void testCloseWithSubscriberError() {
        // Test that close handles subscriber errors gracefully
        CredentialsOverrider invalidCredentials = new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(new StsCredentials("invalid", "invalid", "invalid"))
            .build();
        
        GcpSubscription failingSubscription = new GcpSubscription.Builder()
            .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
            .withCredentialsOverrider(invalidCredentials)
            .build();
        
        // Test that close() works even when subscriber creation fails
        // We don't need to call doReceiveBatch since it will fail anyway
        // The important thing is that close() doesn't throw
        assertDoesNotThrow(() -> failingSubscription.close());
    }


    @Test
    void testSendAckWithValidAckID() {
        try (MockedStatic<GrpcSubscriberStub> mockedGrpcSubscriberStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            GrpcSubscriberStub mockGrpcSubscriberStub = mock(GrpcSubscriberStub.class);
            mockedGrpcSubscriberStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mockGrpcSubscriberStub);
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
            AckID mockAckID = new GcpSubscription.GcpAckID("test-ack-id");
            
            assertDoesNotThrow(() -> testSubscription.sendAck(mockAckID));
        }
    }

    @Test
    void testSendAckWithNullAckID() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mock(GrpcSubscriberStub.class));
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
            InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
                () -> testSubscription.sendAck(null));
            assertTrue(exception.getMessage().contains("AckID cannot be null"));
        }
    }


    @Test
    void testSendAcksWithValidAckIDs() {
        try (MockedStatic<GrpcSubscriberStub> mockedGrpcSubscriberStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            GrpcSubscriberStub mockGrpcSubscriberStub = mock(GrpcSubscriberStub.class);
            mockedGrpcSubscriberStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mockGrpcSubscriberStub);
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
        AckID mockAckID1 = new GcpSubscription.GcpAckID("test-ack-id-1");
        AckID mockAckID2 = new GcpSubscription.GcpAckID("test-ack-id-2");
            List<AckID> ackIDs = List.of(mockAckID1, mockAckID2);
            
            var future = testSubscription.sendAcks(ackIDs);
            assertNotNull(future);
            assertTrue(future.isDone());
        }
    }

    @Test
    void testSendAcksWithEmptyList() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mock(GrpcSubscriberStub.class));
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
            var future = testSubscription.sendAcks(List.of());
            assertNotNull(future);
            assertTrue(future.isDone());
        }
    }

    @Test
    void testSendAcksWithNullList() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mock(GrpcSubscriberStub.class));
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
            var future = testSubscription.sendAcks(null);
            assertNotNull(future);
            assertTrue(future.isDone());
        }
    }

    @Test
    void testSendAcksWithNullAckID() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mock(GrpcSubscriberStub.class));
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
            List<AckID> ackIDs = new ArrayList<>();
            ackIDs.add(new GcpSubscription.GcpAckID("test-ack-id"));
            ackIDs.add(null);
            
            InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
                () -> testSubscription.sendAcks(ackIDs));
            assertTrue(exception.getMessage().contains("AckID cannot be null in batch acknowledgment"));
        }
    }



    @Test
    void testIsRetryableWithNull() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mock(GrpcSubscriberStub.class));
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
            assertFalse(testSubscription.isRetryable(null));
        }
    }

    @Test
    void testIsRetryableWithNonApiException() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mock(GrpcSubscriberStub.class));
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
            RuntimeException runtimeException = new RuntimeException("Some error");
            assertFalse(testSubscription.isRetryable(runtimeException));
        }
    }

    @Test
    void testDoSendAcksWhenClosed() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mock(GrpcSubscriberStub.class));
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
            testSubscription.close();
            
            AckID mockAckID = new GcpSubscription.GcpAckID("test-ack-id");
            List<AckID> ackIDs = List.of(mockAckID);
            
            IllegalStateException exception = assertThrows(IllegalStateException.class, 
                () -> testSubscription.doSendAcks(ackIDs));
            assertTrue(exception.getMessage().contains("Subscription is closed, cannot send ack"));
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    void testDoSendAcksWithSuccessfulCall() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            GrpcSubscriberStub mockGrpcSubscriberStub = mock(GrpcSubscriberStub.class);
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mockGrpcSubscriberStub);
            
            com.google.api.gax.rpc.UnaryCallable<com.google.pubsub.v1.AcknowledgeRequest, com.google.protobuf.Empty> mockCallable = 
                mock(com.google.api.gax.rpc.UnaryCallable.class);
            when(mockGrpcSubscriberStub.acknowledgeCallable()).thenReturn(mockCallable);
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
        AckID mockAckID1 = new GcpSubscription.GcpAckID("test-ack-id-1");
        AckID mockAckID2 = new GcpSubscription.GcpAckID("test-ack-id-2");
            // StringAckID doesn't have getAckId method, it uses toString()
            
            List<AckID> ackIDs = List.of(mockAckID1, mockAckID2);
            
            assertDoesNotThrow(() -> testSubscription.doSendAcks(ackIDs));
            
            
            
            verify(mockCallable).call(any(com.google.pubsub.v1.AcknowledgeRequest.class));
        }
    }

    @Test
    void testDoSendNacksWithValidAckIDs() throws Exception {
        GcpSubscription testSubscription = new GcpSubscription.Builder()
            .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
            .withCredentialsOverrider(mockCredentialsOverrider)
            .withEndpoint(URI.create("https://pubsub.googleapis.com"))
            .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
            .build();
        
        SubscriberStub mockStub = mock(SubscriberStub.class);
        UnaryCallable<ModifyAckDeadlineRequest, Empty> mockCallable = mock(UnaryCallable.class);
        when(mockStub.modifyAckDeadlineCallable()).thenReturn(mockCallable);
        when(mockCallable.call(any(ModifyAckDeadlineRequest.class))).thenReturn(Empty.getDefaultInstance());
        
        Field subscriberStubField = GcpSubscription.class.getDeclaredField("subscriberStub");
        subscriberStubField.setAccessible(true);
        subscriberStubField.set(testSubscription, mockStub);
        
        AckID mockAckID = new GcpSubscription.GcpAckID("test-ack-id");
        List<AckID> ackIDs = List.of(mockAckID);
        
        assertDoesNotThrow(() -> testSubscription.doSendNacks(ackIDs));
        
        verify(mockCallable).call(any(ModifyAckDeadlineRequest.class));
    }

    @Test
    void testGetMessageIdWithStringAckID() {
        GcpSubscription testSubscription = new GcpSubscription.Builder()
            .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
            .withCredentialsOverrider(mockCredentialsOverrider)
            .withEndpoint(URI.create("https://pubsub.googleapis.com"))
            .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
            .build();
        
        AckID ackID = new GcpSubscription.GcpAckID("test-ack-id");
        
        String messageId = testSubscription.getMessageId(ackID);
        
        assertEquals("test-ack-id", messageId);
    }


    @Test
    void testIsRetryableWithExecutionExceptionNullCause() {
        GcpSubscription testSubscription = new GcpSubscription.Builder()
            .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
            .withCredentialsOverrider(mockCredentialsOverrider)
            .withEndpoint(URI.create("https://pubsub.googleapis.com"))
            .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
            .build();
        
        java.util.concurrent.ExecutionException executionException = 
            new java.util.concurrent.ExecutionException("No cause", null);
        
        assertFalse(testSubscription.isRetryable(executionException));
    }
    
    @Test
    void testSendNacksWithValidAckID() {
        try (MockedStatic<GrpcSubscriberStub> mockedGrpcSubscriberStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            GrpcSubscriberStub mockGrpcSubscriberStub = mock(GrpcSubscriberStub.class);
            mockedGrpcSubscriberStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mockGrpcSubscriberStub);
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
            AckID mockAckID = new GcpSubscription.GcpAckID("test-ack-id");
            
            assertDoesNotThrow(() -> testSubscription.sendNack(mockAckID));
        }
    }

    @Test
    void testSendNacksWithNullAckID() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mock(GrpcSubscriberStub.class));
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
            InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
                () -> testSubscription.sendNack(null));
            assertTrue(exception.getMessage().contains("AckID cannot be null"));
        }
    }

    @Test
    void testSendNackWithMockAckID() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mock(GrpcSubscriberStub.class));
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
            AckID mockAckID = new GcpSubscription.GcpAckID("test-ack-id");
            
            assertDoesNotThrow(() -> testSubscription.sendNack(mockAckID));
        }
    }

    @Test
    void testSendNacksWithValidAckIDs() {
        try (MockedStatic<GrpcSubscriberStub> mockedGrpcSubscriberStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            GrpcSubscriberStub mockGrpcSubscriberStub = mock(GrpcSubscriberStub.class);
            mockedGrpcSubscriberStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mockGrpcSubscriberStub);
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
        AckID mockAckID1 = new GcpSubscription.GcpAckID("test-ack-id-1");
        AckID mockAckID2 = new GcpSubscription.GcpAckID("test-ack-id-2");
            List<AckID> ackIDs = List.of(mockAckID1, mockAckID2);
            
            var future = testSubscription.sendNacks(ackIDs);
            assertNotNull(future);
            assertTrue(future.isDone());
        }
    }

    @Test
    void testSendNacksWithEmptyList() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mock(GrpcSubscriberStub.class));
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
            var future = testSubscription.sendNacks(List.of());
            assertNotNull(future);
            assertTrue(future.isDone());
        }
    }

    @Test
    void testSendNacksWithNullList() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mock(GrpcSubscriberStub.class));
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
            InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
                () -> testSubscription.sendNacks(null));
            assertTrue(exception.getMessage().contains("AckIDs list cannot be null"));
        }
    }

    @Test
    void testSendNacksWithNullAckIDInList() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mock(GrpcSubscriberStub.class));
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
            List<AckID> ackIDs = new ArrayList<>();
            ackIDs.add(new GcpSubscription.GcpAckID("test-ack-id"));
            ackIDs.add(null);
            
            InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
                () -> testSubscription.sendNacks(ackIDs));
            assertTrue(exception.getMessage().contains("AckID cannot be null in batch negative acknowledgment"));
        }
    }

    @Test
    void testSendNacksWithMockAckIDsInList() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mock(GrpcSubscriberStub.class));
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
            AckID mockAckID1 = new GcpSubscription.GcpAckID("test-ack-id-1");
            AckID mockAckID2 = new GcpSubscription.GcpAckID("test-ack-id-2");
            List<AckID> ackIDs = List.of(mockAckID1, mockAckID2);
            
            assertDoesNotThrow(() -> testSubscription.sendNacks(ackIDs));
        }
    }

    @Test
    void testDoSendNacksWithSuccessfulCall() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            GrpcSubscriberStub mockGrpcSubscriberStub = mock(GrpcSubscriberStub.class);
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mockGrpcSubscriberStub);
            
            UnaryCallable<ModifyAckDeadlineRequest, Empty> mockCallable = 
                mock(UnaryCallable.class);
            when(mockGrpcSubscriberStub.modifyAckDeadlineCallable()).thenReturn(mockCallable);
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
        AckID mockAckID1 = new GcpSubscription.GcpAckID("test-ack-id-1");
        AckID mockAckID2 = new GcpSubscription.GcpAckID("test-ack-id-2");
            // StringAckID doesn't have getAckId method, it uses toString()
            
            List<AckID> ackIDs = List.of(mockAckID1, mockAckID2);
            
            assertDoesNotThrow(() -> testSubscription.doSendNacks(ackIDs));
            
            
            
            verify(mockCallable).call(any(ModifyAckDeadlineRequest.class));
        }
    }

    @Test
    void testDoSendNacksWhenClosed() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mock(GrpcSubscriberStub.class));
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
            testSubscription.close();
            
            AckID mockAckID = new GcpSubscription.GcpAckID("test-ack-id");
            List<AckID> ackIDs = List.of(mockAckID);
            
            IllegalStateException exception = assertThrows(IllegalStateException.class, 
                () -> testSubscription.doSendNacks(ackIDs));
            assertTrue(exception.getMessage().contains("Subscription is closed, cannot send nack"));
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    void testDoSendNacksWithEmptyAckIDs() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mock(GrpcSubscriberStub.class));
            
            GcpSubscription testSubscription = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .build();
            
            List<AckID> emptyAckIDs = List.of();
            
            assertDoesNotThrow(() -> testSubscription.doSendNacks(emptyAckIDs));
        }
    }


    @Test
    void testCanNack() {
        GcpSubscription testSubscription = new GcpSubscription.Builder()
            .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
            .withCredentialsOverrider(mockCredentialsOverrider)
            .withEndpoint(URI.create("https://pubsub.googleapis.com"))
            .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
            .build();
        
        assertTrue(testSubscription.canNack());
    }

    @Test
    void testNackLazyMode() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mock(GrpcSubscriberStub.class));
            
            GcpSubscription testSubscription = createTestSubscription(true);
            
            AckID mockAckID = new GcpSubscription.GcpAckID("test-ack-id");
            List<AckID> ackIDs = List.of(mockAckID);
            
            // In lazy mode, doSendNacks should return directly without calling ModifyAckDeadline
            assertDoesNotThrow(() -> testSubscription.doSendNacks(ackIDs));
            
            // Verify that getAckId is not called, because it returns directly in lazy mode
           
        }
    }

    @Test
    void testNackLazyModeFalse() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            GrpcSubscriberStub mockGrpcSubscriberStub = mock(GrpcSubscriberStub.class);
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mockGrpcSubscriberStub);
            
            UnaryCallable<ModifyAckDeadlineRequest, Empty> mockCallable = 
                mock(UnaryCallable.class);
            when(mockGrpcSubscriberStub.modifyAckDeadlineCallable()).thenReturn(mockCallable);
            
            GcpSubscription testSubscription = createTestSubscription();
            
            AckID mockAckID = new GcpSubscription.GcpAckID("test-ack-id");
           
            List<AckID> ackIDs = List.of(mockAckID);
            
            // In non-lazy mode, doSendNacks should call ModifyAckDeadline
            assertDoesNotThrow(() -> testSubscription.doSendNacks(ackIDs));
            
          
            verify(mockCallable).call(any(ModifyAckDeadlineRequest.class));
        }
    }

    @Test
    void testNackLazyModeDefault() {
        try (MockedStatic<GrpcSubscriberStub> mockedStub = org.mockito.Mockito.mockStatic(GrpcSubscriberStub.class)) {
            GrpcSubscriberStub mockGrpcSubscriberStub = mock(GrpcSubscriberStub.class);
            mockedStub.when(() -> GrpcSubscriberStub.create(any(SubscriberStubSettings.class)))
                .thenReturn(mockGrpcSubscriberStub);
            
            UnaryCallable<ModifyAckDeadlineRequest, Empty> mockCallable = 
                mock(UnaryCallable.class);
            when(mockGrpcSubscriberStub.modifyAckDeadlineCallable()).thenReturn(mockCallable);
            
            GcpSubscription testSubscription = createTestSubscription();
            
            AckID mockAckID = new GcpSubscription.GcpAckID("test-ack-id");
            List<AckID> ackIDs = List.of(mockAckID);
            
            // In default mode, doSendNacks should call ModifyAckDeadline
            assertDoesNotThrow(() -> testSubscription.doSendNacks(ackIDs));
            
           
            verify(mockCallable).call(any(ModifyAckDeadlineRequest.class));
        }
    }

    @Test
    void testIsRetryableWithApiException() {
        ApiException deadlineException = mock(ApiException.class);
        StatusCode statusCode = mock(StatusCode.class);
        when(deadlineException.getStatusCode()).thenReturn(statusCode);
        when(statusCode.getCode()).thenReturn(StatusCode.Code.DEADLINE_EXCEEDED);
        
        assertTrue(subscription.isRetryable(deadlineException));
        
        when(statusCode.getCode()).thenReturn(StatusCode.Code.INVALID_ARGUMENT);
        assertFalse(subscription.isRetryable(deadlineException));
    }

    @Test
    void testIsRetryableWithExecutionExceptionWrappingApiException() {
        ApiException apiException = mock(ApiException.class);
        StatusCode statusCode = mock(StatusCode.class);
        when(apiException.getStatusCode()).thenReturn(statusCode);
        when(statusCode.getCode()).thenReturn(StatusCode.Code.DEADLINE_EXCEEDED);
        
        ExecutionException executionException = new ExecutionException("test", apiException);
        
        assertTrue(subscription.isRetryable(executionException));
    }
}
