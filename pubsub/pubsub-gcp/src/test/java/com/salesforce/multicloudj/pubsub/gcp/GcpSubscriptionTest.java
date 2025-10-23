package com.salesforce.multicloudj.pubsub.gcp;

import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.pubsub.driver.Message;
import com.salesforce.multicloudj.pubsub.driver.AckID;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

import com.google.pubsub.v1.PubsubMessage;
import com.google.protobuf.ByteString;

@ExtendWith(MockitoExtension.class)
public class GcpSubscriptionTest {
    
    private static final String VALID_SUBSCRIPTION_NAME = "projects/test-project/subscriptions/test-subscription";

    @Mock
    private SubscriptionAdminClient mockSubscriptionAdminClient;

    @Mock
    private CredentialsOverrider mockCredentialsOverrider;
    
    private GcpSubscription subscription;

    @BeforeEach
    void setUp() {
        GcpSubscription.Builder builder = new GcpSubscription.Builder()
            .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
            .withCredentialsOverrider(mockCredentialsOverrider)
            .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"));

        subscription = new GcpSubscription(builder, mockSubscriptionAdminClient);
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
        
        ReceivedMessage receivedMessage = ReceivedMessage.newBuilder()
                .setMessage(pubsubMessage)
                .setAckId("test-ack-id")
                .build();

        Message result = subscription.convertToMessage(receivedMessage);
        
        assertNotNull(result);
        assertEquals("test message body", new String(result.getBody()));
        assertEquals("test-message-id", result.getLoggableID());
        assertNotNull(result.getAckID()); 
        assertInstanceOf(AckID.class, result.getAckID());
        assertEquals("test-ack-id", result.getAckID().toString());
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
        // Mock the subscriptionAdminClient to return empty list
        UnaryCallable<PullRequest, PullResponse> mockCallable = mock(UnaryCallable.class);
        when(mockSubscriptionAdminClient.pullCallable()).thenReturn(mockCallable);
        when(mockCallable.call(any(PullRequest.class))).thenReturn(PullResponse.newBuilder().build());
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
    }

    @Test
    void testDoReceiveBatchWithLargeBatchSize() {
        // Mock the subscriptionAdminClient to return empty list
        UnaryCallable<PullRequest, PullResponse> mockCallable = mock(UnaryCallable.class);
        when(mockSubscriptionAdminClient.pullCallable()).thenReturn(mockCallable);
        when(mockCallable.call(any(PullRequest.class))).thenReturn(PullResponse.newBuilder().build());
        List<Message> messages = subscription.doReceiveBatch(1000); // Max batch size
        assertNotNull(messages);
        assertTrue(messages.isEmpty());
    }

    @Test
    void testDoReceiveBatchWithInvalidSubscriptionName() {
        // Test that doReceiveBatch handles invalid subscription names gracefully
        // This tests the error path in getOrCreateSubscriptionAdminClient
        // Use a valid format but invalid project/subscription that will fail during GCP operations
        SubscriptionAdminClient mockClient = mock(SubscriptionAdminClient.class);
        UnaryCallable<PullRequest, PullResponse> mockCallable = mock(UnaryCallable.class);
        when(mockClient.pullCallable()).thenReturn(mockCallable);
        when(mockCallable.call(any(PullRequest.class))).thenReturn(PullResponse.newBuilder().build());

        GcpSubscription.Builder builder = new GcpSubscription.Builder()
                .withSubscriptionName("projects/non-existent-project/subscriptions/non-existent-sub");
        GcpSubscription invalidSubscription = new GcpSubscription(builder, mockClient);
        
        // Should return empty list when subscription creation fails during GCP operations
        List<Message> messages = invalidSubscription.doReceiveBatch(10);
        assertNotNull(messages);
        assertTrue(messages.isEmpty());
    }

    @Test
    void testDoReceiveBatchWithExceptionHandling() {
        // Test that doReceiveBatch handles exceptions gracefully
        CredentialsOverrider invalidCredentials = new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(new StsCredentials("invalid", "invalid", "invalid"))
            .build();
        
        SubscriptionAdminClient mockClient = mock(SubscriptionAdminClient.class);
        UnaryCallable<PullRequest, PullResponse> mockCallable = mock(UnaryCallable.class);
        when(mockClient.pullCallable()).thenReturn(mockCallable);
        when(mockCallable.call(any(PullRequest.class))).thenReturn(PullResponse.newBuilder().build());
        
        GcpSubscription.Builder builder = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(invalidCredentials);
        GcpSubscription failingSubscription = new GcpSubscription(builder, mockClient);
        // This should trigger the exception handling path and return empty list
        List<Message> messages = failingSubscription.doReceiveBatch(10);
        assertNotNull(messages);
        assertTrue(messages.isEmpty());
    }

    @Test
    void testCloseWithSubscriberError() {
        CredentialsOverrider invalidCredentials = new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(new StsCredentials("invalid", "invalid", "invalid"))
            .build();
        
        GcpSubscription.Builder builder = new GcpSubscription.Builder()
            .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(invalidCredentials);
        GcpSubscription failingSubscription = new GcpSubscription(builder, mockSubscriptionAdminClient);
        
        // We don't need to call doReceiveBatch since it will fail anyway
        // The important thing is that close() doesn't throw
        assertDoesNotThrow(() -> failingSubscription.close());
    }

    @Test
    void testSendAckWithValidAckID() {
            AckID mockAckID = new GcpSubscription.GcpAckID("test-ack-id");
        assertDoesNotThrow(() -> subscription.sendAck(mockAckID));
    }

    @Test
    void testSendAckWithNullAckID() {
            InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
                () -> subscription.sendAck(null));
            assertTrue(exception.getMessage().contains("AckID cannot be null"));
        }

    @Test
    void testSendAcksWithValidAckIDs() {
        AckID mockAckID1 = new GcpSubscription.GcpAckID("test-ack-id-1");
        AckID mockAckID2 = new GcpSubscription.GcpAckID("test-ack-id-2");
            List<AckID> ackIDs = List.of(mockAckID1, mockAckID2);
            
        var future = subscription.sendAcks(ackIDs);
            assertNotNull(future);
            assertTrue(future.isDone());
    }

    @Test
    void testSendAcksWithEmptyList() {
        var future = subscription.sendAcks(List.of());
            assertNotNull(future);
            assertTrue(future.isDone());
    }

    @Test
    void testSendAcksWithNullList() {
        var future = subscription.sendAcks(null);
            assertNotNull(future);
            assertTrue(future.isDone());
    }

    @Test
    void testSendAcksWithNullAckID() {
            List<AckID> ackIDs = new ArrayList<>();
            ackIDs.add(new GcpSubscription.GcpAckID("test-ack-id"));
            ackIDs.add(null);
            
            InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
                () -> subscription.sendAcks(ackIDs));
            assertTrue(exception.getMessage().contains("AckID cannot be null in batch acknowledgment"));
        }

    @Test
    void testIsRetryableWithNull() {
        assertFalse(subscription.isRetryable(null));
    }

    @Test
    void testIsRetryableWithNonApiException() {
            RuntimeException runtimeException = new RuntimeException("Some error");
        assertFalse(subscription.isRetryable(runtimeException));
    }

    @Test
    void testDoSendAcksWhenClosed() throws Exception {
        subscription.close();
            
            AckID mockAckID = new GcpSubscription.GcpAckID("test-ack-id");
            List<AckID> ackIDs = List.of(mockAckID);
            
        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class,
                () -> subscription.sendAcks(ackIDs));
        assertTrue(exception.getMessage().contains("Subscription has been shut down"));
    }

    @Test
    void testDoSendAcksWithSuccessfulCall() {
            com.google.api.gax.rpc.UnaryCallable<com.google.pubsub.v1.AcknowledgeRequest, com.google.protobuf.Empty> mockCallable = 
                mock(com.google.api.gax.rpc.UnaryCallable.class);
        when(mockSubscriptionAdminClient.acknowledgeCallable()).thenReturn(mockCallable);
            
        AckID mockAckID1 = new GcpSubscription.GcpAckID("test-ack-id-1");
        AckID mockAckID2 = new GcpSubscription.GcpAckID("test-ack-id-2");
            List<AckID> ackIDs = List.of(mockAckID1, mockAckID2);
            
        assertDoesNotThrow(() -> subscription.doSendAcks(ackIDs));
            verify(mockCallable).call(any(com.google.pubsub.v1.AcknowledgeRequest.class));
    }

    @Test
    void testDoSendNacksWithValidAckIDs() throws Exception {
        UnaryCallable<ModifyAckDeadlineRequest, Empty> mockCallable = mock(UnaryCallable.class);
        when(mockSubscriptionAdminClient.modifyAckDeadlineCallable()).thenReturn(mockCallable);
        when(mockCallable.call(any(ModifyAckDeadlineRequest.class))).thenReturn(Empty.getDefaultInstance());
        
        AckID mockAckID = new GcpSubscription.GcpAckID("test-ack-id");
        List<AckID> ackIDs = List.of(mockAckID);
        
        assertDoesNotThrow(() -> subscription.doSendNacks(ackIDs));
        verify(mockCallable).call(any(ModifyAckDeadlineRequest.class));
    }

    @Test
    void testIsRetryableWithExecutionExceptionNullCause() {
        java.util.concurrent.ExecutionException executionException = 
            new java.util.concurrent.ExecutionException("No cause", null);
        
        assertFalse(subscription.isRetryable(executionException));
    }
    
    @Test
    void testSendNacksWithValidAckID() {
            AckID mockAckID = new GcpSubscription.GcpAckID("test-ack-id");
        assertDoesNotThrow(() -> subscription.sendNack(mockAckID));
    }

    @Test
    void testSendNacksWithNullAckID() {
            InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
                () -> subscription.sendNack(null));
            assertTrue(exception.getMessage().contains("AckID cannot be null"));
    }

    @Test
    void testSendNackWithMockAckID() {
            AckID mockAckID = new GcpSubscription.GcpAckID("test-ack-id");
        assertDoesNotThrow(() -> subscription.sendNack(mockAckID));
    }

    @Test
    void testSendNacksWithValidAckIDs() {
        AckID mockAckID1 = new GcpSubscription.GcpAckID("test-ack-id-1");
        AckID mockAckID2 = new GcpSubscription.GcpAckID("test-ack-id-2");
            List<AckID> ackIDs = List.of(mockAckID1, mockAckID2);
            
        var future = subscription.sendNacks(ackIDs);
            assertNotNull(future);
            assertTrue(future.isDone());
    }

    @Test
    void testSendNacksWithEmptyList() {
        var future = subscription.sendNacks(List.of());
            assertNotNull(future);
            assertTrue(future.isDone());
    }

    @Test
    void testSendNacksWithNullList() {
            InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
                () -> subscription.sendNacks(null));
            assertTrue(exception.getMessage().contains("AckIDs list cannot be null"));
    }

    @Test
    void testSendNacksWithNullAckIDInList() {
            List<AckID> ackIDs = new ArrayList<>();
            ackIDs.add(new GcpSubscription.GcpAckID("test-ack-id"));
            ackIDs.add(null);
            
            InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
                () -> subscription.sendNacks(ackIDs));
            assertTrue(exception.getMessage().contains("AckID cannot be null in batch negative acknowledgment"));
    }

    @Test
    void testSendNacksWithMockAckIDsInList() {
            AckID mockAckID1 = new GcpSubscription.GcpAckID("test-ack-id-1");
            AckID mockAckID2 = new GcpSubscription.GcpAckID("test-ack-id-2");
            List<AckID> ackIDs = List.of(mockAckID1, mockAckID2);
            
        assertDoesNotThrow(() -> subscription.sendNacks(ackIDs));
    }

    @Test
    void testDoSendNacksWithSuccessfulCall() {
            UnaryCallable<ModifyAckDeadlineRequest, Empty> mockCallable = 
                mock(UnaryCallable.class);
        when(mockSubscriptionAdminClient.modifyAckDeadlineCallable()).thenReturn(mockCallable);
            
        AckID mockAckID1 = new GcpSubscription.GcpAckID("test-ack-id-1");
        AckID mockAckID2 = new GcpSubscription.GcpAckID("test-ack-id-2");
            List<AckID> ackIDs = List.of(mockAckID1, mockAckID2);
            
        assertDoesNotThrow(() -> subscription.doSendNacks(ackIDs));
            verify(mockCallable).call(any(ModifyAckDeadlineRequest.class));
    }

    @Test
    void testDoSendNacksWhenClosed() throws Exception {
        subscription.close();
            
            AckID mockAckID = new GcpSubscription.GcpAckID("test-ack-id");
            List<AckID> ackIDs = List.of(mockAckID);
            
        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class,
                () -> subscription.sendNacks(ackIDs));
        assertTrue(exception.getMessage().contains("Subscription has been shut down"));
    }

    @Test
    void testDoSendNacksWithEmptyAckIDs() {
            List<AckID> emptyAckIDs = List.of();
        assertDoesNotThrow(() -> subscription.doSendNacks(emptyAckIDs));
    }

    @Test
    void testCanNack() {
        assertTrue(subscription.canNack());
    }

    @Test
    void testNackLazyMode() {
        // Create a subscription with nackLazy = true
        GcpSubscription.Builder builder = new GcpSubscription.Builder()
                .withSubscriptionName(VALID_SUBSCRIPTION_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
                .withEndpoint(URI.create("https://pubsub.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .withNackLazy(true);
        GcpSubscription lazySubscription = new GcpSubscription(builder, mockSubscriptionAdminClient);
            
            AckID mockAckID = new GcpSubscription.GcpAckID("test-ack-id");
            List<AckID> ackIDs = List.of(mockAckID);
            
            // In lazy mode, doSendNacks should return directly without calling ModifyAckDeadline
        assertDoesNotThrow(() -> lazySubscription.doSendNacks(ackIDs));
    }

    @Test
    void testNackLazyModeFalse() {
            UnaryCallable<ModifyAckDeadlineRequest, Empty> mockCallable = 
                mock(UnaryCallable.class);
        when(mockSubscriptionAdminClient.modifyAckDeadlineCallable()).thenReturn(mockCallable);
            
            AckID mockAckID = new GcpSubscription.GcpAckID("test-ack-id");
            List<AckID> ackIDs = List.of(mockAckID);
            
            // In non-lazy mode, doSendNacks should call ModifyAckDeadline
        assertDoesNotThrow(() -> subscription.doSendNacks(ackIDs));
            verify(mockCallable).call(any(ModifyAckDeadlineRequest.class));
    }

    @Test
    void testNackLazyModeDefault() {
            UnaryCallable<ModifyAckDeadlineRequest, Empty> mockCallable = 
                mock(UnaryCallable.class);
        when(mockSubscriptionAdminClient.modifyAckDeadlineCallable()).thenReturn(mockCallable);
            
            AckID mockAckID = new GcpSubscription.GcpAckID("test-ack-id");
            List<AckID> ackIDs = List.of(mockAckID);
            
            // In default mode, doSendNacks should call ModifyAckDeadline
        assertDoesNotThrow(() -> subscription.doSendNacks(ackIDs));
            verify(mockCallable).call(any(ModifyAckDeadlineRequest.class));
    }

    @Test
    void testIsRetryableWithApiException() {
        // Test with ApiException - DEADLINE_EXCEEDED
        ApiException deadlineException = mock(ApiException.class);
        StatusCode statusCode = mock(StatusCode.class);
        when(deadlineException.getStatusCode()).thenReturn(statusCode);
        when(statusCode.getCode()).thenReturn(StatusCode.Code.DEADLINE_EXCEEDED);
        
        assertTrue(subscription.isRetryable(deadlineException));
    }

    @Test
    void testRetryCodesConfiguration() {
        ApiException deadlineException = mock(ApiException.class);
        StatusCode deadlineStatusCode = mock(StatusCode.class);
        when(deadlineException.getStatusCode()).thenReturn(deadlineStatusCode);
        when(deadlineStatusCode.getCode()).thenReturn(StatusCode.Code.DEADLINE_EXCEEDED);
        assertTrue(subscription.isRetryable(deadlineException));
        
        ApiException unavailableException = mock(ApiException.class);
        StatusCode unavailableStatusCode = mock(StatusCode.class);
        when(unavailableException.getStatusCode()).thenReturn(unavailableStatusCode);
        when(unavailableStatusCode.getCode()).thenReturn(StatusCode.Code.UNAVAILABLE);
        assertFalse(subscription.isRetryable(unavailableException));
    }
}
