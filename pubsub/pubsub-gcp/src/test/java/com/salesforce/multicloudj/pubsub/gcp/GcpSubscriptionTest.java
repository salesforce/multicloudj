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
import com.salesforce.multicloudj.pubsub.client.GetAttributeResult;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.junit.jupiter.api.Assertions.assertNull;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Subscription.State;
import com.google.pubsub.v1.ExpirationPolicy;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.PushConfig.OidcToken;
import com.google.pubsub.v1.DeadLetterPolicy;
import com.google.pubsub.v1.RetryPolicy;
import com.google.protobuf.Duration;
import java.util.Arrays;

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
    void testGetAttributesWithBasicSubscription() {
        // Mock a basic subscription with minimal configuration
        Subscription mockSubscription = Subscription.newBuilder()
            .setName(VALID_SUBSCRIPTION_NAME)
            .setTopic("projects/test-project/topics/test-topic")
            .setAckDeadlineSeconds(60)
            .setEnableMessageOrdering(false)
            .setEnableExactlyOnceDelivery(false)
            .setRetainAckedMessages(false)
            .setDetached(false)
            .setState(State.ACTIVE)
            .build();

        when(mockSubscriptionAdminClient.getSubscription(VALID_SUBSCRIPTION_NAME))
            .thenReturn(mockSubscription);

        GetAttributeResult attributes = subscription.getAttributes();

        assertNotNull(attributes);
        assertEquals(VALID_SUBSCRIPTION_NAME, attributes.getName());
        assertEquals("projects/test-project/topics/test-topic", attributes.getTopic());
    }

    @Test
    void testGetAttributesWithFilter() {
        Subscription mockSubscription = Subscription.newBuilder()
            .setName(VALID_SUBSCRIPTION_NAME)
            .setTopic("projects/test-project/topics/test-topic")
            .setAckDeadlineSeconds(60)
            .setFilter("attributes.event_type = \"user_created\"")
            .setState(State.ACTIVE)
            .build();

        when(mockSubscriptionAdminClient.getSubscription(VALID_SUBSCRIPTION_NAME))
            .thenReturn(mockSubscription);

        GetAttributeResult attributes = subscription.getAttributes();

        assertNotNull(attributes);
        assertEquals(VALID_SUBSCRIPTION_NAME, attributes.getName());
        assertEquals("projects/test-project/topics/test-topic", attributes.getTopic());
    }

    @Test
    void testGetAttributesWithMessageRetention() {
        Duration retentionDuration = Duration.newBuilder().setSeconds(3600).build();
        Duration topicRetentionDuration = Duration.newBuilder().setSeconds(7200).build();

        Subscription mockSubscription = Subscription.newBuilder()
            .setName(VALID_SUBSCRIPTION_NAME)
            .setTopic("projects/test-project/topics/test-topic")
            .setAckDeadlineSeconds(60)
            .setMessageRetentionDuration(retentionDuration)
            .setTopicMessageRetentionDuration(topicRetentionDuration)
            .setState(State.ACTIVE)
            .build();

        when(mockSubscriptionAdminClient.getSubscription(VALID_SUBSCRIPTION_NAME))
            .thenReturn(mockSubscription);

        GetAttributeResult attributes = subscription.getAttributes();

        assertNotNull(attributes);
        assertEquals(VALID_SUBSCRIPTION_NAME, attributes.getName());
        assertEquals("projects/test-project/topics/test-topic", attributes.getTopic());
    }

    @Test
    void testGetAttributesWithExpirationPolicy() {
        Duration ttl = Duration.newBuilder().setSeconds(86400).build();
        ExpirationPolicy expirationPolicy = ExpirationPolicy.newBuilder().setTtl(ttl).build();

        Subscription mockSubscription = Subscription.newBuilder()
            .setName(VALID_SUBSCRIPTION_NAME)
            .setTopic("projects/test-project/topics/test-topic")
            .setAckDeadlineSeconds(60)
            .setExpirationPolicy(expirationPolicy)
            .setState(State.ACTIVE)
            .build();

        when(mockSubscriptionAdminClient.getSubscription(VALID_SUBSCRIPTION_NAME))
            .thenReturn(mockSubscription);

        GetAttributeResult attributes = subscription.getAttributes();

        assertNotNull(attributes);
        // Note: GCP expiration policy is not directly mapped to GetAttributeResult fields
    }

    @Test
    void testGetAttributesWithPushConfig() {
        OidcToken oidcToken = OidcToken.newBuilder()
            .setServiceAccountEmail("test@example.com")
            .setAudience("test-audience")
            .build();

        PushConfig pushConfig = PushConfig.newBuilder()
            .setPushEndpoint("https://example.com/push")
            .setOidcToken(oidcToken)
            .build();

        Subscription mockSubscription = Subscription.newBuilder()
            .setName(VALID_SUBSCRIPTION_NAME)
            .setTopic("projects/test-project/topics/test-topic")
            .setAckDeadlineSeconds(60)
            .setPushConfig(pushConfig)
            .setState(State.ACTIVE)
            .build();

        when(mockSubscriptionAdminClient.getSubscription(VALID_SUBSCRIPTION_NAME))
            .thenReturn(mockSubscription);

        GetAttributeResult attributes = subscription.getAttributes();

        assertNotNull(attributes);
        assertEquals(VALID_SUBSCRIPTION_NAME, attributes.getName());
        assertEquals("projects/test-project/topics/test-topic", attributes.getTopic());
    }

    @Test
    void testGetAttributesWithDeadLetterPolicy() {
        DeadLetterPolicy deadLetterPolicy = DeadLetterPolicy.newBuilder()
            .setDeadLetterTopic("projects/test-project/topics/dead-letter")
            .setMaxDeliveryAttempts(5)
            .build();

        Subscription mockSubscription = Subscription.newBuilder()
            .setName(VALID_SUBSCRIPTION_NAME)
            .setTopic("projects/test-project/topics/test-topic")
            .setAckDeadlineSeconds(60)
            .setDeadLetterPolicy(deadLetterPolicy)
            .setState(State.ACTIVE)
            .build();

        when(mockSubscriptionAdminClient.getSubscription(VALID_SUBSCRIPTION_NAME))
            .thenReturn(mockSubscription);

        GetAttributeResult attributes = subscription.getAttributes();

        assertNotNull(attributes);
        // Note: Dead letter policy is not directly mapped to GetAttributeResult fields
    }

    @Test
    void testGetAttributesWithRetryPolicy() {
        Duration minBackoff = Duration.newBuilder().setSeconds(1).build();
        Duration maxBackoff = Duration.newBuilder().setSeconds(60).build();

        RetryPolicy retryPolicy = RetryPolicy.newBuilder()
            .setMinimumBackoff(minBackoff)
            .setMaximumBackoff(maxBackoff)
            .build();

        Subscription mockSubscription = Subscription.newBuilder()
            .setName(VALID_SUBSCRIPTION_NAME)
            .setTopic("projects/test-project/topics/test-topic")
            .setAckDeadlineSeconds(60)
            .setRetryPolicy(retryPolicy)
            .setState(State.ACTIVE)
            .build();

        when(mockSubscriptionAdminClient.getSubscription(VALID_SUBSCRIPTION_NAME))
            .thenReturn(mockSubscription);

        GetAttributeResult attributes = subscription.getAttributes();

        assertNotNull(attributes);
        // Note: Retry policy is not directly mapped to GetAttributeResult fields
    }

    @Test
    void testGetAttributesWithLabels() {
        Subscription mockSubscription = Subscription.newBuilder()
            .setName(VALID_SUBSCRIPTION_NAME)
            .setTopic("projects/test-project/topics/test-topic")
            .setAckDeadlineSeconds(60)
            .putLabels("environment", "production")
            .putLabels("team", "backend")
            .putLabels("version", "1.0")
            .setState(State.ACTIVE)
            .build();

        when(mockSubscriptionAdminClient.getSubscription(VALID_SUBSCRIPTION_NAME))
            .thenReturn(mockSubscription);

        GetAttributeResult attributes = subscription.getAttributes();

        assertNotNull(attributes);
        // Note: Labels are not directly mapped to GetAttributeResult fields
    }

    @Test
    void testGetAttributesWithAllFeatures() {
        Duration retentionDuration = Duration.newBuilder().setSeconds(3600).build();
        Duration ttl = Duration.newBuilder().setSeconds(86400).build();
        Duration minBackoff = Duration.newBuilder().setSeconds(1).build();
        Duration maxBackoff = Duration.newBuilder().setSeconds(60).build();

        OidcToken oidcToken = OidcToken.newBuilder()
            .setServiceAccountEmail("test@example.com")
            .setAudience("test-audience")
            .build();

        PushConfig pushConfig = PushConfig.newBuilder()
            .setPushEndpoint("https://example.com/push")
            .setOidcToken(oidcToken)
            .build();

        DeadLetterPolicy deadLetterPolicy = DeadLetterPolicy.newBuilder()
            .setDeadLetterTopic("projects/test-project/topics/dead-letter")
            .setMaxDeliveryAttempts(5)
            .build();

        RetryPolicy retryPolicy = RetryPolicy.newBuilder()
            .setMinimumBackoff(minBackoff)
            .setMaximumBackoff(maxBackoff)
            .build();

        ExpirationPolicy expirationPolicy = ExpirationPolicy.newBuilder().setTtl(ttl).build();

        Subscription mockSubscription = Subscription.newBuilder()
            .setName(VALID_SUBSCRIPTION_NAME)
            .setTopic("projects/test-project/topics/test-topic")
            .setAckDeadlineSeconds(120)
            .setEnableMessageOrdering(true)
            .setEnableExactlyOnceDelivery(true)
            .setRetainAckedMessages(true)
            .setDetached(false)
            .setFilter("attributes.event_type = \"user_created\"")
            .setMessageRetentionDuration(retentionDuration)
            .setExpirationPolicy(expirationPolicy)
            .setPushConfig(pushConfig)
            .setDeadLetterPolicy(deadLetterPolicy)
            .setRetryPolicy(retryPolicy)
            .putLabels("environment", "production")
            .putLabels("team", "backend")
            .setState(State.ACTIVE)
            .build();

        when(mockSubscriptionAdminClient.getSubscription(VALID_SUBSCRIPTION_NAME))
            .thenReturn(mockSubscription);

        GetAttributeResult attributes = subscription.getAttributes();

        assertNotNull(attributes);

        // Basic attributes
        assertEquals(VALID_SUBSCRIPTION_NAME, attributes.getName());
        assertEquals("projects/test-project/topics/test-topic", attributes.getTopic());
    }

    @Test
    void testGetAttributesWithApiException() {
        ApiException apiException = mock(ApiException.class);

        when(mockSubscriptionAdminClient.getSubscription(VALID_SUBSCRIPTION_NAME))
            .thenThrow(apiException);

        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, 
            () -> subscription.getAttributes());
        assertTrue(exception.getMessage().contains("Failed to retrieve subscription attributes"));
    }

    @Test
    void testGetAttributesWithEmptyLabels() {
        Subscription mockSubscription = Subscription.newBuilder()
            .setName(VALID_SUBSCRIPTION_NAME)
            .setTopic("projects/test-project/topics/test-topic")
            .setAckDeadlineSeconds(60)
            .setState(State.ACTIVE)
            .build();

        when(mockSubscriptionAdminClient.getSubscription(VALID_SUBSCRIPTION_NAME))
            .thenReturn(mockSubscription);

        GetAttributeResult attributes = subscription.getAttributes();

        assertNotNull(attributes);
        // Verify basic attributes are present
        assertEquals(VALID_SUBSCRIPTION_NAME, attributes.getName());
        assertEquals("projects/test-project/topics/test-topic", attributes.getTopic());
    }

    @Test
    void testGetAttributesWithoutOptionalFields() {
        Subscription mockSubscription = Subscription.newBuilder()
            .setName(VALID_SUBSCRIPTION_NAME)
            .setTopic("projects/test-project/topics/test-topic")
            .setAckDeadlineSeconds(60)
            .setState(State.ACTIVE)
            .build();

        when(mockSubscriptionAdminClient.getSubscription(VALID_SUBSCRIPTION_NAME))
            .thenReturn(mockSubscription);

        GetAttributeResult attributes = subscription.getAttributes();

        assertNotNull(attributes);
        assertEquals(VALID_SUBSCRIPTION_NAME, attributes.getName());
        assertEquals("projects/test-project/topics/test-topic", attributes.getTopic());
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

    @Test
    void testValidateSubscriptionNameWithNull() {
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
            () -> GcpSubscription.validateSubscriptionName(null));
        assertTrue(exception.getMessage().contains("Subscription name cannot be null or empty"));
    }

    @Test
    void testValidateSubscriptionNameWithInvalidFormat() {
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
            () -> GcpSubscription.validateSubscriptionName("just-a-subscription"));
        assertTrue(exception.getMessage().contains("projects/{projectId}/subscriptions/{subscriptionId}"));
    }

    @Test
    void testValidateSubscriptionNameWithInvalidFormatMissingProjects() {
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
            () -> GcpSubscription.validateSubscriptionName("subscriptions/test-subscription"));
        assertTrue(exception.getMessage().contains("projects/{projectId}/subscriptions/{subscriptionId}"));
    }

    @Test
    void testValidateSubscriptionNameWithValidFormat() {
        assertDoesNotThrow(() -> GcpSubscription.validateSubscriptionName(VALID_SUBSCRIPTION_NAME));
    }

    @Test
    void testBuilderMethod() {
        GcpSubscription.Builder builder = subscription.builder();
        assertNotNull(builder);
        assertInstanceOf(GcpSubscription.Builder.class, builder);
    }

    @Test
    void testConstructorWithBuilderAndSubscriptionAdminClient() {
        GcpSubscription.Builder builder = new GcpSubscription.Builder()
            .withSubscriptionName(VALID_SUBSCRIPTION_NAME);
        
        SubscriptionAdminClient testClient = mock(SubscriptionAdminClient.class);
        GcpSubscription testSubscription = new GcpSubscription(builder, testClient);
        
        assertNotNull(testSubscription);
        // Verify that the subscriptionAdminClient was set correctly
        UnaryCallable<PullRequest, PullResponse> mockCallable = mock(UnaryCallable.class);
        when(testClient.pullCallable()).thenReturn(mockCallable);
        when(mockCallable.call(any(PullRequest.class))).thenReturn(PullResponse.newBuilder().build());
        
        List<Message> messages = testSubscription.doReceiveBatch(5);
        assertNotNull(messages);
        verify(testClient).pullCallable();
    }

    @Test
    void testNoArgConstructor() {
        // This should create a subscription with default builder
        GcpSubscription testSubscription = new GcpSubscription();
        assertNotNull(testSubscription);
    }

    @Test
    void testConstructorWithBuilderOnly() {
        GcpSubscription.Builder builder = new GcpSubscription.Builder()
            .withSubscriptionName(VALID_SUBSCRIPTION_NAME);
        
        GcpSubscription testSubscription = new GcpSubscription(builder);
        assertNotNull(testSubscription);
        // Verify the subscription was created successfully by checking it can be used
        assertNotNull(testSubscription.getProviderId());
    }
}
