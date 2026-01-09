package com.salesforce.multicloudj.pubsub.aws;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.pubsub.client.GetAttributeResult;
import com.salesforce.multicloudj.pubsub.driver.AckID;
import com.salesforce.multicloudj.pubsub.driver.Message;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchResponse;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AwsSubscriptionTest {

    @Mock
    private SqsClient mockSqsClient;

    private AwsSubscription subscription;
    private AwsSubscription.Builder builder;

    @BeforeEach
    void setUp() {
        builder = new AwsSubscription.Builder();
        builder.withSubscriptionName("test-queue");
        builder.withRegion("us-east-1");
        builder.withSqsClient(mockSqsClient);
        
        // Mock getQueueUrl for all tests
        GetQueueUrlResponse mockResponse = GetQueueUrlResponse.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue")
            .build();
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
            .thenReturn(mockResponse);
    }

    @Test
    void testBuilderConstructor() {
        subscription = builder.build();

        assertNotNull(subscription);
        assertEquals("aws", subscription.getProviderId());
    }

    @Test
    void testBuilderWithCredentials() {
        StsCredentials credentials = new StsCredentials("key", "secret", "token");
        CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(credentials)
                .build();

        subscription = builder
                .withCredentialsOverrider(credentialsOverrider)
                .build();

        assertNotNull(subscription);
        assertEquals("aws", subscription.getProviderId());
    }

    @Test
    void testBuilderWithEndpoint() {
        URI endpoint = URI.create("https://sqs.us-east-1.amazonaws.com");

        subscription = builder
                .withEndpoint(endpoint)
                .build();

        assertNotNull(subscription);
    }

    @Test
    void testBuilderWithNackLazy() {
        subscription = builder
                .withNackLazy(true)
                .build();

        assertNotNull(subscription);
    }

    @Test
    void testBuilderWithWaitTimeSeconds() {
        subscription = builder
                .withWaitTimeSeconds(10)
                .build();

        assertNotNull(subscription);
    }

    @Test
    void testGetException_SubstrateSdkException() {
        subscription = builder.build();
        InvalidArgumentException sdkException = new InvalidArgumentException("Test error");

        Class<? extends SubstrateSdkException> result = subscription.getException(sdkException);

        assertEquals(InvalidArgumentException.class, result);
    }

    @Test
    void testGetException_AwsServiceException_QueueDoesNotExist() {
        subscription = builder.build();
        AwsErrorDetails errorDetails = AwsErrorDetails.builder()
                .errorCode("QueueDoesNotExist")
                .build();
        AwsServiceException serviceException = AwsServiceException.builder()
                .awsErrorDetails(errorDetails)
                .build();

        Class<? extends SubstrateSdkException> result = subscription.getException(serviceException);

        assertEquals(ResourceNotFoundException.class, result);
    }

    @Test
    void testGetException_AwsServiceException_AccessDenied() {
        subscription = builder.build();
        AwsErrorDetails errorDetails = AwsErrorDetails.builder()
                .errorCode("AccessDenied")
                .build();
        AwsServiceException serviceException = AwsServiceException.builder()
                .awsErrorDetails(errorDetails)
                .build();

        Class<? extends SubstrateSdkException> result = subscription.getException(serviceException);

        assertEquals(UnAuthorizedException.class, result);
    }

    @Test
    void testGetException_AwsServiceException_ThrottlingException() {
        subscription = builder.build();
        AwsErrorDetails errorDetails = AwsErrorDetails.builder()
                .errorCode("ThrottlingException")
                .build();
        AwsServiceException serviceException = AwsServiceException.builder()
                .awsErrorDetails(errorDetails)
                .build();

        Class<? extends SubstrateSdkException> result = subscription.getException(serviceException);

        assertEquals(com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException.class, result);
    }

    @Test
    void testGetException_AwsServiceException_NoErrorDetails() {
        subscription = builder.build();
        AwsServiceException serviceException = AwsServiceException.builder()
                .awsErrorDetails(null)
                .build();

        Class<? extends SubstrateSdkException> result = subscription.getException(serviceException);

        assertEquals(UnknownException.class, result);
    }

    @Test
    void testGetException_SdkClientException_UnableToLoadCredentials() {
        subscription = builder.build();
        SdkClientException clientException = SdkClientException.builder()
                .message("Unable to load credentials")
                .build();

        Class<? extends SubstrateSdkException> result = subscription.getException(clientException);

        assertEquals(InvalidArgumentException.class, result);
    }

    @Test
    void testGetException_SdkClientException_UnableToConnect() {
        subscription = builder.build();
        SdkClientException clientException = SdkClientException.builder()
                .message("Unable to connect")
                .build();

        Class<? extends SubstrateSdkException> result = subscription.getException(clientException);

        assertEquals(InvalidArgumentException.class, result);
    }

    @Test
    void testGetException_IllegalArgumentException() {
        subscription = builder.build();
        IllegalArgumentException illegalArgException = new IllegalArgumentException("Invalid argument");

        Class<? extends SubstrateSdkException> result = subscription.getException(illegalArgException);

        assertEquals(InvalidArgumentException.class, result);
    }

    @Test
    void testGetException_UnknownException() {
        subscription = builder.build();
        RuntimeException unknownException = new RuntimeException("Unknown error");

        Class<? extends SubstrateSdkException> result = subscription.getException(unknownException);

        assertEquals(UnknownException.class, result);
    }

    @Test
    void testCanNack() {
        subscription = builder.build();

        boolean result = subscription.canNack();

        assertTrue(result);
    }

    @Test
    void testIsRetryable() {
        subscription = builder.build();
        RuntimeException error = new RuntimeException("Test error");

        boolean result = subscription.isRetryable(error);

        assertFalse(result);
    }

    @Test
    void testGetAttributes() {
        String queueName = "test-queue";
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";
        String queueArn = "arn:aws:sqs:us-east-1:123456789012:test-queue";
        
        GetQueueUrlResponse mockQueueUrlResponse = GetQueueUrlResponse.builder()
            .queueUrl(queueUrl)
            .build();
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
            .thenReturn(mockQueueUrlResponse);
        
        builder.withSubscriptionName(queueName);
        subscription = builder.build();
        
        Map<QueueAttributeName, String> attributes = Map.of(
            QueueAttributeName.QUEUE_ARN, queueArn
        );
        
        GetQueueAttributesResponse mockResponse = GetQueueAttributesResponse.builder()
            .attributes(attributes)
            .build();
        
        when(mockSqsClient.getQueueAttributes(any(GetQueueAttributesRequest.class)))
            .thenReturn(mockResponse);
        
        GetAttributeResult result = subscription.getAttributes();
        
        assertNotNull(result);
        assertEquals(queueUrl, result.getName());
        assertEquals(queueArn, result.getTopic());
    }

    @Test
    void testGetAttributesWithAwsServiceException() {
        subscription = builder.build();
        
        AwsServiceException awsException = AwsServiceException.builder()
            .message("Queue does not exist")
            .build();
        
        when(mockSqsClient.getQueueAttributes(any(GetQueueAttributesRequest.class)))
            .thenThrow(awsException);
        
        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            subscription.getAttributes();
        });
        
        assertTrue(exception.getMessage().contains("Failed to retrieve subscription attributes"));
    }

    @Test
    void testGetAttributesWithSdkClientException() {
        subscription = builder.build();
        
        SdkClientException sdkException = SdkClientException.builder()
            .message("Unable to execute HTTP request")
            .build();
        
        when(mockSqsClient.getQueueAttributes(any(GetQueueAttributesRequest.class)))
            .thenThrow(sdkException);
        
        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            subscription.getAttributes();
        });
        
        assertTrue(exception.getMessage().contains("Failed to retrieve subscription attributes"));
    }

    @Test
    void testSendAck() {
        subscription = builder.build();
        AckID ackID = new AwsSubscription.AwsAckID("test-receipt-handle");

        assertDoesNotThrow(() -> subscription.sendAck(ackID));
    }

    @Test
    void testSendAcks() {
        subscription = builder.build();
        List<AckID> ackIDs = Arrays.asList(
            new AwsSubscription.AwsAckID("receipt-1"),
            new AwsSubscription.AwsAckID("receipt-2")
        );

        CompletableFuture<Void> result = subscription.sendAcks(ackIDs);

        assertNotNull(result);
    }

    @Test
    void testSendNack() {
        subscription = builder.build();
        AckID ackID = new AwsSubscription.AwsAckID("test-receipt-handle");

        assertDoesNotThrow(() -> subscription.sendNack(ackID));
    }

    @Test
    void testSendNacks() {
        subscription = builder.build();
        List<AckID> ackIDs = Arrays.asList(
            new AwsSubscription.AwsAckID("receipt-1"),
            new AwsSubscription.AwsAckID("receipt-2")
        );

        CompletableFuture<Void> result = subscription.sendNacks(ackIDs);

        assertNotNull(result);
    }


    @Test
    void testValidateSubscriptionName_NullName() {
        assertThrows(InvalidArgumentException.class, () -> {
            new AwsSubscription.Builder()
                    .withSubscriptionName(null)
                    .build();
        });
    }

    @Test
    void testValidateSubscriptionName_EmptyName() {
        assertThrows(InvalidArgumentException.class, () -> {
            new AwsSubscription.Builder()
                    .withSubscriptionName("")
                    .build();
        });
    }

    @Test
    void testValidateSubscriptionName_AcceptsQueueName() {
        // Valid queue name should be accepted
        GetQueueUrlResponse mockResponse = GetQueueUrlResponse.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
            .build();
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
            .thenReturn(mockResponse);
        
        AwsSubscription.Builder builder = new AwsSubscription.Builder();
        builder.withSubscriptionName("my-queue");
        builder.withSqsClient(mockSqsClient);
        builder.withRegion("us-east-1");
        assertDoesNotThrow(() -> builder.build());
    }


    @Test
    void testConvertToMessage() {
        subscription = builder.build();
        
        software.amazon.awssdk.services.sqs.model.Message sqsMessage = 
            software.amazon.awssdk.services.sqs.model.Message.builder()
                .body("test message body")
                .messageId("test-message-id")
                .receiptHandle("test-receipt-handle")
                .messageAttributes(Map.of(
                    "key1", MessageAttributeValue.builder().stringValue("value1").build(),
                    "key2", MessageAttributeValue.builder().stringValue("value2").build()
                ))
                .build();
        
        Message result = subscription.convertToMessage(sqsMessage);
        
        assertNotNull(result);
        assertEquals("test message body", new String(result.getBody()));
        assertEquals("test-message-id", result.getLoggableID());
        assertNotNull(result.getAckID());
        assertEquals("test-receipt-handle", result.getAckID().toString());
        assertEquals("value1", result.getMetadata().get("key1"));
        assertEquals("value2", result.getMetadata().get("key2"));
    }

    @Test
    void testDoReceiveBatch() {
        SqsClient mockSqsClient = mock(SqsClient.class);
        GetQueueUrlResponse mockQueueUrlResponse = GetQueueUrlResponse.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue")
            .build();
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
            .thenReturn(mockQueueUrlResponse);
        when(mockSqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().build());
        
        subscription = builder.withSqsClient(mockSqsClient).build();
        
        List<Message> messages = subscription.doReceiveBatch(5);
        assertNotNull(messages);
        assertTrue(messages.isEmpty());
        
        verify(mockSqsClient).receiveMessage(any(ReceiveMessageRequest.class));
    }

    @Test
    void testDecodeMetadataKey() {
        subscription = builder.build();
        
        assertEquals("", subscription.decodeMetadataKey(null));
        assertEquals("", subscription.decodeMetadataKey(""));
        assertEquals("simple.key", subscription.decodeMetadataKey("simple.key"));
        assertEquals("key.with.dots", subscription.decodeMetadataKey("key__0x2E__with__0x2E__dots"));
        assertEquals("key:with:colons", subscription.decodeMetadataKey("key__0x3A__with__0x3A__colons"));
        assertEquals("invalid.key", subscription.decodeMetadataKey("invalid__0x2E__key"));
    }

    @Test
    void testDecodeMetadataValue() {
        subscription = builder.build();
        
        assertEquals("", subscription.decodeMetadataValue(null));
        assertEquals("", subscription.decodeMetadataValue(""));
        assertEquals("simple value", subscription.decodeMetadataValue("simple value"));
        assertEquals("value with spaces", subscription.decodeMetadataValue("value%20with%20spaces"));
        assertEquals("value+with+plus", subscription.decodeMetadataValue("value%2Bwith%2Bplus"));
        assertEquals("invalid%value", subscription.decodeMetadataValue("invalid%value"));
    }

    @Test
    void testDoReceiveBatch_InterruptedException() throws Exception {
        subscription = builder.build();
        
        // Create a mock that returns empty messages, triggering sleep
        when(mockSqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().build());
        
        // Interrupt the current thread
        Thread currentThread = Thread.currentThread();
        Thread interruptThread = new Thread(() -> {
            try {
                Thread.sleep(10); // Wait a bit for the test to start
                currentThread.interrupt();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        interruptThread.start();
        
        // This should throw RuntimeException due to InterruptedException
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            subscription.doReceiveBatch(5);
        });
        
        assertEquals("Interrupted while waiting for messages", exception.getMessage());
        assertNotNull(exception.getCause());
        assertTrue(exception.getCause() instanceof InterruptedException);
        
        // Clear interrupt flag
        Thread.interrupted();
        interruptThread.join();
    }

    @Test
    void testConvertToMessage_WithBase64EncodedFlag() {
        subscription = builder.build();
        
        // Create a message with base64encoded flag
        String originalBody = "Hello, World!";
        String base64Body = Base64.getEncoder().encodeToString(originalBody.getBytes(StandardCharsets.UTF_8));
        
        software.amazon.awssdk.services.sqs.model.Message sqsMessage = 
            software.amazon.awssdk.services.sqs.model.Message.builder()
                .body(base64Body)
                .messageId("test-message-id")
                .receiptHandle("test-receipt-handle")
                .messageAttributes(Map.of(
                    "base64encoded", MessageAttributeValue.builder().stringValue("true").build(),
                    "key1", MessageAttributeValue.builder().stringValue("value1").build()
                ))
                .build();
        
        Message result = subscription.convertToMessage(sqsMessage);
        
        assertNotNull(result);
        assertEquals(originalBody, new String(result.getBody()));
        // base64encoded key should not appear in metadata
        assertFalse(result.getMetadata().containsKey("base64encoded"));
        assertEquals("value1", result.getMetadata().get("key1"));
    }

    @Test
    void testConvertToMessage_WithBase64EncodedFlag_InvalidBase64() {
        subscription = builder.build();
        
        // Create a message with base64encoded flag but invalid Base64 string
        String invalidBase64Body = "!!!Invalid Base64!!!";
        
        software.amazon.awssdk.services.sqs.model.Message sqsMessage = 
            software.amazon.awssdk.services.sqs.model.Message.builder()
                .body(invalidBase64Body)
                .messageId("test-message-id")
                .receiptHandle("test-receipt-handle")
                .messageAttributes(Map.of(
                    "base64encoded", MessageAttributeValue.builder().stringValue("true").build()
                ))
                .build();
        
        Message result = subscription.convertToMessage(sqsMessage);
        
        assertNotNull(result);
        // Should fall back to using raw message as UTF-8 bytes
        assertEquals(invalidBase64Body, new String(result.getBody(), StandardCharsets.UTF_8));
    }

    @Test
    void testConvertToMessage_WithoutBase64EncodedFlag() {
        subscription = builder.build();
        
        String plainBody = "Hello, World!";
        
        software.amazon.awssdk.services.sqs.model.Message sqsMessage = 
            software.amazon.awssdk.services.sqs.model.Message.builder()
                .body(plainBody)
                .messageId("test-message-id")
                .receiptHandle("test-receipt-handle")
                .messageAttributes(Map.of(
                    "key1", MessageAttributeValue.builder().stringValue("value1").build()
                ))
                .build();
        
        Message result = subscription.convertToMessage(sqsMessage);
        
        assertNotNull(result);
        assertEquals(plainBody, new String(result.getBody(), StandardCharsets.UTF_8));
    }

    @Test
    void testDecodeMetadataKey_InvalidHexFormat() {
        subscription = builder.build();
        
        // Test with invalid hex format "__0xGG__" (G is not a valid hex digit)
        String invalidHex = "key__0xGG__suffix";
        String result = subscription.decodeMetadataKey(invalidHex);
        
        // Should treat as regular character, not decode
        assertEquals("key__0xGG__suffix", result);
    }

    @Test
    void testDecodeMetadataKey_IncompletePattern() {
        subscription = builder.build();
        
        // Test with incomplete pattern "__0x2F" (no closing "__")
        String incompletePattern = "key__0x2F";
        String result = subscription.decodeMetadataKey(incompletePattern);
        
        // Should treat as regular characters
        assertEquals("key__0x2F", result);
    }

    @Test
    void testDecodeMetadataKey_IncompletePatternAtEnd() {
        subscription = builder.build();
        
        // Test with incomplete pattern at the end "__0x2F" (no closing "__")
        String incompletePattern = "prefix__0x2F";
        String result = subscription.decodeMetadataKey(incompletePattern);
        
        // Should treat as regular characters
        assertEquals("prefix__0x2F", result);
    }


    @Test
    void testDoSendAcks_Success() {
        subscription = builder.build();
        List<AckID> ackIDs = Arrays.asList(
            new AwsSubscription.AwsAckID("receipt-1"),
            new AwsSubscription.AwsAckID("receipt-2")
        );

        DeleteMessageBatchResponse mockResponse = DeleteMessageBatchResponse.builder()
            .successful(List.of())
            .failed(List.of())
            .build();

        when(mockSqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(mockResponse);

        assertDoesNotThrow(() -> subscription.doSendAcks(ackIDs));

        verify(mockSqsClient).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void testDoSendAcks_EmptyList() {
        subscription = builder.build();
        List<AckID> ackIDs = new ArrayList<>();

        assertDoesNotThrow(() -> subscription.doSendAcks(ackIDs));

        verify(mockSqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void testDoSendAcks_BatchFailure() {
        subscription = builder.build();
        List<AckID> ackIDs = Arrays.asList(
            new AwsSubscription.AwsAckID("receipt-1")
        );

        BatchResultErrorEntry errorEntry = BatchResultErrorEntry.builder()
            .id("0")
            .code("InvalidReceiptHandle")
            .message("The receipt handle is invalid")
            .build();

        DeleteMessageBatchResponse mockResponse = DeleteMessageBatchResponse.builder()
            .successful(List.of())
            .failed(List.of(errorEntry))
            .build();

        when(mockSqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(mockResponse);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            subscription.doSendAcks(ackIDs);
        });

        assertTrue(exception.getMessage().contains("SQS DeleteMessageBatch failed"));
        assertTrue(exception.getMessage().contains("InvalidReceiptHandle"));
    }

    @Test
    void testDoSendAcks_LargeBatch() {
        subscription = builder.build();
        // Create 15 ackIDs to test batching (should split into 2 batches of 10 and 5)
        List<AckID> ackIDs = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            ackIDs.add(new AwsSubscription.AwsAckID("receipt-" + i));
        }

        DeleteMessageBatchResponse mockResponse = DeleteMessageBatchResponse.builder()
            .successful(List.of())
            .failed(List.of())
            .build();

        when(mockSqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(mockResponse);

        assertDoesNotThrow(() -> subscription.doSendAcks(ackIDs));

        // Should be called twice: once for 10 messages, once for 5 messages
        verify(mockSqsClient, times(2)).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void testDoSendNacks_Success() {
        subscription = builder.withNackLazy(false).build();
        List<AckID> ackIDs = Arrays.asList(
            new AwsSubscription.AwsAckID("receipt-1"),
            new AwsSubscription.AwsAckID("receipt-2")
        );

        ChangeMessageVisibilityBatchResponse mockResponse = ChangeMessageVisibilityBatchResponse.builder()
            .successful(List.of())
            .failed(List.of())
            .build();

        when(mockSqsClient.changeMessageVisibilityBatch(any(ChangeMessageVisibilityBatchRequest.class)))
            .thenReturn(mockResponse);

        assertDoesNotThrow(() -> subscription.doSendNacks(ackIDs));

        verify(mockSqsClient).changeMessageVisibilityBatch(any(ChangeMessageVisibilityBatchRequest.class));
    }

    @Test
    void testDoSendNacks_NackLazyMode() {
        subscription = builder.withNackLazy(true).build();
        List<AckID> ackIDs = Arrays.asList(
            new AwsSubscription.AwsAckID("receipt-1"),
            new AwsSubscription.AwsAckID("receipt-2")
        );

        assertDoesNotThrow(() -> subscription.doSendNacks(ackIDs));

        // Should not call changeMessageVisibilityBatch in lazy mode
        verify(mockSqsClient, never()).changeMessageVisibilityBatch(any(ChangeMessageVisibilityBatchRequest.class));
    }

    @Test
    void testDoSendNacks_EmptyList() {
        subscription = builder.build();
        List<AckID> ackIDs = new ArrayList<>();

        assertDoesNotThrow(() -> subscription.doSendNacks(ackIDs));

        verify(mockSqsClient, never()).changeMessageVisibilityBatch(any(ChangeMessageVisibilityBatchRequest.class));
    }

    @Test
    void testDoSendNacks_BatchFailure() {
        subscription = builder.withNackLazy(false).build();
        List<AckID> ackIDs = Arrays.asList(
            new AwsSubscription.AwsAckID("receipt-1")
        );

        BatchResultErrorEntry errorEntry = BatchResultErrorEntry.builder()
            .id("0")
            .code("InvalidReceiptHandle")
            .message("The receipt handle is invalid")
            .build();

        ChangeMessageVisibilityBatchResponse mockResponse = ChangeMessageVisibilityBatchResponse.builder()
            .successful(List.of())
            .failed(List.of(errorEntry))
            .build();

        when(mockSqsClient.changeMessageVisibilityBatch(any(ChangeMessageVisibilityBatchRequest.class)))
            .thenReturn(mockResponse);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            subscription.doSendNacks(ackIDs);
        });

        assertTrue(exception.getMessage().contains("SQS ChangeMessageVisibilityBatch failed"));
        assertTrue(exception.getMessage().contains("InvalidReceiptHandle"));
    }

    @Test
    void testDoSendNacks_ReceiptHandleIsInvalid_Ignored() {
        subscription = builder.withNackLazy(false).build();
        List<AckID> ackIDs = Arrays.asList(
            new AwsSubscription.AwsAckID("receipt-1")
        );

        // ReceiptHandleIsInvalid should be filtered out and not cause an exception
        BatchResultErrorEntry ignoredError = BatchResultErrorEntry.builder()
            .id("0")
            .code("ReceiptHandleIsInvalid")
            .message("The receipt handle is invalid")
            .build();

        ChangeMessageVisibilityBatchResponse mockResponse = ChangeMessageVisibilityBatchResponse.builder()
            .successful(List.of())
            .failed(List.of(ignoredError))
            .build();

        when(mockSqsClient.changeMessageVisibilityBatch(any(ChangeMessageVisibilityBatchRequest.class)))
            .thenReturn(mockResponse);

        // Should not throw exception because ReceiptHandleIsInvalid is filtered out
        assertDoesNotThrow(() -> subscription.doSendNacks(ackIDs));

        verify(mockSqsClient).changeMessageVisibilityBatch(any(ChangeMessageVisibilityBatchRequest.class));
    }

    @Test
    void testDoSendNacks_MixedFailures_ReceiptHandleIsInvalidFiltered() {
        subscription = builder.withNackLazy(false).build();
        List<AckID> ackIDs = Arrays.asList(
            new AwsSubscription.AwsAckID("receipt-1"),
            new AwsSubscription.AwsAckID("receipt-2")
        );

        BatchResultErrorEntry ignoredError = BatchResultErrorEntry.builder()
            .id("0")
            .code("ReceiptHandleIsInvalid")
            .message("The receipt handle is invalid")
            .build();

        BatchResultErrorEntry realError = BatchResultErrorEntry.builder()
            .id("1")
            .code("InvalidParameter")
            .message("Invalid parameter")
            .build();

        ChangeMessageVisibilityBatchResponse mockResponse = ChangeMessageVisibilityBatchResponse.builder()
            .successful(List.of())
            .failed(Arrays.asList(ignoredError, realError))
            .build();

        when(mockSqsClient.changeMessageVisibilityBatch(any(ChangeMessageVisibilityBatchRequest.class)))
            .thenReturn(mockResponse);

        // Should throw exception because there's a real error (InvalidParameter) after filtering
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            subscription.doSendNacks(ackIDs);
        });

        assertTrue(exception.getMessage().contains("SQS ChangeMessageVisibilityBatch failed"));
        assertTrue(exception.getMessage().contains("InvalidParameter"));
    }

    @Test
    void testDoSendNacks_LargeBatch() {
        subscription = builder.withNackLazy(false).build();
        // Create 25 ackIDs to test batching (should split into 3 batches of 10, 10, and 5)
        List<AckID> ackIDs = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            ackIDs.add(new AwsSubscription.AwsAckID("receipt-" + i));
        }

        ChangeMessageVisibilityBatchResponse mockResponse = ChangeMessageVisibilityBatchResponse.builder()
            .successful(List.of())
            .failed(List.of())
            .build();

        when(mockSqsClient.changeMessageVisibilityBatch(any(ChangeMessageVisibilityBatchRequest.class)))
            .thenReturn(mockResponse);

        assertDoesNotThrow(() -> subscription.doSendNacks(ackIDs));

        // Should be called 3 times: 10, 10, and 5 messages
        verify(mockSqsClient, times(3)).changeMessageVisibilityBatch(any(ChangeMessageVisibilityBatchRequest.class));
    }

    @Test
    void testBuildWithQueueName_CallsGetQueueUrl() {
        String queueName = "test-queue";
        String expectedQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";
        
        SqsClient mockSqsClient = mock(SqsClient.class);
        GetQueueUrlResponse mockResponse = GetQueueUrlResponse.builder()
            .queueUrl(expectedQueueUrl)
            .build();
        
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
            .thenReturn(mockResponse);
        
        AwsSubscription.Builder testBuilder = new AwsSubscription.Builder();
        testBuilder.withSubscriptionName(queueName);
        testBuilder.withRegion("us-east-1");
        testBuilder.withSqsClient(mockSqsClient);
        subscription = testBuilder.build();
        
        // Verify that getQueueUrl was called and capture the request
        ArgumentCaptor<GetQueueUrlRequest> requestCaptor = ArgumentCaptor.forClass(GetQueueUrlRequest.class);
        verify(mockSqsClient, times(1)).getQueueUrl(requestCaptor.capture());
        GetQueueUrlRequest capturedRequest = requestCaptor.getValue();
        assertEquals(queueName, capturedRequest.queueName());
        
        // Verify that getAttributes returns the resolved URL
        Map<QueueAttributeName, String> attributes = Map.of(
            QueueAttributeName.QUEUE_ARN, "arn:aws:sqs:us-east-1:123456789012:test-queue"
        );
        GetQueueAttributesResponse attrsResponse = GetQueueAttributesResponse.builder()
            .attributes(attributes)
            .build();
        when(mockSqsClient.getQueueAttributes(any(GetQueueAttributesRequest.class)))
            .thenReturn(attrsResponse);
        
        GetAttributeResult result = subscription.getAttributes();
        assertEquals(expectedQueueUrl, result.getName());
    }

    @Test
    void testBuildWithUrl_RejectsUrl() {
        // We rely on AWS to validate the queue name and throw appropriate exceptions.
        String fullQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";
        
        SqsClient mockSqsClient = mock(SqsClient.class);
        
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
            .thenThrow(SdkClientException.builder()
                .message("Invalid queue name format")
                .build());
        
        AwsSubscription.Builder testBuilder = new AwsSubscription.Builder();
        testBuilder.withSubscriptionName(fullQueueUrl);
        testBuilder.withRegion("us-east-1");
        testBuilder.withSqsClient(mockSqsClient);
        
        assertThrows(SdkClientException.class, () -> {
            testBuilder.build();
        });
    }

    @Test
    void testBuildWithQueueName_GetQueueUrlFails() {
        String queueName = "non-existent-queue";
        
        SqsClient mockSqsClient = mock(SqsClient.class);
        AwsServiceException awsException = AwsServiceException.builder()
            .message("The specified queue does not exist.")
            .build();
        
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
            .thenThrow(awsException);
        
        AwsSubscription.Builder testBuilder = new AwsSubscription.Builder();
        testBuilder.withSubscriptionName(queueName);
        testBuilder.withRegion("us-east-1");
        testBuilder.withSqsClient(mockSqsClient);
        
        AwsServiceException exception = assertThrows(AwsServiceException.class, () -> {
            testBuilder.build();
        });
        
        assertEquals("The specified queue does not exist.", exception.getMessage());
        verify(mockSqsClient, times(1)).getQueueUrl(any(GetQueueUrlRequest.class));
    }

    @Test
    void testGetQueueUrlCalledBeforeClientCreation() {
        // Test that getQueueUrl is called during build(), before the client is fully created
        String queueName = "test-queue";
        String expectedQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";
        
        SqsClient mockSqsClient = mock(SqsClient.class);
        GetQueueUrlResponse mockResponse = GetQueueUrlResponse.builder()
            .queueUrl(expectedQueueUrl)
            .build();
        
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
            .thenReturn(mockResponse);
        
        AwsSubscription.Builder builder = new AwsSubscription.Builder();
        builder.withSubscriptionName(queueName);
        builder.withRegion("us-east-1");
        builder.withSqsClient(mockSqsClient);
        
        // Build should call getQueueUrl and resolve the queue URL
        AwsSubscription subscription = builder.build();
        
        // Verify that getQueueUrl was called during build
        ArgumentCaptor<GetQueueUrlRequest> requestCaptor = ArgumentCaptor.forClass(GetQueueUrlRequest.class);
        verify(mockSqsClient, times(1)).getQueueUrl(requestCaptor.capture());
        GetQueueUrlRequest capturedRequest = requestCaptor.getValue();
        assertEquals(queueName, capturedRequest.queueName());
        
        // Verify that the subscription was created successfully (queue URL was resolved)
        assertNotNull(subscription);
    }

    // SNS JSON Message Parsing Tests

    @Test
    void testConvertToMessage_WithSnsJsonMessage_Simple() {
        subscription = builder.build();
        
        // SNS JSON message format
        String snsJsonBody = "{\n" +
            "  \"Type\": \"Notification\",\n" +
            "  \"TopicArn\": \"arn:aws:sns:us-east-1:123456789012:test-topic\",\n" +
            "  \"Message\": \"Hello from SNS\"\n" +
            "}";
        
        software.amazon.awssdk.services.sqs.model.Message sqsMessage = 
            software.amazon.awssdk.services.sqs.model.Message.builder()
                .body(snsJsonBody)
                .messageId("test-message-id")
                .receiptHandle("test-receipt-handle")
                .messageAttributes(Map.of())
                .build();
        
        Message result = subscription.convertToMessage(sqsMessage);
        
        assertNotNull(result);
        assertEquals("Hello from SNS", new String(result.getBody(), StandardCharsets.UTF_8));
        assertEquals("test-message-id", result.getLoggableID());
        assertEquals("test-receipt-handle", result.getAckID().toString());
    }

    @Test
    void testConvertToMessage_WithSnsJsonMessage_WithMessageAttributes() {
        subscription = builder.build();
        
        // SNS JSON message with MessageAttributes, plus SQS message attributes
        // When SQS attributes are present, SNS parsing is skipped (rawAttrs.isEmpty() check)
        String snsJsonBody = "{\n" +
            "  \"Type\": \"Notification\",\n" +
            "  \"TopicArn\": \"arn:aws:sns:us-east-1:123456789012:test-topic\",\n" +
            "  \"Message\": \"Hello from SNS\",\n" +
            "  \"MessageAttributes\": {\n" +
            "    \"attr1\": {\n" +
            "      \"Type\": \"String\",\n" +
            "      \"Value\": \"value1\"\n" +
            "    },\n" +
            "    \"attr2\": {\n" +
            "      \"Type\": \"String\",\n" +
            "      \"Value\": \"value2\"\n" +
            "    }\n" +
            "  }\n" +
            "}";
        
        software.amazon.awssdk.services.sqs.model.Message sqsMessage = 
            software.amazon.awssdk.services.sqs.model.Message.builder()
                .body(snsJsonBody)
                .messageId("test-message-id")
                .receiptHandle("test-receipt-handle")
                .messageAttributes(Map.of(
                    "sqs-attr", MessageAttributeValue.builder().stringValue("sqs-value").build()
                ))
                .build();
        
        Message result = subscription.convertToMessage(sqsMessage);
        
        assertNotNull(result);
        // When SQS attributes are present, SNS is not parsed, so body remains as raw JSON
        assertEquals(snsJsonBody, new String(result.getBody(), StandardCharsets.UTF_8));
        // Only SQS attributes should be present (SNS not parsed)
        assertEquals("sqs-value", result.getMetadata().get("sqs-attr"));
        assertNull(result.getMetadata().get("attr1"));
        assertNull(result.getMetadata().get("attr2"));
    }


    @Test
    void testConvertToMessage_WithNonSnsJson_NotParsed() {
        subscription = builder.build();
        
        // Regular JSON that is not SNS format (no TopicArn)
        String regularJsonBody = "{\n" +
            "  \"message\": \"Hello World\",\n" +
            "  \"timestamp\": \"2023-01-01T00:00:00Z\"\n" +
            "}";
        
        software.amazon.awssdk.services.sqs.model.Message sqsMessage = 
            software.amazon.awssdk.services.sqs.model.Message.builder()
                .body(regularJsonBody)
                .messageId("test-message-id")
                .receiptHandle("test-receipt-handle")
                .messageAttributes(Map.of())
                .build();
        
        Message result = subscription.convertToMessage(sqsMessage);
        
        assertNotNull(result);
        // Should be treated as raw message, not parsed as SNS
        assertEquals(regularJsonBody, new String(result.getBody(), StandardCharsets.UTF_8));
    }

    @Test
    void testConvertToMessage_WithNonJson_NotParsed() {
        subscription = builder.build();
        
        // Plain text message (not JSON)
        String plainTextBody = "Hello World - plain text message";
        
        software.amazon.awssdk.services.sqs.model.Message sqsMessage = 
            software.amazon.awssdk.services.sqs.model.Message.builder()
                .body(plainTextBody)
                .messageId("test-message-id")
                .receiptHandle("test-receipt-handle")
                .messageAttributes(Map.of())
                .build();
        
        Message result = subscription.convertToMessage(sqsMessage);
        
        assertNotNull(result);
        // Should be treated as raw message
        assertEquals(plainTextBody, new String(result.getBody(), StandardCharsets.UTF_8));
    }

    @Test
    void testConvertToMessage_WithSnsJson_NoTopicArn_NotParsed() {
        subscription = builder.build();
        
        // JSON that looks like SNS but missing TopicArn or TopicArn is null
        // Test case 1: Missing TopicArn field
        String jsonBodyMissing = "{\n" +
            "  \"Type\": \"Notification\",\n" +
            "  \"Message\": \"Hello from SNS\"\n" +
            "}";
        
        software.amazon.awssdk.services.sqs.model.Message sqsMessage1 = 
            software.amazon.awssdk.services.sqs.model.Message.builder()
                .body(jsonBodyMissing)
                .messageId("test-message-id-1")
                .receiptHandle("test-receipt-handle-1")
                .messageAttributes(Map.of())
                .build();
        
        Message result1 = subscription.convertToMessage(sqsMessage1);
        assertNotNull(result1);
        assertEquals(jsonBodyMissing, new String(result1.getBody(), StandardCharsets.UTF_8));
        
        // Test case 2: TopicArn is null
        String jsonBodyNull = "{\n" +
            "  \"Type\": \"Notification\",\n" +
            "  \"TopicArn\": null,\n" +
            "  \"Message\": \"Hello from SNS\"\n" +
            "}";
        
        software.amazon.awssdk.services.sqs.model.Message sqsMessage2 = 
            software.amazon.awssdk.services.sqs.model.Message.builder()
                .body(jsonBodyNull)
                .messageId("test-message-id-2")
                .receiptHandle("test-receipt-handle-2")
                .messageAttributes(Map.of())
                .build();
        
        Message result2 = subscription.convertToMessage(sqsMessage2);
        assertNotNull(result2);
        // Should be treated as raw message (missing or null TopicArn means not SNS format)
        assertEquals(jsonBodyNull, new String(result2.getBody(), StandardCharsets.UTF_8));
    }

    @Test
    void testConvertToMessage_WithSnsJson_NoMessage_NotParsed() {
        subscription = builder.build();
        
        // JSON with TopicArn but no Message field
        // When Message field is missing, it should default to empty string
        String jsonBody = "{\n" +
            "  \"Type\": \"Notification\",\n" +
            "  \"TopicArn\": \"arn:aws:sns:us-east-1:123456789012:test-topic\"\n" +
            "}";
        
        software.amazon.awssdk.services.sqs.model.Message sqsMessage = 
            software.amazon.awssdk.services.sqs.model.Message.builder()
                .body(jsonBody)
                .messageId("test-message-id")
                .receiptHandle("test-receipt-handle")
                .messageAttributes(Map.of())
                .build();
        
        Message result = subscription.convertToMessage(sqsMessage);
        
        assertNotNull(result);
        assertEquals("", new String(result.getBody(), StandardCharsets.UTF_8));
    }

    @Test
    void testConvertToMessage_WithInvalidJson_NotParsed() {
        subscription = builder.build();
        
        // Invalid JSON
        String invalidJsonBody = "{ invalid json }";
        
        software.amazon.awssdk.services.sqs.model.Message sqsMessage = 
            software.amazon.awssdk.services.sqs.model.Message.builder()
                .body(invalidJsonBody)
                .messageId("test-message-id")
                .receiptHandle("test-receipt-handle")
                .messageAttributes(Map.of())
                .build();
        
        Message result = subscription.convertToMessage(sqsMessage);
        
        assertNotNull(result);
        // Should be treated as raw message (invalid JSON means not SNS format)
        assertEquals(invalidJsonBody, new String(result.getBody(), StandardCharsets.UTF_8));
    }

    @Test
    void testConvertToMessage_WithEmptyBody_NotParsed() {
        subscription = builder.build();
        
        software.amazon.awssdk.services.sqs.model.Message sqsMessage = 
            software.amazon.awssdk.services.sqs.model.Message.builder()
                .body("")
                .messageId("test-message-id")
                .receiptHandle("test-receipt-handle")
                .messageAttributes(Map.of())
                .build();
        
        Message result = subscription.convertToMessage(sqsMessage);
        
        assertNotNull(result);
        // Empty body should be treated as raw message
        assertEquals("", new String(result.getBody(), StandardCharsets.UTF_8));
    }

    @Test
    void testConvertToMessage_WithWhitespaceBody_NotParsed() {
        subscription = builder.build();
        
        software.amazon.awssdk.services.sqs.model.Message sqsMessage = 
            software.amazon.awssdk.services.sqs.model.Message.builder()
                .body("   \n\t  ")
                .messageId("test-message-id")
                .receiptHandle("test-receipt-handle")
                .messageAttributes(Map.of())
                .build();
        
        Message result = subscription.convertToMessage(sqsMessage);
        
        assertNotNull(result);
        // Whitespace-only body should be treated as raw message
        assertEquals("   \n\t  ", new String(result.getBody(), StandardCharsets.UTF_8));
    }

    @Test
    void testConvertToMessage_WithSnsJson_MessageAttributesWithoutValue() {
        subscription = builder.build();
        
        // SNS JSON message with MessageAttributes that don't have Value field
        String snsJsonBody = "{\n" +
            "  \"Type\": \"Notification\",\n" +
            "  \"TopicArn\": \"arn:aws:sns:us-east-1:123456789012:test-topic\",\n" +
            "  \"Message\": \"Hello from SNS\",\n" +
            "  \"MessageAttributes\": {\n" +
            "    \"attr1\": {\n" +
            "      \"Type\": \"String\"\n" +
            "    },\n" +
            "    \"attr2\": {\n" +
            "      \"Type\": \"String\",\n" +
            "      \"Value\": \"value2\"\n" +
            "    }\n" +
            "  }\n" +
            "}";
        
        software.amazon.awssdk.services.sqs.model.Message sqsMessage = 
            software.amazon.awssdk.services.sqs.model.Message.builder()
                .body(snsJsonBody)
                .messageId("test-message-id")
                .receiptHandle("test-receipt-handle")
                .messageAttributes(Map.of())
                .build();
        
        Message result = subscription.convertToMessage(sqsMessage);
        
        assertNotNull(result);
        assertEquals("Hello from SNS", new String(result.getBody(), StandardCharsets.UTF_8));
        // Only attr2 should be present (attr1 has no Value field)
        assertFalse(result.getMetadata().containsKey("attr1"));
        assertEquals("value2", result.getMetadata().get("attr2"));
    }

    @Test
    void testConvertToMessage_WithSnsJson_WhitespaceAroundBody() {
        subscription = builder.build();
        
        // SNS JSON message with whitespace before and after
        String snsJsonBody = "  \n  {\n" +
            "  \"Type\": \"Notification\",\n" +
            "  \"TopicArn\": \"arn:aws:sns:us-east-1:123456789012:test-topic\",\n" +
            "  \"Message\": \"Hello from SNS\"\n" +
            "}  \n  ";
        
        software.amazon.awssdk.services.sqs.model.Message sqsMessage = 
            software.amazon.awssdk.services.sqs.model.Message.builder()
                .body(snsJsonBody)
                .messageId("test-message-id")
                .receiptHandle("test-receipt-handle")
                .messageAttributes(Map.of())
                .build();
        
        Message result = subscription.convertToMessage(sqsMessage);
        
        assertNotNull(result);
        // Whitespace should be trimmed and message should be extracted
        assertEquals("Hello from SNS", new String(result.getBody(), StandardCharsets.UTF_8));
    }

    @Test
    void testWithRaw_True_SnsJsonNotParsed() {
        // Test that withRaw(true) prevents SNS JSON parsing
        subscription = builder.withRaw(true).build();
        
        String snsJsonBody = "{\n" +
            "  \"Type\": \"Notification\",\n" +
            "  \"TopicArn\": \"arn:aws:sns:us-east-1:123456789012:test-topic\",\n" +
            "  \"Message\": \"Hello from SNS\",\n" +
            "  \"MessageAttributes\": {\n" +
            "    \"attr1\": {\n" +
            "      \"Type\": \"String\",\n" +
            "      \"Value\": \"value1\"\n" +
            "    }\n" +
            "  }\n" +
            "}";
        
        software.amazon.awssdk.services.sqs.model.Message sqsMessage = 
            software.amazon.awssdk.services.sqs.model.Message.builder()
                .body(snsJsonBody)
                .messageId("test-message-id")
                .receiptHandle("test-receipt-handle")
                .messageAttributes(Map.of())
                .build();
        
        Message result = subscription.convertToMessage(sqsMessage);
        
        assertNotNull(result);
        // With raw=true, SNS JSON should NOT be parsed, body should remain as raw JSON
        assertEquals(snsJsonBody, new String(result.getBody(), StandardCharsets.UTF_8));
        // SNS MessageAttributes should NOT be extracted
        assertNull(result.getMetadata().get("attr1"));
    }

    @Test
    void testWithRaw_Default_False() {
        // Test that default value of raw is false
        subscription = builder.build();
        
        String snsJsonBody = "{\n" +
            "  \"Type\": \"Notification\",\n" +
            "  \"TopicArn\": \"arn:aws:sns:us-east-1:123456789012:test-topic\",\n" +
            "  \"Message\": \"Hello from SNS\"\n" +
            "}";
        
        software.amazon.awssdk.services.sqs.model.Message sqsMessage = 
            software.amazon.awssdk.services.sqs.model.Message.builder()
                .body(snsJsonBody)
                .messageId("test-message-id")
                .receiptHandle("test-receipt-handle")
                .messageAttributes(Map.of())
                .build();
        
        Message result = subscription.convertToMessage(sqsMessage);
        
        assertNotNull(result);
        // Default behavior (raw=false) should parse SNS JSON
        assertEquals("Hello from SNS", new String(result.getBody(), StandardCharsets.UTF_8));
    }

    @Test
    void testWithRaw_False_WithTopLevelAttributes_TreatedAsRaw() {
        // Test that even with raw=false, if there are top-level MessageAttributes,
        // the message is treated as raw 
        subscription = builder.withRaw(false).build();
        
        String snsJsonBody = "{\n" +
            "  \"Type\": \"Notification\",\n" +
            "  \"TopicArn\": \"arn:aws:sns:us-east-1:123456789012:test-topic\",\n" +
            "  \"Message\": \"Hello from SNS\"\n" +
            "}";
        
        software.amazon.awssdk.services.sqs.model.Message sqsMessage = 
            software.amazon.awssdk.services.sqs.model.Message.builder()
                .body(snsJsonBody)
                .messageId("test-message-id")
                .receiptHandle("test-receipt-handle")
                .messageAttributes(Map.of(
                    "sqs-attr", MessageAttributeValue.builder().stringValue("sqs-value").build()
                ))
                .build();
        
        Message result = subscription.convertToMessage(sqsMessage);
        
        assertNotNull(result);
        // With top-level attributes present, SNS JSON should NOT be parsed
        assertEquals(snsJsonBody, new String(result.getBody(), StandardCharsets.UTF_8));
        // Only SQS attributes should be present
        assertEquals("sqs-value", result.getMetadata().get("sqs-attr"));
    }
}
