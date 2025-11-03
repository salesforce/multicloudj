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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class AwsSubscriptionTest {

    @Mock
    private SqsClient mockSqsClient;

    private AwsSubscription subscription;
    private AwsSubscription.Builder builder;

    @BeforeEach
    void setUp() {
        builder = new AwsSubscription.Builder();
        builder.withSubscriptionName("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        builder.withRegion("us-east-1");
        builder.withSqsClient(mockSqsClient);
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
    void testAwsAckID_ValidReceiptHandle() {
        String receiptHandle = "test-receipt-handle";

        AwsSubscription.AwsAckID ackID = new AwsSubscription.AwsAckID(receiptHandle);

        assertNotNull(ackID);
        assertEquals(receiptHandle, ackID.getReceiptHandle());
        assertEquals(receiptHandle, ackID.toString());
    }

    @Test
    void testAwsAckID_NullReceiptHandle() {
        assertThrows(IllegalArgumentException.class, () -> {
            new AwsSubscription.AwsAckID(null);
        });
    }

    @Test
    void testAwsAckID_EmptyReceiptHandle() {
        assertThrows(IllegalArgumentException.class, () -> {
            new AwsSubscription.AwsAckID("");
        });
    }

    @Test
    void testAwsAckID_BlankReceiptHandle() {
        assertThrows(IllegalArgumentException.class, () -> {
            new AwsSubscription.AwsAckID("   ");
        });
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
        subscription = builder.build();

        GetAttributeResult result = subscription.getAttributes();

        assertNotNull(result);
        assertEquals("aws-subscription", result.getName());
        assertEquals("aws-topic", result.getTopic());
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

        assertNull(result);
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

        assertNull(result);
    }

    @Test
    void testValidateSubscriptionName_InvalidName() {
        String invalidName = "invalid@name";

        assertThrows(InvalidArgumentException.class, () -> {
            new AwsSubscription.Builder()
                    .withSubscriptionName(invalidName)
                    .build();
        });
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


}
