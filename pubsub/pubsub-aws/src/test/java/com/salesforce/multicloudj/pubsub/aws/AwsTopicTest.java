package com.salesforce.multicloudj.pubsub.aws;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
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


import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResultEntry;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("rawtypes")
@MockitoSettings(strictness = Strictness.LENIENT)
public class AwsTopicTest {
    
    private static final String VALID_SQS_TOPIC_NAME = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";
    
    @Mock
    private SqsClient mockSqsClient;
    
    private AwsTopic sqsTopic;
    private CredentialsOverrider mockCredentialsOverrider;

    @BeforeEach
    void setUp() {
        mockCredentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(new StsCredentials("key-1", "secret-1", "token-1"))
            .build();
            
        AwsTopic.Builder builder = new AwsTopic.Builder();
        builder.withTopicName(VALID_SQS_TOPIC_NAME);
        builder.withCredentialsOverrider(mockCredentialsOverrider);
        builder.withSqsClient(mockSqsClient);
        sqsTopic = builder.build();
    }

    @Test
    void testTopicNameValidation() {
        // Valid SQS topic names should not throw
        AwsTopic.Builder builder1 = new AwsTopic.Builder();
        builder1.withTopicName("https://sqs.us-west-2.amazonaws.com/123456789012/my-queue");
        builder1.withSqsClient(mockSqsClient);
        assertDoesNotThrow(() -> builder1.build());
        
        // Invalid topic names should throw
        AwsTopic.Builder builder2 = new AwsTopic.Builder();
        builder2.withTopicName("just-a-topic");
        builder2.withSqsClient(mockSqsClient);
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, () -> builder2.build());
        assertTrue(exception.getMessage().contains("SQS topic name must be in format: https://sqs.region.amazonaws.com/account/queue-name"));
        
        // Invalid SQS URL should throw
        AwsTopic.Builder builder3 = new AwsTopic.Builder();
        builder3.withTopicName("https://sqs.invalid.com/queue");
        builder3.withSqsClient(mockSqsClient);
        exception = assertThrows(InvalidArgumentException.class, () -> builder3.build());
        assertTrue(exception.getMessage().contains("SQS topic name must be in format: https://sqs.region.amazonaws.com/account/queue-name"));
    }

    @Test
    void testTopicNameValidation_Null() {
        AwsTopic.Builder builder = new AwsTopic.Builder();
        builder.withSqsClient(mockSqsClient);
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, () -> builder.build());
        assertTrue(exception.getMessage().contains("SQS topic name cannot be null"));
    }

    @Test
    void testTopicNameValidation_Empty() {
        AwsTopic.Builder builder = new AwsTopic.Builder();
        builder.withTopicName("");
        builder.withSqsClient(mockSqsClient);
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, () -> builder.build());
        assertTrue(exception.getMessage().contains("SQS topic name cannot be empty"));
    }

    @Test
    void testBuilder() {
        AwsTopic.Builder builder1 = new AwsTopic.Builder();
        builder1.withTopicName(VALID_SQS_TOPIC_NAME);
        builder1.withRegion("us-east-1");
        builder1.withCredentialsOverrider(mockCredentialsOverrider);
        builder1.withSqsClient(mockSqsClient);
        AwsTopic builtSqsTopic = builder1.build();
            
        assertEquals("aws", builtSqsTopic.getProviderId());
        assertNotNull(builtSqsTopic);
        
        // Test that missing topic name throws exception
        AwsTopic.Builder builder2 = new AwsTopic.Builder();
        builder2.withRegion("us-east-1");
        builder2.withSqsClient(mockSqsClient);
        assertThrows(InvalidArgumentException.class, () -> builder2.build());
        
        // Test custom endpoint
        AwsTopic.Builder builder3 = new AwsTopic.Builder();
        builder3.withTopicName(VALID_SQS_TOPIC_NAME);
        builder3.withEndpoint(URI.create("https://custom-endpoint.com"));
        builder3.withCredentialsOverrider(mockCredentialsOverrider);
        builder3.withSqsClient(mockSqsClient);
        AwsTopic topicWithEndpoint = builder3.build();
        assertNotNull(topicWithEndpoint);
    }

    @Test
    void testBatcherOptions() {
        var batcherOptions = sqsTopic.createBatcherOptions();
        assertNotNull(batcherOptions);
        
        assertEquals(10, batcherOptions.getMaxBatchSize());
        assertEquals(256 * 1024, batcherOptions.getMaxBatchByteSize());
    }


    @Test
    void testDoSendBatchSqsSuccess() throws Exception {
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder()
            .withBody("test message".getBytes())
            .withMetadata(Map.of("test-key", "test-value"))
            .build());

        SendMessageBatchResultEntry successEntry = SendMessageBatchResultEntry.builder()
            .id("0")
            .messageId("msg-123")
            .build();
            
        SendMessageBatchResponse mockResponse = SendMessageBatchResponse.builder()
            .successful(List.of(successEntry))
            .failed(new ArrayList<>())
            .build();
            
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class)))
            .thenReturn(mockResponse);

        assertDoesNotThrow(() -> sqsTopic.doSendBatch(messages));
        
        verify(mockSqsClient).sendMessageBatch(any(SendMessageBatchRequest.class));
    }


    @Test
    void testDoSendBatchSqsWithFifoAttributes() throws Exception {
        List<Message> messages = new ArrayList<>();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("DeduplicationId", "dedup-123");
        metadata.put("MessageGroupId", "group-456");
        metadata.put("test-key", "test-value");
        
        messages.add(Message.builder()
            .withBody("test message".getBytes())
            .withMetadata(metadata)
            .build());

        SendMessageBatchResultEntry successEntry = SendMessageBatchResultEntry.builder()
            .id("0")
            .messageId("msg-123")
            .build();
            
        SendMessageBatchResponse mockResponse = SendMessageBatchResponse.builder()
            .successful(List.of(successEntry))
            .failed(new ArrayList<>())
            .build();
            
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class)))
            .thenReturn(mockResponse);

        assertDoesNotThrow(() -> sqsTopic.doSendBatch(messages));
        
        verify(mockSqsClient).sendMessageBatch(any(SendMessageBatchRequest.class));
    }




    @Test
    void testDoSendBatchSqsFailure() throws Exception {
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder()
            .withBody("test message".getBytes())
            .build());

        software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry errorEntry = software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry.builder()
            .id("0")
            .code("InvalidParameter")
            .message("Invalid parameter")
            .build();
            
        SendMessageBatchResponse mockResponse = SendMessageBatchResponse.builder()
            .successful(new ArrayList<>())
            .failed(List.of(errorEntry))
            .build();
            
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class)))
            .thenReturn(mockResponse);

        assertThrows(SubstrateSdkException.class, () -> sqsTopic.doSendBatch(messages));
        
        verify(mockSqsClient).sendMessageBatch(any(SendMessageBatchRequest.class));
    }


    @Test
    void testDoSendBatchSqsSdkException() throws Exception {
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder()
            .withBody("test message".getBytes())
            .build());

        SdkException sdkException = SdkException.builder().message("AWS SQS error").build();
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class)))
            .thenThrow(sdkException);
        SdkException thrown = assertThrows(SdkException.class, () -> sqsTopic.doSendBatch(messages));
        assertEquals("AWS SQS error", thrown.getMessage());
        
        verify(mockSqsClient).sendMessageBatch(any(SendMessageBatchRequest.class));
    }

    @Test
    void testGetExceptionWithAccessDeniedError() {
        AwsServiceException serviceException = AwsServiceException.builder()
            .message("AccessDeniedException: User is not authorized")
            .awsErrorDetails(AwsErrorDetails.builder()
                .errorCode("AccessDeniedException")
                .build())
            .build();

        Class<? extends SubstrateSdkException> result = sqsTopic.getException(serviceException);

        assertEquals(UnAuthorizedException.class, result);
    }

    @Test
    void testGetExceptionWithInvalidParameterError() {
        AwsServiceException serviceException = AwsServiceException.builder()
            .message("ValidationError: The parameter is invalid")
            .awsErrorDetails(AwsErrorDetails.builder()
                .errorCode("ValidationError")
                .build())
            .build();

        Class<? extends SubstrateSdkException> result = sqsTopic.getException(serviceException);

        assertEquals(InvalidArgumentException.class, result);
    }

    @Test
    void testGetExceptionWithUnknownError() {
        SdkException sdkException = SdkException.builder()
            .message("Unknown error")
            .build();

        Class<? extends SubstrateSdkException> result = sqsTopic.getException(sdkException);

        assertEquals(UnknownException.class, result);
    }

    @Test
    void testGetExceptionWithSdkClientException() {
        SdkClientException clientException = SdkClientException.builder()
            .message("Client error")
            .build();

        Class<? extends SubstrateSdkException> result = sqsTopic.getException(clientException);

        assertEquals(InvalidArgumentException.class, result);
    }

    @Test
    void testGetExceptionWithIllegalArgumentException() {
        IllegalArgumentException illegalArgException = new IllegalArgumentException("Invalid argument");

        Class<? extends SubstrateSdkException> result = sqsTopic.getException(illegalArgException);

        assertEquals(InvalidArgumentException.class, result);
    }

    @Test
    void testGetExceptionWithSubstrateSdkExceptionSubclass_InvalidArgumentException() {
        InvalidArgumentException subException = new InvalidArgumentException("Invalid argument");
        
        Class<? extends SubstrateSdkException> result = sqsTopic.getException(subException);
        
        assertEquals(InvalidArgumentException.class, result);
    }

    @Test
    void testGetExceptionWithSubstrateSdkExceptionSubclass_UnknownException() {
        UnknownException subException = new UnknownException("Unknown error");
        
        Class<? extends SubstrateSdkException> result = sqsTopic.getException(subException);
        
        assertEquals(UnknownException.class, result);
    }

    @Test
    void testGetExceptionWithSubstrateSdkExceptionSubclass_UnAuthorizedException() {
        UnAuthorizedException subException = new UnAuthorizedException("Unauthorized");
        
        Class<? extends SubstrateSdkException> result = sqsTopic.getException(subException);
        
        assertEquals(UnAuthorizedException.class, result);
    }

    @Test
    void testGetExceptionWithSubstrateSdkExceptionBaseClass() {

        SubstrateSdkException baseException = new SubstrateSdkException("Base exception");
        
        Class<? extends SubstrateSdkException> result = sqsTopic.getException(baseException);
        
        assertEquals(UnknownException.class, result);
    }

    @Test
    void testGetExceptionWithAwsServiceExceptionNoErrorDetails() {
        AwsServiceException serviceException = AwsServiceException.builder()
            .message("Service error without details")
            .awsErrorDetails(null)
            .build();

        Class<? extends SubstrateSdkException> result = sqsTopic.getException(serviceException);

        assertEquals(UnknownException.class, result);
    }

    @Test
    void testGetExceptionWithUnknownException() {
        RuntimeException runtimeException = new RuntimeException("Generic runtime error");

        Class<? extends SubstrateSdkException> result = sqsTopic.getException(runtimeException);

        assertEquals(UnknownException.class, result);
    }

    @Test
    void testDoSendBatchEmptyMessages() throws Exception {
        List<Message> messages = new ArrayList<>();

        assertDoesNotThrow(() -> sqsTopic.doSendBatch(messages));
        
        verifyNoInteractions(mockSqsClient);
    }

    @Test
    void testDoSendBatchNullMessages() throws Exception {
        assertDoesNotThrow(() -> sqsTopic.doSendBatch(null));
        
        verifyNoInteractions(mockSqsClient);
    }

    @Test
    void testClose() throws Exception {
        assertDoesNotThrow(() -> sqsTopic.close());
    }

    @Test
    void testBuilderReturnsNonNullInstance() {
        AwsTopic.Builder builder = new AwsTopic.Builder();
        builder.withTopicName(VALID_SQS_TOPIC_NAME);
        builder.withSqsClient(mockSqsClient);
        AwsTopic topic = builder.build();
        assertNotNull(topic.builder());
    }

    @Test
    void testMetadataKeysConstants() {
        assertNotNull(MetadataKeys.DEDUPLICATION_ID);
        assertNotNull(MetadataKeys.MESSAGE_GROUP_ID);
        assertEquals("DeduplicationId", MetadataKeys.DEDUPLICATION_ID);
        assertEquals("MessageGroupId", MetadataKeys.MESSAGE_GROUP_ID);
    }

    @Test
    void testMetadataEncodingWithSpecialCharacters() throws Exception {
        // Test metadata with special characters that need encoding
        Map<String, String> metadata = new HashMap<>();
        metadata.put("file/path", "value1");
        metadata.put("user name", "value2");
        metadata.put("type:message", "value3");
        metadata.put("normal-key", "value4");
        
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder()
            .withBody("test message".getBytes())
            .withMetadata(metadata)
            .build());

        SendMessageBatchResultEntry successEntry = SendMessageBatchResultEntry.builder()
            .id("0")
            .messageId("msg-123")
            .build();
            
        SendMessageBatchResponse mockResponse = SendMessageBatchResponse.builder()
            .successful(List.of(successEntry))
            .failed(new ArrayList<>())
            .build();
            
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class)))
            .thenReturn(mockResponse);

        assertDoesNotThrow(() -> sqsTopic.doSendBatch(messages));
        
        verify(mockSqsClient).sendMessageBatch(any(SendMessageBatchRequest.class));
    }

    @Test
    void testMessageAttributesLimit_LessThan10() throws Exception {
        Map<String, String> metadata = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            metadata.put("key-" + i, "value-" + i);
        }
        
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder()
            .withBody("test message".getBytes())
            .withMetadata(metadata)
            .build());

        SendMessageBatchResultEntry successEntry = SendMessageBatchResultEntry.builder()
            .id("0")
            .messageId("msg-123")
            .build();
            
        SendMessageBatchResponse mockResponse = SendMessageBatchResponse.builder()
            .successful(List.of(successEntry))
            .failed(new ArrayList<>())
            .build();
            
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class)))
            .thenReturn(mockResponse);

        ArgumentCaptor<SendMessageBatchRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);

        assertDoesNotThrow(() -> sqsTopic.doSendBatch(messages));
        
        verify(mockSqsClient).sendMessageBatch(requestCaptor.capture());
        Map<String, software.amazon.awssdk.services.sqs.model.MessageAttributeValue> messageAttributes = 
            requestCaptor.getValue().entries().get(0).messageAttributes();
        assertEquals(5, messageAttributes.size());
    }

    @Test
    void testMessageAttributesLimit_Exactly10() throws Exception {
        Map<String, String> metadata = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            metadata.put("key-" + i, "value-" + i);
        }
        
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder()
            .withBody("test message".getBytes())
            .withMetadata(metadata)
            .build());

        SendMessageBatchResultEntry successEntry = SendMessageBatchResultEntry.builder()
            .id("0")
            .messageId("msg-123")
            .build();
            
        SendMessageBatchResponse mockResponse = SendMessageBatchResponse.builder()
            .successful(List.of(successEntry))
            .failed(new ArrayList<>())
            .build();
            
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class)))
            .thenReturn(mockResponse);

        ArgumentCaptor<SendMessageBatchRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);

        assertDoesNotThrow(() -> sqsTopic.doSendBatch(messages));
        
        verify(mockSqsClient).sendMessageBatch(requestCaptor.capture());
        Map<String, software.amazon.awssdk.services.sqs.model.MessageAttributeValue> messageAttributes = 
            requestCaptor.getValue().entries().get(0).messageAttributes();
        assertEquals(10, messageAttributes.size());
    }

    @Test
    void testMessageAttributesLimit_MoreThan10() throws Exception {
        Map<String, String> metadata = new HashMap<>();
        for (int i = 0; i < 15; i++) {
            metadata.put("key-" + i, "value-" + i);
        }
        
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder()
            .withBody("test message".getBytes())
            .withMetadata(metadata)
            .build());

        SendMessageBatchResultEntry successEntry = SendMessageBatchResultEntry.builder()
            .id("0")
            .messageId("msg-123")
            .build();
            
        SendMessageBatchResponse mockResponse = SendMessageBatchResponse.builder()
            .successful(List.of(successEntry))
            .failed(new ArrayList<>())
            .build();
            
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class)))
            .thenReturn(mockResponse);

        ArgumentCaptor<SendMessageBatchRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);

        assertDoesNotThrow(() -> sqsTopic.doSendBatch(messages));
        
        verify(mockSqsClient).sendMessageBatch(requestCaptor.capture());
        Map<String, software.amazon.awssdk.services.sqs.model.MessageAttributeValue> messageAttributes = 
            requestCaptor.getValue().entries().get(0).messageAttributes();
        assertEquals(10, messageAttributes.size());
    }

    @Test
    void testBase64EncodingFlag_NonUtf8Body() throws Exception {
        // Create a message with non-UTF8 byte array (e.g., binary data)
        byte[] binaryData = new byte[]{(byte)0xFF, (byte)0xFE, (byte)0xFD, (byte)0xFC};
        
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder()
            .withBody(binaryData)
            .withMetadata(Map.of("test-key", "test-value"))
            .build());

        SendMessageBatchResultEntry successEntry = SendMessageBatchResultEntry.builder()
            .id("0")
            .messageId("msg-123")
            .build();
            
        SendMessageBatchResponse mockResponse = SendMessageBatchResponse.builder()
            .successful(List.of(successEntry))
            .failed(new ArrayList<>())
            .build();
            
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class)))
            .thenReturn(mockResponse);

        ArgumentCaptor<SendMessageBatchRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);

        assertDoesNotThrow(() -> sqsTopic.doSendBatch(messages));
        
        verify(mockSqsClient).sendMessageBatch(requestCaptor.capture());
        Map<String, software.amazon.awssdk.services.sqs.model.MessageAttributeValue> messageAttributes = 
            requestCaptor.getValue().entries().get(0).messageAttributes();
        
        // Verify BASE64_ENCODED flag is present
        assertTrue(messageAttributes.containsKey("base64encoded"));
        assertEquals("String", messageAttributes.get("base64encoded").dataType());
        assertEquals("true", messageAttributes.get("base64encoded").stringValue());
    }

    @Test
    void testBase64EncodingFlag_Utf8Body() throws Exception {
        // Create a message with valid UTF-8 text
        String utf8Text = "Hello, World!";
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder()
            .withBody(utf8Text.getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .withMetadata(Map.of("test-key", "test-value"))
            .build());

        SendMessageBatchResultEntry successEntry = SendMessageBatchResultEntry.builder()
            .id("0")
            .messageId("msg-123")
            .build();
            
        SendMessageBatchResponse mockResponse = SendMessageBatchResponse.builder()
            .successful(List.of(successEntry))
            .failed(new ArrayList<>())
            .build();
            
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class)))
            .thenReturn(mockResponse);

        ArgumentCaptor<SendMessageBatchRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);

        assertDoesNotThrow(() -> sqsTopic.doSendBatch(messages));
        
        verify(mockSqsClient).sendMessageBatch(requestCaptor.capture());
        Map<String, software.amazon.awssdk.services.sqs.model.MessageAttributeValue> messageAttributes = 
            requestCaptor.getValue().entries().get(0).messageAttributes();
        
        // Verify BASE64_ENCODED flag is NOT present for UTF-8 content
        assertFalse(messageAttributes.containsKey("base64encoded"));
    }

    @Test
    void testBase64EncodingFlag_EmptyBody() throws Exception {
        // Test with empty body
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder()
            .withBody(new byte[0])
            .withMetadata(Map.of("test-key", "test-value"))
            .build());

        SendMessageBatchResultEntry successEntry = SendMessageBatchResultEntry.builder()
            .id("0")
            .messageId("msg-123")
            .build();
            
        SendMessageBatchResponse mockResponse = SendMessageBatchResponse.builder()
            .successful(List.of(successEntry))
            .failed(new ArrayList<>())
            .build();
            
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class)))
            .thenReturn(mockResponse);

        ArgumentCaptor<SendMessageBatchRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);

        assertDoesNotThrow(() -> sqsTopic.doSendBatch(messages));
        
        verify(mockSqsClient).sendMessageBatch(requestCaptor.capture());
        Map<String, software.amazon.awssdk.services.sqs.model.MessageAttributeValue> messageAttributes = 
            requestCaptor.getValue().entries().get(0).messageAttributes();
        
        // Empty body is valid UTF-8, so no BASE64_ENCODED flag
        assertFalse(messageAttributes.containsKey("base64encoded"));
    }
}
