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
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishBatchRequest;
import software.amazon.awssdk.services.sns.model.PublishBatchResponse;
import software.amazon.awssdk.services.sns.model.PublishBatchRequestEntry;
import software.amazon.awssdk.services.sns.model.PublishBatchResultEntry;
import software.amazon.awssdk.services.sns.model.BatchResultErrorEntry;

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
    
    private static final String VALID_SQS_TOPIC_NAME = "test-queue";
    private static final String VALID_SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";
    
    @Mock
    private SqsClient mockSqsClient;
    
    private AwsTopic sqsTopic;
    private CredentialsOverrider mockCredentialsOverrider;

    @BeforeEach
    void setUp() {
        mockCredentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(new StsCredentials("key-1", "secret-1", "token-1"))
            .build();
        
        // Mock getQueueUrl for all tests
        GetQueueUrlResponse mockResponse = GetQueueUrlResponse.builder()
            .queueUrl(VALID_SQS_QUEUE_URL)
            .build();
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
            .thenReturn(mockResponse);
            
        AwsTopic.Builder builder = new AwsTopic.Builder();
        builder.withTopicName(VALID_SQS_TOPIC_NAME);
        builder.withServiceType(AwsTopic.ServiceType.SQS);
        builder.withCredentialsOverrider(mockCredentialsOverrider);
        builder.withSqsClient(mockSqsClient);
        builder.withRegion("us-east-1");
        sqsTopic = builder.build();
    }

    @Test
    void testTopicNameValidation_Null() {
        AwsTopic.Builder builder = new AwsTopic.Builder();
        builder.withServiceType(AwsTopic.ServiceType.SQS);
        builder.withSqsClient(mockSqsClient);
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, () -> builder.build());
        assertTrue(exception.getMessage().contains("Topic name/ARN cannot be null or empty"));
    }

    @Test
    void testTopicNameValidation_Empty() {
        AwsTopic.Builder builder = new AwsTopic.Builder();
        builder.withTopicName("");
        builder.withServiceType(AwsTopic.ServiceType.SQS);
        builder.withSqsClient(mockSqsClient);
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, () -> builder.build());
        assertTrue(exception.getMessage().contains("cannot be null or empty"));
    }

    @Test
    void testTopicNameValidation_AcceptsQueueName() {
        // Valid queue name should be accepted
        GetQueueUrlResponse mockResponse = GetQueueUrlResponse.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
            .build();
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
            .thenReturn(mockResponse);
        
        AwsTopic.Builder builder = new AwsTopic.Builder();
        builder.withTopicName("my-queue");
        builder.withServiceType(AwsTopic.ServiceType.SQS);
        builder.withSqsClient(mockSqsClient);
        builder.withRegion("us-east-1");
        assertDoesNotThrow(() -> builder.build());
    }

    @Test
    void testBuilder() {
        AwsTopic.Builder builder1 = new AwsTopic.Builder();
        builder1.withTopicName(VALID_SQS_TOPIC_NAME);
        builder1.withServiceType(AwsTopic.ServiceType.SQS);
        builder1.withRegion("us-east-1");
        builder1.withCredentialsOverrider(mockCredentialsOverrider);
        builder1.withSqsClient(mockSqsClient);
        AwsTopic builtSqsTopic = builder1.build();
            
        assertEquals("aws", builtSqsTopic.getProviderId());
        assertNotNull(builtSqsTopic);
        
        // Test that missing topic name throws exception
        AwsTopic.Builder builder2 = new AwsTopic.Builder();
        builder2.withServiceType(AwsTopic.ServiceType.SQS);
        builder2.withRegion("us-east-1");
        builder2.withSqsClient(mockSqsClient);
        assertThrows(InvalidArgumentException.class, () -> builder2.build());
        
        // Test custom endpoint
        GetQueueUrlResponse mockResponse = GetQueueUrlResponse.builder()
            .queueUrl(VALID_SQS_QUEUE_URL)
            .build();
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
            .thenReturn(mockResponse);
        
        AwsTopic.Builder builder3 = new AwsTopic.Builder();
        builder3.withTopicName(VALID_SQS_TOPIC_NAME);
        builder3.withServiceType(AwsTopic.ServiceType.SQS);
        builder3.withEndpoint(URI.create("https://custom-endpoint.com"));
        builder3.withCredentialsOverrider(mockCredentialsOverrider);
        builder3.withSqsClient(mockSqsClient);
        builder3.withRegion("us-east-1");
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
        
        // Only verify that sendMessageBatch was not called (getQueueUrl was called in setUp)
        verify(mockSqsClient, never()).sendMessageBatch(any(SendMessageBatchRequest.class));
    }

    @Test
    void testDoSendBatchNullMessages() throws Exception {
        assertDoesNotThrow(() -> sqsTopic.doSendBatch(null));
        
        // Only verify that sendMessageBatch was not called (getQueueUrl was called in setUp)
        verify(mockSqsClient, never()).sendMessageBatch(any(SendMessageBatchRequest.class));
    }

    @Test
    void testClose() throws Exception {
        assertDoesNotThrow(() -> sqsTopic.close());
    }

    @Test
    void testBuilderReturnsNonNullInstance() {
        AwsTopic.Builder builder = new AwsTopic.Builder();
        builder.withTopicName(VALID_SQS_TOPIC_NAME);
        builder.withServiceType(AwsTopic.ServiceType.SQS);
        builder.withSqsClient(mockSqsClient);
        builder.withRegion("us-east-1");
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
        
        AwsTopic.Builder testBuilder = new AwsTopic.Builder();
        testBuilder.withTopicName(queueName);
        testBuilder.withServiceType(AwsTopic.ServiceType.SQS);
        testBuilder.withRegion("us-east-1");
        testBuilder.withSqsClient(mockSqsClient);
        testBuilder.build();
        
        // Verify that getQueueUrl was called and capture the request
        ArgumentCaptor<GetQueueUrlRequest> requestCaptor = ArgumentCaptor.forClass(GetQueueUrlRequest.class);
        verify(mockSqsClient, times(1)).getQueueUrl(requestCaptor.capture());
        GetQueueUrlRequest capturedRequest = requestCaptor.getValue();
        assertEquals(queueName, capturedRequest.queueName());
    }

    @Test
    void testBuildWithUrl_AcceptsUrl() {
        String fullQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";
        
        SqsClient mockSqsClient = mock(SqsClient.class);
        
        AwsTopic.Builder testBuilder = new AwsTopic.Builder();
        testBuilder.withTopicName(fullQueueUrl);  
        testBuilder.withRegion("us-east-1");
        testBuilder.withSqsClient(mockSqsClient);
        
        assertDoesNotThrow(() -> testBuilder.build());
        
        verify(mockSqsClient, never()).getQueueUrl(any(GetQueueUrlRequest.class));
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
        
        AwsTopic.Builder testBuilder = new AwsTopic.Builder();
        testBuilder.withTopicName(queueName);
        testBuilder.withServiceType(AwsTopic.ServiceType.SQS);
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
        
        AwsTopic.Builder builder = new AwsTopic.Builder();
        builder.withTopicName(queueName);
        builder.withServiceType(AwsTopic.ServiceType.SQS);
        builder.withRegion("us-east-1");
        builder.withSqsClient(mockSqsClient);
        
        // Build should call getQueueUrl and resolve the queue URL
        AwsTopic topic = builder.build();
        
        // Verify that getQueueUrl was called during build
        ArgumentCaptor<GetQueueUrlRequest> requestCaptor = ArgumentCaptor.forClass(GetQueueUrlRequest.class);
        verify(mockSqsClient, times(1)).getQueueUrl(requestCaptor.capture());
        GetQueueUrlRequest capturedRequest = requestCaptor.getValue();
        assertEquals(queueName, capturedRequest.queueName());
        
        // Verify that the topic was created successfully (queue URL was resolved)
        assertNotNull(topic);
    }

    @Test
    void testInvalidQueueNameFormat_ThrowsException() {
        // Test various invalid queue name formats
        String[] invalidQueueNames = {
            "",  // empty string
            "   ",  // whitespace only
            "queue@name",  // invalid character @
            "queue name",  // space in name
            "queue/name",  // slash in name
            "queue.name.",  // trailing dot
            ".queue.name",  // leading dot
            "queue..name",  // consecutive dots
            "a".repeat(81),  // too long (SQS queue name max is 80 chars)
        };
        
        for (String invalidName : invalidQueueNames) {
            SqsClient mockSqsClient = mock(SqsClient.class);
            
            // For empty/whitespace, should throw InvalidArgumentException from validation
            if (invalidName == null || invalidName.trim().isEmpty()) {
                AwsTopic.Builder builder = new AwsTopic.Builder();
                builder.withTopicName(invalidName);
                builder.withServiceType(AwsTopic.ServiceType.SQS);
                builder.withSqsClient(mockSqsClient);
                builder.withRegion("us-east-1");
                
                assertThrows(InvalidArgumentException.class, () -> builder.build(),
                    "Should throw InvalidArgumentException for empty/whitespace queue name: " + invalidName);
            } else {
                // For other invalid formats, AWS SDK should throw SdkClientException
                when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
                    .thenThrow(SdkClientException.builder()
                        .message("Invalid queue name format")
                        .build());
                
                AwsTopic.Builder builder = new AwsTopic.Builder();
                builder.withTopicName(invalidName);
                builder.withServiceType(AwsTopic.ServiceType.SQS);
                builder.withSqsClient(mockSqsClient);
                builder.withRegion("us-east-1");
                
                assertThrows(SdkClientException.class, () -> builder.build(),
                    "Should throw SdkClientException for invalid queue name format: " + invalidName);
            }
        }
    }

    // SNS-related tests

    private static final String VALID_SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:123456789012:test-topic";

    @Mock
    private SnsClient mockSnsClient;

    private AwsTopic snsTopic;

    @BeforeEach
    void setUpSnsTopic() {
        AwsTopic.Builder builder = new AwsTopic.Builder();
        builder.withTopicName(VALID_SNS_TOPIC_ARN);
        builder.withServiceType(AwsTopic.ServiceType.SNS);
        builder.withSnsClient(mockSnsClient);
        builder.withTopicName(VALID_SNS_TOPIC_ARN); 
        builder.withRegion("us-east-1");
        snsTopic = builder.build();
    }

    @Test
    void testBuildSnsTopic_WithTopicArn() {
        assertNotNull(snsTopic);
        assertEquals("aws", snsTopic.getProviderId());
    }

    @Test
    void testBuildSnsTopic_WithoutTopicArn_ThrowsException() {
        AwsTopic.Builder builder = new AwsTopic.Builder();
        builder.withTopicName("my-topic"); 
        builder.withServiceType(AwsTopic.ServiceType.SNS);
        builder.withSnsClient(mockSnsClient);
        builder.withRegion("us-east-1");

        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, () -> builder.build());
        assertTrue(exception.getMessage().contains("Topic ARN must be set when using SNS"));
    }

    @Test
    void testBuildSnsTopic_WithoutServiceType_AutoDetected() {
        AwsTopic.Builder builder = new AwsTopic.Builder();
        builder.withTopicName(VALID_SNS_TOPIC_ARN); 
        builder.withSnsClient(mockSnsClient);

        assertDoesNotThrow(() -> builder.build());
    }

    @Test
    void testDoSendBatchSnsSuccess() throws Exception {
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder()
            .withBody("test message".getBytes())
            .withMetadata(Map.of("test-key", "test-value"))
            .build());

        PublishBatchResultEntry successEntry = PublishBatchResultEntry.builder()
            .id("0")
            .messageId("msg-123")
            .build();

        PublishBatchResponse mockResponse = PublishBatchResponse.builder()
            .successful(List.of(successEntry))
            .failed(new ArrayList<>())
            .build();

        when(mockSnsClient.publishBatch(any(PublishBatchRequest.class)))
            .thenReturn(mockResponse);

        assertDoesNotThrow(() -> snsTopic.doSendBatch(messages));

        ArgumentCaptor<PublishBatchRequest> requestCaptor = ArgumentCaptor.forClass(PublishBatchRequest.class);
        verify(mockSnsClient).publishBatch(requestCaptor.capture());

        PublishBatchRequest capturedRequest = requestCaptor.getValue();
        assertEquals(VALID_SNS_TOPIC_ARN, capturedRequest.topicArn());
        assertEquals(1, capturedRequest.publishBatchRequestEntries().size());
        
        // Verify message attributes are converted
        PublishBatchRequestEntry entry = capturedRequest.publishBatchRequestEntries().get(0);
        assertNotNull(entry.messageAttributes());
        assertTrue(entry.messageAttributes().containsKey("test-key"));
    }

    @Test
    void testDoSendBatchSnsWithSubject() throws Exception {
        List<Message> messages = new ArrayList<>();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("Subject", "Test Subject");
        metadata.put("test-key", "test-value");

        messages.add(Message.builder()
            .withBody("test message".getBytes())
            .withMetadata(metadata)
            .build());

        PublishBatchResultEntry successEntry = PublishBatchResultEntry.builder()
            .id("0")
            .messageId("msg-123")
            .build();

        PublishBatchResponse mockResponse = PublishBatchResponse.builder()
            .successful(List.of(successEntry))
            .failed(new ArrayList<>())
            .build();

        when(mockSnsClient.publishBatch(any(PublishBatchRequest.class)))
            .thenReturn(mockResponse);

        assertDoesNotThrow(() -> snsTopic.doSendBatch(messages));

        ArgumentCaptor<PublishBatchRequest> requestCaptor = ArgumentCaptor.forClass(PublishBatchRequest.class);
        verify(mockSnsClient).publishBatch(requestCaptor.capture());

        PublishBatchRequest capturedRequest = requestCaptor.getValue();
        PublishBatchRequestEntry entry = capturedRequest.publishBatchRequestEntries().get(0);
        assertEquals("Test Subject", entry.subject());
    }

    @Test
    void testDoSendBatchSnsWithFifoAttributes() throws Exception {
        List<Message> messages = new ArrayList<>();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("DeduplicationId", "dedup-123");
        metadata.put("MessageGroupId", "group-456");

        messages.add(Message.builder()
            .withBody("test message".getBytes())
            .withMetadata(metadata)
            .build());

        PublishBatchResultEntry successEntry = PublishBatchResultEntry.builder()
            .id("0")
            .messageId("msg-123")
            .build();

        PublishBatchResponse mockResponse = PublishBatchResponse.builder()
            .successful(List.of(successEntry))
            .failed(new ArrayList<>())
            .build();

        when(mockSnsClient.publishBatch(any(PublishBatchRequest.class)))
            .thenReturn(mockResponse);

        assertDoesNotThrow(() -> snsTopic.doSendBatch(messages));

        ArgumentCaptor<PublishBatchRequest> requestCaptor = ArgumentCaptor.forClass(PublishBatchRequest.class);
        verify(mockSnsClient).publishBatch(requestCaptor.capture());

        PublishBatchRequest capturedRequest = requestCaptor.getValue();
        PublishBatchRequestEntry entry = capturedRequest.publishBatchRequestEntries().get(0);
        assertEquals("dedup-123", entry.messageDeduplicationId());
        assertEquals("group-456", entry.messageGroupId());
    }

    @Test
    void testDoSendBatchSnsWithBase64Encoding() throws Exception {
        // Create topic with ALWAYS base64 encoding
        AwsTopic.TopicOptions topicOptions = new AwsTopic.TopicOptions()
            .withBodyBase64Encoding(AwsTopic.BodyBase64Encoding.ALWAYS);
        AwsTopic.Builder builder = new AwsTopic.Builder();
        builder.withTopicName(VALID_SNS_TOPIC_ARN);
        builder.withServiceType(AwsTopic.ServiceType.SNS);
        builder.withSnsClient(mockSnsClient);
        builder.withTopicName(VALID_SNS_TOPIC_ARN); 
        builder.withRegion("us-east-1");
        builder.withTopicOptions(topicOptions);
        AwsTopic topicWithEncoding = builder.build();

        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder()
            .withBody("test message".getBytes())
            .build());

        PublishBatchResultEntry successEntry = PublishBatchResultEntry.builder()
            .id("0")
            .messageId("msg-123")
            .build();

        PublishBatchResponse mockResponse = PublishBatchResponse.builder()
            .successful(List.of(successEntry))
            .failed(new ArrayList<>())
            .build();

        when(mockSnsClient.publishBatch(any(PublishBatchRequest.class)))
            .thenReturn(mockResponse);

        assertDoesNotThrow(() -> topicWithEncoding.doSendBatch(messages));

        ArgumentCaptor<PublishBatchRequest> requestCaptor = ArgumentCaptor.forClass(PublishBatchRequest.class);
        verify(mockSnsClient).publishBatch(requestCaptor.capture());

        PublishBatchRequest capturedRequest = requestCaptor.getValue();
        PublishBatchRequestEntry entry = capturedRequest.publishBatchRequestEntries().get(0);
        // Verify base64 encoding flag is set
        assertTrue(entry.messageAttributes().containsKey("base64encoded"));
        assertEquals("true", entry.messageAttributes().get("base64encoded").stringValue());
    }

    @Test
    void testDoSendBatchSnsWithMultipleMessages() throws Exception {
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder().withBody("message1".getBytes()).build());
        messages.add(Message.builder().withBody("message2".getBytes()).build());
        messages.add(Message.builder().withBody("message3".getBytes()).build());

        PublishBatchResultEntry entry1 = PublishBatchResultEntry.builder().id("0").messageId("msg-1").build();
        PublishBatchResultEntry entry2 = PublishBatchResultEntry.builder().id("1").messageId("msg-2").build();
        PublishBatchResultEntry entry3 = PublishBatchResultEntry.builder().id("2").messageId("msg-3").build();

        PublishBatchResponse mockResponse = PublishBatchResponse.builder()
            .successful(List.of(entry1, entry2, entry3))
            .failed(new ArrayList<>())
            .build();

        when(mockSnsClient.publishBatch(any(PublishBatchRequest.class)))
            .thenReturn(mockResponse);

        assertDoesNotThrow(() -> snsTopic.doSendBatch(messages));

        ArgumentCaptor<PublishBatchRequest> requestCaptor = ArgumentCaptor.forClass(PublishBatchRequest.class);
        verify(mockSnsClient).publishBatch(requestCaptor.capture());

        PublishBatchRequest capturedRequest = requestCaptor.getValue();
        assertEquals(3, capturedRequest.publishBatchRequestEntries().size());
    }

    @Test
    void testDoSendBatchSnsFailure() throws Exception {
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder()
            .withBody("test message".getBytes())
            .build());

        BatchResultErrorEntry errorEntry = BatchResultErrorEntry.builder()
            .id("0")
            .code("InvalidParameter")
            .message("Invalid parameter")
            .build();

        PublishBatchResponse mockResponse = PublishBatchResponse.builder()
            .successful(new ArrayList<>())
            .failed(List.of(errorEntry))
            .build();

        when(mockSnsClient.publishBatch(any(PublishBatchRequest.class)))
            .thenReturn(mockResponse);

        assertThrows(SubstrateSdkException.class, () -> snsTopic.doSendBatch(messages));

        verify(mockSnsClient).publishBatch(any(PublishBatchRequest.class));
    }

    @Test
    void testDoSendBatchSnsWithEmptyOrNullMessages() throws Exception {
        // Test empty messages
        assertDoesNotThrow(() -> snsTopic.doSendBatch(new ArrayList<>()));
        verify(mockSnsClient, never()).publishBatch(any(PublishBatchRequest.class));

        // Reset mock
        reset(mockSnsClient);

        // Test null messages
        assertDoesNotThrow(() -> snsTopic.doSendBatch(null));
        verify(mockSnsClient, never()).publishBatch(any(PublishBatchRequest.class));
    }
}
