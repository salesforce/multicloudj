package com.salesforce.multicloudj.pubsub.aws;

import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.pubsub.driver.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AwsSqsTopicTest {

    @Mock
    private SqsClient mockSqsClient;

    private AwsSqsTopic topic;

    private static final String QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";

    @BeforeEach
    void setUp() {
        // Mock GetQueueAttributes for URL validation
        GetQueueAttributesResponse mockAttributesResponse = GetQueueAttributesResponse.builder()
            .attributes(Map.of(QueueAttributeName.QUEUE_ARN, "arn:aws:sqs:us-east-1:123456789012:test-queue"))
            .build();
        when(mockSqsClient.getQueueAttributes(any(GetQueueAttributesRequest.class)))
            .thenReturn(mockAttributesResponse);

        AwsSqsTopic.Builder builder = new AwsSqsTopic.Builder();
        builder.withTopicName(QUEUE_URL);
        builder.withRegion("us-east-1");
        builder.withSqsClient(mockSqsClient);
        topic = builder.build();
    }

    @AfterEach
    void tearDown() {
        if (topic != null) {
            try {
                topic.close();
            } catch (Exception e) {
            }
        }
    }

    @Test
    void testProviderId() {
        assertEquals(AwsConstants.PROVIDER_ID_SQS, topic.getProviderId());
    }

    @Test
    void testExceptionHandling() {
        // Test AwsServiceException with error code
        AwsServiceException awsException = AwsServiceException.builder()
            .awsErrorDetails(AwsErrorDetails.builder()
                .errorCode("AccessDenied")
                .build())
            .build();
        Class<? extends SubstrateSdkException> exceptionClass = topic.getException(awsException);
        assertEquals(UnAuthorizedException.class, exceptionClass);

        // Test SdkClientException
        SdkClientException sdkException = SdkClientException.builder().build();
        exceptionClass = topic.getException(sdkException);
        assertEquals(InvalidArgumentException.class, exceptionClass);

        // Test UnknownException
        RuntimeException runtimeException = new RuntimeException("Unknown error");
        exceptionClass = topic.getException(runtimeException);
        assertEquals(UnknownException.class, exceptionClass);
    }

    @Test
    void testSend() {
        Message message = Message.builder()
            .withBody("test message".getBytes(StandardCharsets.UTF_8))
            .build();

        // Mock successful send
        SendMessageBatchResponse mockResponse = SendMessageBatchResponse.builder()
            .successful(List.of())
            .failed(List.of())
            .build();
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class)))
            .thenReturn(mockResponse);

        assertDoesNotThrow(() -> {
            topic.send(message);
        });

        verify(mockSqsClient).sendMessageBatch(any(SendMessageBatchRequest.class));
    }

    @Test
    void testSendWithMetadata() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");
        metadata.put(AwsTopicUtils.MetadataKeys.DEDUPLICATION_ID, "dedup-123");
        metadata.put(AwsTopicUtils.MetadataKeys.MESSAGE_GROUP_ID, "group-456");

        Message message = Message.builder()
            .withBody("test message".getBytes(StandardCharsets.UTF_8))
            .withMetadata(metadata)
            .build();

        // Mock successful send
        SendMessageBatchResponse mockResponse = SendMessageBatchResponse.builder()
            .successful(List.of())
            .failed(List.of())
            .build();
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class)))
            .thenReturn(mockResponse);

        assertDoesNotThrow(() -> {
            topic.send(message);
        });

        verify(mockSqsClient).sendMessageBatch(any(SendMessageBatchRequest.class));
    }

    @Test
    void testSendBatchFailure() {
        Message message = Message.builder()
            .withBody("test message".getBytes(StandardCharsets.UTF_8))
            .build();

        // Mock failed send
        BatchResultErrorEntry errorEntry = BatchResultErrorEntry.builder()
            .code("InvalidMessageContents")
            .message("Message content is invalid")
            .build();
        SendMessageBatchResponse mockResponse = SendMessageBatchResponse.builder()
            .successful(List.of())
            .failed(List.of(errorEntry))
            .build();
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class)))
            .thenReturn(mockResponse);

        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            topic.send(message);
        });

        assertTrue(exception.getMessage().contains("SQS SendMessageBatch failed"));
        verify(mockSqsClient).sendMessageBatch(any(SendMessageBatchRequest.class));
    }
}
