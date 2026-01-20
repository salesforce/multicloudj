package com.salesforce.multicloudj.pubsub.aws;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.pubsub.driver.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.GetTopicAttributesRequest;
import software.amazon.awssdk.services.sns.model.GetTopicAttributesResponse;
import software.amazon.awssdk.services.sns.model.PublishBatchRequest;
import software.amazon.awssdk.services.sns.model.PublishBatchResponse;
import software.amazon.awssdk.services.sns.model.BatchResultErrorEntry;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AwsSnsTopicTest {

    @Mock
    private SnsClient mockSnsClient;

    private AwsSnsTopic topic;

    private static final String TOPIC_ARN = "arn:aws:sns:us-east-1:123456789012:test-topic";

    @BeforeEach
    void setUp() {
        // Mock GetTopicAttributes for ARN validation
        GetTopicAttributesResponse mockAttributesResponse = GetTopicAttributesResponse.builder()
            .attributes(Map.of("TopicArn", TOPIC_ARN))
            .build();
        when(mockSnsClient.getTopicAttributes(any(GetTopicAttributesRequest.class)))
            .thenReturn(mockAttributesResponse);

        AwsSnsTopic.Builder builder = new AwsSnsTopic.Builder();
        builder.withTopicName(TOPIC_ARN);
        builder.withRegion("us-east-1");
        builder.withSnsClient(mockSnsClient);
        topic = builder.build();
    }

    @AfterEach
    void tearDown() {
        if (topic != null) {
            try {
                topic.close();
            } catch (Exception e) {
                // Ignore
            }
        }
    }

    @Test
    void testProviderId() {
        assertEquals(AwsSnsTopic.PROVIDER_ID, topic.getProviderId());
    }


    @Test
    void testSend() {
        Message message = Message.builder()
            .withBody("test message".getBytes(StandardCharsets.UTF_8))
            .build();

        // Mock successful send
        PublishBatchResponse mockResponse = PublishBatchResponse.builder()
            .successful(List.of())
            .failed(List.of())
            .build();
        when(mockSnsClient.publishBatch(any(PublishBatchRequest.class)))
            .thenReturn(mockResponse);

        assertDoesNotThrow(() -> {
            topic.send(message);
        });

        verify(mockSnsClient).publishBatch(any(PublishBatchRequest.class));
    }

    @Test
    void testSendWithMetadata() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");
        metadata.put(AwsBaseTopic.MetadataKeys.SUBJECT, "Test Subject");
        metadata.put(AwsBaseTopic.MetadataKeys.DEDUPLICATION_ID, "dedup-123");
        metadata.put(AwsBaseTopic.MetadataKeys.MESSAGE_GROUP_ID, "group-456");

        Message message = Message.builder()
            .withBody("test message".getBytes(StandardCharsets.UTF_8))
            .withMetadata(metadata)
            .build();

        // Mock successful send
        PublishBatchResponse mockResponse = PublishBatchResponse.builder()
            .successful(List.of())
            .failed(List.of())
            .build();
        when(mockSnsClient.publishBatch(any(PublishBatchRequest.class)))
            .thenReturn(mockResponse);

        topic.send(message);

        // Verify request content
        ArgumentCaptor<PublishBatchRequest> requestCaptor = ArgumentCaptor.forClass(PublishBatchRequest.class);
        verify(mockSnsClient).publishBatch(requestCaptor.capture());
        
        PublishBatchRequest capturedRequest = requestCaptor.getValue();
        assertEquals(TOPIC_ARN, capturedRequest.topicArn());
        assertEquals(1, capturedRequest.publishBatchRequestEntries().size());
        
        var entry = capturedRequest.publishBatchRequestEntries().get(0);
        assertEquals("test message", entry.message());
        assertEquals("Test Subject", entry.subject());
        assertEquals("dedup-123", entry.messageDeduplicationId());
        assertEquals("group-456", entry.messageGroupId());
        assertNotNull(entry.messageAttributes());
    }

    @Test
    void testSendBatchFailure() {
        Message message = Message.builder()
            .withBody("test message".getBytes(StandardCharsets.UTF_8))
            .build();

        // Mock failed send
        BatchResultErrorEntry errorEntry = BatchResultErrorEntry.builder()
            .code("InvalidMessage")
            .message("Message content is invalid")
            .build();
        PublishBatchResponse mockResponse = PublishBatchResponse.builder()
            .successful(List.of())
            .failed(List.of(errorEntry))
            .build();
        when(mockSnsClient.publishBatch(any(PublishBatchRequest.class)))
            .thenReturn(mockResponse);

        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            topic.send(message);
        });

        assertTrue(exception.getMessage().contains("SNS PublishBatch failed"));
        verify(mockSnsClient).publishBatch(any(PublishBatchRequest.class));
    }
}
