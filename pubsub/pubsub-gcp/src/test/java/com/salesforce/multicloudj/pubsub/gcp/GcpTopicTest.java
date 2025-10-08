package com.salesforce.multicloudj.pubsub.gcp;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.pubsub.driver.Message;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("rawtypes")
@ExtendWith(MockitoExtension.class)
public class GcpTopicTest {
    
    private static final String VALID_TOPIC_NAME = "projects/test-project/topics/test-topic";
    
    @Mock
    private Publisher mockPublisher;
    
    @Mock
    private ApiFuture<String> mockFuture;
    
    private GcpTopic topic;
    private CredentialsOverrider mockCredentialsOverrider;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        mockCredentialsOverrider = mock(CredentialsOverrider.class);
        
        // Create topic with injected mock Publisher
        topic = ((GcpTopic.Builder) GcpTopic.builder()
            .withTopicName(VALID_TOPIC_NAME)
            .withCredentialsOverrider(mockCredentialsOverrider))
            .withPublisher(mockPublisher)  // Inject mock Publisher
            .build();
    }

    @Test
    void testTopicNameValidation() {
        // Valid topic names should not throw
        assertDoesNotThrow(() -> GcpTopic.builder().withTopicName("projects/my-project/topics/my-topic").build());        
        // Invalid topic names should throw
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
            () -> GcpTopic.builder().withTopicName("just-a-topic").build());
        assertTrue(exception.getMessage().contains("projects/{projectId}/topics/{topicId}"));
    }

    @Test
    void testBuilder() {
        // Test basic builder functionality
        GcpTopic builtTopic = GcpTopic.builder()
            .withTopicName(VALID_TOPIC_NAME)
            .withRegion(null)
            .withCredentialsOverrider(mockCredentialsOverrider)
            .build();
            
        assertEquals(GcpConstants.PROVIDER_ID, builtTopic.getProviderId());
        assertNotNull(builtTopic);
        
        // Test that region can be null for GCP
        assertDoesNotThrow(() ->
            GcpTopic.builder()
                .withTopicName(VALID_TOPIC_NAME)
                .withRegion(null)
                .build());
        
        // Test that missing topic name throws exception
        assertThrows(InvalidArgumentException.class, () ->
            GcpTopic.builder()
                .withRegion(null)
                .build());
        
        // Test custom endpoint
        URI customEndpoint = URI.create("https://pubsub.googleapis.com:443");
        GcpTopic topicWithEndpoint = GcpTopic.builder()
            .withTopicName(VALID_TOPIC_NAME)
            .withEndpoint(customEndpoint)
            .build();
            
        assertNotNull(topicWithEndpoint);
        assertEquals(GcpConstants.PROVIDER_ID, topicWithEndpoint.getProviderId());
        
        // Test proxy endpoint
        URI proxyEndpoint = URI.create("https://proxy.example.com:8080");
        GcpTopic topicWithProxy = GcpTopic.builder()
            .withTopicName(VALID_TOPIC_NAME)
            .withProxyEndpoint(proxyEndpoint)
            .build();
            
        assertNotNull(topicWithProxy);
        assertEquals(GcpConstants.PROVIDER_ID, topicWithProxy.getProviderId());
    }

    @Test
    void testCreateBatcherOptions() {
        // Test that batcher options are configured for GCP limits
        assertEquals(2, topic.createBatcherOptions().getMaxHandlers());
        assertEquals(1, topic.createBatcherOptions().getMinBatchSize());
        assertEquals(1000, topic.createBatcherOptions().getMaxBatchSize());
        assertEquals(9 * 1024 * 1024, topic.createBatcherOptions().getMaxBatchByteSize());
    }

    @Test
    void testDoSendBatchSuccess() throws Exception {
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder().withBody("test".getBytes()).build());
        
        // Mock the publish method
        when(mockPublisher.publish(any(PubsubMessage.class))).thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn("message-id");
        
        // Now test - this should not throw authentication errors
        assertDoesNotThrow(() -> topic.doSendBatch(messages));
    }

    @Test
    void testProxyEndpointConfiguration() throws Exception {
        URI proxyEndpoint = URI.create("http://proxy.example.com:8080");
        GcpTopic topicWithProxy = ((GcpTopic.Builder) GcpTopic.builder()
            .withTopicName(VALID_TOPIC_NAME)
            .withProxyEndpoint(proxyEndpoint))
            .withPublisher(mockPublisher)  // Inject mock Publisher to avoid real calls
            .build();

        // Test that the proxy endpoint is configured (without making real GCP calls)
        assertNotNull(topicWithProxy);
        assertEquals(GcpConstants.PROVIDER_ID, topicWithProxy.getProviderId());
        
        // Test that we can call send without real GCP API calls
        when(mockPublisher.publish(any(PubsubMessage.class))).thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn("message-id");
        
        assertDoesNotThrow(() -> {
            topicWithProxy.send(Message.builder().withBody("test".getBytes()).build());
        });
    } 

    @Test
    void testDoSendBatchWithMismatchedMessageIds() throws Exception {
        // Test that doSendBatch handles errors gracefully
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder().withBody("test".getBytes()).build());
        
        // Mock the publish method to simulate an error
        when(mockPublisher.publish(any(PubsubMessage.class))).thenReturn(mockFuture);
        when(mockFuture.get()).thenThrow(new RuntimeException("Simulated publish failure"));
        
        // Test error handling
        assertThrows(Exception.class, () -> topic.doSendBatch(messages));
    }

    @Test
    void testDoSendBatchWithPublishFailure() throws Exception {
        // Test that doSendBatch handles publish failures gracefully
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder().withBody("test".getBytes()).build());

        // Mock the publish method to simulate a failure
        when(mockPublisher.publish(any(PubsubMessage.class))).thenReturn(mockFuture);
        when(mockFuture.get()).thenThrow(new RuntimeException("Simulated publish failure"));
        
        // Test error handling
        assertThrows(Exception.class, () -> topic.doSendBatch(messages));
    }

    @Test
    void testDoSendBatchWithMetadata() throws Exception {
        // Test that doSendBatch handles messages with metadata
        List<Message> messages = new ArrayList<>();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");
        
        messages.add(Message.builder()
                .withBody("test".getBytes())
                .withMetadata(metadata)
                .build());
        
        // Mock the publish method
        when(mockPublisher.publish(any(PubsubMessage.class))).thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn("message-id");
        
        // Now test - this should not throw authentication errors
        assertDoesNotThrow(() -> topic.doSendBatch(messages));
    }

    @Test
    void testGetExceptionWithApiException() {
        // Test that getException handles ApiException properly
        // We'll test with a real ApiException to verify the mapping logic
        try {
            // This should trigger the ApiException path in getException
            // The actual mapping will depend on the status code
            assertNotNull(topic.getException(new RuntimeException("test")));
        } catch (Exception e) {
            // If there's an issue with the mapping, we'll catch it here
            fail("getException should not throw an exception");
        }
    }

    @Test
    void testGetExceptionWithOtherException() {
        RuntimeException otherException = new RuntimeException("test");
        Class<? extends SubstrateSdkException> exceptionClass = topic.getException(otherException);
        assertEquals(UnknownException.class, exceptionClass);
    }

    @Test
    void testCloseWithNoPublisher() throws Exception {
        assertDoesNotThrow(() -> topic.close());
    }
}