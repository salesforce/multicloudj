package com.salesforce.multicloudj.pubsub.gcp;

import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.salesforce.multicloudj.common.exceptions.DeadlineExceededException;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.pubsub.driver.Message;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

@SuppressWarnings("rawtypes")
@ExtendWith(MockitoExtension.class)
public class GcpTopicTest {
    
    private static final String VALID_TOPIC_NAME = "projects/test-project/topics/test-topic";
    private GcpTopic topic;
    private CredentialsOverrider mockCredentialsOverrider;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        mockCredentialsOverrider = mock(CredentialsOverrider.class);
        GcpTopic tempTopic = new GcpTopic();
        topic = tempTopic.builder()
            .withTopicName(VALID_TOPIC_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider)
            .build();
    }

    @Test
    void testTopicNameValidation() {
        // Valid topic names should not throw
        GcpTopic tempTopic1 = new GcpTopic();
        assertDoesNotThrow(() -> tempTopic1.builder().withTopicName("projects/my-project/topics/my-topic").build());        
        // Invalid topic names should throw
        GcpTopic tempTopic2 = new GcpTopic();
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, 
            () -> tempTopic2.builder().withTopicName("just-a-topic").build());
        assertTrue(exception.getMessage().contains("projects/{projectId}/topics/{topicId}"));
    }

    @Test
    void testBuilder() {
        // Test basic builder functionality
        GcpTopic tempTopic1 = new GcpTopic();
        GcpTopic builtTopic = tempTopic1.builder()
            .withTopicName(VALID_TOPIC_NAME)
            .withRegion(null)
            .withCredentialsOverrider(mockCredentialsOverrider)
            .build();
            
        assertEquals(GcpConstants.PROVIDER_ID, builtTopic.getProviderId());
        assertNotNull(builtTopic);
        
        // Test that region can be null for GCP
        GcpTopic tempTopic2 = new GcpTopic();
        assertDoesNotThrow(() ->
            tempTopic2.builder()
                .withTopicName(VALID_TOPIC_NAME)
                .withRegion(null)
                .build());
        
        // Test that missing topic name throws exception
        GcpTopic tempTopic3 = new GcpTopic();
        assertThrows(InvalidArgumentException.class, () ->
            tempTopic3.builder()
                .withRegion(null)
                .build());
        
        // Test custom endpoint
        URI customEndpoint = URI.create("https://pubsub.googleapis.com:443");
        GcpTopic tempTopic4 = new GcpTopic();
        GcpTopic topicWithEndpoint = tempTopic4.builder()
            .withTopicName(VALID_TOPIC_NAME)
            .withEndpoint(customEndpoint)
            .build();
            
        assertNotNull(topicWithEndpoint);
        assertEquals(GcpConstants.PROVIDER_ID, topicWithEndpoint.getProviderId());
        
        // Test proxy endpoint
        URI proxyEndpoint = URI.create("https://proxy.example.com:8080");
        GcpTopic tempTopic5 = new GcpTopic();
        GcpTopic topicWithProxy = tempTopic5.builder()
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
        // Test that doSendBatch succeeds with proper mocking
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder().withBody("test".getBytes()).build());
        
        // Mock TopicAdminClient to return successful response
        TopicAdminClient mockClient = mock(TopicAdminClient.class);
        @SuppressWarnings("unchecked")
        UnaryCallable<PublishRequest, PublishResponse> mockCallable = mock(UnaryCallable.class);
        @SuppressWarnings("unchecked")
        ApiFuture<PublishResponse> mockFuture = mock(ApiFuture.class);
        
        PublishResponse publishResponse = PublishResponse.newBuilder()
            .addMessageIds("test-message-id")
            .build();
        
        when(mockClient.publishCallable()).thenReturn(mockCallable);
        when(mockCallable.futureCall(org.mockito.ArgumentMatchers.any(PublishRequest.class))).thenReturn(mockFuture);
        doReturn(publishResponse).when(mockFuture).get();

        GcpTopic tempTopicForBuilder = new GcpTopic();
        GcpTopic.Builder builder = (GcpTopic.Builder) tempTopicForBuilder.builder()
                .withTopicName(VALID_TOPIC_NAME)
                .withCredentialsOverrider(mockCredentialsOverrider);
        GcpTopic topicWithMockClient = new GcpTopic(builder, mockClient);
        
        try {
            assertDoesNotThrow(() -> topicWithMockClient.doSendBatch(messages));
            verify(mockClient).publishCallable();
        } finally {
            topicWithMockClient.close();
        }
    }

    @Test
    void testProxyEndpointConfiguration() throws Exception {
        URI proxyEndpoint = URI.create("http://proxy.example.com:8080");

        TopicAdminClient mockClient = mock(TopicAdminClient.class);
        @SuppressWarnings("unchecked")
        UnaryCallable<PublishRequest, PublishResponse> mockCallable = mock(UnaryCallable.class);
        @SuppressWarnings("unchecked")
        ApiFuture<PublishResponse> mockFuture = mock(ApiFuture.class);
        
        PublishResponse publishResponse = PublishResponse.newBuilder()
            .addMessageIds("test-message-id")
            .build();
        
        when(mockClient.publishCallable()).thenReturn(mockCallable);
        when(mockCallable.futureCall(org.mockito.ArgumentMatchers.any(PublishRequest.class))).thenReturn(mockFuture);
        doReturn(publishResponse).when(mockFuture).get();
        
        GcpTopic tempTopic = new GcpTopic();
        GcpTopic.Builder builder = (GcpTopic.Builder) tempTopic.builder()
            .withTopicName(VALID_TOPIC_NAME)
            .withProxyEndpoint(proxyEndpoint)
            .withCredentialsOverrider(mockCredentialsOverrider);
        GcpTopic topicWithProxy = new GcpTopic(builder, mockClient);

        try {
            assertDoesNotThrow(() -> {
                topicWithProxy.send(Message.builder().withBody("test".getBytes()).build());
            });
            verify(mockClient).publishCallable();
        } finally {
            topicWithProxy.close();
        }
    }

    @Test
    public void testGetExceptionWithApiException() {
        GcpTopic topic = new GcpTopic().builder().withTopicName(VALID_TOPIC_NAME).build();

        // Test various status codes
        assertExceptionMapping(topic, StatusCode.Code.CANCELLED, UnknownException.class);
        assertExceptionMapping(topic, StatusCode.Code.UNKNOWN, UnknownException.class);
        assertExceptionMapping(topic, StatusCode.Code.INVALID_ARGUMENT, InvalidArgumentException.class);
        assertExceptionMapping(topic, StatusCode.Code.DEADLINE_EXCEEDED, DeadlineExceededException.class);
        assertExceptionMapping(topic, StatusCode.Code.NOT_FOUND, ResourceNotFoundException.class);
        assertExceptionMapping(topic, StatusCode.Code.ALREADY_EXISTS, ResourceAlreadyExistsException.class);
        assertExceptionMapping(topic, StatusCode.Code.PERMISSION_DENIED, UnAuthorizedException.class);
        assertExceptionMapping(topic, StatusCode.Code.RESOURCE_EXHAUSTED, ResourceExhaustedException.class);
        assertExceptionMapping(topic, StatusCode.Code.FAILED_PRECONDITION, FailedPreconditionException.class);
        assertExceptionMapping(topic, StatusCode.Code.ABORTED, DeadlineExceededException.class);
        assertExceptionMapping(topic, StatusCode.Code.OUT_OF_RANGE, InvalidArgumentException.class);
        assertExceptionMapping(topic, StatusCode.Code.UNIMPLEMENTED, UnSupportedOperationException.class);
        assertExceptionMapping(topic, StatusCode.Code.INTERNAL, UnknownException.class);
        assertExceptionMapping(topic, StatusCode.Code.UNAVAILABLE, UnknownException.class);
        assertExceptionMapping(topic, StatusCode.Code.DATA_LOSS, UnknownException.class);
        assertExceptionMapping(topic, StatusCode.Code.UNAUTHENTICATED, UnAuthorizedException.class);
    }

    @Test
    void testGetExceptionWithOtherException() {
        RuntimeException otherException = new RuntimeException("test");
        Class<? extends SubstrateSdkException> exceptionClass = topic.getException(otherException);
        assertEquals(UnknownException.class, exceptionClass);
    }

    @Test
    void testDoSendBatchWithEmptyMessages() throws Exception {
        TopicAdminClient mockClient = mock(TopicAdminClient.class);
        GcpTopic topicWithMockClient = new GcpTopic(
            (GcpTopic.Builder) new GcpTopic().builder().withTopicName(VALID_TOPIC_NAME),
            mockClient
        );
        List<Message> emptyMessages = new ArrayList<>();
        
        try {
            assertDoesNotThrow(() -> topicWithMockClient.doSendBatch(emptyMessages));
            verify(mockClient, Mockito.never()).publishCallable();
        } finally {
            topicWithMockClient.close();
        }
    }

    @Test
    void testDoSendBatchThrowsOnInterruptedException() throws Exception {
        TopicAdminClient mockClient = mock(TopicAdminClient.class);
        @SuppressWarnings("unchecked")
        UnaryCallable<PublishRequest, PublishResponse> mockCallable = mock(UnaryCallable.class);
        @SuppressWarnings("unchecked")
        ApiFuture<PublishResponse> mockFuture = mock(ApiFuture.class);
        
        when(mockClient.publishCallable()).thenReturn(mockCallable);
        when(mockCallable.futureCall(org.mockito.ArgumentMatchers.any(PublishRequest.class))).thenReturn(mockFuture);
        doThrow(new InterruptedException("interrupted")).when(mockFuture).get();
        
        GcpTopic topicWithMockClient = new GcpTopic(
            (GcpTopic.Builder) new GcpTopic().builder().withTopicName(VALID_TOPIC_NAME),
            mockClient
        );
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder().withBody("test".getBytes()).build());
        
        try {
            SubstrateSdkException exception = assertThrows(SubstrateSdkException.class,
                () -> topicWithMockClient.doSendBatch(messages));
            assertTrue(exception.getMessage().contains("Interrupted"));
            assertTrue(Thread.currentThread().isInterrupted());
            Thread.interrupted(); // Clear interrupt flag
        } finally {
            topicWithMockClient.close();
        }
    }

    @Test
    void testDoSendBatchThrowsOnExecutionExceptionWithRuntimeCause() throws Exception {
        TopicAdminClient mockClient = mock(TopicAdminClient.class);
        @SuppressWarnings("unchecked")
        UnaryCallable<PublishRequest, PublishResponse> mockCallable = mock(UnaryCallable.class);
        @SuppressWarnings("unchecked")
        ApiFuture<PublishResponse> mockFuture = mock(ApiFuture.class);
        
        RuntimeException runtimeException = new RuntimeException("test error");
        ExecutionException executionException = new ExecutionException(runtimeException);
        
        when(mockClient.publishCallable()).thenReturn(mockCallable);
        when(mockCallable.futureCall(org.mockito.ArgumentMatchers.any(PublishRequest.class))).thenReturn(mockFuture);
        doThrow(executionException).when(mockFuture).get();
        
        GcpTopic topicWithMockClient = new GcpTopic(
            (GcpTopic.Builder) new GcpTopic().builder().withTopicName(VALID_TOPIC_NAME),
            mockClient
        );
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder().withBody("test".getBytes()).build());
        
        try {
            RuntimeException exception = assertThrows(RuntimeException.class,
                () -> topicWithMockClient.doSendBatch(messages));
            assertEquals(runtimeException, exception);
        } finally {
            topicWithMockClient.close();
        }
    }

    @Test
    void testDoSendBatchThrowsOnExecutionExceptionWithOtherCause() throws Exception {
        TopicAdminClient mockClient = mock(TopicAdminClient.class);
        @SuppressWarnings("unchecked")
        UnaryCallable<PublishRequest, PublishResponse> mockCallable = mock(UnaryCallable.class);
        @SuppressWarnings("unchecked")
        ApiFuture<PublishResponse> mockFuture = mock(ApiFuture.class);
        
        IOException ioException = new IOException("io error");
        ExecutionException executionException = new ExecutionException(ioException);
        
        when(mockClient.publishCallable()).thenReturn(mockCallable);
        when(mockCallable.futureCall(org.mockito.ArgumentMatchers.any(PublishRequest.class))).thenReturn(mockFuture);
        doThrow(executionException).when(mockFuture).get();
        
        GcpTopic topicWithMockClient = new GcpTopic(
            (GcpTopic.Builder) new GcpTopic().builder().withTopicName(VALID_TOPIC_NAME),
            mockClient
        );
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder().withBody("test".getBytes()).build());
        
        try {
            SubstrateSdkException exception = assertThrows(SubstrateSdkException.class,
                () -> topicWithMockClient.doSendBatch(messages));
            assertTrue(exception.getMessage().contains("Publish failed"));
        } finally {
            topicWithMockClient.close();
        }
    }

    @Test
    void testDoSendBatchThrowsOnExecutionExceptionWithNullCause() throws Exception {
        TopicAdminClient mockClient = mock(TopicAdminClient.class);
        @SuppressWarnings("unchecked")
        UnaryCallable<PublishRequest, PublishResponse> mockCallable = mock(UnaryCallable.class);
        @SuppressWarnings("unchecked")
        ApiFuture<PublishResponse> mockFuture = mock(ApiFuture.class);
        
        ExecutionException executionException = new ExecutionException(null);
        
        when(mockClient.publishCallable()).thenReturn(mockCallable);
        when(mockCallable.futureCall(org.mockito.ArgumentMatchers.any(PublishRequest.class))).thenReturn(mockFuture);
        doThrow(executionException).when(mockFuture).get();
        
        GcpTopic topicWithMockClient = new GcpTopic(
            (GcpTopic.Builder) new GcpTopic().builder().withTopicName(VALID_TOPIC_NAME),
            mockClient
        );
        List<Message> messages = new ArrayList<>();
        messages.add(Message.builder().withBody("test".getBytes()).build());
        
        try {
            SubstrateSdkException exception = assertThrows(SubstrateSdkException.class,
                () -> topicWithMockClient.doSendBatch(messages));
            assertTrue(exception.getMessage().contains("Publish failed"));
        } finally {
            topicWithMockClient.close();
        }
    }

    private void assertExceptionMapping(GcpTopic topic, StatusCode.Code statusCode, Class<? extends SubstrateSdkException> expectedExceptionClass) {
        ApiException apiException = Mockito.mock(ApiException.class);
        StatusCode mockStatusCode = Mockito.mock(StatusCode.class);
        Mockito.when(apiException.getStatusCode()).thenReturn(mockStatusCode);
        Mockito.when(mockStatusCode.getCode()).thenReturn(statusCode);

        Class<? extends SubstrateSdkException> actualExceptionClass = topic.getException(apiException);
        Assertions.assertEquals(expectedExceptionClass, actualExceptionClass,
                "Expected " + expectedExceptionClass.getSimpleName() + " for status code " + statusCode);
    }
}