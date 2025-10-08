package com.salesforce.multicloudj.pubsub.client;

import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.pubsub.driver.Message;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TopicClientTest {

    static class MockTopicClient extends TopicClient {
        public MockTopicClient(AbstractTopic<?> topic) {
            super(topic);
        }
    }

    @Mock
    private AbstractTopic mockTopic;

    @Mock
    private AbstractTopic.Builder mockBuilder;

    private MockTopicClient topicClient;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        topicClient = new MockTopicClient(mockTopic);
    }

    @Test
    public void testSend() {
        // Arrange
        Message message = Message.builder()
            .withBody("test".getBytes())
            .build();

        // Act
        topicClient.send(message);

        // Assert
        verify(mockTopic).send(message);
    }

    @Test
    public void testClose() throws Exception {
        // Act
        topicClient.close();

        // Assert
        verify(mockTopic).close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testBuilderWithAllMethods() {
        try (MockedStatic<ProviderSupplier> mockedStatic = mockStatic(ProviderSupplier.class)) {
            // Arrange
            mockedStatic.when(() -> ProviderSupplier.findTopicProviderBuilder("test-provider"))
                .thenReturn(mockBuilder);
            when(mockBuilder.withTopicName(any())).thenReturn(mockBuilder);
            when(mockBuilder.withRegion(any())).thenReturn(mockBuilder);
            when(mockBuilder.withEndpoint(any())).thenReturn(mockBuilder);
            when(mockBuilder.withProxyEndpoint(any())).thenReturn(mockBuilder);
            when(mockBuilder.withCredentialsOverrider(any())).thenReturn(mockBuilder);
            when(mockBuilder.build()).thenReturn(mockTopic);

            // Act
            TopicClient result = TopicClient.builder("test-provider")
                .withTopicName("test-topic")
                .withRegion("us-west-2")
                .withEndpoint(URI.create("https://test.example.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com"))
                .withCredentialsOverrider(mock(CredentialsOverrider.class))
                .build();

            // Assert
            assertNotNull(result);
            verify(mockBuilder).withTopicName("test-topic");
            verify(mockBuilder).withRegion("us-west-2");
            verify(mockBuilder).withEndpoint(URI.create("https://test.example.com"));
            verify(mockBuilder).withProxyEndpoint(URI.create("https://proxy.example.com"));
            verify(mockBuilder).withCredentialsOverrider(any(CredentialsOverrider.class));
            verify(mockBuilder).build();
        }
    }

    @Test
    public void testSendWithException() {
        // Arrange
        Message message = Message.builder()
            .withBody("test".getBytes())
            .build();
        RuntimeException originalException = new RuntimeException("send error");
        doThrow(originalException).when(mockTopic).send(message);
        when(mockTopic.getException(originalException)).thenReturn((Class) UnknownException.class);

        try (MockedStatic<ExceptionHandler> mockedHandler = mockStatic(ExceptionHandler.class)) {
            mockedHandler.when(() -> ExceptionHandler.handleAndPropagate(UnknownException.class, originalException))
                .thenThrow(new UnknownException(originalException));

            // Act & Assert
            assertThrows(UnknownException.class, () -> topicClient.send(message));
            mockedHandler.verify(() -> ExceptionHandler.handleAndPropagate(UnknownException.class, originalException));
        }
    }

    @Test
    public void testCloseWithException() {
        // Arrange
        RuntimeException originalException = new RuntimeException("close error");
        try {
            doThrow(originalException).when(mockTopic).close();
        } catch (Exception e) {
            // This should not happen in the test setup
        }
        when(mockTopic.getException(originalException)).thenReturn((Class) UnknownException.class);

        try (MockedStatic<ExceptionHandler> mockedHandler = mockStatic(ExceptionHandler.class)) {
            mockedHandler.when(() -> ExceptionHandler.handleAndPropagate(UnknownException.class, originalException))
                .thenThrow(new UnknownException(originalException));

            // Act & Assert
            assertThrows(UnknownException.class, () -> topicClient.close());
            mockedHandler.verify(() -> ExceptionHandler.handleAndPropagate(UnknownException.class, originalException));
        }
    }

    @Test
    public void testSendNullMessage() {
        // Act & Assert
        assertDoesNotThrow(() -> topicClient.send(null));
        verify(mockTopic).send(null);
    }

    @Test
    public void testMultipleSends() {
        // Arrange
        Message message1 = Message.builder().withBody("test1".getBytes()).build();
        Message message2 = Message.builder().withBody("test2".getBytes()).build();

        // Act
        topicClient.send(message1);
        topicClient.send(message2);

        // Assert
        verify(mockTopic).send(message1);
        verify(mockTopic).send(message2);
    }
} 