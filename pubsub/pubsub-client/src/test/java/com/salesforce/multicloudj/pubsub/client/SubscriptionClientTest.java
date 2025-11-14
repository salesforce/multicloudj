package com.salesforce.multicloudj.pubsub.client;

import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.Message;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SubscriptionClientTest {

    @Mock
    private AbstractSubscription mockSubscription;

    @Mock
    private AbstractSubscription.Builder mockBuilder;

    private SubscriptionClient subscriptionClient;

    @BeforeEach
    public void setUp() {
        subscriptionClient = new SubscriptionClient(mockSubscription);
    }

    @Test
    public void testReceive() {
        // Arrange
        Message expectedMessage = Message.builder().withBody("test1".getBytes()).build();
        when(mockSubscription.receive()).thenReturn(expectedMessage);

        // Act
        Message result = subscriptionClient.receive();

        // Assert
        assertEquals(expectedMessage, result);
        verify(mockSubscription).receive();
    }

    @Test
    public void testSendAck() {
        // Arrange
        String ackID = "test-ack-id";

        // Act
        subscriptionClient.sendAck(ackID);

        // Assert
        verify(mockSubscription).sendAck(ackID);
    }

    @Test
    public void testSendAcks() {
        // Arrange
        List<String> ackIDs = Arrays.asList("test-ack-id-1", "test-ack-id-2");
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        when(mockSubscription.sendAcks(ackIDs)).thenReturn(future);

        // Act
        CompletableFuture<Void> result = subscriptionClient.sendAcks(ackIDs);

        // Assert
        assertSame(future, result);
        verify(mockSubscription).sendAcks(ackIDs);
    }

    @Test
    public void testSendNack() {
        // Arrange
        String ackID = "test-ack-id";

        // Act
        subscriptionClient.sendNack(ackID);

        // Assert
        verify(mockSubscription).sendNack(ackID);
    }

    @Test
    public void testSendNacks() {
        // Arrange
        List<String> ackIDs = Arrays.asList("test-ack-id-1", "test-ack-id-2");
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        when(mockSubscription.sendNacks(ackIDs)).thenReturn(future);

        // Act
        CompletableFuture<Void> result = subscriptionClient.sendNacks(ackIDs);

        // Assert
        assertSame(future, result);
        verify(mockSubscription).sendNacks(ackIDs);
    }

    @Test
    public void testCanNack() {
        // Arrange
        when(mockSubscription.canNack()).thenReturn(true);

        // Act
        boolean result = subscriptionClient.canNack();

        // Assert
        assertTrue(result);
        verify(mockSubscription).canNack();
    }

    @Test
    public void testGetAttributes() {
        // Arrange
        GetAttributeResult expectedAttributes = new GetAttributeResult.Builder()
        .name("test-subscription")
        .topic("test-topic")
        .build();
        when(mockSubscription.getAttributes()).thenReturn(expectedAttributes);

        // Act
        GetAttributeResult result = subscriptionClient.getAttributes();

        // Assert
        assertEquals(expectedAttributes, result);
        verify(mockSubscription).getAttributes();
    }

    @Test
    public void testIsRetryable() {
        // Arrange
        Throwable error = new RuntimeException("test error");
        when(mockSubscription.isRetryable(error)).thenReturn(true);

        // Act
        boolean result = subscriptionClient.isRetryable(error);

        // Assert
        assertTrue(result);
        verify(mockSubscription).isRetryable(error);
    }

    @Test
    public void testClose() throws Exception {
        // Act
        subscriptionClient.close();

        // Assert
        verify(mockSubscription).close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testBuilderWithAllMethods() {
        try (MockedStatic<ProviderSupplier> mockedStatic = mockStatic(ProviderSupplier.class)) {
            // Arrange
            mockedStatic.when(() -> ProviderSupplier.findSubscriptionProviderBuilder("test-provider"))
                .thenReturn(mockBuilder);
            when(mockBuilder.withSubscriptionName(any())).thenReturn(mockBuilder);
            when(mockBuilder.withRegion(any())).thenReturn(mockBuilder);
            when(mockBuilder.withEndpoint(any())).thenReturn(mockBuilder);
            when(mockBuilder.withProxyEndpoint(any())).thenReturn(mockBuilder);
            when(mockBuilder.withCredentialsOverrider(any())).thenReturn(mockBuilder);
            when(mockBuilder.build()).thenReturn(mockSubscription);

            // Act
            SubscriptionClient result = SubscriptionClient.builder("test-provider")
                .withSubscriptionName("test-sub")
                .withRegion("us-west-2")
                .withEndpoint(URI.create("https://test.example.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com"))
                .withCredentialsOverrider(mock(CredentialsOverrider.class))
                .build();

            // Assert
            assertNotNull(result);
            verify(mockBuilder).withSubscriptionName("test-sub");
            verify(mockBuilder).withRegion("us-west-2");
            verify(mockBuilder).withEndpoint(URI.create("https://test.example.com"));
            verify(mockBuilder).withProxyEndpoint(URI.create("https://proxy.example.com"));
            verify(mockBuilder).withCredentialsOverrider(any(CredentialsOverrider.class));
            verify(mockBuilder).build();
        }
    }

    @Test
    public void testReceiveWithException() {
        // Arrange
        RuntimeException originalException = new RuntimeException("test error");
        when(mockSubscription.receive()).thenThrow(originalException);
        when(mockSubscription.getException(originalException)).thenReturn((Class) UnknownException.class);

        try (MockedStatic<ExceptionHandler> mockedHandler = mockStatic(ExceptionHandler.class)) {
            mockedHandler.when(() -> ExceptionHandler.handleAndPropagate(UnknownException.class, originalException))
                .thenThrow(new UnknownException(originalException));

            // Act & Assert
            assertThrows(UnknownException.class, () -> subscriptionClient.receive());
            mockedHandler.verify(() -> ExceptionHandler.handleAndPropagate(UnknownException.class, originalException));
        }
    }

    @Test
    public void testSendAckWithException() {
        // Arrange
        String ackID = "test-ack-id";
        RuntimeException originalException = new RuntimeException("ack error");
        doThrow(originalException).when(mockSubscription).sendAck(ackID);
        when(mockSubscription.getException(originalException)).thenReturn((Class) UnknownException.class);

        try (MockedStatic<ExceptionHandler> mockedHandler = mockStatic(ExceptionHandler.class)) {
            mockedHandler.when(() -> ExceptionHandler.handleAndPropagate(UnknownException.class, originalException))
                .thenThrow(new UnknownException(originalException));

            // Act & Assert
            assertThrows(UnknownException.class, () -> subscriptionClient.sendAck(ackID));
        }
    }

    @Test
    public void testSendAcksWithException() {
        // Arrange
        List<String> ackIDs = Arrays.asList("test-ack-id-1");
        RuntimeException originalException = new RuntimeException("acks error");
        when(mockSubscription.sendAcks(ackIDs)).thenThrow(originalException);
        when(mockSubscription.getException(originalException)).thenReturn((Class) UnknownException.class);

        try (MockedStatic<ExceptionHandler> mockedHandler = mockStatic(ExceptionHandler.class)) {
            mockedHandler.when(() -> ExceptionHandler.handleAndPropagate(UnknownException.class, originalException))
                .thenThrow(new UnknownException(originalException));

            // Act & Assert
            assertThrows(UnknownException.class, () -> subscriptionClient.sendAcks(ackIDs));
        }
    }

    @Test
    public void testSendNackWithException() {
        // Arrange
        String ackID = "test-ack-id";
        RuntimeException originalException = new RuntimeException("nack error");
        doThrow(originalException).when(mockSubscription).sendNack(ackID);
        when(mockSubscription.getException(originalException)).thenReturn((Class) UnknownException.class);

        try (MockedStatic<ExceptionHandler> mockedHandler = mockStatic(ExceptionHandler.class)) {
            mockedHandler.when(() -> ExceptionHandler.handleAndPropagate(UnknownException.class, originalException))
                .thenThrow(new UnknownException(originalException));

            // Act & Assert
            assertThrows(UnknownException.class, () -> subscriptionClient.sendNack(ackID));
        }
    }

    @Test
    public void testSendNacksWithException() {
        // Arrange
        List<String> ackIDs = Arrays.asList("test-ack-id-1");
        RuntimeException originalException = new RuntimeException("nacks error");
        when(mockSubscription.sendNacks(ackIDs)).thenThrow(originalException);
        when(mockSubscription.getException(originalException)).thenReturn((Class) UnknownException.class);

        try (MockedStatic<ExceptionHandler> mockedHandler = mockStatic(ExceptionHandler.class)) {
            mockedHandler.when(() -> ExceptionHandler.handleAndPropagate(UnknownException.class, originalException))
                .thenThrow(new UnknownException(originalException));

            // Act & Assert
            assertThrows(UnknownException.class, () -> subscriptionClient.sendNacks(ackIDs));
        }
    }

    @Test
    public void testGetAttributesWithException() {
        // Arrange
        RuntimeException originalException = new RuntimeException("attributes error");
        when(mockSubscription.getAttributes()).thenThrow(originalException);
        when(mockSubscription.getException(originalException)).thenReturn((Class) UnknownException.class);

        try (MockedStatic<ExceptionHandler> mockedHandler = mockStatic(ExceptionHandler.class)) {
            mockedHandler.when(() -> ExceptionHandler.handleAndPropagate(UnknownException.class, originalException))
                .thenThrow(new UnknownException(originalException));

            // Act & Assert
            assertThrows(UnknownException.class, () -> subscriptionClient.getAttributes());
        }
    }

    @Test
    public void testCloseWithException() {
        // Arrange
        RuntimeException originalException = new RuntimeException("close error");
        try {
            doThrow(originalException).when(mockSubscription).close();
        } catch (Exception e) {
            // This should not happen in the test setup
        }
        when(mockSubscription.getException(originalException)).thenReturn((Class) UnknownException.class);

        try (MockedStatic<ExceptionHandler> mockedHandler = mockStatic(ExceptionHandler.class)) {
            mockedHandler.when(() -> ExceptionHandler.handleAndPropagate(UnknownException.class, originalException))
                .thenThrow(new UnknownException(originalException));

            // Act & Assert
            assertThrows(UnknownException.class, () -> subscriptionClient.close());
        }
    }
} 