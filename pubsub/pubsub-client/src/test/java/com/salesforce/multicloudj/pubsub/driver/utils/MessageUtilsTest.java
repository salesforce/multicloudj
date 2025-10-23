package com.salesforce.multicloudj.pubsub.driver.utils;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.pubsub.driver.Message;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for MessageUtils validation and utility methods.
 */
public class MessageUtilsTest {

    @Test
    public void testValidateMessage_ValidMessage() {
        // Arrange
        Message message = Message.builder()
            .withBody("test content".getBytes())
            .withMetadata("key", "value")
            .build();

        // Act & Assert - Should not throw
        assertDoesNotThrow(() -> MessageUtils.validateMessage(message));
    }

    @Test
    public void testValidateMessage_NullMessage() {
        // Act & Assert
        InvalidArgumentException exception = assertThrows(
            InvalidArgumentException.class,
            () -> MessageUtils.validateMessage(null)
        );
        assertEquals("Message cannot be null", exception.getMessage());
    }

    @Test
    public void testValidateMessage_NullBody() {
        // Arrange
        Message message = Message.builder()
            .withMetadata("key", "value")
            .build(); // Body is null by default

        // Act & Assert
        InvalidArgumentException exception = assertThrows(
            InvalidArgumentException.class,
            () -> MessageUtils.validateMessage(message)
        );
        assertEquals("Message body cannot be null", exception.getMessage());
    }

    @Test
    public void testValidateMessage_EmptyBodyAllowed() {
        // Arrange
        Message message = Message.builder()
            .withBody(new byte[0]) // Empty body should be allowed
            .build();

        // Act & Assert - Should not throw
        assertDoesNotThrow(() -> MessageUtils.validateMessage(message));
    }

    @Test
    public void testValidateMessage_LoggableIDNotAllowed() {
        // Arrange
        Message message = Message.builder()
            .withBody("test content".getBytes())
            .withLoggableID("some-logging-id") // This should not be allowed on outgoing messages
            .build();

        // Act & Assert
        InvalidArgumentException exception = assertThrows(
            InvalidArgumentException.class,
            () -> MessageUtils.validateMessage(message)
        );
        assertTrue(exception.getMessage().contains("LoggableID cannot be set on outgoing messages"));
        assertTrue(exception.getMessage().contains("reserved for received messages"));
    }

    @Test
    public void testValidateMessage_ValidMetadata() {
        // Arrange
        Map<String, String> metadata = new HashMap<>();
        metadata.put("validKey", "validValue");
        metadata.put("anotherKey", "anotherValue");
        
        Message message = Message.builder()
            .withBody("test".getBytes())
            .withMetadata(metadata)
            .build();

        // Act & Assert - Should not throw
        assertDoesNotThrow(() -> MessageUtils.validateMessage(message));
    }

    @Test
    public void testValidateMessage_InvalidMetadataKey() {
        // Arrange
        Map<String, String> metadata = new HashMap<>();
        metadata.put("", "value"); // Empty key should be invalid
        
        Message message = Message.builder()
            .withBody("test".getBytes())
            .withMetadata(metadata)
            .build();

        // Act & Assert
        InvalidArgumentException exception = assertThrows(
            InvalidArgumentException.class,
            () -> MessageUtils.validateMessage(message)
        );
        assertTrue(exception.getMessage().contains("Metadata keys cannot be null or empty"));
    }

    @Test
    public void testValidateMessage_ValidMetadataWithControlCharacters() {
        // Arrange
        Map<String, String> metadata = new HashMap<>();
        metadata.put("key\twith\ttabs", "value\nwith\nnewlines\rand\rcarriage\u0001returns"); // Control characters should be allowed
        
        Message message = Message.builder()
            .withBody("test".getBytes())
            .withMetadata(metadata)
            .build();

        // Act & Assert - Should not throw since control characters are allowed in UTF-8 validation
        assertDoesNotThrow(() -> MessageUtils.validateMessage(message));
    }

    @Test
    public void testValidateMessageBatch_ValidBatch() {
        // Arrange
        List<Message> messages = Arrays.asList(
            Message.builder().withBody("message1".getBytes()).build(),
            Message.builder().withBody("message2".getBytes()).build()
        );

        // Act & Assert - Should not throw
        assertDoesNotThrow(() -> MessageUtils.validateMessageBatch(messages));
    }

    @Test
    public void testValidateMessageBatch_NullList() {
        // Act & Assert
        InvalidArgumentException exception = assertThrows(
            InvalidArgumentException.class,
            () -> MessageUtils.validateMessageBatch(null)
        );
        assertEquals("Messages list cannot be null", exception.getMessage());
    }

    @Test
    public void testValidateMessageBatch_NullMessageInBatch() {
        // Arrange
        List<Message> messages = Arrays.asList(
            Message.builder().withBody("message1".getBytes()).build(),
            null, // Null message in the list
            Message.builder().withBody("message3".getBytes()).build()
        );

        // Act & Assert
        InvalidArgumentException exception = assertThrows(
            InvalidArgumentException.class,
            () -> MessageUtils.validateMessageBatch(messages)
        );
        assertTrue(exception.getMessage().contains("Message at index 1 cannot be null"));
    }

    @Test
    public void testValidateMessageBatch_InvalidMessageInBatch() {
        // Arrange
        Message invalidMessage = Message.builder()
            .withBody("valid body".getBytes())
            .withLoggableID("invalid-for-outgoing") // This makes the message invalid
            .build();
            
        List<Message> messages = Arrays.asList(
            Message.builder().withBody("message1".getBytes()).build(),
            invalidMessage
        );

        // Act & Assert
        InvalidArgumentException exception = assertThrows(
            InvalidArgumentException.class,
            () -> MessageUtils.validateMessageBatch(messages)
        );
        assertTrue(exception.getMessage().contains("LoggableID cannot be set on outgoing messages"));
    }

    @Test
    public void testCalculateByteSize_NullMessage() {
        // Act
        int size = MessageUtils.calculateByteSize(null);

        // Assert
        assertEquals(0, size);
    }

    @Test
    public void testCalculateByteSize_ValidMessage() {
        // Arrange
        String bodyContent = "test message";
        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");

        Message message = Message.builder()
            .withBody(bodyContent.getBytes())
            .withMetadata(metadata)
            .build();

        // Act
        int size = MessageUtils.calculateByteSize(message);

        // Assert
        // Size should only include body size (metadata and protocol overhead not included)
        assertEquals(bodyContent.length(), size);
    }

    @Test
    public void testCalculateByteSize_BodyOnlyMessage() {
        // Arrange
        String bodyContent = "simple message";
        Message message = Message.builder()
            .withBody(bodyContent.getBytes())
            .build();

        // Act
        int size = MessageUtils.calculateByteSize(message);

        // Assert
        assertEquals(bodyContent.length(), size);
    }

    @Test
    public void testCalculateByteSize_EmptyBody() {
        // Arrange
        Message message = Message.builder()
            .withBody(new byte[0])
            .build();

        // Act
        int size = MessageUtils.calculateByteSize(message);

        // Assert
        assertEquals(0, size);
    }


}