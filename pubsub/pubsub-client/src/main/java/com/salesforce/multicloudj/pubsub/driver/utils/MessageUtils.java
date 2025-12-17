package com.salesforce.multicloudj.pubsub.driver.utils;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.pubsub.driver.Message;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.List;

/**
 * Utility class for common message operations across all cloud providers.
 * 
 * This class provides utility methods for message validation
 * that can be used by any
 * topic or subscription implementation.
 */
public final class MessageUtils {
    
    private MessageUtils() {
    }
    
    /**
     * Validates that a message is not null and has valid content.
     * 
     * @param message The message to validate
     * @throws InvalidArgumentException if the message is invalid
     */
    public static void validateMessage(Message message) {
        if (message == null) {
            throw new InvalidArgumentException("Message cannot be null");
        }
        
        // Validate body is not null (empty is allowed)
        if (message.getBody() == null) {
            throw new InvalidArgumentException("Message body cannot be null");
        }
        
        // Prevent setting LoggableID on outgoing messages - this is for received messages only
        if (message.getLoggableID() != null) {
            throw new InvalidArgumentException("LoggableID cannot be set on outgoing messages. This field is reserved for received messages and is set internally by drivers.");
        }
        
        // Validate metadata keys and values if present
        if (message.getMetadata() != null) {
            validateMetadata(message.getMetadata());
        }
    }
    
    /**
     * Validates a list of messages for batch operations.
     * 
     * @param messages The messages to validate
     * @throws InvalidArgumentException if any message is invalid
     */
    public static void validateMessageBatch(List<Message> messages) {
        if (messages == null) {
            throw new InvalidArgumentException("Messages list cannot be null");
        }
        
        for (int i = 0; i < messages.size(); i++) {
            Message message = messages.get(i);
            if (message == null) {
                throw new InvalidArgumentException("Message at index " + i + " cannot be null");
            }
            validateMessage(message);
        }
    }
    
    /**
     * Calculates the byte size of a message body.
     * 
     * @param message The message to calculate size for
     * @return The message body byte size
     */
    public static int calculateByteSize(Message message) {
        if (message == null) {
            return 0;
        }
        
        // Return only body size
        if (message.getBody() != null) {
            return message.getBody().length;
        }
        
        return 0;
    }
    
    /**
     * Validates metadata keys and values.
     */
    private static void validateMetadata(Map<String, String> metadata) {
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            
            // Validate key
            if (key == null || key.trim().isEmpty()) {
                throw new InvalidArgumentException("Metadata keys cannot be null or empty");
            }
            
            // Check for valid UTF-8 encoding in key
            if (!isValidUtf8String(key)) {
                throw new InvalidArgumentException("Metadata key is not valid UTF-8: " + key);
            }
            
            // Validate value (null is allowed, but if present should be valid UTF-8)
            if (value != null && !isValidUtf8String(value)) {
                throw new InvalidArgumentException("Metadata value is not valid UTF-8 for key: " + key);
            }
        }
    }
    
    /**
     * Checks if a string is valid UTF-8.
     */
    private static boolean isValidUtf8String(String str) {
        if (str == null) {
            return true;
        }
        
        try {
            // Test if the string can be properly encoded and decoded as UTF-8
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            String decoded = new String(bytes, StandardCharsets.UTF_8);
            return str.equals(decoded);
        } catch (Exception e) {
            return false;
        }
    }
}