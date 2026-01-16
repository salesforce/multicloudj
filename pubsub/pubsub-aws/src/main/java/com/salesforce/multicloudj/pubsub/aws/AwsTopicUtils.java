package com.salesforce.multicloudj.pubsub.aws;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;

/**
 * Utility class for AWS topic implementations (SQS and SNS).
 * Contains common logic for metadata encoding, base64 encoding, and error handling.
 */
public final class AwsTopicUtils {
    
    private AwsTopicUtils() {} // Prevent instantiation

    // Batch processing constants (shared by SQS and SNS)
    public static final int MAX_BATCH_HANDLERS = 100;
    public static final int MIN_BATCH_SIZE = 1;
    public static final int MAX_BATCH_SIZE = 10;
    public static final int MAX_BATCH_BYTE_SIZE = 256 * 1024;

    /**
     * Metadata keys used for setting message attributes on AWS messages (SQS and SNS).
     */
    public static class MetadataKeys {
        public static final String DEDUPLICATION_ID = "DeduplicationId";
        public static final String MESSAGE_GROUP_ID = "MessageGroupId";
        public static final String SUBJECT = "Subject";
        public static final String BASE64_ENCODED = "base64encoded";
    }

    /**
     * Encodes metadata keys that contain special characters.
     * 
     * AWS SQS/SNS message attribute names can only contain alphanumeric characters,
     * underscores (_), hyphens (-), and periods (.). Special characters like
     * spaces, slashes, etc. are not allowed. Additionally, periods cannot be at
     * the start of the key or follow another period.
     * 
     * To work around this limitation, we use a custom encoding scheme:
     * special characters are encoded as "__0xHH__" where HH is the hex value
     * of the character.
     * 
     * Examples:
     * - "file/path" → "file__0x2F__path" (0x2F = '/')
     * - "user name" → "user__0x20__name" (0x20 = ' ')
     * - "type:message" → "type__0x3A__message" (0x3A = ':')
     * - ".key" → "__0x2E__key" (period at start)
     * - "key..name" → "key__0x2E____0x2E__name" (consecutive periods)
     */
    public static String encodeMetadataKey(String key) {
        if (key == null) return "";
        
        // AWS MessageAttributeName only allows [a-zA-Z0-9_.-]
        // reference: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html
        // reference: https://docs.aws.amazon.com/sns/latest/dg/sns-message-attributes.html
        // Encode special characters using hex pattern "__0xHH__"
        // Periods at start or consecutive periods must be encoded
        StringBuilder encoded = new StringBuilder();
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            boolean isValid = false;
            
            if ((c >= 'a' && c <= 'z') || 
                (c >= 'A' && c <= 'Z') || 
                (c >= '0' && c <= '9') || 
                c == '_' || c == '-') {
                // Valid character, append directly
                isValid = true;
            } else if (c == '.') {
                // Period is valid only if not at start and previous char is not a period
                if (i != 0 && key.charAt(i - 1) != '.') {
                    isValid = true;
                }
            }
            
            if (isValid) {
                encoded.append(c);
            } else {
                // Invalid character, encode as "__0xHH__"
                String hexValue = Integer.toHexString(c).toUpperCase();
                encoded.append("__0x").append(hexValue).append("__");
            }
        }
        return encoded.toString();
    }

    /**
     * Encodes metadata value using URL encoding.
     */
    public static String encodeMetadataValue(String value) {
        if (value == null) return "";
        return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    /**
     * Decides whether body should be base64-encoded based on encoding option.
     * Returns true if encoding occurred, false otherwise.
     */
    public static boolean maybeEncodeBody(byte[] body, BodyBase64Encoding encoding) {
        if (body == null) return false;
        
        switch (encoding) {
            case ALWAYS:
                return true;
            case NEVER:
                return false;
            case AUTO:
            default:
                return !isValidUtf8(body);
        }
    }

    /**
     * Checks if byte array contains valid UTF-8.
     */
    public static boolean isValidUtf8(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return true;
        }
        
        try {
            CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
            decoder.onMalformedInput(CodingErrorAction.REPORT);
            decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
            decoder.decode(ByteBuffer.wrap(bytes));
            return true;
        } catch (CharacterCodingException e) {
            return false;
        }
    }

    /**
     * Maps AWS exceptions to SubstrateSdkException types.
     */
    public static Class<? extends SubstrateSdkException> getException(Throwable t) {
        if (t instanceof SubstrateSdkException) {
            return (Class<? extends SubstrateSdkException>) t.getClass();
        }
        if (t instanceof AwsServiceException) {
            AwsServiceException serviceException = (AwsServiceException) t;
            if (serviceException.awsErrorDetails() != null) {
                String errorCode = serviceException.awsErrorDetails().errorCode();
                return ErrorCodeMapping.getException(errorCode);
            }
            return UnknownException.class;
        }
        if (t instanceof SdkClientException || t instanceof IllegalArgumentException) {
            return InvalidArgumentException.class;
        }
        return UnknownException.class;
    }

    /**
     * BodyBase64Encoding is an enum of strategies for when to base64 message bodies.
     */
    public enum BodyBase64Encoding {
        /**
         * Automatically determines if encoding is required.
         * Valid UTF-8 text is sent as-is; invalid sequences are base64 encoded,
         * and a MessageAttribute with key "base64encoded" is added to the message.
         */
        AUTO,
        /**
         * Always means that all message bodies are base64 encoded.
         * A MessageAttribute with key "base64encoded" is added to the message.
         */
        ALWAYS,
        /**
         * Never means that message bodies are never base64 encoded. Non-UTF-8
         * bytes in message bodies may be modified by SNS/SQS.
         */
        NEVER
    }

    /**
     * TopicOptions contains configuration options for topics.
     */
    public static class TopicOptions {
        private BodyBase64Encoding bodyBase64Encoding = BodyBase64Encoding.AUTO;
        
        public BodyBase64Encoding getBodyBase64Encoding() {
            return bodyBase64Encoding;
        }
        
        public TopicOptions withBodyBase64Encoding(BodyBase64Encoding encoding) {
            this.bodyBase64Encoding = encoding;
            return this;
        }
    }
}
