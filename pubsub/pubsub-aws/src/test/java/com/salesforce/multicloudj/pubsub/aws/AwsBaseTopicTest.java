package com.salesforce.multicloudj.pubsub.aws;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.pubsub.driver.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.spy;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AwsBaseTopicTest {

    private TestAwsBaseTopic topic;

    @BeforeEach
    void setUp() {
        topic = spy(new TestAwsBaseTopic());
    }

    @Test
    void testEncodeMetadataKey() {
        // Valid characters
        assertEquals("simpleKey", topic.encodeMetadataKey("simpleKey"));
        assertEquals("key_with_underscore", topic.encodeMetadataKey("key_with_underscore"));
        assertEquals("key.name", topic.encodeMetadataKey("key.name"));
        
        // Invalid characters - should be encoded
        assertEquals("file__0x2F__path", topic.encodeMetadataKey("file/path"));
        assertEquals("user__0x20__name", topic.encodeMetadataKey("user name"));
        assertEquals("type__0x3A__message", topic.encodeMetadataKey("type:message"));
        
        // Period at start or consecutive periods
        assertEquals("__0x2E__key", topic.encodeMetadataKey(".key"));
        assertEquals("key__0x2E____0x2E__name", topic.encodeMetadataKey("key..name"));
        
        // Edge cases
        assertEquals("", topic.encodeMetadataKey(null));
        assertEquals("", topic.encodeMetadataKey(""));
    }

    @Test
    void testEncodeMetadataValue() {
        assertEquals("simpleValue", topic.encodeMetadataValue("simpleValue"));
        assertEquals("value+with+spaces", topic.encodeMetadataValue("value with spaces"));
        assertEquals("value%2Fwith%2Fslash", topic.encodeMetadataValue("value/with/slash"));
        assertEquals("", topic.encodeMetadataValue(null));
        assertEquals("", topic.encodeMetadataValue(""));
    }

    @Test
    void testIsValidUtf8() {
        assertTrue(topic.isValidUtf8("Hello World".getBytes(StandardCharsets.UTF_8)));
        assertTrue(topic.isValidUtf8("测试中文".getBytes(StandardCharsets.UTF_8)));
        assertTrue(topic.isValidUtf8(null));
        assertTrue(topic.isValidUtf8(new byte[0]));
        
        // Invalid UTF-8
        byte[] invalidUtf8 = {(byte) 0xFF, (byte) 0xFE};
        assertFalse(topic.isValidUtf8(invalidUtf8));
    }

    @Test
    void testMaybeEncodeBody() {
        byte[] validUtf8 = "test".getBytes(StandardCharsets.UTF_8);
        byte[] invalidUtf8 = {(byte) 0xFF, (byte) 0xFE};
        
        assertTrue(topic.maybeEncodeBody(validUtf8, AwsBaseTopic.BodyBase64Encoding.ALWAYS));
        assertFalse(topic.maybeEncodeBody(validUtf8, AwsBaseTopic.BodyBase64Encoding.NEVER));
        assertFalse(topic.maybeEncodeBody(validUtf8, AwsBaseTopic.BodyBase64Encoding.AUTO));
        assertTrue(topic.maybeEncodeBody(invalidUtf8, AwsBaseTopic.BodyBase64Encoding.AUTO));
        assertFalse(topic.maybeEncodeBody(null, AwsBaseTopic.BodyBase64Encoding.AUTO));
    }

    @Test
    void testEncodeMessageBody() {
        // Valid UTF-8 with AUTO encoding
        Message validMessage = Message.builder()
            .withBody("test message".getBytes(StandardCharsets.UTF_8))
            .build();
        AwsBaseTopic.BodyEncodingResult result = topic.encodeMessageBody(validMessage);
        assertEquals("test message", result.getBody());
        assertFalse(result.isBase64Encoded());
        
        // Invalid UTF-8 with AUTO encoding
        byte[] invalidUtf8 = {(byte) 0xFF, (byte) 0xFE};
        Message invalidMessage = Message.builder().withBody(invalidUtf8).build();
        result = topic.encodeMessageBody(invalidMessage);
        assertTrue(result.isBase64Encoded());
        
        // ALWAYS encoding
        topic.setBodyBase64Encoding(AwsBaseTopic.BodyBase64Encoding.ALWAYS);
        result = topic.encodeMessageBody(validMessage);
        assertEquals("dGVzdCBtZXNzYWdl", result.getBody());
        assertTrue(result.isBase64Encoded());
    }

    @Test
    void testGetException() {
        // AwsServiceException with error code
        AwsServiceException awsException = AwsServiceException.builder()
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDenied").build())
            .build();
        assertEquals(UnAuthorizedException.class, topic.getException(awsException));
        
        // AwsServiceException without error code
        AwsServiceException awsExceptionNoCode = AwsServiceException.builder().build();
        assertEquals(UnknownException.class, topic.getException(awsExceptionNoCode));
        
        // SdkClientException
        assertEquals(InvalidArgumentException.class, topic.getException(SdkClientException.builder().build()));
        
        // IllegalArgumentException
        assertEquals(InvalidArgumentException.class, topic.getException(new IllegalArgumentException()));
        
        // Unknown exception
        assertEquals(UnknownException.class, topic.getException(new RuntimeException()));
        
        // SubstrateSdkException
        InvalidArgumentException substrateException = new InvalidArgumentException("test");
        assertEquals(InvalidArgumentException.class, topic.getException(substrateException));
    }

    @Test
    void testCreateBatcherOptions() {
        Batcher.Options options = topic.createBatcherOptions();
        assertEquals(100, options.getMaxHandlers());
        assertEquals(1, options.getMinBatchSize());
        assertEquals(10, options.getMaxBatchSize());
        assertEquals(256 * 1024, options.getMaxBatchByteSize());
    }

    /**
     * Test implementation of AwsBaseTopic for unit testing.
     */
    private static class TestAwsBaseTopic extends AwsBaseTopic<TestAwsBaseTopic> {
        
        public TestAwsBaseTopic() {
            super(new Builder());
        }

        public TestAwsBaseTopic(Builder builder) {
            super(builder);
        }

        public void setBodyBase64Encoding(AwsBaseTopic.BodyBase64Encoding encoding) {
            // Create new TopicOptions with the desired encoding
            AwsBaseTopic.TopicOptions newOptions = new AwsBaseTopic.TopicOptions()
                .withBodyBase64Encoding(encoding);
            // Use reflection to set the field since it's protected in parent class
            try {
                java.lang.reflect.Field field = AwsBaseTopic.class.getDeclaredField("topicOptions");
                field.setAccessible(true);
                field.set(this, newOptions);
            } catch (Exception e) {
                throw new RuntimeException("Failed to set topicOptions", e);
            }
        }

        // Expose protected methods for testing
        public String encodeMetadataKey(String key) {
            return super.encodeMetadataKey(key);
        }

        public String encodeMetadataValue(String value) {
            return super.encodeMetadataValue(value);
        }

        public boolean isValidUtf8(byte[] bytes) {
            return super.isValidUtf8(bytes);
        }

        public boolean maybeEncodeBody(byte[] body, AwsBaseTopic.BodyBase64Encoding encoding) {
            return super.maybeEncodeBody(body, encoding);
        }

        public AwsBaseTopic.BodyEncodingResult encodeMessageBody(Message message) {
            return super.encodeMessageBody(message);
        }

        @Override
        public String getProviderId() {
            return "test-provider";
        }

        @Override
        protected void doSendBatch(java.util.List<Message> messages) {
            // No-op for testing
        }

        @Override
        public Builder builder() {
            return new Builder();
        }

        public static class Builder extends AwsBaseTopic.Builder<Builder, TestAwsBaseTopic> {
            @Override
            public TestAwsBaseTopic build() {
                return new TestAwsBaseTopic(this);
            }

            @Override
            protected Builder self() {
                return this;
            }
        }
    }
}
