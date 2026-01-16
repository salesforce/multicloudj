package com.salesforce.multicloudj.pubsub.aws;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
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

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AwsTopicUtilsTest {

    @Test
    void testEncodeMetadataKey() {
        // Valid characters don't need encoding
        assertEquals("key.name", AwsTopicUtils.encodeMetadataKey("key.name"));
        
        // Special characters need encoding
        assertEquals("file__0x2F__path", AwsTopicUtils.encodeMetadataKey("file/path"));
        assertEquals("user__0x20__name", AwsTopicUtils.encodeMetadataKey("user name"));
        
        // Period at start must be encoded
        assertEquals("__0x2E__key", AwsTopicUtils.encodeMetadataKey(".key"));
        
        // Consecutive periods: second one gets encoded
        assertEquals("key.__0x2E__name", AwsTopicUtils.encodeMetadataKey("key..name"));
        
        // Null/empty handling
        assertEquals("", AwsTopicUtils.encodeMetadataKey(null));
        assertEquals("", AwsTopicUtils.encodeMetadataKey(""));
    }

    @Test
    void testEncodeMetadataValue() {
        assertEquals("test+value", AwsTopicUtils.encodeMetadataValue("test value"));
        String encoded = AwsTopicUtils.encodeMetadataValue("test&value=123");
        assertTrue(encoded.contains("%26") && encoded.contains("%3D"));
        assertEquals("", AwsTopicUtils.encodeMetadataValue(null));
    }

    @Test
    void testIsValidUtf8() {
        assertTrue(AwsTopicUtils.isValidUtf8("Hello, 世界!".getBytes(StandardCharsets.UTF_8)));
        assertFalse(AwsTopicUtils.isValidUtf8(new byte[]{(byte) 0xFF, (byte) 0xFE}));
        assertTrue(AwsTopicUtils.isValidUtf8(null));
    }

    @Test
    void testMaybeEncodeBody() {
        byte[] validUtf8 = "test".getBytes(StandardCharsets.UTF_8);
        byte[] invalidUtf8 = {(byte) 0xFF, (byte) 0xFE};
        
        assertTrue(AwsTopicUtils.maybeEncodeBody(validUtf8, AwsTopicUtils.BodyBase64Encoding.ALWAYS));
        assertFalse(AwsTopicUtils.maybeEncodeBody(validUtf8, AwsTopicUtils.BodyBase64Encoding.NEVER));
        assertFalse(AwsTopicUtils.maybeEncodeBody(validUtf8, AwsTopicUtils.BodyBase64Encoding.AUTO));
        assertTrue(AwsTopicUtils.maybeEncodeBody(invalidUtf8, AwsTopicUtils.BodyBase64Encoding.AUTO));
    }

    @Test
    void testGetException() {
        // SubstrateSdkException returns itself
        UnAuthorizedException exception = new UnAuthorizedException("test");
        assertEquals(UnAuthorizedException.class, AwsTopicUtils.getException(exception));
        
        // AwsServiceException with error code
        AwsServiceException awsException = AwsServiceException.builder()
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDenied").build())
            .build();
        assertEquals(UnAuthorizedException.class, AwsTopicUtils.getException(awsException));
        
        // AwsServiceException without error code
        AwsServiceException awsExceptionNoCode = AwsServiceException.builder().build();
        assertEquals(UnknownException.class, AwsTopicUtils.getException(awsExceptionNoCode));
        
        // SdkClientException and IllegalArgumentException
        assertEquals(InvalidArgumentException.class, AwsTopicUtils.getException(SdkClientException.builder().build()));
        assertEquals(InvalidArgumentException.class, AwsTopicUtils.getException(new IllegalArgumentException()));
        
        // Other exceptions
        assertEquals(UnknownException.class, AwsTopicUtils.getException(new RuntimeException()));
    }

    @Test
    void testTopicOptions() {
        AwsTopicUtils.TopicOptions options = new AwsTopicUtils.TopicOptions();
        assertEquals(AwsTopicUtils.BodyBase64Encoding.AUTO, options.getBodyBase64Encoding());
        
        assertSame(options, options.withBodyBase64Encoding(AwsTopicUtils.BodyBase64Encoding.ALWAYS));
        assertEquals(AwsTopicUtils.BodyBase64Encoding.ALWAYS, options.getBodyBase64Encoding());
    }
}
