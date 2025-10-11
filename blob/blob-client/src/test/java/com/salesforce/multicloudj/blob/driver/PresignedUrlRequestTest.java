package com.salesforce.multicloudj.blob.driver;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class PresignedUrlRequestTest {

    @Test
    void testBuilderWithAllFields() {
        String key = "test-key";
        Duration duration = Duration.ofHours(1);
        PresignedOperation type = PresignedOperation.UPLOAD;
        Map<String, String> metadata = Map.of("key1", "value1");
        Map<String, String> tags = Map.of("tag1", "tagValue1");
        String kmsKeyId = "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012";

        PresignedUrlRequest request = PresignedUrlRequest.builder()
                .key(key)
                .duration(duration)
                .type(type)
                .metadata(metadata)
                .tags(tags)
                .kmsKeyId(kmsKeyId)
                .build();

        assertEquals(key, request.getKey());
        assertEquals(duration, request.getDuration());
        assertEquals(type, request.getType());
        assertEquals(metadata, request.getMetadata());
        assertEquals(tags, request.getTags());
        assertEquals(kmsKeyId, request.getKmsKeyId());
    }

    @Test
    void testBuilderWithNullKmsKey() {
        String key = "test-key";
        Duration duration = Duration.ofHours(1);
        PresignedOperation type = PresignedOperation.DOWNLOAD;

        PresignedUrlRequest request = PresignedUrlRequest.builder()
                .key(key)
                .duration(duration)
                .type(type)
                .kmsKeyId(null)
                .build();

        assertEquals(key, request.getKey());
        assertEquals(duration, request.getDuration());
        assertEquals(type, request.getType());
        assertNull(request.getKmsKeyId());
    }

    @Test
    void testBuilderWithEmptyKmsKey() {
        String key = "test-key";
        Duration duration = Duration.ofHours(1);
        PresignedOperation type = PresignedOperation.UPLOAD;

        PresignedUrlRequest request = PresignedUrlRequest.builder()
                .key(key)
                .duration(duration)
                .type(type)
                .kmsKeyId("")
                .build();

        assertEquals(key, request.getKey());
        assertEquals(duration, request.getDuration());
        assertEquals(type, request.getType());
        assertEquals("", request.getKmsKeyId());
    }

    @Test
    void testBuilderWithoutKmsKey() {
        String key = "test-key";
        Duration duration = Duration.ofHours(1);
        PresignedOperation type = PresignedOperation.UPLOAD;
        Map<String, String> metadata = Map.of("key1", "value1");

        PresignedUrlRequest request = PresignedUrlRequest.builder()
                .key(key)
                .duration(duration)
                .type(type)
                .metadata(metadata)
                .build();

        assertEquals(key, request.getKey());
        assertEquals(duration, request.getDuration());
        assertEquals(type, request.getType());
        assertEquals(metadata, request.getMetadata());
        assertNull(request.getKmsKeyId());
    }
}
