package com.salesforce.multicloudj.blob.driver;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class UploadRequestTest {

    @Test
    void testBuilder_WithAllFields() {
        // Given
        String key = "test-key";
        long contentLength = 1024L;
        Map<String, String> metadata = Map.of("key1", "value1");
        Map<String, String> tags = Map.of("tag1", "tagValue1");
        String kmsKeyId = "arn:aws:kms:us-east-1:123456789012:key/test-key-id";

        // When
        UploadRequest request = UploadRequest.builder()
                .withKey(key)
                .withContentLength(contentLength)
                .withMetadata(metadata)
                .withTags(tags)
                .withKmsKeyId(kmsKeyId)
                .build();

        // Then
        assertEquals(key, request.getKey());
        assertEquals(contentLength, request.getContentLength());
        assertEquals(metadata, request.getMetadata());
        assertEquals(tags, request.getTags());
        assertEquals(kmsKeyId, request.getKmsKeyId());
    }

    @Test
    void testBuilder_WithoutKmsKeyId() {
        // Given
        String key = "test-key";

        // When
        UploadRequest request = UploadRequest.builder()
                .withKey(key)
                .build();

        // Then
        assertEquals(key, request.getKey());
        assertNull(request.getKmsKeyId());
    }

    @Test
    void testBuilder_WithEmptyKmsKeyId() {
        // Given
        String key = "test-key";
        String kmsKeyId = "";

        // When
        UploadRequest request = UploadRequest.builder()
                .withKey(key)
                .withKmsKeyId(kmsKeyId)
                .build();

        // Then
        assertEquals(key, request.getKey());
        assertEquals(kmsKeyId, request.getKmsKeyId());
    }

    @Test
    void testBuilder_MinimalFields() {
        // Given
        String key = "test-key";

        // When
        UploadRequest request = UploadRequest.builder()
                .withKey(key)
                .build();

        // Then
        assertNotNull(request);
        assertEquals(key, request.getKey());
        assertNotNull(request.getMetadata());
        assertNotNull(request.getTags());
    }
}
