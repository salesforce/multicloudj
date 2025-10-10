package com.salesforce.multicloudj.blob.driver;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class DownloadRequestTest {

    @Test
    void testBuilder_WithAllFields() {
        // Given
        String key = "test-key";
        String versionId = "v1";
        Long start = 0L;
        Long end = 100L;
        String kmsKeyId = "arn:aws:kms:us-east-1:123456789012:key/test-key-id";

        // When
        DownloadRequest request = DownloadRequest.builder()
                .withKey(key)
                .withVersionId(versionId)
                .withRange(start, end)
                .withKmsKeyId(kmsKeyId)
                .build();

        // Then
        assertEquals(key, request.getKey());
        assertEquals(versionId, request.getVersionId());
        assertEquals(start, request.getStart());
        assertEquals(end, request.getEnd());
        assertEquals(kmsKeyId, request.getKmsKeyId());
    }

    @Test
    void testBuilder_WithoutKmsKeyId() {
        // Given
        String key = "test-key";

        // When
        DownloadRequest request = DownloadRequest.builder()
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
        DownloadRequest request = DownloadRequest.builder()
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
        DownloadRequest request = DownloadRequest.builder()
                .withKey(key)
                .build();

        // Then
        assertNotNull(request);
        assertEquals(key, request.getKey());
    }

    @Test
    void testBuilder_WithRange() {
        // Given
        String key = "test-key";
        Long start = 100L;
        Long end = 200L;

        // When
        DownloadRequest request = DownloadRequest.builder()
                .withKey(key)
                .withRange(start, end)
                .build();

        // Then
        assertEquals(key, request.getKey());
        assertEquals(start, request.getStart());
        assertEquals(end, request.getEnd());
    }
}
