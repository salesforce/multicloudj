package com.salesforce.multicloudj.blob.driver;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for BlobInfo class
 */
public class BlobInfoTest {

    @Test
    void testBuilderWithTimestamp() {
        Instant timestamp = Instant.now();
        BlobInfo blobInfo = BlobInfo.builder()
                .withKey("test-key")
                .withObjectSize(1024L)
                .withLastModified(timestamp)
                .build();

        assertEquals("test-key", blobInfo.getKey());
        assertEquals(1024L, blobInfo.getObjectSize());
        assertEquals(timestamp, blobInfo.getLastModified());
    }

    @Test
    void testBuilderWithoutTimestamp() {
        BlobInfo blobInfo = BlobInfo.builder()
                .withKey("test-key")
                .withObjectSize(1024L)
                .build();

        assertEquals("test-key", blobInfo.getKey());
        assertEquals(1024L, blobInfo.getObjectSize());
        assertNull(blobInfo.getLastModified());
    }

    @Test
    void testEqualsWithTimestamp() {
        Instant timestamp = Instant.now();
        BlobInfo blobInfo1 = BlobInfo.builder()
                .withKey("test-key")
                .withObjectSize(1024L)
                .withLastModified(timestamp)
                .build();

        BlobInfo blobInfo2 = BlobInfo.builder()
                .withKey("test-key")
                .withObjectSize(1024L)
                .withLastModified(timestamp)
                .build();

        assertEquals(blobInfo1, blobInfo2);
        assertEquals(blobInfo1.hashCode(), blobInfo2.hashCode());
    }

    @Test
    void testEqualsWithDifferentTimestamps() {
        Instant timestamp1 = Instant.now();
        Instant timestamp2 = timestamp1.plusSeconds(100);

        BlobInfo blobInfo1 = BlobInfo.builder()
                .withKey("test-key")
                .withObjectSize(1024L)
                .withLastModified(timestamp1)
                .build();

        BlobInfo blobInfo2 = BlobInfo.builder()
                .withKey("test-key")
                .withObjectSize(1024L)
                .withLastModified(timestamp2)
                .build();

        assertNotEquals(blobInfo1, blobInfo2);
    }

    @Test
    void testEqualsWithNullTimestamp() {
        BlobInfo blobInfo1 = BlobInfo.builder()
                .withKey("test-key")
                .withObjectSize(1024L)
                .build();

        BlobInfo blobInfo2 = BlobInfo.builder()
                .withKey("test-key")
                .withObjectSize(1024L)
                .build();

        assertEquals(blobInfo1, blobInfo2);
        assertEquals(blobInfo1.hashCode(), blobInfo2.hashCode());
    }

    @Test
    void testEqualsWithOneNullTimestamp() {
        Instant timestamp = Instant.now();
        BlobInfo blobInfo1 = BlobInfo.builder()
                .withKey("test-key")
                .withObjectSize(1024L)
                .withLastModified(timestamp)
                .build();

        BlobInfo blobInfo2 = BlobInfo.builder()
                .withKey("test-key")
                .withObjectSize(1024L)
                .build();

        assertNotEquals(blobInfo1, blobInfo2);
    }
}

