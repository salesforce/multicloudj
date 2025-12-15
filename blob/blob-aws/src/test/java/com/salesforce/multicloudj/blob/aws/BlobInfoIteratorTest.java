package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for BlobInfoIterator
 */
public class BlobInfoIteratorTest {

    private S3Client mockS3Client;
    private static final String TEST_BUCKET = "test-bucket";

    @BeforeEach
    void setUp() {
        mockS3Client = mock(S3Client.class);
    }

    @Test
    void testBlobInfoIteratorIncludesTimestamp() {
        // Given
        Instant timestamp1 = Instant.now();
        Instant timestamp2 = timestamp1.plusSeconds(100);

        S3Object s3Object1 = S3Object.builder()
                .key("test-key-1")
                .size(1024L)
                .lastModified(timestamp1)
                .build();

        S3Object s3Object2 = S3Object.builder()
                .key("test-key-2")
                .size(2048L)
                .lastModified(timestamp2)
                .build();

        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(s3Object1, s3Object2)
                .isTruncated(false)
                .build();

        when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

        ListBlobsRequest listRequest = ListBlobsRequest.builder().build();
        BlobInfoIterator iterator = new BlobInfoIterator(mockS3Client, TEST_BUCKET, listRequest);

        // When
        assertTrue(iterator.hasNext());
        BlobInfo blobInfo1 = iterator.next();
        assertTrue(iterator.hasNext());
        BlobInfo blobInfo2 = iterator.next();

        // Then
        assertNotNull(blobInfo1);
        assertEquals("test-key-1", blobInfo1.getKey());
        assertEquals(1024L, blobInfo1.getObjectSize());
        assertEquals(timestamp1, blobInfo1.getLastModified());

        assertNotNull(blobInfo2);
        assertEquals("test-key-2", blobInfo2.getKey());
        assertEquals(2048L, blobInfo2.getObjectSize());
        assertEquals(timestamp2, blobInfo2.getLastModified());
    }

    @Test
    void testBlobInfoIteratorWithPrefix() {
        // Given
        Instant timestamp = Instant.now();
        S3Object s3Object = S3Object.builder()
                .key("prefix/test-key")
                .size(1024L)
                .lastModified(timestamp)
                .build();

        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(s3Object)
                .isTruncated(false)
                .build();

        when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

        ListBlobsRequest listRequest = ListBlobsRequest.builder()
                .withPrefix("prefix/")
                .build();
        BlobInfoIterator iterator = new BlobInfoIterator(mockS3Client, TEST_BUCKET, listRequest);

        // When
        assertTrue(iterator.hasNext());
        BlobInfo blobInfo = iterator.next();

        // Then
        assertNotNull(blobInfo);
        assertEquals("prefix/test-key", blobInfo.getKey());
        assertEquals(1024L, blobInfo.getObjectSize());
        assertEquals(timestamp, blobInfo.getLastModified());
    }
}

