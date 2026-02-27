package com.salesforce.multicloudj.blob.gcp;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.PresignedOperation;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.blob.driver.ObjectLockConfiguration;
import com.salesforce.multicloudj.blob.driver.RetentionMode;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.function.Executable;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GcpTransformerTest {

    private static final String TEST_BUCKET = "test-bucket";
    private static final String TEST_KEY = "test-key";
    private static final String TEST_VERSION_ID = "12345";
    private static final String TEST_ETAG = "test-etag";
    private static final String TEST_DEST_BUCKET = "test-dest-bucket";
    private static final Long TEST_GENERATION = 12345L;
    private static final Long TEST_SIZE = 1024L;

    private GcpTransformer transformer;

    @Mock
    private Blob mockBlob;

    @BeforeEach
    void setUp() {
        transformer = new GcpTransformer(TEST_BUCKET);
    }

    /**
     * Helper method to get temporaryHold value from BlobInfo using reflection.
     * GCP's BlobInfo doesn't expose getTemporaryHold() method, so we use reflection
     * to access the builder's internal state.
     */
    private Boolean getTemporaryHold(BlobInfo blobInfo) {
        try {
            // Try direct method first
            Method method = blobInfo.getClass().getMethod("getTemporaryHold");
            return (Boolean) method.invoke(blobInfo);
        } catch (NoSuchMethodException e) {
            // Method doesn't exist, try to get from toBuilder() and access builder's state
            try {
                Method toBuilderMethod = blobInfo.getClass().getMethod("toBuilder");
                Object builder = toBuilderMethod.invoke(blobInfo);
                // Try to get the field directly from builder using reflection
                java.lang.reflect.Field field = builder.getClass().getDeclaredField("temporaryHold");
                field.setAccessible(true);
                return (Boolean) field.get(builder);
            } catch (Exception ex) {
                // If all else fails, return null (holds not set)
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Helper method to get eventBasedHold value from BlobInfo using reflection.
     * GCP's BlobInfo doesn't expose getEventBasedHold() method, so we use reflection
     * to access the builder's internal state.
     */
    private Boolean getEventBasedHold(BlobInfo blobInfo) {
        try {
            // Try direct method first
            Method method = blobInfo.getClass().getMethod("getEventBasedHold");
            return (Boolean) method.invoke(blobInfo);
        } catch (NoSuchMethodException e) {
            // Method doesn't exist, try to get from toBuilder() and access builder's state
            try {
                Method toBuilderMethod = blobInfo.getClass().getMethod("toBuilder");
                Object builder = toBuilderMethod.invoke(blobInfo);
                // Try to get the field directly from builder using reflection
                java.lang.reflect.Field field = builder.getClass().getDeclaredField("eventBasedHold");
                field.setAccessible(true);
                return (Boolean) field.get(builder);
            } catch (Exception ex) {
                // If all else fails, return null (holds not set)
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Helper method to get retention from BlobInfo.
     */
    private com.google.cloud.storage.BlobInfo.Retention getRetention(BlobInfo blobInfo) {
        try {
            Method method = blobInfo.getClass().getMethod("getRetention");
            return (com.google.cloud.storage.BlobInfo.Retention) method.invoke(blobInfo);
        } catch (Exception e) {
            return null;
        }
    }

    @Test
    void testGetBucket() {
        assertEquals(TEST_BUCKET, transformer.getBucket());
    }

    @Test
    void testToBlobInfo_WithMetadataAndNoTags() {
        // Given
        Map<String, String> metadata = new HashMap<>();
        metadata.put("content-type", "application/json");
        metadata.put("custom-header", "custom-value");

        Map<String, String> tags = new HashMap<>();

        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .withMetadata(metadata)
                .withTags(tags)
                .build();

        // When
        BlobInfo blobInfo = transformer.toBlobInfo(uploadRequest);

        // Then
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(TEST_KEY, blobInfo.getName());
        assertNotNull(blobInfo.getMetadata());
        assertEquals("application/json", blobInfo.getMetadata().get("content-type"));
        assertEquals("custom-value", blobInfo.getMetadata().get("custom-header"));
    }

    @Test
    void testToBlobInfo_WithMetadataAndTags() {
        // Given
        Map<String, String> metadata = new HashMap<>();
        metadata.put("content-type", "application/json");
        metadata.put("custom-header", "custom-value");

        Map<String, String> tags = new HashMap<>();
        tags.put("environment", "production");
        tags.put("owner", "team-a");

        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .withMetadata(metadata)
                .withTags(tags)
                .build();

        // When
        BlobInfo blobInfo = transformer.toBlobInfo(uploadRequest);

        // Then
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(TEST_KEY, blobInfo.getName());
        assertNotNull(blobInfo.getMetadata());
        
        // Verify original metadata is preserved
        assertEquals("application/json", blobInfo.getMetadata().get("content-type"));
        assertEquals("custom-value", blobInfo.getMetadata().get("custom-header"));
        
        // Verify tags are added with TAG_PREFIX
        assertEquals("production", blobInfo.getMetadata().get("gcp-tag-environment"));
        assertEquals("team-a", blobInfo.getMetadata().get("gcp-tag-owner"));
    }

    @Test
    void testToBlobInfo_WithEmptyMetadataAndTags() {
        // Given
        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .withMetadata(new HashMap<>())
                .withTags(new HashMap<>())
                .build();

        // When
        BlobInfo blobInfo = transformer.toBlobInfo(uploadRequest);

        // Then
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(TEST_KEY, blobInfo.getName());
        assertNotNull(blobInfo.getMetadata());
        assertTrue(blobInfo.getMetadata().isEmpty());
    }

    @Test
    void testToBlobInfo_WithNullMetadataAndTags() {
        // Given
        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .build();

        // When
        BlobInfo blobInfo = transformer.toBlobInfo(uploadRequest);

        // Then
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(TEST_KEY, blobInfo.getName());
        assertNotNull(blobInfo.getMetadata());
        assertTrue(blobInfo.getMetadata().isEmpty());
    }

    @Test
    void testToBlobInfo_WithTagsOnly() {
        // Given
        Map<String, String> tags = new HashMap<>();
        tags.put("environment", "production");
        tags.put("owner", "team-a");

        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .withTags(tags)
                .build();

        // When
        BlobInfo blobInfo = transformer.toBlobInfo(uploadRequest);

        // Then
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(TEST_KEY, blobInfo.getName());
        assertNotNull(blobInfo.getMetadata());
        
        // Verify tags are added with TAG_PREFIX
        assertEquals("production", blobInfo.getMetadata().get("gcp-tag-environment"));
        assertEquals("team-a", blobInfo.getMetadata().get("gcp-tag-owner"));
        assertEquals(2, blobInfo.getMetadata().size());
    }

    @Test
    void testGetKmsTargetOptions_WithKmsKey() {
        // Given
        String kmsKeyId = "projects/my-project/locations/us-east1/keyRings/my-ring/cryptoKeys/my-key";
        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .withKmsKeyId(kmsKeyId)
                .build();

        // When
        Storage.BlobTargetOption[] options = transformer.getKmsTargetOptions(uploadRequest);

        // Then
        assertEquals(1, options.length);
        assertEquals(Storage.BlobTargetOption.kmsKeyName(kmsKeyId), options[0]);
    }

    @Test
    void testGetKmsTargetOptions_WithoutKmsKey() {
        // Given
        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .build();

        // When
        Storage.BlobTargetOption[] options = transformer.getKmsTargetOptions(uploadRequest);

        // Then
        assertEquals(0, options.length);
    }

    @Test
    void testGetKmsTargetOptions_WithEmptyKmsKey() {
        // Given
        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .withKmsKeyId("")
                .build();

        // When
        Storage.BlobTargetOption[] options = transformer.getKmsTargetOptions(uploadRequest);

        // Then
        assertEquals(0, options.length);
    }

    @Test
    void testGetKmsWriteOptions_WithKmsKey() {
        // Given
        String kmsKeyId = "projects/my-project/locations/us-east1/keyRings/my-ring/cryptoKeys/my-key";
        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .withKmsKeyId(kmsKeyId)
                .build();

        // When
        Storage.BlobWriteOption[] options = transformer.getKmsWriteOptions(uploadRequest);

        // Then
        assertEquals(1, options.length);
        assertEquals(Storage.BlobWriteOption.kmsKeyName(kmsKeyId), options[0]);
    }

    @Test
    void testGetKmsWriteOptions_WithoutKmsKey() {
        // Given
        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .build();

        // When
        Storage.BlobWriteOption[] options = transformer.getKmsWriteOptions(uploadRequest);

        // Then
        assertEquals(0, options.length);
    }

    @Test
    void testGetKmsWriteOptions_WithEmptyKmsKey() {
        // Given
        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .withKmsKeyId("")
                .build();

        // When
        Storage.BlobWriteOption[] options = transformer.getKmsWriteOptions(uploadRequest);

        // Then
        assertEquals(0, options.length);
    }

    @Test
    void testToUploadResponse_WithAllFields() {
        // Given
        when(mockBlob.getName()).thenReturn(TEST_KEY);
        when(mockBlob.getGeneration()).thenReturn(TEST_GENERATION);
        when(mockBlob.getEtag()).thenReturn(TEST_ETAG);

        // When
        UploadResponse response = transformer.toUploadResponse(mockBlob);

        // Then
        assertEquals(TEST_KEY, response.getKey());
        assertEquals(TEST_VERSION_ID, response.getVersionId());
        assertEquals(TEST_ETAG, response.getETag());
    }

    @Test
    void testToUploadResponse_WithNullGeneration() {
        // Given
        when(mockBlob.getName()).thenReturn(TEST_KEY);
        when(mockBlob.getGeneration()).thenReturn(null);
        when(mockBlob.getEtag()).thenReturn(TEST_ETAG);

        // When
        UploadResponse response = transformer.toUploadResponse(mockBlob);

        // Then
        assertEquals(TEST_KEY, response.getKey());
        assertNull(response.getVersionId());
        assertEquals(TEST_ETAG, response.getETag());
    }

    @Test
    void testToBlobId_WithVersionId() {
        // Given
        DownloadRequest downloadRequest = DownloadRequest.builder()
                .withKey(TEST_KEY)
                .withVersionId(TEST_VERSION_ID)
                .build();

        // When
        BlobId blobId = transformer.toBlobId(downloadRequest);

        // Then
        assertEquals(TEST_BUCKET, blobId.getBucket());
        assertEquals(TEST_KEY, blobId.getName());
        assertEquals(TEST_GENERATION, blobId.getGeneration());
    }

    @Test
    void testToBlobId_WithoutVersionId() {
        // Given
        DownloadRequest downloadRequest = DownloadRequest.builder()
                .withKey(TEST_KEY)
                .build();

        // When
        BlobId blobId = transformer.toBlobId(downloadRequest);

        // Then
        assertEquals(TEST_BUCKET, blobId.getBucket());
        assertEquals(TEST_KEY, blobId.getName());
        assertNull(blobId.getGeneration());
    }

    @Test
    void testToBlobId_WithKeyAndVersionId() {
        // When
        BlobId blobId = transformer.toBlobId(TEST_BUCKET, TEST_KEY, TEST_VERSION_ID);

        // Then
        assertEquals(TEST_BUCKET, blobId.getBucket());
        assertEquals(TEST_KEY, blobId.getName());
        assertEquals(TEST_GENERATION, blobId.getGeneration());
    }

    @Test
    void testToBlobId_WithKeyAndNullVersionId() {
        // When
        BlobId blobId = transformer.toBlobId(TEST_BUCKET, TEST_KEY, null);

        // Then
        assertEquals(TEST_BUCKET, blobId.getBucket());
        assertEquals(TEST_KEY, blobId.getName());
        assertNull(blobId.getGeneration());
    }

    @Test
    void testComputeRange() {
        Pair<Long, Long> result = transformer.computeRange(null, null, 2000L);
        assertNull(result.getLeft());
        assertNull(result.getRight());

        result = transformer.computeRange(0L, 500L, 2000L);
        assertEquals(result.getLeft(), 0);
        assertEquals(result.getRight(), 501);

        result = transformer.computeRange(100L, 600L, 2000L);
        assertEquals(result.getLeft(), 100);
        assertEquals(result.getRight(), 601);

        result = transformer.computeRange(null, 500L, 2000L);
        assertEquals(result.getLeft(), 1500);
        assertEquals(result.getRight(), 2001);

        result = transformer.computeRange(500L, null, 2000L);
        assertEquals(result.getLeft(), 500);
        assertNull(result.getRight());
    }

    @Test
    void testToGenerationId_WithValidVersionId() {
        // When
        Long generationId = transformer.toGenerationId(TEST_VERSION_ID);

        // Then
        assertEquals(TEST_GENERATION, generationId);
    }

    @Test
    void testToGenerationId_WithNullVersionId() {
        // When
        Long generationId = transformer.toGenerationId(null);

        // Then
        assertNull(generationId);
    }

    @Test
    void testToGenerationId_WithLargeNumber() {
        // Given
        String largeVersionId = "9223372036854775807"; // Long.MAX_VALUE

        // When
        Long generationId = transformer.toGenerationId(largeVersionId);

        // Then
        assertEquals(Long.MAX_VALUE, generationId);
    }

    @Test
    void testToDownloadResponse_WithAllFields() {
        // Given
        Map<String, String> metadata = new HashMap<>();
        metadata.put("content-type", "application/json");
        metadata.put("custom-header", "custom-value"); // Non-tag metadata that should be included
        
        OffsetDateTime updateTime = OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC);
        
        when(mockBlob.getName()).thenReturn(TEST_KEY);
        when(mockBlob.getGeneration()).thenReturn(TEST_GENERATION);
        when(mockBlob.getEtag()).thenReturn(TEST_ETAG);
        when(mockBlob.getSize()).thenReturn(TEST_SIZE);
        when(mockBlob.getMetadata()).thenReturn(metadata);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(updateTime);

        // When
        DownloadResponse response = transformer.toDownloadResponse(mockBlob);

        // Then
        assertEquals(TEST_KEY, response.getKey());
        assertNotNull(response.getMetadata());
        assertEquals(TEST_KEY, response.getMetadata().getKey());
        assertEquals(TEST_VERSION_ID, response.getMetadata().getVersionId());
        assertEquals(TEST_ETAG, response.getMetadata().getETag());
        assertEquals(TEST_SIZE, response.getMetadata().getObjectSize());
        
        // Only non-tag metadata should be included
        Map<String, String> expectedMetadata = new HashMap<>();
        expectedMetadata.put("content-type", "application/json");
        expectedMetadata.put("custom-header", "custom-value");
        assertEquals(expectedMetadata, response.getMetadata().getMetadata());
        
        assertEquals(updateTime.toInstant(), response.getMetadata().getLastModified());
    }

    @Test
    void testToDownloadResponseInputStream_WithAllFields() {
        // Given
        Map<String, String> metadata = new HashMap<>();
        metadata.put("content-type", "application/json");
        metadata.put("custom-header", "custom-value"); // Non-tag metadata that should be included

        OffsetDateTime updateTime = OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC);

        when(mockBlob.getName()).thenReturn(TEST_KEY);
        when(mockBlob.getGeneration()).thenReturn(TEST_GENERATION);
        when(mockBlob.getEtag()).thenReturn(TEST_ETAG);
        when(mockBlob.getSize()).thenReturn(TEST_SIZE);
        when(mockBlob.getMetadata()).thenReturn(metadata);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(updateTime);

        DownloadResponse response = transformer.toDownloadResponse(mockBlob, new InputStream() {
            @Override
            public int read() throws IOException {
                return 0;
            }
        });

        assertEquals(TEST_KEY, response.getKey());
        assertNotNull(response.getMetadata());
        assertEquals(TEST_KEY, response.getMetadata().getKey());
        assertEquals(TEST_VERSION_ID, response.getMetadata().getVersionId());
        assertEquals(TEST_ETAG, response.getMetadata().getETag());
        assertEquals(TEST_SIZE, response.getMetadata().getObjectSize());
        assertNotNull(response.getInputStream());
    }

    @Test
    void testToDownloadResponse_WithNullFields() {
        // Given
        when(mockBlob.getName()).thenReturn(TEST_KEY);
        when(mockBlob.getGeneration()).thenReturn(null);
        when(mockBlob.getEtag()).thenReturn(null);
        when(mockBlob.getMetadata()).thenReturn(null);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(null);

        // When
        DownloadResponse response = transformer.toDownloadResponse(mockBlob);

        // Then
        assertEquals(TEST_KEY, response.getKey());
        assertNotNull(response.getMetadata());
        assertEquals(TEST_KEY, response.getMetadata().getKey());
        assertNull(response.getMetadata().getVersionId());
        assertNull(response.getMetadata().getETag());
        assertTrue(response.getMetadata().getMetadata().isEmpty());
        assertNull(response.getMetadata().getLastModified());
    }

    @Test
    void testToBlobMetadata_WithAllFields() {
        // Given
        Map<String, String> metadata = new HashMap<>();
        metadata.put("content-type", "application/json");
        metadata.put("custom-header", "custom-value"); // Non-tag metadata that should be included
        
        OffsetDateTime updateTime = OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC);
        
        when(mockBlob.getName()).thenReturn(TEST_KEY);
        when(mockBlob.getGeneration()).thenReturn(TEST_GENERATION);
        when(mockBlob.getEtag()).thenReturn(TEST_ETAG);
        when(mockBlob.getSize()).thenReturn(TEST_SIZE);
        when(mockBlob.getMetadata()).thenReturn(metadata);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(updateTime);
        when(mockBlob.getMd5()).thenReturn("5d41402abc4b2a76b9719d911017c592");

        // When
        BlobMetadata blobMetadata = transformer.toBlobMetadata(mockBlob);

        // Then
        assertEquals(TEST_KEY, blobMetadata.getKey());
        assertEquals(TEST_VERSION_ID, blobMetadata.getVersionId());
        assertEquals(TEST_ETAG, blobMetadata.getETag());
        assertEquals(TEST_SIZE, blobMetadata.getObjectSize());

        byte[] expectedMd5 = {93, 65, 64, 42, -68, 75, 42, 118, -71, 113, -99, -111, 16, 23, -59, -110};
        assertArrayEquals(expectedMd5, blobMetadata.getMd5());
        
        // Only non-tag metadata should be included
        Map<String, String> expectedMetadata = new HashMap<>();
        expectedMetadata.put("content-type", "application/json");
        expectedMetadata.put("custom-header", "custom-value");
        assertEquals(expectedMetadata, blobMetadata.getMetadata());
        
        assertEquals(updateTime.toInstant(), blobMetadata.getLastModified());
    }

    @Test
    void testToBlobMetadata_WithNullFields() {
        // Given
        when(mockBlob.getName()).thenReturn(TEST_KEY);
        when(mockBlob.getGeneration()).thenReturn(null);
        when(mockBlob.getEtag()).thenReturn(null);
        when(mockBlob.getMetadata()).thenReturn(null);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(null);
        when(mockBlob.getMd5()).thenReturn(null);

        // When
        BlobMetadata blobMetadata = transformer.toBlobMetadata(mockBlob);

        // Then
        assertEquals(TEST_KEY, blobMetadata.getKey());
        assertNull(blobMetadata.getVersionId());
        assertNull(blobMetadata.getETag());
        assertTrue(blobMetadata.getMetadata().isEmpty());
        assertNull(blobMetadata.getLastModified());
        assertArrayEquals(new byte[0], blobMetadata.getMd5());
    }

    @Test
    void testToCopyRequest_WithVersionId() {
        // Given
        CopyRequest copyRequest = CopyRequest.builder()
                .srcKey("source-key")
                .srcVersionId(TEST_VERSION_ID)
                .destBucket(TEST_DEST_BUCKET)
                .destKey("dest-key")
                .build();

        // When
        Storage.CopyRequest gcpCopyRequest = transformer.toCopyRequest(copyRequest);

        // Then
        assertNotNull(gcpCopyRequest);
        assertEquals(TEST_BUCKET, gcpCopyRequest.getSource().getBucket());
        assertEquals("source-key", gcpCopyRequest.getSource().getName());
        assertEquals(TEST_GENERATION, gcpCopyRequest.getSource().getGeneration());
        assertEquals(TEST_DEST_BUCKET, gcpCopyRequest.getTarget().getBucket());
        assertEquals("dest-key", gcpCopyRequest.getTarget().getName());
        assertNull(gcpCopyRequest.getTarget().getGeneration());
    }

    @Test
    void testToCopyRequest_WithoutVersionId() {
        // Given
        CopyRequest copyRequest = CopyRequest.builder()
                .srcKey("source-key")
                .destBucket(TEST_DEST_BUCKET)
                .destKey("dest-key")
                .build();

        // When
        Storage.CopyRequest gcpCopyRequest = transformer.toCopyRequest(copyRequest);

        // Then
        assertNotNull(gcpCopyRequest);
        assertEquals(TEST_BUCKET, gcpCopyRequest.getSource().getBucket());
        assertEquals("source-key", gcpCopyRequest.getSource().getName());
        assertNull(gcpCopyRequest.getSource().getGeneration());
        assertEquals(TEST_DEST_BUCKET, gcpCopyRequest.getTarget().getBucket());
        assertEquals("dest-key", gcpCopyRequest.getTarget().getName());
        assertNull(gcpCopyRequest.getTarget().getGeneration());
    }

    @Test
    void testToCopyResponse_WithAllFields() {
        // Given
        OffsetDateTime updateTime = OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC);
        
        when(mockBlob.getName()).thenReturn(TEST_KEY);
        when(mockBlob.getGeneration()).thenReturn(TEST_GENERATION);
        when(mockBlob.getEtag()).thenReturn(TEST_ETAG);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(updateTime);

        // When
        CopyResponse response = transformer.toCopyResponse(mockBlob);

        // Then
        assertEquals(TEST_KEY, response.getKey());
        assertEquals(TEST_VERSION_ID, response.getVersionId());
        assertEquals(TEST_ETAG, response.getETag());
        assertEquals(updateTime.toInstant(), response.getLastModified());
    }

    @Test
    void testToCopyResponse_WithNullFields() {
        // Given
        when(mockBlob.getName()).thenReturn(TEST_KEY);
        when(mockBlob.getGeneration()).thenReturn(null);
        when(mockBlob.getEtag()).thenReturn(null);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(null);

        // When & Then
        assertThrows(NullPointerException.class, () -> {
            transformer.toCopyResponse(mockBlob);
        });
    }

    @Test
    void testPresignedToBlobInfo() {
        Map<String, String> metadata = Map.of("some-key", "some-value");
        PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.UPLOAD)
                .key(TEST_KEY)
                .duration(Duration.ofHours(4))
                .metadata(metadata)
                .build();

        var blobInfo = transformer.toBlobInfo(presignedUrlRequest);
        assertNotNull(blobInfo);
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(TEST_KEY, blobInfo.getName());
        assertEquals(metadata, blobInfo.getMetadata());
    }

    @Test
    void testPresignedToBlobInfo_WithTags() {
        Map<String, String> metadata = Map.of("some-key", "some-value");
        Map<String, String> tags = Map.of("tag-key", "tag-value");
        PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.UPLOAD)
                .key("object-1")
                .duration(Duration.ofHours(4))
                .metadata(metadata)
                .tags(tags)
                .build();

        // When
        BlobInfo blobInfo = transformer.toBlobInfo(presignedUrlRequest);

        // Then
        assertNotNull(blobInfo);
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals("object-1", blobInfo.getName());
        
        // Verify original metadata is preserved
        assertEquals("some-value", blobInfo.getMetadata().get("some-key"));
        
        // Verify tags are added with TAG_PREFIX
        assertEquals("tag-value", blobInfo.getMetadata().get("gcp-tag-tag-key"));
    }

    @Test
    void testPresignedToBlobInfo_WithTagsOnly() {
        Map<String, String> tags = Map.of("tag-key", "tag-value", "environment", "production");
        PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.UPLOAD)
                .key("object-1")
                .duration(Duration.ofHours(4))
                .tags(tags)
                .build();

        // When
        BlobInfo blobInfo = transformer.toBlobInfo(presignedUrlRequest);

        // Then
        assertNotNull(blobInfo);
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals("object-1", blobInfo.getName());
        
        // Verify tags are added with TAG_PREFIX
        assertEquals("tag-value", blobInfo.getMetadata().get("gcp-tag-tag-key"));
        assertEquals("production", blobInfo.getMetadata().get("gcp-tag-environment"));
        assertEquals(2, blobInfo.getMetadata().size());
    }

    @Test
    void testToBlobListOptions() {
        ListBlobsPageRequest request = ListBlobsPageRequest
                .builder()
                .withDelimiter(":")
                .withPrefix("some/prefix/path/thingie")
                .withPaginationToken("next-token")
                .withMaxResults(100)
                .build();

        Storage.BlobListOption[] actual = transformer.toBlobListOptions(request);
        
        assertEquals(4, actual.length);
    }

    @Test
    void testToBlobListOptions_WithNullValues() {
        ListBlobsPageRequest request = ListBlobsPageRequest.builder().build();

        Storage.BlobListOption[] actual = transformer.toBlobListOptions(request);
        
        assertEquals(0, actual.length);
    }

    @Test
    public void testToBlobInfo() {
        Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
        BlobInfo blobInfo = transformer.toBlobInfo(TEST_KEY, metadata);
        assertEquals(TEST_KEY, blobInfo.getName());
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(metadata, blobInfo.getMetadata());

        blobInfo = transformer.toBlobInfo(TEST_KEY, new HashMap<>());
        assertEquals(TEST_KEY, blobInfo.getName());
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(0, blobInfo.getMetadata().size());

        blobInfo = transformer.toBlobInfo(TEST_KEY, null);
        assertEquals(TEST_KEY, blobInfo.getName());
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(0, blobInfo.getMetadata().size());
    }

    @Test
    public void testToBlobInfo_multipartUpload() {
        MultipartUpload mpu = MultipartUpload.builder()
                .bucket(TEST_BUCKET)
                .key(TEST_KEY)
                .id("uuid1")
                .build();
        BlobInfo blobInfo = transformer.toBlobInfo(mpu);
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(TEST_KEY, blobInfo.getName());
        assertEquals(0, blobInfo.getMetadata().size());
    }

    @Test
    public void testToBlobInfo_multipartUploadWithTags() {
        Map<String, String> tags = Map.of("tag1", "value1", "tag2", "value2");
        MultipartUpload mpu = MultipartUpload.builder()
                .bucket(TEST_BUCKET)
                .key(TEST_KEY)
                .id("uuid1")
                .tags(tags)
                .build();
        BlobInfo blobInfo = transformer.toBlobInfo(mpu);
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(TEST_KEY, blobInfo.getName());
        assertEquals(2, blobInfo.getMetadata().size());
        assertEquals("value1", blobInfo.getMetadata().get("gcp-tag-tag1"));
        assertEquals("value2", blobInfo.getMetadata().get("gcp-tag-tag2"));
    }

    @Test
    public void testToBlobInfo_multipartUploadRequest() {
        MultipartUploadRequest request = new MultipartUploadRequest.Builder()
                .withKey(TEST_KEY)
                .build();
        BlobInfo blobInfo = transformer.toBlobInfo(request);
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(TEST_KEY, blobInfo.getName());
        assertNotNull(blobInfo.getMetadata());
        assertTrue(blobInfo.getMetadata().isEmpty());
    }

    @Test
    public void testToBlobInfo_multipartUploadRequestWithMetadataAndTags() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("content-type", "application/json");
        metadata.put("custom-header", "custom-value");

        Map<String, String> tags = new HashMap<>();
        tags.put("environment", "production");
        tags.put("owner", "team-a");

        MultipartUploadRequest request = new MultipartUploadRequest.Builder()
                .withKey(TEST_KEY)
                .withMetadata(metadata)
                .withTags(tags)
                .build();

        BlobInfo blobInfo = transformer.toBlobInfo(request);

        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(TEST_KEY, blobInfo.getName());
        assertNotNull(blobInfo.getMetadata());

        // Verify original metadata is preserved
        assertEquals("application/json", blobInfo.getMetadata().get("content-type"));
        assertEquals("custom-value", blobInfo.getMetadata().get("custom-header"));

        // Verify tags are added with TAG_PREFIX
        assertEquals("production", blobInfo.getMetadata().get("gcp-tag-environment"));
        assertEquals("team-a", blobInfo.getMetadata().get("gcp-tag-owner"));
    }

    @Test
    public void testToBlobInfo_multipartUploadRequestWithTagsOnly() {
        Map<String, String> tags = new HashMap<>();
        tags.put("environment", "production");
        tags.put("owner", "team-a");

        MultipartUploadRequest request = new MultipartUploadRequest.Builder()
                .withKey(TEST_KEY)
                .withTags(tags)
                .build();

        BlobInfo blobInfo = transformer.toBlobInfo(request);

        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(TEST_KEY, blobInfo.getName());
        assertNotNull(blobInfo.getMetadata());

        // Verify tags are added with TAG_PREFIX
        assertEquals("production", blobInfo.getMetadata().get("gcp-tag-environment"));
        assertEquals("team-a", blobInfo.getMetadata().get("gcp-tag-owner"));
        assertEquals(2, blobInfo.getMetadata().size());
    }

    @Test
    public void testToFilePaths_UploadRequest() throws IOException {
        // Create a temporary directory structure
        Path tempDir = Files.createTempDirectory("test-upload");
        Path file1 = tempDir.resolve("file1.txt");
        Path file2 = tempDir.resolve("subdir/file2.txt");
        Files.createDirectories(file2.getParent());
        Files.write(file1, "content1".getBytes());
        Files.write(file2, "content2".getBytes());

        try {
            DirectoryUploadRequest request = DirectoryUploadRequest.builder()
                    .localSourceDirectory(tempDir.toString())
                    .prefix("uploads/")
                    .includeSubFolders(true)
                    .build();

            List<Path> filePaths = transformer.toFilePaths(request);

            assertEquals(2, filePaths.size());
            assertTrue(filePaths.contains(file1));
            assertTrue(filePaths.contains(file2));
        } finally {
            // Clean up
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    public void testToFilePaths_UploadRequest_NoSubFolders() throws IOException {
        // Create a temporary directory structure
        Path tempDir = Files.createTempDirectory("test-upload");
        Path file1 = tempDir.resolve("file1.txt");
        Path file2 = tempDir.resolve("subdir/file2.txt");
        Files.createDirectories(file2.getParent());
        Files.write(file1, "content1".getBytes());
        Files.write(file2, "content2".getBytes());

        try {
            DirectoryUploadRequest request = DirectoryUploadRequest.builder()
                    .localSourceDirectory(tempDir.toString())
                    .prefix("uploads/")
                    .includeSubFolders(false)
                    .build();

            List<Path> filePaths = transformer.toFilePaths(request);

            assertEquals(1, filePaths.size());
            assertTrue(filePaths.contains(file1));
            assertFalse(filePaths.contains(file2));
        } finally {
            // Clean up
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    public void testToBlobKey() {
        Path sourceDir = Paths.get("/source");
        Path filePath = Paths.get("/source/subdir/file.txt");
        String prefix = "uploads/";

        String blobKey = transformer.toBlobKey(sourceDir, filePath, prefix);

        assertEquals("uploads/subdir/file.txt", blobKey);
    }

    @Test
    public void testToBlobKey_NoPrefix() {
        Path sourceDir = Paths.get("/source");
        Path filePath = Paths.get("/source/subdir/file.txt");
        String prefix = null;

        String blobKey = transformer.toBlobKey(sourceDir, filePath, prefix);

        assertEquals("subdir/file.txt", blobKey);
    }



    @Test
    public void testComputeRange_EdgeCases() {
        // Test with null start and end
        Pair<Long, Long> range1 = transformer.computeRange(null, null, 1000L);
        assertNull(range1.getLeft());
        assertNull(range1.getRight());

        // Test with start = 0, end = null
        Pair<Long, Long> range2 = transformer.computeRange(0L, null, 1000L);
        assertEquals(0L, range2.getLeft().longValue());
        assertNull(range2.getRight());

        // Test with start = null, end = 500
        // When start is null and end is not null, it calculates start = fileSize - end
        Pair<Long, Long> range3 = transformer.computeRange(null, 500L, 1000L);
        assertEquals(500L, range3.getLeft().longValue()); // fileSize - end = 1000 - 500 = 500
        assertEquals(1001L, range3.getRight().longValue()); // fileSize + 1 = 1000 + 1 = 1001

        // Test with start = 100, end = 200
        Pair<Long, Long> range4 = transformer.computeRange(100L, 200L, 1000L);
        assertEquals(100L, range4.getLeft().longValue());
        assertEquals(201L, range4.getRight().longValue());
    }

    @Test
    public void testPartitionList_EmptyList() {
        List<com.salesforce.multicloudj.blob.driver.BlobInfo> emptyList = new ArrayList<>();
        List<List<com.salesforce.multicloudj.blob.driver.BlobInfo>> result = transformer.partitionList(emptyList, 10);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testPartitionList_SingleElement() {
        List<com.salesforce.multicloudj.blob.driver.BlobInfo> singleList = new ArrayList<>();
        singleList.add(com.salesforce.multicloudj.blob.driver.BlobInfo.builder()
            .withKey("test-key")
            .withObjectSize(100L)
            .build());
        
        List<List<com.salesforce.multicloudj.blob.driver.BlobInfo>> result = transformer.partitionList(singleList, 10);
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).size());
        assertEquals("test-key", result.get(0).get(0).getKey());
    }

    @Test
    public void testPartitionList_ExactPartitionSize() {
        List<com.salesforce.multicloudj.blob.driver.BlobInfo> list = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            list.add(com.salesforce.multicloudj.blob.driver.BlobInfo.builder()
                .withKey("key-" + i)
                .withObjectSize(100L)
                .build());
        }
        
        List<List<com.salesforce.multicloudj.blob.driver.BlobInfo>> result = transformer.partitionList(list, 10);
        assertEquals(2, result.size());
        assertEquals(10, result.get(0).size());
        assertEquals(10, result.get(1).size());
    }

    @Test
    public void testPartitionList_RemainderElements() {
        List<com.salesforce.multicloudj.blob.driver.BlobInfo> list = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            list.add(com.salesforce.multicloudj.blob.driver.BlobInfo.builder()
                .withKey("key-" + i)
                .withObjectSize(100L)
                .build());
        }
        
        List<List<com.salesforce.multicloudj.blob.driver.BlobInfo>> result = transformer.partitionList(list, 10);
        assertEquals(3, result.size());
        assertEquals(10, result.get(0).size());
        assertEquals(10, result.get(1).size());
        assertEquals(5, result.get(2).size());
    }

    @Test
    public void testToBlobIdentifiers_EmptyList() {
        List<com.salesforce.multicloudj.blob.driver.BlobInfo> emptyList = new ArrayList<>();
        List<com.salesforce.multicloudj.blob.driver.BlobIdentifier> result = transformer.toBlobIdentifiers(emptyList);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testToBlobIdentifiers_SingleElement() {
        List<com.salesforce.multicloudj.blob.driver.BlobInfo> list = new ArrayList<>();
        list.add(com.salesforce.multicloudj.blob.driver.BlobInfo.builder()
            .withKey("test-key")
            .withObjectSize(100L)
            .build());
        
        List<com.salesforce.multicloudj.blob.driver.BlobIdentifier> result = transformer.toBlobIdentifiers(list);
        assertEquals(1, result.size());
        assertEquals("test-key", result.get(0).getKey());
        assertNull(result.get(0).getVersionId());
    }

    @Test
    public void testToBlobIdentifiers_MultipleElements() {
        List<com.salesforce.multicloudj.blob.driver.BlobInfo> list = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            list.add(com.salesforce.multicloudj.blob.driver.BlobInfo.builder()
                .withKey("key-" + i)
                .withObjectSize(100L)
                .build());
        }
        
        List<com.salesforce.multicloudj.blob.driver.BlobIdentifier> result = transformer.toBlobIdentifiers(list);
        assertEquals(5, result.size());
        for (int i = 0; i < 5; i++) {
            assertEquals("key-" + i, result.get(i).getKey());
            assertNull(result.get(i).getVersionId());
        }
    }

    @Test
    public void testToFilePaths_IOException() throws IOException {
        // Create a temporary directory that we'll delete to cause IOException
        Path tempDir = Files.createTempDirectory("test-dir");
        Files.delete(tempDir); // Delete the directory to cause IOException
        
        DirectoryUploadRequest request = DirectoryUploadRequest.builder()
            .localSourceDirectory(tempDir.toString())
            .prefix("test/")
            .includeSubFolders(true)
            .build();
        
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            transformer.toFilePaths(request);
        });
        
        assertTrue(exception.getMessage().contains("Failed to traverse directory"));
        assertTrue(exception.getCause() instanceof IOException);
    }

    @Test
    public void testToBlobKey_WindowsPathSeparator() {
        // Use actual Windows-style paths that exist on the filesystem
        Path sourceDir = Paths.get("source", "dir");
        Path filePath = Paths.get("source", "dir", "subdir", "file.txt");
        String prefix = "uploads/";
        
        String result = transformer.toBlobKey(sourceDir, filePath, prefix);
        assertEquals("uploads/subdir/file.txt", result);
    }

    @Test
    public void testToBlobKey_UnixPathSeparator() {
        Path sourceDir = Paths.get("/source/dir");
        Path filePath = Paths.get("/source/dir/subdir/file.txt");
        String prefix = "uploads/";
        
        String result = transformer.toBlobKey(sourceDir, filePath, prefix);
        assertEquals("uploads/subdir/file.txt", result);
    }

    @Test
    public void testToBlobKey_EmptyPrefix() {
        Path sourceDir = Paths.get("/source/dir");
        Path filePath = Paths.get("/source/dir/file.txt");
        String prefix = "";
        
        String result = transformer.toBlobKey(sourceDir, filePath, prefix);
        assertEquals("file.txt", result);
    }

    @Test
    public void testToBlobKey_NullPrefix() {
        Path sourceDir = Paths.get("/source/dir");
        Path filePath = Paths.get("/source/dir/file.txt");
        String prefix = null;
        
        String result = transformer.toBlobKey(sourceDir, filePath, prefix);
        assertEquals("file.txt", result);
    }

    @Test
    public void testToBlobKey_PrefixWithoutSlash() {
        Path sourceDir = Paths.get("/source/dir");
        Path filePath = Paths.get("/source/dir/file.txt");
        String prefix = "uploads";
        
        String result = transformer.toBlobKey(sourceDir, filePath, prefix);
        assertEquals("uploads/file.txt", result);
    }

    @Test
    public void testToBlobKey_PrefixWithSlash() {
        Path sourceDir = Paths.get("/source/dir");
        Path filePath = Paths.get("/source/dir/file.txt");
        String prefix = "uploads/";
        
        String result = transformer.toBlobKey(sourceDir, filePath, prefix);
        assertEquals("uploads/file.txt", result);
    }

    @Test
    public void testComputeRange_ZeroFileSize() {
        Pair<Long, Long> range = transformer.computeRange(0L, 0L, 0L);
        assertEquals(0L, range.getLeft().longValue());
        assertEquals(1L, range.getRight().longValue());
    }

    @Test
    public void testComputeRange_StartEqualsEnd() {
        Pair<Long, Long> range = transformer.computeRange(5L, 5L, 100L);
        assertEquals(5L, range.getLeft().longValue());
        assertEquals(6L, range.getRight().longValue());
    }

    @Test
    public void testComputeRange_EndGreaterThanFileSize() {
        Pair<Long, Long> range = transformer.computeRange(0L, 150L, 100L);
        assertEquals(0L, range.getLeft().longValue());
        assertEquals(151L, range.getRight().longValue());
    }

    @Test
    public void testComputeRange_StartGreaterThanFileSize() {
        // When start is greater than file size, should throw IllegalArgumentException
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            transformer.computeRange(150L, 200L, 100L);
        });
        assertEquals("Start of range cannot be greater than file size: 150", exception.getMessage());
    }

    @Test
    public void testToBlobInfoWithStorageClass() {
        String key = "test-key";
        Map<String, String> metadata = Map.of("key1", "value1");
        String storageClass = "NEARLINE";

        BlobInfo result = transformer.toBlobInfo(key, metadata, storageClass);

        assertEquals(TEST_BUCKET, result.getBucket());
        assertEquals(key, result.getName());
        assertEquals(metadata, result.getMetadata());
        assertEquals(com.google.cloud.storage.StorageClass.NEARLINE, result.getStorageClass());
    }

    @Test
    public void testToBlobInfoWithStandardStorageClass() {
        String key = "test-key";
        Map<String, String> metadata = Map.of("key1", "value1");
        String storageClass = "STANDARD";

        BlobInfo result = transformer.toBlobInfo(key, metadata, storageClass);

        assertEquals(com.google.cloud.storage.StorageClass.STANDARD, result.getStorageClass());
    }

    @Test
    public void testToBlobInfoWithColdlineStorageClass() {
        String key = "test-key";
        Map<String, String> metadata = Map.of("key1", "value1");
        String storageClass = "COLDLINE";

        BlobInfo result = transformer.toBlobInfo(key, metadata, storageClass);

        assertEquals(com.google.cloud.storage.StorageClass.COLDLINE, result.getStorageClass());
    }

    @Test
    public void testToBlobInfoWithArchiveStorageClass() {
        String key = "test-key";
        Map<String, String> metadata = Map.of("key1", "value1");
        String storageClass = "ARCHIVE";

        BlobInfo result = transformer.toBlobInfo(key, metadata, storageClass);

        assertEquals(com.google.cloud.storage.StorageClass.ARCHIVE, result.getStorageClass());
    }

    @Test
    public void testToBlobInfoWithNullStorageClass() {
        String key = "test-key";
        Map<String, String> metadata = Map.of("key1", "value1");
        String storageClass = null;

        BlobInfo result = transformer.toBlobInfo(key, metadata, storageClass);

        assertEquals(TEST_BUCKET, result.getBucket());
        assertEquals(key, result.getName());
        assertEquals(metadata, result.getMetadata());
        assertNull(result.getStorageClass());
    }

    @Test
    public void testToBlobInfoWithEmptyStorageClass() {
        String key = "test-key";
        Map<String, String> metadata = Map.of("key1", "value1");
        String storageClass = "";

        BlobInfo result = transformer.toBlobInfo(key, metadata, storageClass);

        assertEquals(TEST_BUCKET, result.getBucket());
        assertEquals(key, result.getName());
        assertEquals(metadata, result.getMetadata());
        assertNull(result.getStorageClass());
    }


    @Test
    public void testToBlobInfoWithCaseInsensitiveStorageClass() {
        String key = "test-key";
        Map<String, String> metadata = Map.of("key1", "value1");
        String storageClass = "nearline"; // lowercase

        BlobInfo result = transformer.toBlobInfo(key, metadata, storageClass);

        assertEquals(com.google.cloud.storage.StorageClass.NEARLINE, result.getStorageClass());
    }

    @Test
    public void testUploadRequestWithStorageClass() {
        String key = "test-key";
        Map<String, String> metadata = Map.of("key1", "value1");
        String storageClass = "NEARLINE";

        UploadRequest request = UploadRequest.builder()
                .withKey(key)
                .withMetadata(metadata)
                .withStorageClass(storageClass)
                .build();

        BlobInfo result = transformer.toBlobInfo(request);

        assertEquals(TEST_BUCKET, result.getBucket());
        assertEquals(key, result.getName());
        assertEquals(metadata, result.getMetadata());
        assertEquals(com.google.cloud.storage.StorageClass.NEARLINE, result.getStorageClass());
    }

    @Test
    public void testUploadRequestWithoutStorageClass() {
        String key = "test-key";
        Map<String, String> metadata = Map.of("key1", "value1");

        UploadRequest request = UploadRequest.builder()
                .withKey(key)
                .withMetadata(metadata)
                .build();

        BlobInfo result = transformer.toBlobInfo(request);

        assertEquals(TEST_BUCKET, result.getBucket());
        assertEquals(key, result.getName());
        assertEquals(metadata, result.getMetadata());
        assertNull(result.getStorageClass());
    }

    @Test
    public void testToBlobInfo_WithObjectLockTemporaryHold() {
        // Given
        ObjectLockConfiguration lockConfig = ObjectLockConfiguration.builder()
                .legalHold(true)
                .useEventBasedHold(false)
                .build();

        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .withObjectLock(lockConfig)
                .build();

        // When
        BlobInfo blobInfo = transformer.toBlobInfo(uploadRequest);

        // Then
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(TEST_KEY, blobInfo.getName());
        Boolean tempHold = getTemporaryHold(blobInfo);
        Boolean eventHold = getEventBasedHold(blobInfo);
        assertTrue(tempHold != null && tempHold, "Temporary hold should be set to true");
        assertTrue(eventHold == null || !eventHold, "Event-based hold should be false or null");
        // No retention should be set when retainUntilDate is not provided
        assertNull(getRetention(blobInfo), "Retention should be null when retainUntilDate is not provided");
    }

    @Test
    public void testToBlobInfo_WithObjectLockEventBasedHold() {
        // Given
        ObjectLockConfiguration lockConfig = ObjectLockConfiguration.builder()
                .legalHold(true)
                .useEventBasedHold(true)
                .build();

        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .withObjectLock(lockConfig)
                .build();

        // When
        BlobInfo blobInfo = transformer.toBlobInfo(uploadRequest);

        // Then
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(TEST_KEY, blobInfo.getName());
        Boolean tempHold = getTemporaryHold(blobInfo);
        Boolean eventHold = getEventBasedHold(blobInfo);
        assertTrue(tempHold == null || !tempHold, "Temporary hold should be false or null");
        assertTrue(eventHold != null && eventHold, "Event-based hold should be set to true");
    }

    @Test
    public void testToBlobInfo_WithObjectLockTemporaryHoldDefault() {
        // Given - useEventBasedHold is null, should default to temporary hold
        ObjectLockConfiguration lockConfig = ObjectLockConfiguration.builder()
                .legalHold(true)
                .useEventBasedHold(null)
                .build();

        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .withObjectLock(lockConfig)
                .build();

        // When
        BlobInfo blobInfo = transformer.toBlobInfo(uploadRequest);

        // Then
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(TEST_KEY, blobInfo.getName());
        Boolean tempHold = getTemporaryHold(blobInfo);
        Boolean eventHold = getEventBasedHold(blobInfo);
        assertTrue(tempHold != null && tempHold, "Temporary hold should be set to true (default when useEventBasedHold is null)");
        assertTrue(eventHold == null || !eventHold, "Event-based hold should be false or null");
    }

    @Test
    public void testToBlobInfo_WithObjectLockNoLegalHold() {
        // Given - legalHold is false, no holds should be set
        ObjectLockConfiguration lockConfig = ObjectLockConfiguration.builder()
                .legalHold(false)
                .useEventBasedHold(true)
                .build();

        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .withObjectLock(lockConfig)
                .build();

        // When
        BlobInfo blobInfo = transformer.toBlobInfo(uploadRequest);

        // Then
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(TEST_KEY, blobInfo.getName());
        Boolean tempHold = getTemporaryHold(blobInfo);
        Boolean eventHold = getEventBasedHold(blobInfo);
        assertTrue(tempHold == null || !tempHold, "Temporary hold should be false or null when legalHold is false");
        assertTrue(eventHold == null || !eventHold, "Event-based hold should be false or null when legalHold is false");
    }

    @Test
    public void testToBlobInfo_WithoutObjectLock() {
        // Given
        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .build();

        // When
        BlobInfo blobInfo = transformer.toBlobInfo(uploadRequest);

        // Then
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(TEST_KEY, blobInfo.getName());
        assertNull(getTemporaryHold(blobInfo));
        assertNull(getEventBasedHold(blobInfo));
        assertNull(getRetention(blobInfo));
    }

    @Test
    public void testToBlobInfo_WithObjectLockRetentionGovernance() {
        // Given
        java.time.Instant retainUntil = java.time.Instant.now().plusSeconds(86400); // 1 day from now
        ObjectLockConfiguration lockConfig = ObjectLockConfiguration.builder()
                .mode(RetentionMode.GOVERNANCE)
                .retainUntilDate(retainUntil)
                .legalHold(true)
                .useEventBasedHold(false)
                .build();

        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .withObjectLock(lockConfig)
                .build();

        // When
        BlobInfo blobInfo = transformer.toBlobInfo(uploadRequest);

        // Then
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(TEST_KEY, blobInfo.getName());
        com.google.cloud.storage.BlobInfo.Retention retention = getRetention(blobInfo);
        assertNotNull(retention, "Retention should be set");
        assertEquals(com.google.cloud.storage.BlobInfo.Retention.Mode.UNLOCKED, retention.getMode());
        assertEquals(retainUntil, retention.getRetainUntilTime().toInstant());
        Boolean tempHold = getTemporaryHold(blobInfo);
        assertTrue(tempHold != null && tempHold, "Temporary hold should be set to true");
    }

    @Test
    public void testToBlobInfo_WithObjectLockRetentionCompliance() {
        // Given
        java.time.Instant retainUntil = java.time.Instant.now().plusSeconds(86400); // 1 day from now
        ObjectLockConfiguration lockConfig = ObjectLockConfiguration.builder()
                .mode(RetentionMode.COMPLIANCE)
                .retainUntilDate(retainUntil)
                .legalHold(true)
                .useEventBasedHold(false)
                .build();

        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .withObjectLock(lockConfig)
                .build();

        // When
        BlobInfo blobInfo = transformer.toBlobInfo(uploadRequest);

        // Then
        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(TEST_KEY, blobInfo.getName());
        com.google.cloud.storage.BlobInfo.Retention retention = getRetention(blobInfo);
        assertNotNull(retention, "Retention should be set");
        assertEquals(com.google.cloud.storage.BlobInfo.Retention.Mode.LOCKED, retention.getMode());
        assertEquals(retainUntil, retention.getRetainUntilTime().toInstant());
        Boolean tempHold = getTemporaryHold(blobInfo);
        assertTrue(tempHold != null && tempHold, "Temporary hold should be set to true");
    }

    @Test
    public void testToBlobMetadata_WithTemporaryHold() {
        // Given
        Map<String, String> metadata = new HashMap<>();
        metadata.put("content-type", "application/json");
        
        OffsetDateTime updateTime = OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC);
        
        when(mockBlob.getName()).thenReturn(TEST_KEY);
        when(mockBlob.getGeneration()).thenReturn(TEST_GENERATION);
        when(mockBlob.getEtag()).thenReturn(TEST_ETAG);
        when(mockBlob.getSize()).thenReturn(TEST_SIZE);
        when(mockBlob.getMetadata()).thenReturn(metadata);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(updateTime);
        when(mockBlob.getMd5()).thenReturn("5d41402abc4b2a76b9719d911017c592");
        when(mockBlob.getTemporaryHold()).thenReturn(true);
        when(mockBlob.getEventBasedHold()).thenReturn(false);

        // When
        BlobMetadata blobMetadata = transformer.toBlobMetadata(mockBlob);

        // Then
        assertEquals(TEST_KEY, blobMetadata.getKey());
        assertNotNull(blobMetadata.getObjectLockInfo());
        assertTrue(blobMetadata.getObjectLockInfo().isLegalHold());
        assertFalse(blobMetadata.getObjectLockInfo().getUseEventBasedHold());
        // No retention mode when only holds are present
        assertNull(blobMetadata.getObjectLockInfo().getMode());
        assertNull(blobMetadata.getObjectLockInfo().getRetainUntilDate());
    }

    @Test
    public void testToBlobMetadata_WithEventBasedHold() {
        // Given
        Map<String, String> metadata = new HashMap<>();
        metadata.put("content-type", "application/json");
        
        OffsetDateTime updateTime = OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC);
        
        when(mockBlob.getName()).thenReturn(TEST_KEY);
        when(mockBlob.getGeneration()).thenReturn(TEST_GENERATION);
        when(mockBlob.getEtag()).thenReturn(TEST_ETAG);
        when(mockBlob.getSize()).thenReturn(TEST_SIZE);
        when(mockBlob.getMetadata()).thenReturn(metadata);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(updateTime);
        when(mockBlob.getMd5()).thenReturn("5d41402abc4b2a76b9719d911017c592");
        when(mockBlob.getTemporaryHold()).thenReturn(false);
        when(mockBlob.getEventBasedHold()).thenReturn(true);

        // When
        BlobMetadata blobMetadata = transformer.toBlobMetadata(mockBlob);

        // Then
        assertEquals(TEST_KEY, blobMetadata.getKey());
        assertNotNull(blobMetadata.getObjectLockInfo());
        assertTrue(blobMetadata.getObjectLockInfo().isLegalHold());
        assertTrue(blobMetadata.getObjectLockInfo().getUseEventBasedHold());
        // No retention mode when only holds are present
        assertNull(blobMetadata.getObjectLockInfo().getMode());
        assertNull(blobMetadata.getObjectLockInfo().getRetainUntilDate());
    }

    @Test
    public void testToBlobMetadata_WithBothHolds() {
        // Given - both holds are true (edge case)
        Map<String, String> metadata = new HashMap<>();
        metadata.put("content-type", "application/json");
        
        OffsetDateTime updateTime = OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC);
        
        when(mockBlob.getName()).thenReturn(TEST_KEY);
        when(mockBlob.getGeneration()).thenReturn(TEST_GENERATION);
        when(mockBlob.getEtag()).thenReturn(TEST_ETAG);
        when(mockBlob.getSize()).thenReturn(TEST_SIZE);
        when(mockBlob.getMetadata()).thenReturn(metadata);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(updateTime);
        when(mockBlob.getMd5()).thenReturn("5d41402abc4b2a76b9719d911017c592");
        when(mockBlob.getTemporaryHold()).thenReturn(true);
        when(mockBlob.getEventBasedHold()).thenReturn(true);

        // When
        BlobMetadata blobMetadata = transformer.toBlobMetadata(mockBlob);

        // Then
        assertEquals(TEST_KEY, blobMetadata.getKey());
        assertNotNull(blobMetadata.getObjectLockInfo());
        assertTrue(blobMetadata.getObjectLockInfo().isLegalHold());
        assertTrue(blobMetadata.getObjectLockInfo().getUseEventBasedHold());
    }

    @Test
    public void testToBlobMetadata_WithoutHolds() {
        // Given - no holds
        Map<String, String> metadata = new HashMap<>();
        metadata.put("content-type", "application/json");
        
        OffsetDateTime updateTime = OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC);
        
        when(mockBlob.getName()).thenReturn(TEST_KEY);
        when(mockBlob.getGeneration()).thenReturn(TEST_GENERATION);
        when(mockBlob.getEtag()).thenReturn(TEST_ETAG);
        when(mockBlob.getSize()).thenReturn(TEST_SIZE);
        when(mockBlob.getMetadata()).thenReturn(metadata);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(updateTime);
        when(mockBlob.getMd5()).thenReturn("5d41402abc4b2a76b9719d911017c592");
        when(mockBlob.getTemporaryHold()).thenReturn(false);
        when(mockBlob.getEventBasedHold()).thenReturn(false);

        // When
        BlobMetadata blobMetadata = transformer.toBlobMetadata(mockBlob);

        // Then
        assertEquals(TEST_KEY, blobMetadata.getKey());
        assertNull(blobMetadata.getObjectLockInfo());
    }

    @Test
    public void testToBlobMetadata_WithRetentionGovernance() {
        // Given
        Map<String, String> metadata = new HashMap<>();
        metadata.put("content-type", "application/json");
        
        OffsetDateTime updateTime = OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC);
        OffsetDateTime retainUntilTime = OffsetDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC);
        
        com.google.cloud.storage.BlobInfo.Retention retention = com.google.cloud.storage.BlobInfo.Retention.newBuilder()
                .setMode(com.google.cloud.storage.BlobInfo.Retention.Mode.UNLOCKED)
                .setRetainUntilTime(retainUntilTime)
                .build();
        
        when(mockBlob.getName()).thenReturn(TEST_KEY);
        when(mockBlob.getGeneration()).thenReturn(TEST_GENERATION);
        when(mockBlob.getEtag()).thenReturn(TEST_ETAG);
        when(mockBlob.getSize()).thenReturn(TEST_SIZE);
        when(mockBlob.getMetadata()).thenReturn(metadata);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(updateTime);
        when(mockBlob.getMd5()).thenReturn("5d41402abc4b2a76b9719d911017c592");
        when(mockBlob.getRetention()).thenReturn(retention);
        when(mockBlob.getTemporaryHold()).thenReturn(false);
        when(mockBlob.getEventBasedHold()).thenReturn(false);

        // When
        BlobMetadata blobMetadata = transformer.toBlobMetadata(mockBlob);

        // Then
        assertEquals(TEST_KEY, blobMetadata.getKey());
        assertNotNull(blobMetadata.getObjectLockInfo());
        assertEquals(RetentionMode.GOVERNANCE, blobMetadata.getObjectLockInfo().getMode());
        assertEquals(retainUntilTime.toInstant(), blobMetadata.getObjectLockInfo().getRetainUntilDate());
        assertFalse(blobMetadata.getObjectLockInfo().isLegalHold());
    }

    @Test
    public void testToBlobMetadata_WithRetentionCompliance() {
        // Given
        Map<String, String> metadata = new HashMap<>();
        metadata.put("content-type", "application/json");
        
        OffsetDateTime updateTime = OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC);
        OffsetDateTime retainUntilTime = OffsetDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC);
        
        com.google.cloud.storage.BlobInfo.Retention retention = com.google.cloud.storage.BlobInfo.Retention.newBuilder()
                .setMode(com.google.cloud.storage.BlobInfo.Retention.Mode.LOCKED)
                .setRetainUntilTime(retainUntilTime)
                .build();
        
        when(mockBlob.getName()).thenReturn(TEST_KEY);
        when(mockBlob.getGeneration()).thenReturn(TEST_GENERATION);
        when(mockBlob.getEtag()).thenReturn(TEST_ETAG);
        when(mockBlob.getSize()).thenReturn(TEST_SIZE);
        when(mockBlob.getMetadata()).thenReturn(metadata);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(updateTime);
        when(mockBlob.getMd5()).thenReturn("5d41402abc4b2a76b9719d911017c592");
        when(mockBlob.getRetention()).thenReturn(retention);
        when(mockBlob.getTemporaryHold()).thenReturn(false);
        when(mockBlob.getEventBasedHold()).thenReturn(false);

        // When
        BlobMetadata blobMetadata = transformer.toBlobMetadata(mockBlob);

        // Then
        assertEquals(TEST_KEY, blobMetadata.getKey());
        assertNotNull(blobMetadata.getObjectLockInfo());
        assertEquals(RetentionMode.COMPLIANCE, blobMetadata.getObjectLockInfo().getMode());
        assertEquals(retainUntilTime.toInstant(), blobMetadata.getObjectLockInfo().getRetainUntilDate());
        assertFalse(blobMetadata.getObjectLockInfo().isLegalHold());
    }

    @Test
    public void testToBlobMetadata_WithNullHolds() {
        // Given - holds are null
        Map<String, String> metadata = new HashMap<>();
        metadata.put("content-type", "application/json");
        
        OffsetDateTime updateTime = OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC);
        
        when(mockBlob.getName()).thenReturn(TEST_KEY);
        when(mockBlob.getGeneration()).thenReturn(TEST_GENERATION);
        when(mockBlob.getEtag()).thenReturn(TEST_ETAG);
        when(mockBlob.getSize()).thenReturn(TEST_SIZE);
        when(mockBlob.getMetadata()).thenReturn(metadata);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(updateTime);
        when(mockBlob.getMd5()).thenReturn("5d41402abc4b2a76b9719d911017c592");
        when(mockBlob.getTemporaryHold()).thenReturn(null);
        when(mockBlob.getEventBasedHold()).thenReturn(null);

        // When
        BlobMetadata blobMetadata = transformer.toBlobMetadata(mockBlob);

        // Then
        assertEquals(TEST_KEY, blobMetadata.getKey());
        assertNull(blobMetadata.getObjectLockInfo());
    }

    @Test
    public void testToBlobMetadata_WithNullUpdateTime() {
        // Given - updateTime is null
        Map<String, String> metadata = new HashMap<>();
        metadata.put("content-type", "application/json");
        
        when(mockBlob.getName()).thenReturn(TEST_KEY);
        when(mockBlob.getGeneration()).thenReturn(TEST_GENERATION);
        when(mockBlob.getEtag()).thenReturn(TEST_ETAG);
        when(mockBlob.getSize()).thenReturn(TEST_SIZE);
        when(mockBlob.getMetadata()).thenReturn(metadata);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(null);
        when(mockBlob.getMd5()).thenReturn("5d41402abc4b2a76b9719d911017c592");
        when(mockBlob.getTemporaryHold()).thenReturn(null);
        when(mockBlob.getEventBasedHold()).thenReturn(null);

        // When
        BlobMetadata blobMetadata = transformer.toBlobMetadata(mockBlob);

        // Then
        assertEquals(TEST_KEY, blobMetadata.getKey());
        assertNull(blobMetadata.getLastModified());
    }

    @Test
    public void testToGcpRetrySettings_Exponential() {
        // Given
        RetryConfig retryConfig = RetryConfig.builder()
                .mode(RetryConfig.Mode.EXPONENTIAL)
                .maxAttempts(3)
                .initialDelayMillis(100L)
                .multiplier(2.0)
                .maxDelayMillis(5000L)
                .totalTimeout(30000L)
                .build();

        // When
        RetrySettings settings = transformer.toGcpRetrySettings(retryConfig);

        // Then
        assertEquals(3, settings.getMaxAttempts());
        assertEquals(Duration.ofMillis(100), settings.getInitialRetryDelayDuration());
        assertEquals(2.0, settings.getRetryDelayMultiplier());
        assertEquals(Duration.ofMillis(5000), settings.getMaxRetryDelayDuration());
        assertEquals(Duration.ofMillis(30000), settings.getTotalTimeoutDuration());
    }

    @Test
    public void testToGcpRetrySettings_Fixed() {
        // Given
        RetryConfig retryConfig = RetryConfig.builder()
                .mode(RetryConfig.Mode.FIXED)
                .maxAttempts(5)
                .fixedDelayMillis(1000L)
                .build();

        // When
        RetrySettings settings = transformer.toGcpRetrySettings(retryConfig);

        // Then
        assertEquals(5, settings.getMaxAttempts());
        assertEquals(Duration.ofMillis(1000), settings.getInitialRetryDelayDuration());
        assertEquals(1.0, settings.getRetryDelayMultiplier());
        assertEquals(Duration.ofMillis(1000), settings.getMaxRetryDelayDuration());
    }

    @Test
    public void testToGcpRetrySettings_NullConfig() {
        assertThrows(InvalidArgumentException.class, () -> {
            transformer.toGcpRetrySettings(null);
        });
    }

    @Test
    public void testToGcpRetrySettings_InvalidMaxAttempts() {
        RetryConfig retryConfig = RetryConfig.builder()
                .maxAttempts(0)
                .build();
        assertThrows(InvalidArgumentException.class, () -> {
            transformer.toGcpRetrySettings(retryConfig);
        });
    }

    @Test
    public void testToGcpRetrySettings_InvalidExponentialDelays() {
        RetryConfig retryConfig1 = RetryConfig.builder()
                .mode(RetryConfig.Mode.EXPONENTIAL)
                .initialDelayMillis(0)
                .build();
        assertThrows(InvalidArgumentException.class, () -> {
            transformer.toGcpRetrySettings(retryConfig1);
        });

        RetryConfig retryConfig2 = RetryConfig.builder()
                .mode(RetryConfig.Mode.EXPONENTIAL)
                .initialDelayMillis(100)
                .maxDelayMillis(0)
                .build();
        assertThrows(InvalidArgumentException.class, () -> {
            transformer.toGcpRetrySettings(retryConfig2);
        });
    }

    @Test
    public void testToGcpRetrySettings_InvalidFixedDelay() {
        RetryConfig retryConfig = RetryConfig.builder()
                .mode(RetryConfig.Mode.FIXED)
                .fixedDelayMillis(0)
                .build();
        assertThrows(InvalidArgumentException.class, () -> {
            transformer.toGcpRetrySettings(retryConfig);
        });
    }

    @Test
    public void testToGcpRetrySettings_InvalidTotalTimeout() {
        RetryConfig retryConfig = RetryConfig.builder()
                .totalTimeout(0L)
                .build();
        assertThrows(InvalidArgumentException.class, () -> {
            transformer.toGcpRetrySettings(retryConfig);
        });
    }

    @Test
    public void testToGcpRetrySettings_AttemptTimeout() {
        // Given
        RetryConfig retryConfig = RetryConfig.builder()
                .attemptTimeout(2000L)
                .build();

        // When
        RetrySettings settings = transformer.toGcpRetrySettings(retryConfig);

        // Then
        assertEquals(Duration.ofMillis(2000), settings.getInitialRpcTimeoutDuration());
        assertEquals(1.0, settings.getRpcTimeoutMultiplier());
        assertEquals(Duration.ofMillis(2000), settings.getMaxRpcTimeoutDuration());
    }

    @Test
    public void testToGcpRetrySettings_InvalidAttemptTimeout() {
        RetryConfig retryConfig = RetryConfig.builder()
                .attemptTimeout(0L)
                .build();
        assertThrows(InvalidArgumentException.class, () -> {
            transformer.toGcpRetrySettings(retryConfig);
        });
    }

    @Test
    public void testToGenerationId_WithInvalidFormat() {
        // Given
        String invalidVersionId = "not-a-number";

        // When/Then
        assertThrows(NumberFormatException.class, () -> {
            transformer.toGenerationId(invalidVersionId);
        });
    }
} 
