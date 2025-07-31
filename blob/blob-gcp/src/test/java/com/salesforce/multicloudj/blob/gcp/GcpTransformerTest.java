package com.salesforce.multicloudj.blob.gcp;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.PresignedOperation;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
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

        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            transformer.toBlobInfo(uploadRequest);
        });
        assertEquals("Tags are not supported by GCP", exception.getMessage());
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
        BlobId blobId = transformer.toBlobId(TEST_KEY, TEST_VERSION_ID);

        // Then
        assertEquals(TEST_BUCKET, blobId.getBucket());
        assertEquals(TEST_KEY, blobId.getName());
        assertEquals(TEST_GENERATION, blobId.getGeneration());
    }

    @Test
    void testToBlobId_WithKeyAndNullVersionId() {
        // When
        BlobId blobId = transformer.toBlobId(TEST_KEY, null);

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

        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            transformer.toBlobInfo(presignedUrlRequest);
        });
        assertEquals("Tags are not supported by GCP", exception.getMessage());
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
    public void testToPartName() {
        MultipartUpload mpu = MultipartUpload.builder()
                .bucket(TEST_BUCKET)
                .key(TEST_KEY)
                .id("id")
                .build();
        assertEquals(TEST_KEY+"/id/part-1", transformer.toPartName(mpu, 1));
        assertEquals(TEST_KEY+"/id/part-8", transformer.toPartName(mpu, 8));
        mpu = MultipartUpload.builder()
                .bucket(TEST_BUCKET)
                .key(TEST_KEY+"_test")
                .id("id2")
                .build();
        assertEquals(TEST_KEY+"_test/id2/part-4", transformer.toPartName(mpu, 4));
    }

    @Test
    public void testToUploadRequest() {
        MultipartUpload mpu = MultipartUpload.builder()
                .bucket(TEST_BUCKET)
                .key(TEST_KEY)
                .id("id")
                .build();
        MultipartPart mpp = new MultipartPart(1, "Test Data".getBytes());

        UploadRequest uploadRequest = transformer.toUploadRequest(mpu, mpp);
        assertEquals(TEST_KEY+"/id/part-1", uploadRequest.getKey());
        assertEquals(9, uploadRequest.getContentLength());
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
    public void testToBlobIdList() {
        Page<Blob> page = mock(Page.class);
        Blob mockBlob1 = mock(Blob.class);
        BlobId mockBlobId1 = mock(BlobId.class);
        doReturn(mockBlobId1).when(mockBlob1).getBlobId();
        Blob mockBlob2 = mock(Blob.class);
        BlobId mockBlobId2 = mock(BlobId.class);
        doReturn(mockBlobId2).when(mockBlob2).getBlobId();
        Stream<Blob> stream = Stream.of(mockBlob1, mockBlob2);
        doReturn(stream).when(page).streamAll();
        transformer.toBlobIdList(page);
    }

    @Test
    public void testToUploadPartResponseList() {
        Page<Blob> page = mock(Page.class);
        Blob mockBlob1 = mock(Blob.class);
        doReturn(TEST_KEY+"/123456/part-1").when(mockBlob1).getName();
        doReturn("part-1-etag").when(mockBlob1).getEtag();
        doReturn(100L).when(mockBlob1).getSize();
        Blob mockBlob2 = mock(Blob.class);
        doReturn(TEST_KEY+"/123456/part-2").when(mockBlob2).getName();
        doReturn("part-2-etag").when(mockBlob2).getEtag();
        doReturn(200L).when(mockBlob2).getSize();
        Stream<Blob> stream = Stream.of(mockBlob1, mockBlob2);
        doReturn(stream).when(page).streamAll();

        List<UploadPartResponse> response = transformer.toUploadPartResponseList(page);

        assertEquals(1, response.get(0).getPartNumber());
        assertEquals(100L, response.get(0).getSizeInBytes());
        assertEquals("part-1-etag", response.get(0).getEtag());
        assertEquals(2, response.get(1).getPartNumber());
        assertEquals(200L, response.get(1).getSizeInBytes());
        assertEquals("part-2-etag", response.get(1).getEtag());
    }
} 