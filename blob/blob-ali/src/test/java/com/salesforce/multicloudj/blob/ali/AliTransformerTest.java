package com.salesforce.multicloudj.blob.ali;

import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.internal.OSSHeaders;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartListing;
import com.aliyun.oss.model.PartSummary;
import com.aliyun.oss.model.PutObjectResult;
import com.aliyun.oss.model.UploadPartResult;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.PresignedOperation;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class AliTransformerTest {

    private static final String BUCKET = "some-bucket";
    private final AliTransformer transformer = new AliTransformer(BUCKET);

    @Test
    void testBucket() {
        assertEquals(BUCKET, transformer.getBucket());
    }

    @Test
    void testToPutObjectRequest() {
        var key = "some-key";
        var metadata = Map.of("some-key", "some-value");
        var tags = Map.of("tag-key", "tag-value");

        var request = UploadRequest
                .builder()
                .withKey(key)
                .withMetadata(metadata)
                .withTags(tags)
                .build();
        InputStream inputStream = mock(InputStream.class);
        File file = mock(File.class);

        var actual = transformer.toPutObjectRequest(request, inputStream);
        assertEquals(BUCKET, actual.getBucketName());
        assertEquals(key, actual.getKey());
        assertEquals(metadata, actual.getMetadata().getUserMetadata());
        assertEquals("tag-key=tag-value", actual.getMetadata().getRawMetadata().get("x-oss-tagging"));
        assertEquals(inputStream, actual.getInputStream());

        actual = transformer.toPutObjectRequest(request, file);
        assertEquals(BUCKET, actual.getBucketName());
        assertEquals(key, actual.getKey());
        assertEquals(metadata, actual.getMetadata().getUserMetadata());
        assertEquals("tag-key=tag-value", actual.getMetadata().getRawMetadata().get("x-oss-tagging"));
        assertEquals(file, actual.getFile());
    }

    @Test
    void testToUploadResponse() {
        UploadRequest request = UploadRequest
                .builder()
                .withKey("some-key")
                .withMetadata(Map.of("some-key", "some-value"))
                .build();
        PutObjectResult result = mock(PutObjectResult.class);
        doReturn("etag").when(result).getETag();
        doReturn("version-1").when(result).getVersionId();

        var actual = transformer.toUploadResponse(request, result);

        assertEquals(request.getKey(), actual.getKey());
        assertEquals("version-1", actual.getVersionId());
        assertEquals("etag", actual.getETag());
    }

    @Test
    void testToGetObjectRequest() {
        var request = DownloadRequest
                .builder()
                .withKey("some/key/path.file")
                .withVersionId("version-1")
                .withRange(0L, 500L)
                .build();

        var actual = transformer.toGetObjectRequest(request);

        assertEquals(BUCKET, actual.getBucketName());
        assertEquals(request.getKey(), actual.getKey());
        assertEquals("version-1", actual.getVersionId());
        assertEquals(0L, actual.getRange()[0]);
        assertEquals(500L, actual.getRange()[1]);
    }

    @Test
    void testComputeRange() {
        Pair<Long, Long> result = transformer.computeRange(0L, 500L);
        assertEquals(result.getLeft(), 0);
        assertEquals(result.getRight(), 500);

        result = transformer.computeRange(100L, 600L);
        assertEquals(result.getLeft(), 100);
        assertEquals(result.getRight(), 600);

        result = transformer.computeRange(null, 500L);
        assertEquals(result.getLeft(), -1);
        assertEquals(result.getRight(), 500);

        result = transformer.computeRange(500L, null);
        assertEquals(result.getLeft(), 500);
        assertEquals(result.getRight(), -1);
    }

    @Test
    void testToDownloadResponse() {
        OSSObject ossObject = mock(OSSObject.class);
        doReturn("key").when(ossObject).getKey();
        ObjectMetadata objectMetadata = mock(ObjectMetadata.class);
        doReturn(objectMetadata).when(ossObject).getObjectMetadata();
        doReturn("version-1").when(objectMetadata).getVersionId();
        doReturn("etag").when(objectMetadata).getETag();
        Date date = Date.from(Instant.now());
        doReturn(date).when(objectMetadata).getLastModified();
        Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
        doReturn(metadata).when(objectMetadata).getUserMetadata();
        doReturn(100L).when(objectMetadata).getContentLength();

        var actual = transformer.toDownloadResponse(ossObject);

        assertEquals("key", actual.getKey());
        BlobMetadata blobMetadata = actual.getMetadata();
        assertEquals("key", blobMetadata.getKey());
        assertEquals("version-1", blobMetadata.getVersionId());
        assertEquals("etag", blobMetadata.getETag());
        assertEquals(metadata, blobMetadata.getMetadata());
        assertEquals(date.toInstant(), blobMetadata.getLastModified());
        assertEquals(100L, blobMetadata.getObjectSize());
    }

    @Test
    void testToDeleteObjectsRequest() {
        Collection<BlobIdentifier> objects = List.of(new BlobIdentifier("key1", null), new BlobIdentifier("key2", null));

        var actual = transformer.toDeleteObjectsRequest(objects);

        assertEquals(BUCKET, actual.getBucketName());
        var keys = actual.getKeys();
        assertEquals("key1", keys.get(0));
        assertEquals("key2", keys.get(1));
    }

    @Test
    void testToDeleteVersionsRequest() {
        Collection<BlobIdentifier> objects = List.of(new BlobIdentifier("key1", "v1"), new BlobIdentifier("key2", "v2"));

        var actual = transformer.toDeleteVersionsRequest(objects);

        assertEquals(BUCKET, actual.getBucketName());
        var keys = actual.getKeys();
        assertEquals("key1", keys.get(0).getKey());
        assertEquals("v1", keys.get(0).getVersion());
        assertEquals("key2", keys.get(1).getKey());
        assertEquals("v2", keys.get(1).getVersion());
    }

    @Test
    void testToCopyObjectRequest() {
        CopyRequest request = CopyRequest.builder()
                .srcKey("key1")
                .srcVersionId("v1")
                .destBucket("bucket2")
                .destKey("key2")
                .build();

        var actual = transformer.toCopyObjectRequest(request);

        assertEquals(BUCKET, actual.getSourceBucketName());
        assertEquals("key1", actual.getSourceKey());
        assertEquals("v1", actual.getSourceVersionId());
        assertEquals("bucket2", actual.getDestinationBucketName());
        assertEquals("key2", actual.getDestinationKey());
    }

    @Test
    void testToCopyResponse() {
        CopyObjectResult result = mock(CopyObjectResult.class);
        doReturn("v2").when(result).getVersionId();
        doReturn("etag").when(result).getETag();
        Date lastModified = Date.from(Instant.now());
        doReturn(lastModified).when(result).getLastModified();

        var actual = transformer.toCopyResponse("key2", result);

        assertEquals("key2", actual.getKey());
        assertEquals("v2", actual.getVersionId());
        assertEquals("etag", actual.getETag());
        assertEquals(lastModified.toInstant(), actual.getLastModified());
    }

    @Test
    void testToMetadataRequest() {
        var actual = transformer.toMetadataRequest("key", "v1");
        assertEquals(BUCKET, actual.getBucketName());
        assertEquals("key", actual.getKey());
        assertEquals("v1", actual.getVersionId());
    }

    @Test
    void testToBlobMetadata() {
        ObjectMetadata objectMetadata = mock(ObjectMetadata.class);
        doReturn("version-1").when(objectMetadata).getVersionId();
        doReturn("etag").when(objectMetadata).getETag();
        Date date = Date.from(Instant.now());
        doReturn(date).when(objectMetadata).getLastModified();
        Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
        doReturn(metadata).when(objectMetadata).getUserMetadata();
        doReturn(100L).when(objectMetadata).getContentLength();

        var actual = transformer.toBlobMetadata("key", objectMetadata);

        assertEquals("key", actual.getKey());
        assertEquals("version-1", actual.getVersionId());
        assertEquals("etag", actual.getETag());
        assertEquals(metadata, actual.getMetadata());
        assertEquals(date.toInstant(), actual.getLastModified());
        assertEquals(100L, actual.getObjectSize());
    }

    @Test
    void testToInitiateMultipartUploadRequest() {
        Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
        MultipartUploadRequest request = new MultipartUploadRequest.Builder()
                .withKey("key")
                .withMetadata(metadata)
                .build();

        var actual = transformer.toInitiateMultipartUploadRequest(request);

        assertEquals(BUCKET, actual.getBucketName());
        assertEquals("key", actual.getKey());
        assertEquals(metadata, actual.getObjectMetadata().getUserMetadata());
    }

    @Test
    void testToMultipartUpload() {
        InitiateMultipartUploadResult initiateMultipartUploadResult = mock(InitiateMultipartUploadResult.class);
        doReturn(BUCKET).when(initiateMultipartUploadResult).getBucketName();
        doReturn("key").when(initiateMultipartUploadResult).getKey();
        doReturn("uploadId").when(initiateMultipartUploadResult).getUploadId();

        var actual = transformer.toMultipartUpload(initiateMultipartUploadResult);

        assertEquals(BUCKET, actual.getBucket());
        assertEquals("key", actual.getKey());
        assertEquals("uploadId", actual.getId());
    }

    @Test
    void testToUploadPartRequest() {
        MultipartUpload mpu = new MultipartUpload(BUCKET, "key", "uploadId");
        byte[] content = "Test data".getBytes();
        InputStream inputStream = new ByteArrayInputStream(content);
        MultipartPart mpp = new MultipartPart(1, inputStream, content.length);

        var actual = transformer.toUploadPartRequest(mpu, mpp);

        assertEquals(BUCKET, actual.getBucketName());
        assertEquals("key", actual.getKey());
        assertEquals("uploadId", actual.getUploadId());
        assertEquals(1, actual.getPartNumber());
        assertEquals(inputStream, actual.getInputStream());
        assertEquals(content.length, actual.getPartSize());
    }

    @Test
    void testToUploadPartResponse() {
        byte[] content = "Test data".getBytes();
        InputStream inputStream = new ByteArrayInputStream(content);
        MultipartPart mpp = new MultipartPart(1, inputStream, content.length);
        UploadPartResult uploadPartResult = new UploadPartResult();
        uploadPartResult.setETag("etag");
        uploadPartResult.setPartNumber(1);
        uploadPartResult.setPartSize(content.length);

        var actual = transformer.toUploadPartResponse(mpp, uploadPartResult);

        assertEquals("etag", actual.getEtag());
        assertEquals(1, actual.getPartNumber());
        assertEquals(content.length, actual.getSizeInBytes());
    }

    @Test
    void testToCompleteMultipartUploadRequest() {
        MultipartUpload mpu = new MultipartUpload(BUCKET, "key", "uploadId");
        List<UploadPartResponse> parts = List.of(
                new UploadPartResponse(1, "etag1", 50),
                new UploadPartResponse(2, "etag2", 50));

        var actual = transformer.toCompleteMultipartUploadRequest(mpu, parts);

        assertEquals(BUCKET, actual.getBucketName());
        assertEquals("key", actual.getKey());
        assertEquals("uploadId", actual.getUploadId());
        var actualParts = actual.getPartETags();
        assertEquals(1, actualParts.get(0).getPartNumber());
        assertEquals("etag1", actualParts.get(0).getETag());
        assertEquals(2, actualParts.get(1).getPartNumber());
        assertEquals("etag2", actualParts.get(1).getETag());
    }

    @Test
    void testToListPartsRequest() {
        MultipartUpload mpu = new MultipartUpload(BUCKET, "key", "uploadId");

        var actual = transformer.toListPartsRequest(mpu);

        assertEquals(BUCKET, actual.getBucketName());
        assertEquals("key", actual.getKey());
        assertEquals("uploadId", actual.getUploadId());
    }

    @Test
    void testToListUploadPartResponse() {
        PartListing partListing = new PartListing();
        var part2 = new PartSummary();
        part2.setETag("etag2");
        part2.setPartNumber(2);
        part2.setSize(50L);
        partListing.addPart(part2);     // Intentionally out of order, to verify sort
        var part1 = new PartSummary();
        part1.setETag("etag1");
        part1.setPartNumber(1);
        part1.setSize(100L);
        partListing.addPart(part1);

        var actual = transformer.toListUploadPartResponse(partListing);

        assertEquals(1, actual.get(0).getPartNumber());
        assertEquals("etag1", actual.get(0).getEtag());
        assertEquals(100L, actual.get(0).getSizeInBytes());
        assertEquals(2, actual.get(1).getPartNumber());
        assertEquals("etag2", actual.get(1).getEtag());
        assertEquals(50L, actual.get(1).getSizeInBytes());
    }

    @Test
    void testToAbortMultipartUploadRequest() {
        MultipartUpload mpu = new MultipartUpload(BUCKET, "key", "uploadId");

        var actual = transformer.toAbortMultipartUploadRequest(mpu);

        assertEquals(BUCKET, actual.getBucketName());
        assertEquals("key", actual.getKey());
        assertEquals("uploadId", actual.getUploadId());
    }

    @Test
    void testToPresignedUrlUploadRequest() {
        Map<String, String> metadata = Map.of("key-1", "value-1");
        Map<String, String> tags = Map.of("tag-1", "tag-value-1");
        UploadRequest uploadRequest = new UploadRequest.Builder()
                .withKey("object-1")
                .withContentLength(1024)
                .withMetadata(metadata)
                .withTags(tags)
                .build();
        Duration duration = Duration.ofHours(12);
        PresignedUrlRequest presignedUploadRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.UPLOAD)
                .key(uploadRequest.getKey())
                .metadata(uploadRequest.getMetadata())
                .tags(uploadRequest.getTags())
                .duration(duration)
                .build();

        var actual = transformer.toPresignedUrlUploadRequest(presignedUploadRequest);

        assertEquals(HttpMethod.PUT, actual.getMethod());
        assertEquals(BUCKET, actual.getBucketName());
        assertEquals("object-1", actual.getKey());
        Map<String,String> headers = actual.getHeaders();
        assertEquals("tag-1=tag-value-1", headers.get(OSSHeaders.OSS_TAGGING));
        assertEquals("value-1", actual.getUserMetadata().get("key-1"));
        assertNotNull(actual.getExpiration());
    }

    @Test
    void testToPresignedUrlDownloadRequest() {
        Duration duration = Duration.ofHours(12);
        PresignedUrlRequest presignedDownloadRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.DOWNLOAD)
                .key("object-1")
                .duration(duration)
                .build();

        var actual = transformer.toPresignedUrlDownloadRequest(presignedDownloadRequest);

        assertEquals(HttpMethod.GET, actual.getMethod());
        assertEquals(BUCKET, actual.getBucketName());
        assertEquals("object-1", actual.getKey());
        long diff = Date.from(Instant.now()).getTime() - actual.getExpiration().getTime() + duration.toMillis();
        assertTrue(diff < 1000L);   // The time difference is less than a second from expected
    }

    @Test
    void testToListObjectsRequest() {
        ListBlobsPageRequest request = ListBlobsPageRequest
                .builder()
                .withDelimiter(":")
                .withPrefix("some/prefix/path/thingie")
                .withPaginationToken("next-token")
                .withMaxResults(100)
                .build();

        com.aliyun.oss.model.ListObjectsRequest actual = transformer.toListObjectsRequest(request);
        assertEquals(BUCKET, actual.getBucketName());
        assertEquals(request.getDelimiter(), actual.getDelimiter());
        assertEquals(request.getPrefix(), actual.getPrefix());
        assertEquals(request.getPaginationToken(), actual.getMarker());
        assertEquals(request.getMaxResults(), actual.getMaxKeys());
    }
}
