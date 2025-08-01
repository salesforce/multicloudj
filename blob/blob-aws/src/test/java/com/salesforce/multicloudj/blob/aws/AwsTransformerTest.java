package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsBatch;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.PresignedOperation;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.transfer.s3.config.DownloadFilter;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryDownload;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryUpload;
import software.amazon.awssdk.transfer.s3.model.DownloadDirectoryRequest;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FailedFileDownload;
import software.amazon.awssdk.transfer.s3.model.FailedFileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadDirectoryRequest;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class AwsTransformerTest {

    private static final String BUCKET = "some-bucket";
    private final AwsTransformer transformer = new AwsTransformer(BUCKET);

    @Test
    void testBucket() {
        assertEquals(BUCKET, transformer.getBucket());
    }

    @Test
    void testUpload() {
        var key = "some-key";
        var metadata = Map.of("some-key", "some-value");
        var tags = Map.of("tag-key", "tag-value");

        var request = UploadRequest
                .builder()
                .withKey(key)
                .withMetadata(metadata)
                .withTags(tags)
                .build();

        var expected = PutObjectRequest
                .builder()
                .bucket(BUCKET)
                .key(key)
                .metadata(metadata)
                .tagging("tag-key=tag-value")
                .build();

        assertEquals(expected, transformer.toRequest(request));
    }

    @Test
    void testListBlobsBatch() {
        var prefixes = Arrays.asList("some/prefix", "some/other/prefix");
        var awsPrefixes = prefixes
                .stream()
                .map(prefix -> CommonPrefix.builder().prefix(prefix).build())
                .collect(Collectors.toList());

        var objects = Arrays.asList(
                S3Object.builder().key("some/key/path.file").size(1024L).build(),
                S3Object.builder().key("some/other/key/path.file").size(1025L).build(),
                S3Object.builder().key("yet/another/key/path.file").size(1026L).build()
        );
        var response = ListObjectsV2Response
                .builder()
                .commonPrefixes(awsPrefixes)
                .contents(objects)
                .build();


        var blobs = Arrays.asList(
                BlobInfo.builder().withKey("some/key/path.file").withObjectSize(1024L).build(),
                BlobInfo.builder().withKey("some/other/key/path.file").withObjectSize(1025L).build(),
                BlobInfo.builder().withKey("yet/another/key/path.file").withObjectSize(1026L).build()
        );
        var expected = new ListBlobsBatch(blobs, prefixes);
        var actual = transformer.toBatch(response);
        assertEquals(expected.getBlobs(), actual.getBlobs());
        assertEquals(expected.getCommonPrefixes(), actual.getCommonPrefixes());
    }

    @Test
    void testToInfo() {
        var s3 = S3Object.builder().key("some/key/path.file").size(1024L).build();
        var info = transformer.toInfo(s3);
        assertEquals(s3.key(), info.getKey());
        assertEquals(s3.size(), info.getObjectSize());
    }

    @Test
    void testToListObjectsV2Request() {
        var request = ListBlobsRequest
                .builder()
                .withDelimiter(":")
                .withPrefix("some/prefix/path/thingie")
                .build();

        var actual = transformer.toRequest(request);
        assertEquals(BUCKET, actual.bucket());
        assertEquals(request.getDelimiter(), actual.delimiter());
        assertEquals(request.getPrefix(), actual.prefix());
    }

    @Test
    void testToListObjectsV2PageRequest() {
        ListBlobsPageRequest request = ListBlobsPageRequest
                .builder()
                .withDelimiter(":")
                .withPrefix("some/prefix/path/thingie")
                .withPaginationToken("next-token")
                .withMaxResults(100)
                .build();

        ListObjectsV2Request actual = transformer.toRequest(request);
        assertEquals(BUCKET, actual.bucket());
        assertEquals(request.getDelimiter(), actual.delimiter());
        assertEquals(request.getPrefix(), actual.prefix());
        assertEquals(request.getPaginationToken(), actual.continuationToken());
        assertEquals(request.getMaxResults(), actual.maxKeys());
    }

    @Test
    void testToAsyncRequestBodyInputStream() {
        byte[] content = "This is test data".getBytes();
        InputStream inputStream = new ByteArrayInputStream(content);
        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey("key")
                .withContentLength(content.length)
                .build();

        AsyncRequestBody asyncRequestBody = transformer.toAsyncRequestBody(uploadRequest, inputStream);

        assertTrue(asyncRequestBody.contentLength().isPresent());
        assertEquals(content.length, asyncRequestBody.contentLength().get());
    }

    @Test
    void testUploadRequestToPutObjectRequest() {
        UploadRequest request = UploadRequest
                .builder()
                .withKey("some-key")
                .withMetadata(Map.of("some-key", "some-value"))
                .build();

        var actual = transformer.toRequest(request);
        assertEquals(BUCKET, actual.bucket());
        assertEquals(request.getKey(), actual.key());
        assertEquals(request.getMetadata(), actual.metadata());
    }

    @Test
    void testToGetObjectRequest() {
        var request = DownloadRequest
                .builder()
                .withKey("some/key/path.file")
                .withVersionId("version-1")
                .withRange(0L, 500L)
                .build();
        var actual = transformer.toRequest(request);
        assertEquals(BUCKET, actual.bucket());
        assertEquals(request.getKey(), actual.key());
        assertEquals(request.getVersionId(), actual.versionId());
        assertEquals(request.getStart(), 0);
        assertEquals(request.getEnd(), 500);
    }

    @Test
    void testCreateRangeString() {
        assertEquals("bytes=0-500", transformer.createRangeString(0L, 500L));
        assertEquals("bytes=100-600", transformer.createRangeString(100L, 600L));
        assertEquals("bytes=-500", transformer.createRangeString(null, 500L));
        assertEquals("bytes=500-", transformer.createRangeString(500L, null));
    }

    @Test
    void testToDownloadObjectResponse() {
        var request = DownloadRequest
                .builder()
                .withKey("some/key/path.file")
                .withVersionId("version-1")
                .build();
        Instant now = Instant.now();
        GetObjectResponse getObjectResponse = mock(GetObjectResponse.class);
        doReturn("version-1").when(getObjectResponse).versionId();
        doReturn("etag").when(getObjectResponse).eTag();
        doReturn(now).when(getObjectResponse).lastModified();
        Map<String,String> metadata = Map.of("key1", "value1", "key2", "value2");
        doReturn(metadata).when(getObjectResponse).metadata();
        doReturn(1024L).when(getObjectResponse).contentLength();

        DownloadResponse response = transformer.toDownloadResponse(request, getObjectResponse);

        assertEquals(request.getKey(), response.getKey());
        assertEquals(request.getKey(), response.getMetadata().getKey());
        assertEquals(request.getVersionId(), response.getMetadata().getVersionId());
        assertEquals("etag", response.getMetadata().getETag());
        assertEquals(now, response.getMetadata().getLastModified());
        assertEquals(metadata, response.getMetadata().getMetadata());
        assertEquals(1024L, response.getMetadata().getObjectSize());
    }

    @Test
    void testToDeleteRequest() {
        var key = "some-key";
        var actual = transformer.toDeleteRequest(key, null);
        assertEquals(BUCKET, actual.bucket());
        assertEquals(key, actual.key());
    }

    @Test
    void testToDeleteRequests() {
        var objects = Arrays.asList(new BlobIdentifier("first-key", "version-1"),
                new BlobIdentifier("next/key/path.file", "version-2"),
                new BlobIdentifier("other/key/path.file", null));
        List<String> keys = objects.stream().map(BlobIdentifier::getKey).collect(Collectors.toList());
        var actual = transformer.toDeleteRequests(objects);
        assertEquals(BUCKET, actual.bucket());
        var awsKeys = actual
                .delete()
                .objects()
                .stream()
                .map(ObjectIdentifier::key)
                .collect(Collectors.toList());

        assertTrue(awsKeys.containsAll(keys));
    }

    @Test
    void toCopyRequestObject() {
        var request = CopyRequest
                .builder()
                .srcKey("some-key")
                .srcVersionId("version-1")
                .destKey("some-dest-key")
                .destBucket("other-bucket")
                .build();

        var actual = transformer.toRequest(request);
        assertEquals(BUCKET, actual.sourceBucket());
        assertEquals(request.getSrcKey(), actual.sourceKey());
        assertEquals(request.getSrcVersionId(), actual.sourceVersionId());
        assertEquals(request.getDestBucket(), actual.destinationBucket());
        assertEquals(request.getDestKey(), actual.destinationKey());
    }

    @Test
    void testToHeadRequest() {
        var key = "some-key";
        var versionId = "some-version";
        var actual = transformer.toHeadRequest(key, versionId);
        assertEquals(BUCKET, actual.bucket());
        assertEquals(key, actual.key());
        assertEquals(versionId, actual.versionId());
    }

    @Test
    void testToMetadata() {
        var metadata = Map.of("some-key", "some-value");
        Instant now = Instant.now();
        var response = HeadObjectResponse
                .builder()
                .versionId("v1")
                .eTag("etag")
                .contentLength(1024L)
                .metadata(metadata)
                .lastModified(now)
                .build();
        var actual = transformer.toMetadata(response, "some-key");
        assertEquals("some-key", actual.getKey());
        assertEquals("v1", actual.getVersionId());
        assertEquals("etag", actual.getETag());
        assertEquals(metadata, actual.getMetadata());
        assertEquals(1024L, actual.getObjectSize());
        assertEquals(now, actual.getLastModified());
    }

    @Test
    void testToCreateMultipartUploadRequest() {
        Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
        MultipartUploadRequest mpuRequest = new MultipartUploadRequest.Builder()
                .withKey("object-1")
                .withMetadata(metadata)
                .build();
        CreateMultipartUploadRequest request = transformer.toCreateMultipartUploadRequest(mpuRequest);
        assertEquals("object-1", request.key());
        assertEquals(BUCKET, request.bucket());
        assertEquals(metadata, request.metadata());
    }

    @Test
    void testToUploadPartRequest() {
        Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .metadata(metadata)
                .build();
        byte[] content = "This is test data".getBytes();
        MultipartPart multipartPart = new MultipartPart(1, content);
        UploadPartRequest request = transformer.toUploadPartRequest(multipartUpload, multipartPart);
        assertEquals("object-1", request.key());
        assertEquals(BUCKET, request.bucket());
        assertEquals("mpu-id", request.uploadId());
        assertEquals(content.length, request.contentLength());
    }

    @Test
    void testToCompleteMultipartUploadRequest() {
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .build();
        var listOfParts = List.of(
                new com.salesforce.multicloudj.blob.driver.UploadPartResponse(1, "etag1", 3000),
                new com.salesforce.multicloudj.blob.driver.UploadPartResponse(2, "etag2", 2000),
                new com.salesforce.multicloudj.blob.driver.UploadPartResponse(3, "etag3", 1000));
        CompleteMultipartUploadRequest request = transformer.toCompleteMultipartUploadRequest(multipartUpload, listOfParts);
        assertEquals("object-1", request.key());
        assertEquals(BUCKET, request.bucket());
        assertEquals("mpu-id", request.uploadId());

        List<CompletedPart> parts = request.multipartUpload().parts();
        assertEquals(3, parts.size());
        assertEquals(1, parts.get(0).partNumber());
        assertEquals(2, parts.get(1).partNumber());
        assertEquals(3, parts.get(2).partNumber());
        assertEquals("etag1", parts.get(0).eTag());
        assertEquals("etag2", parts.get(1).eTag());
        assertEquals("etag3", parts.get(2).eTag());
    }

    @Test
    void testToListPartsRequest() {
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .build();
        ListPartsRequest request = transformer.toListPartsRequest(multipartUpload);
        assertEquals("object-1", request.key());
        assertEquals(BUCKET, request.bucket());
        assertEquals("mpu-id", request.uploadId());
    }

    @Test
    void testToAbortMultipartUploadRequest() {
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .build();
        AbortMultipartUploadRequest request = transformer.toAbortMultipartUploadRequest(multipartUpload);
        assertEquals("object-1", request.key());
        assertEquals(BUCKET, request.bucket());
        assertEquals("mpu-id", request.uploadId());
    }

    @Test
    void testToGetObjectTaggingRequest() {
        GetObjectTaggingRequest taggingRequest = transformer.toGetObjectTaggingRequest("object-1");
        assertEquals("object-1", taggingRequest.key());
        assertEquals(BUCKET, taggingRequest.bucket());
    }

    @Test
    void testToPutObjectTaggingRequest() {
        Map<String, String> tags = Map.of("key1", "value1", "key2", "value2");
        PutObjectTaggingRequest taggingRequest = transformer.toPutObjectTaggingRequest("object-1", tags);

        assertEquals("object-1", taggingRequest.key());
        assertEquals(BUCKET, taggingRequest.bucket());
        List<Tag> actualTags = taggingRequest.tagging().tagSet();
        assertTrue(actualTags.contains(Tag.builder().key("key1").value("value1").build()));
        assertTrue(actualTags.contains(Tag.builder().key("key2").value("value2").build()));
    }

    @Test
    void testToPutObjectPresignRequest() {
        Map<String, String> metadata = Map.of("some-key", "some-value");
        Map<String, String> tags = Map.of("tag-key", "tag-value");
        PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.UPLOAD)
                .key("object-1")
                .duration(Duration.ofHours(4))
                .metadata(metadata)
                .tags(tags)
                .build();
        PutObjectPresignRequest actualRequest = transformer.toPutObjectPresignRequest(presignedUrlRequest);
        assertEquals(BUCKET, actualRequest.putObjectRequest().bucket());
        assertEquals("object-1", actualRequest.putObjectRequest().key());
        assertEquals(metadata, actualRequest.putObjectRequest().metadata());
        assertEquals("tag-key=tag-value", actualRequest.putObjectRequest().tagging());
        assertEquals(Duration.ofHours(4), actualRequest.signatureDuration());
    }

    @Test
    void testToGetObjectPresignRequest() {
        PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.DOWNLOAD)
                .key("object-1")
                .duration(Duration.ofHours(4))
                .build();
        GetObjectPresignRequest actualRequest = transformer.toGetObjectPresignRequest(presignedUrlRequest);
        assertEquals(BUCKET, actualRequest.getObjectRequest().bucket());
        assertEquals("object-1", actualRequest.getObjectRequest().key());
        assertEquals(Duration.ofHours(4), actualRequest.signatureDuration());
    }

    @Test
    void testGetPrefixExclusionsFilter() {
        List<String> prefixesToExclude = List.of("files/images", "files/personal");
        DownloadFilter downloadFilter = transformer.getPrefixExclusionsFilter(prefixesToExclude);
        assertFalse(downloadFilter.test(S3Object.builder().key("files/images/image1.jpg").build()));
        assertFalse(downloadFilter.test(S3Object.builder().key("files/imagesFromVacation/image1.jpg").build()));
        assertFalse(downloadFilter.test(S3Object.builder().key("files/personal/taxes.csv").build()));
        assertTrue(downloadFilter.test(S3Object.builder().key("files/documents/business.doc").build()));

        prefixesToExclude = List.of();
        downloadFilter = transformer.getPrefixExclusionsFilter(prefixesToExclude);
        assertTrue(downloadFilter.test(S3Object.builder().key("files/images/image1.jpg").build()));
        assertTrue(downloadFilter.test(S3Object.builder().key("files/imagesFromVacation/image1.jpg").build()));
        assertTrue(downloadFilter.test(S3Object.builder().key("files/personal/taxes.csv").build()));
        assertTrue(downloadFilter.test(S3Object.builder().key("files/documents/business.doc").build()));
    }

    @Test
    void testToDownloadDirectoryRequest() {
        String destination = "/home/documents";
        DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
                .localDestinationDirectory(destination)
                .prefixToDownload("/files")
                .prefixesToExclude(List.of("files/images", "files/personal"))
                .build();

        DownloadDirectoryRequest downloadDirectoryRequest = transformer.toDownloadDirectoryRequest(request);

        assertEquals(BUCKET, downloadDirectoryRequest.bucket());
        assertEquals(destination, downloadDirectoryRequest.destination().toString());
        assertNotNull(downloadDirectoryRequest.filter());
        assertNotNull(downloadDirectoryRequest.listObjectsRequestTransformer());
    }

    @Test
    void testToDirectoryDownloadResponse() {
        Exception exception1 = new RuntimeException("Exception1!");
        Path path1 = Paths.get("/files/document1.txt");
        DownloadFileRequest request1 = mock(DownloadFileRequest.class);
        doReturn(path1).when(request1).destination();
        FailedFileDownload failedDownload1 = FailedFileDownload.builder()
                .request(request1)
                .exception(exception1)
                .build();

        Exception exception2 = new RuntimeException("Exception2!");
        Path path2 = Paths.get("/files/document2.txt");
        DownloadFileRequest request2 = mock(DownloadFileRequest.class);
        doReturn(path2).when(request2).destination();
        FailedFileDownload failedDownload2 = FailedFileDownload.builder()
                .request(request2)
                .exception(exception2)
                .build();
        List<FailedFileDownload> failedTransfers = List.of(failedDownload1, failedDownload2);
        CompletedDirectoryDownload completedDirectoryDownload = mock(CompletedDirectoryDownload.class);
        doReturn(failedTransfers).when(completedDirectoryDownload).failedTransfers();

        DirectoryDownloadResponse response = transformer.toDirectoryDownloadResponse(completedDirectoryDownload);

        assertEquals(2, response.getFailedTransfers().size());
        assertEquals(path1, response.getFailedTransfers().get(0).getDestination());
        assertEquals(exception1, response.getFailedTransfers().get(0).getException());
        assertEquals(path2, response.getFailedTransfers().get(1).getDestination());
        assertEquals(exception2, response.getFailedTransfers().get(1).getException());
    }

    @Test
    void testToUploadDirectoryRequest() {
        DirectoryUploadRequest directoryUploadRequest = DirectoryUploadRequest.builder()
                .localSourceDirectory("/home/documents")
                .prefix("/files")
                .includeSubFolders(true)
                .build();
        UploadDirectoryRequest request = transformer.toUploadDirectoryRequest(directoryUploadRequest);
        assertEquals(BUCKET, request.bucket());
        assertTrue(request.maxDepth().isPresent());
        assertEquals(Integer.MAX_VALUE, request.maxDepth().getAsInt());
        assertTrue(request.s3Prefix().isPresent());
        assertEquals("/files", request.s3Prefix().get());
        assertEquals("/home/documents", request.source().toString());


        directoryUploadRequest = DirectoryUploadRequest.builder()
                .localSourceDirectory("/home/documents")
                .prefix("/files")
                .includeSubFolders(false)
                .build();
        request = transformer.toUploadDirectoryRequest(directoryUploadRequest);
        assertTrue(request.maxDepth().isPresent());
    }

    @Test
    void testToDirectoryUploadResponse() {
        Exception exception1 = new RuntimeException("Exception1!");
        Path path1 = Paths.get("/home/documents/files/document1.txt");
        UploadFileRequest request1 = mock(UploadFileRequest.class);
        doReturn(path1).when(request1).source();
        FailedFileUpload failedUpload1 = FailedFileUpload.builder()
                .request(request1)
                .exception(exception1)
                .build();

        Exception exception2 = new RuntimeException("Exception2!");
        Path path2 = Paths.get("/home/documents/files/document2.txt");
        UploadFileRequest request2 = mock(UploadFileRequest.class);
        doReturn(path2).when(request2).source();
        FailedFileUpload failedUpload2 = FailedFileUpload.builder()
                .request(request2)
                .exception(exception2)
                .build();
        List<FailedFileUpload> failedTransfers = List.of(failedUpload1, failedUpload2);
        CompletedDirectoryUpload completedDirectoryUpload = mock(CompletedDirectoryUpload.class);
        doReturn(failedTransfers).when(completedDirectoryUpload).failedTransfers();

        DirectoryUploadResponse response = transformer.toDirectoryUploadResponse(completedDirectoryUpload);

        assertEquals(2, response.getFailedTransfers().size());
        assertEquals(path1, response.getFailedTransfers().get(0).getSource());
        assertEquals(exception1, response.getFailedTransfers().get(0).getException());
        assertEquals(path2, response.getFailedTransfers().get(1).getSource());
        assertEquals(exception2, response.getFailedTransfers().get(1).getException());
    }
}
