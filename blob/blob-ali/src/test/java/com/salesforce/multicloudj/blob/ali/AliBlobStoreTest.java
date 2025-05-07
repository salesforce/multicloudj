package com.salesforce.multicloudj.blob.ali;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.internal.OSSHeaders;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.DeleteVersionsRequest;
import com.aliyun.oss.model.GeneratePresignedUrlRequest;
import com.aliyun.oss.model.GenericRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.ListPartsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PartListing;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.PutObjectResult;
import com.aliyun.oss.model.TagSet;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.PresignedOperation;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.ali.AliConstants;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AliBlobStoreTest {

    private MockedStatic<OSSClientBuilder> staticMockBuilder;

    private OSS mockOssClient;
    private AliBlobStore ali;

    @BeforeEach
    void setup() {
        mockOssClient = mock(OSS.class);
        staticMockBuilder = mockStatic(OSSClientBuilder.class);
        OSSClientBuilder.OSSClientBuilderImpl mockBuilder = mock(OSSClientBuilder.OSSClientBuilderImpl.class);

        staticMockBuilder.when(OSSClientBuilder::create).thenReturn(mockBuilder);
        when(mockBuilder.region(any())).thenReturn(mockBuilder);
        when(mockBuilder.endpoint(any())).thenReturn(mockBuilder);
        when(mockBuilder.credentialsProvider(any())).thenReturn(mockBuilder);
        when(mockBuilder.build()).thenReturn(mockOssClient);

        StsCredentials creds = new StsCredentials("key-1", "secret-1", "token-1");
        CredentialsOverrider credsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(creds).build();
        ali = new AliBlobStore.Builder()
                .withBucket("bucket-1")
                .withRegion("cn-shanghai")
                .withEndpoint(URI.create("https://test.example.com"))
                .withProxyEndpoint(URI.create("http://proxy.example.com:80"))
                .withCredentialsOverrider(credsOverrider)
                .withSocketTimeout(Duration.ofMinutes(1))
                .withIdleConnectionTimeout(Duration.ofMinutes(5))
                .withMaxConnections(100)
                .build();
        credsOverrider = new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE).withRole("role").build();
        ali = new AliBlobStore.Builder().withBucket("bucket-1").withRegion("cn-shanghai").withCredentialsOverrider(credsOverrider).build();
    }

    @AfterEach
    void teardown() {
        if (staticMockBuilder != null) {
            staticMockBuilder.close();
        }
    }

    @Test
    void testProviderId() {
        assertEquals(AliConstants.PROVIDER_ID, ali.getProviderId());
    }

    @Test
    void testExceptionHandling() {
        OSSException ossException = new OSSException("", "AccessDenied", "", "", "", "", "");
        Class<?> cls = ali.getException(ossException);
        assertEquals(cls, UnAuthorizedException.class);

        ClientException clientException = new ClientException();
        cls = ali.getException(clientException);
        assertEquals(cls, InvalidArgumentException.class);

        cls = ali.getException(new IOException("Channel is closed"));
        assertEquals(cls, UnknownException.class);
    }

    @Test
    void testDoUploadInputStream() {
        doReturn(buildTestPutObjectResult()).when(mockOssClient).putObject(any());
        verifyUploadTestResults(ali.doUpload(getTestUploadRequest(), mock(InputStream.class)));
    }

    @Test
    void testDoUploadByteArray() {
        doReturn(buildTestPutObjectResult()).when(mockOssClient).putObject(any());
        verifyUploadTestResults(ali.doUpload(getTestUploadRequest(), new byte[1024]));
    }

    @Test
    void testDoUploadFile() throws IOException {
        doReturn(buildTestPutObjectResult()).when(mockOssClient).putObject(any());
        Path path = null;
        try {
            path = Files.createTempFile("tempFile", ".txt");
            try(BufferedWriter writer = Files.newBufferedWriter(path)) {
                writer.write(new char[1024]);
            }
            verifyUploadTestResults(ali.doUpload(getTestUploadRequest(), path.toFile()));
        } finally {
            // Clean up temp file even if test fails
            if (path != null) {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    Assertions.fail(e);
                }
            }
        }
    }

    @Test
    void testDoUploadPath() throws IOException {
        doReturn(buildTestPutObjectResult()).when(mockOssClient).putObject(any());
        Path path = Files.createTempFile("tempFile", ".txt");
        try(BufferedWriter writer = Files.newBufferedWriter(path)) {
            writer.write(new char[1024]);
        }
        verifyUploadTestResults(ali.doUpload(getTestUploadRequest(), path));
    }

    @Test
    void testComputeRange() {
        Pair<Long, Long> result = ali.computeRange(0L, 500L);
        assertEquals(result.getLeft(), 0);
        assertEquals(result.getRight(), 500);

        result = ali.computeRange(100L, 600L);
        assertEquals(result.getLeft(), 100);
        assertEquals(result.getRight(), 600);

        result = ali.computeRange(null, 500L);
        assertEquals(result.getLeft(), -1);
        assertEquals(result.getRight(), 500);

        result = ali.computeRange(500L, null);
        assertEquals(result.getLeft(), 500);
        assertEquals(result.getRight(), -1);
    }

    void verifyUploadTestResults(UploadResponse uploadResponse) {

        // Verify the parameters passed into the SDK
        ArgumentCaptor<PutObjectRequest> putObjectRequestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(mockOssClient, times(1)).putObject(putObjectRequestCaptor.capture());
        PutObjectRequest actualPutObjectRequest = putObjectRequestCaptor.getValue();
        assertEquals("object-1", actualPutObjectRequest.getKey());
        assertEquals("bucket-1", actualPutObjectRequest.getBucketName());
        assertEquals("tag-1=tag-value-1", actualPutObjectRequest.getMetadata().getRawMetadata().get(OSSHeaders.OSS_TAGGING));
        assertEquals("value-1", actualPutObjectRequest.getMetadata().getUserMetadata().get("key-1"));

        // Verify the mapping of the response into the UploadResponse object
        assertEquals("object-1", uploadResponse.getKey());
        assertEquals("version-1", uploadResponse.getVersionId());
        assertEquals("etag", uploadResponse.getETag());
    }

    @Test
    void testDoDownloadOutputStream() {
        Instant now = Instant.now();
        doReturn(buildTestGetObjectResult(now)).when(mockOssClient).getObject(any());
        verifyDownloadTestResults(ali.doDownload(getTestDownloadRequest(), mock(OutputStream.class)), now);
    }

    @Test
    void testDoDownloadByteArrayWrapper() {
        Instant now = Instant.now();
        doReturn(buildTestGetObjectResult(now)).when(mockOssClient).getObject(any());
        ByteArray byteArray = new ByteArray();
        verifyDownloadTestResults(ali.doDownload(getTestDownloadRequest(), byteArray), now);
        assertEquals("downloadedData", new String(byteArray.getBytes()));
    }

    @Test
    void testDoDownloadFile() {
        Instant now = Instant.now();
        doReturn(buildTestGetObjectResult(now)).when(mockOssClient).getObject(any());
        Path path = Path.of("tempFile.txt");
        try {
            Files.deleteIfExists(path);
            verifyDownloadTestResults(ali.doDownload(getTestDownloadRequest(), path.toFile()), now);
        } catch (IOException e) {
            Assertions.fail(e);
        } finally {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                Assertions.fail(e);
            }
        }
    }

    @Test
    void testDoDownloadPath() {
        Instant now = Instant.now();
        doReturn(buildTestGetObjectResult(now)).when(mockOssClient).getObject(any());
        Path path = Path.of("tempPath.txt");
        try {
            Files.deleteIfExists(path);
            verifyDownloadTestResults(ali.doDownload(getTestDownloadRequest(), path), now);
        } catch (IOException e) {
            Assertions.fail(e);
        } finally {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                Assertions.fail(e);
            }
        }
    }

    void verifyDownloadTestResults(DownloadResponse response, Instant now) {

        // Verify the parameters passed into the SDK
        ArgumentCaptor<GetObjectRequest> getObjectRequestCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(mockOssClient, times(1)).getObject(getObjectRequestCaptor.capture());
        GetObjectRequest actualGetObjectRequest = getObjectRequestCaptor.getValue();
        assertEquals("object-1", actualGetObjectRequest.getKey());
        assertEquals("bucket-1", actualGetObjectRequest.getBucketName());
        assertEquals(10, actualGetObjectRequest.getRange()[0]);
        assertEquals(110, actualGetObjectRequest.getRange()[1]);

        // Verify the response data is properly mapped into the DownloadResponse object
        assertEquals("object-1", response.getKey());
        assertEquals("object-1", response.getMetadata().getKey());
        assertEquals("version-1", response.getMetadata().getVersionId());
        assertEquals("etag1", response.getMetadata().getETag());
        assertEquals(Date.from(now), Date.from(response.getMetadata().getLastModified()));
        assertEquals(Map.of("key1", "value1", "key2", "value2"), response.getMetadata().getMetadata());
        assertEquals(100, response.getMetadata().getObjectSize());
    }

    @Test
    void testDoDelete() {
        ali.doDelete("object-1", "version-1");

        ArgumentCaptor<String> bucketCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> versionCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockOssClient, times(1)).deleteVersion(bucketCaptor.capture(), keyCaptor.capture(), versionCaptor.capture());
        assertEquals("bucket-1", bucketCaptor.getValue());
        assertEquals("object-1", keyCaptor.getValue());
        assertEquals("version-1", versionCaptor.getValue());
    }

    @Test
    void testDoBulkDelete() {
        List<BlobIdentifier> objects = List.of(new BlobIdentifier("object-1","version-1"),
                new BlobIdentifier("object-2","version-2"),
                new BlobIdentifier("object-3","version-3"));
        ali.doDelete(objects);

        ArgumentCaptor<DeleteVersionsRequest> deleteVersionsRequestCaptor = ArgumentCaptor.forClass(DeleteVersionsRequest.class);
        verify(mockOssClient, times(1)).deleteVersions(deleteVersionsRequestCaptor.capture());
        DeleteVersionsRequest actualDeleteVersionsRequest = deleteVersionsRequestCaptor.getValue();
        assertEquals("bucket-1", actualDeleteVersionsRequest.getBucketName());
        Map<String, String> objectsMap = objects.stream().collect(Collectors.toMap(BlobIdentifier::getKey, BlobIdentifier::getVersionId));
        for(DeleteVersionsRequest.KeyVersion key : actualDeleteVersionsRequest.getKeys()){
            assertEquals(objectsMap.get(key.getKey()), key.getVersion());
        }
    }

    @Test
    void testDoCopy() {
        Instant now = Instant.now();
        CopyObjectResult mockResult = mock(CopyObjectResult.class);
        doReturn("copyVersion-1").when(mockResult).getVersionId();
        doReturn("eTag-1").when(mockResult).getETag();
        doReturn(Date.from(now)).when(mockResult).getLastModified();
        when(mockOssClient.copyObject(any())).thenReturn(mockResult);

        CopyRequest copyRequest = CopyRequest.builder()
                .srcKey("src-object-1")
                .srcVersionId("version-1")
                .destBucket("dest-bucket-1")
                .destKey("dest-object-1")
                .build();

        CopyResponse copyResponse = ali.doCopy(copyRequest);

        assertEquals("dest-object-1", copyResponse.getKey());
        assertEquals("copyVersion-1", copyResponse.getVersionId());
        assertEquals("eTag-1", copyResponse.getETag());
        assertEquals(Date.from(now).toInstant(), copyResponse.getLastModified());

        ArgumentCaptor<CopyObjectRequest> copyObjectRequestCaptor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        verify(mockOssClient, times(1)).copyObject(copyObjectRequestCaptor.capture());
        CopyObjectRequest actualCopyObjectRequest = copyObjectRequestCaptor.getValue();
        assertEquals("bucket-1", actualCopyObjectRequest.getSourceBucketName());
        assertEquals("src-object-1", actualCopyObjectRequest.getSourceKey());
        assertEquals("version-1", actualCopyObjectRequest.getSourceVersionId());
        assertEquals("dest-bucket-1", actualCopyObjectRequest.getDestinationBucketName());
        assertEquals("dest-object-1", actualCopyObjectRequest.getDestinationKey());
    }

    @Test
    void testDoGetMetadata() {
        Instant now = Instant.now();
        Map<String, String> metadataMap = Map.of("key1", "value1", "key2", "value2");
        ObjectMetadata mockResponse = mock(ObjectMetadata.class);
        when(mockResponse.getVersionId()).thenReturn("v1");
        when(mockResponse.getETag()).thenReturn("etag");
        when(mockResponse.getContentLength()).thenReturn(1024L);
        when(mockResponse.getUserMetadata()).thenReturn(metadataMap);
        when(mockResponse.getLastModified()).thenReturn(Date.from(now));
        when(mockOssClient.getObjectMetadata(any())).thenReturn(mockResponse);

        BlobMetadata metadata = ali.doGetMetadata("object-1", "v1");

        ArgumentCaptor<GenericRequest> genericRequestCaptor = ArgumentCaptor.forClass(GenericRequest.class);
        verify(mockOssClient, times(1)).getObjectMetadata(genericRequestCaptor.capture());

        GenericRequest genericRequest = genericRequestCaptor.getValue();
        assertEquals("bucket-1", genericRequest.getBucketName());
        assertEquals("object-1", genericRequest.getKey());
        assertEquals("v1", genericRequest.getVersionId());

        assertEquals("object-1", metadata.getKey());
        assertEquals("v1", metadata.getVersionId());
        assertEquals("etag", metadata.getETag());
        assertEquals(1024L, metadata.getObjectSize());
        assertEquals(metadataMap, metadata.getMetadata());
        assertEquals(Date.from(now), Date.from(metadata.getLastModified()));
    }

    @Test
    void testDoListEmpty() {
        ListBlobsRequest request = new ListBlobsRequest.Builder().build();
        List<OSSObjectSummary> list = List.of();
        ObjectListing mockObjectListing = mock(ObjectListing.class);
        when(mockOssClient.listObjects((ListObjectsRequest) any())).thenReturn(mockObjectListing);
        when(mockObjectListing.getObjectSummaries()).thenReturn(list);

        Iterator<BlobInfo> iterator = ali.doList(request);
        assertThrows(NoSuchElementException.class, () -> {
            iterator.next();
        });
    }

    @Test
    void testDoList() {
        ListBlobsRequest request = new ListBlobsRequest.Builder().withPrefix("abc").withDelimiter("/").build();
        ObjectListing mockObjectListing = mock(ObjectListing.class);
        when(mockOssClient.listObjects((ListObjectsRequest) any())).thenReturn(mockObjectListing);
        List<OSSObjectSummary> list = getList();
        when(mockObjectListing.getObjectSummaries()).thenReturn(list);

        Iterator<BlobInfo> iterator = ali.doList(request);
        assertNotNull(iterator);

        int count = 1;
        while(iterator.hasNext()) {
            BlobInfo blobInfo = iterator.next();
            int current = count++;
            assertEquals("key-" + current, blobInfo.getKey());
            assertEquals(current, blobInfo.getObjectSize());
        }
    }

    private List<OSSObjectSummary> getList() {
        List<OSSObjectSummary> list = new ArrayList<>();
        IntStream.range(1, 100).forEach(
                (i) -> {
                    OSSObjectSummary mockObjectSummary = mock(OSSObjectSummary.class);
                    when(mockObjectSummary.getKey()).thenReturn("key-" + i);
                    when(mockObjectSummary.getSize()).thenReturn((long) i);
                    list.add(mockObjectSummary);
                }
        );
        return list;
    }

    @Test
    void testDoInitiateMultipartUpload() {
        InitiateMultipartUploadResult mockResponse = mock(InitiateMultipartUploadResult.class);
        when(mockOssClient.initiateMultipartUpload((InitiateMultipartUploadRequest) any())).thenReturn(mockResponse);
        Map<String, String> metadata = Map.of("key-1", "value-1");
        MultipartUploadRequest request = new MultipartUploadRequest.Builder().withKey("object-1").withMetadata(metadata).build();

        ali.initiateMultipartUpload(request);

        ArgumentCaptor<InitiateMultipartUploadRequest> requestCaptor = ArgumentCaptor.forClass(InitiateMultipartUploadRequest.class);
        verify(mockOssClient, times(1)).initiateMultipartUpload(requestCaptor.capture());
        InitiateMultipartUploadRequest actualRequest = requestCaptor.getValue();
        assertEquals("object-1", actualRequest.getKey());
        assertEquals("bucket-1", actualRequest.getBucketName());
        assertEquals(metadata, actualRequest.getObjectMetadata().getUserMetadata());
    }

    @Test
    void testDoUploadMultipartPart() {
        UploadPartResult mockResponse = mock(UploadPartResult.class);
        doReturn(new PartETag(1, "etag")).when(mockResponse).getPartETag();
        when(mockOssClient.uploadPart(any())).thenReturn(mockResponse);
        MultipartUpload multipartUpload = new MultipartUpload("bucket-1", "object-1", "mpu-id");
        byte[] content = "This is test data".getBytes(StandardCharsets.UTF_8);
        MultipartPart multipartPart = new MultipartPart(1, content);

        ali.uploadMultipartPart(multipartUpload, multipartPart);

        ArgumentCaptor<UploadPartRequest> requestCaptor = ArgumentCaptor.forClass(UploadPartRequest.class);
        verify(mockOssClient, times(1)).uploadPart(requestCaptor.capture());
        UploadPartRequest actualRequest = requestCaptor.getValue();
        assertEquals("object-1", actualRequest.getKey());
        assertEquals("bucket-1", actualRequest.getBucketName());
        assertEquals("mpu-id", actualRequest.getUploadId());
        assertEquals(1, actualRequest.getPartNumber());
    }

    @Test
    void testDoCompleteMultipartUpload() {
        CompleteMultipartUploadResult mockResponse = mock(CompleteMultipartUploadResult.class);
        when(mockOssClient.completeMultipartUpload(any())).thenReturn(mockResponse);
        MultipartUpload multipartUpload = new MultipartUpload("bucket-1", "object-1", "mpu-id");
        List<com.salesforce.multicloudj.blob.driver.UploadPartResponse> listOfParts = List.of(new com.salesforce.multicloudj.blob.driver.UploadPartResponse(1, "etag", 0));

        ali.completeMultipartUpload(multipartUpload, listOfParts);

        ArgumentCaptor<CompleteMultipartUploadRequest> requestCaptor = ArgumentCaptor.forClass(CompleteMultipartUploadRequest.class);
        verify(mockOssClient, times(1)).completeMultipartUpload(requestCaptor.capture());
        CompleteMultipartUploadRequest actualRequest = requestCaptor.getValue();
        assertEquals("object-1", actualRequest.getKey());
        assertEquals("bucket-1", actualRequest.getBucketName());
        assertEquals("mpu-id", actualRequest.getUploadId());
        List<PartETag> parts = actualRequest.getPartETags();
        assertEquals(1, parts.size());
        assertEquals(1, parts.get(0).getPartNumber());
        assertEquals("etag", parts.get(0).getETag());
    }

    @Test
    void testDoListMultipartUpload() {
        PartListing mockResponse = mock(PartListing.class);
        when(mockOssClient.listParts(any())).thenReturn(mockResponse);
        MultipartUpload multipartUpload = new MultipartUpload("bucket-1", "object-1", "mpu-id");

        ali.listMultipartUpload(multipartUpload);

        ArgumentCaptor<ListPartsRequest> requestCaptor = ArgumentCaptor.forClass(ListPartsRequest.class);
        verify(mockOssClient, times(1)).listParts(requestCaptor.capture());
        ListPartsRequest actualRequest = requestCaptor.getValue();
        assertEquals("object-1", actualRequest.getKey());
        assertEquals("bucket-1", actualRequest.getBucketName());
        assertEquals("mpu-id", actualRequest.getUploadId());
    }

    @Test
    void testDoAbortMultipartUpload() {
        MultipartUpload multipartUpload = new MultipartUpload("bucket-1", "object-1", "mpu-id");

        ali.abortMultipartUpload(multipartUpload);

        ArgumentCaptor<AbortMultipartUploadRequest> requestCaptor = ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
        verify(mockOssClient, times(1)).abortMultipartUpload(requestCaptor.capture());
        AbortMultipartUploadRequest actualRequest = requestCaptor.getValue();
        assertEquals("object-1", actualRequest.getKey());
        assertEquals("bucket-1", actualRequest.getBucketName());
        assertEquals("mpu-id", actualRequest.getUploadId());
    }

    @Test
    void testDoGetTags() {
        TagSet mockResponse = mock(TagSet.class);
        Map<String, String> tags = Map.of("key1", "value1", "key2", "value2");
        doReturn(tags).when(mockResponse).getAllTags();
        when(mockOssClient.getObjectTagging(any(), any())).thenReturn(mockResponse);

        Map<String,String> tagsResult = ali.getTags("object-1");

        assertEquals(tags, tagsResult);
    }

    @Test
    void testDoSetTags() {
        Map<String, String> tags = Map.of("key1", "value1", "key2", "value2");
        ali.setTags("object-1", tags);

        ArgumentCaptor<TagSet> tagSetRequestCaptor = ArgumentCaptor.forClass(TagSet.class);
        ArgumentCaptor<String> bucketNameRequestCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyRequestCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockOssClient, times(1)).setObjectTagging(bucketNameRequestCaptor.capture(), keyRequestCaptor.capture(), tagSetRequestCaptor.capture());

        String actualBucketNameRequestCaptor = bucketNameRequestCaptor.getValue();
        String actualKeyRequestCaptor = keyRequestCaptor.getValue();
        TagSet actualTagSetRequestCaptor = tagSetRequestCaptor.getValue();
        assertEquals("bucket-1", actualBucketNameRequestCaptor);
        assertEquals("object-1", actualKeyRequestCaptor);
        assertEquals(tags, actualTagSetRequestCaptor.getAllTags());
    }

    @Test
    void testDoGeneratePresignedUploadUrl() {
        UploadRequest uploadRequest = getTestUploadRequest();
        Duration duration = Duration.ofHours(12);
        PresignedUrlRequest presignedUploadRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.UPLOAD)
                .key(uploadRequest.getKey())
                .metadata(uploadRequest.getMetadata())
                .tags(uploadRequest.getTags())
                .duration(duration)
                .build();

        ali.doGeneratePresignedUploadUrl(presignedUploadRequest);

        ArgumentCaptor<GeneratePresignedUrlRequest> generatePresignedUrlRequestCaptor = ArgumentCaptor.forClass(GeneratePresignedUrlRequest.class);
        verify(mockOssClient, times(1)).generatePresignedUrl(generatePresignedUrlRequestCaptor.capture());
        GeneratePresignedUrlRequest actualRequest = generatePresignedUrlRequestCaptor.getValue();
        assertEquals(HttpMethod.PUT, actualRequest.getMethod());
        assertEquals("bucket-1", actualRequest.getBucketName());
        assertEquals("object-1", actualRequest.getKey());
        Map<String,String> headers = actualRequest.getHeaders();
        assertEquals("tag-1=tag-value-1", headers.get(OSSHeaders.OSS_TAGGING));
        assertEquals("value-1", actualRequest.getUserMetadata().get("key-1"));
        assertNotNull(actualRequest.getExpiration());
    }

    @Test
    void testDoGeneratePresignedDownloadUrl() {
        Duration duration = Duration.ofHours(12);
        PresignedUrlRequest presignedDownloadRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.DOWNLOAD)
                .key("object-1")
                .duration(duration)
                .build();

        ali.doGeneratePresignedDownloadUrl(presignedDownloadRequest);

        ArgumentCaptor<GeneratePresignedUrlRequest> generatePresignedUrlRequestCaptor = ArgumentCaptor.forClass(GeneratePresignedUrlRequest.class);
        verify(mockOssClient, times(1)).generatePresignedUrl(generatePresignedUrlRequestCaptor.capture());
        GeneratePresignedUrlRequest actualRequest = generatePresignedUrlRequestCaptor.getValue();
        assertEquals(HttpMethod.GET, actualRequest.getMethod());
        assertEquals("bucket-1", actualRequest.getBucketName());
        assertEquals("object-1", actualRequest.getKey());
    }

    private UploadRequest getTestUploadRequest() {
        Map<String, String> metadata = Map.of("key-1", "value-1");
        Map<String, String> tags = Map.of("tag-1", "tag-value-1");
        return new UploadRequest.Builder()
                .withKey("object-1")
                .withContentLength(1024)
                .withMetadata(metadata)
                .withTags(tags)
                .build();
    }

    private PutObjectResult buildTestPutObjectResult() {
        PutObjectResult putObjectResult = mock(PutObjectResult.class);
        doReturn("version-1").when(putObjectResult).getVersionId();
        doReturn("etag").when(putObjectResult).getETag();
        return putObjectResult;
    }

    private DownloadRequest getTestDownloadRequest() {
        return new DownloadRequest.Builder().withKey("object-1").withVersionId("version-1").withRange(10L, 110L).build();
    }

    private OSSObject buildTestGetObjectResult(Instant now) {
        Map<String, String> metadataMap = Map.of("key1", "value1", "key2", "value2");
        InputStream inputStream = new ByteArrayInputStream("downloadedData".getBytes());
        OSSObject ossObject = mock(OSSObject.class);
        ObjectMetadata objectMetadata = mock(ObjectMetadata.class);
        doReturn(objectMetadata).when(ossObject).getObjectMetadata();
        doReturn("object-1").when(ossObject).getKey();
        doReturn("version-1").when(objectMetadata).getVersionId();
        doReturn("etag1").when(objectMetadata).getETag();
        doReturn(Date.from(now)).when(objectMetadata).getLastModified();
        doReturn(metadataMap).when(objectMetadata).getUserMetadata();
        doReturn(100L).when(objectMetadata).getContentLength();
        doReturn(inputStream).when(ossObject).getObjectContent();
        return ossObject;
    }
}
