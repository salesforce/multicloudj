package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.MultipartUploadResponse;
import com.salesforce.multicloudj.blob.driver.PresignedOperation;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectResult;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ListPartsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AwsBlobStoreTest {

    private MockedStatic<S3Client> s3Client;
    private S3Client mockS3Client;
    private AwsBlobStore aws;
    private final AwsTransformerSupplier transformerSupplier = new AwsTransformerSupplier();

    @BeforeEach
    void setup() {
        S3ClientBuilder mockBuilder = mock(S3ClientBuilder.class);
        when(mockBuilder.region(any())).thenReturn(mockBuilder);

        s3Client = mockStatic(S3Client.class);
        s3Client.when(S3Client::builder).thenReturn(mockBuilder);

        mockS3Client = mock(S3Client.class);
        when(mockBuilder.build()).thenReturn(mockS3Client);
        when(mockBuilder.credentialsProvider(any())).thenReturn(mockBuilder);
        StsCredentials sessionCreds = new StsCredentials("key-1", "secret-1", "token-1");
        CredentialsOverrider credsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(sessionCreds).build();

        aws = new AwsBlobStore.Builder().withTransformerSupplier(transformerSupplier)
                .withCredentialsOverrider(credsOverrider)
                .withBucket("bucket-1")
                .withRegion("us-east-2")
                .withEndpoint(URI.create("https://blob.endpoint.com"))
                .withProxyEndpoint(URI.create("https://proxy.endpoint.com:443"))
                .withSocketTimeout(Duration.ofMinutes(1))
                .withIdleConnectionTimeout(Duration.ofMinutes(5))
                .withMaxConnections(100)
                .build();
        credsOverrider = new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE).withRole("some-role").build();
        aws = new AwsBlobStore.Builder().withTransformerSupplier(transformerSupplier)
                .withCredentialsOverrider(credsOverrider)
                .withBucket("bucket-1").withRegion("us-east-2").build();
    }

    @AfterEach
    void teardown() {
        if (s3Client != null) {
            s3Client.close();
        }
    }

    @Test
    void testProviderId() {
        assertEquals("aws", aws.getProviderId());
    }

    @Test
    void testShouldConfigureHttpClient() {
        var builderWithProxy = new AwsBlobStore.Builder()
                .withTransformerSupplier(transformerSupplier)
                .withBucket("bucket-1").withRegion("us-east-2")
                .withProxyEndpoint(URI.create("https://proxy.endpoint.com:443"));
        assertTrue(AwsBlobStore.shouldConfigureHttpClient((AwsBlobStore.Builder)builderWithProxy));

        var builderWithMaxConnections = new AwsBlobStore.Builder()
                .withTransformerSupplier(transformerSupplier)
                .withBucket("bucket-1").withRegion("us-east-2")
                .withMaxConnections(10);
        assertTrue(AwsBlobStore.shouldConfigureHttpClient((AwsBlobStore.Builder)builderWithMaxConnections));

        var builderWithSocketTimeout = new AwsBlobStore.Builder()
                .withTransformerSupplier(transformerSupplier)
                .withBucket("bucket-1").withRegion("us-east-2")
                .withSocketTimeout(Duration.ofSeconds(10));
        assertTrue(AwsBlobStore.shouldConfigureHttpClient((AwsBlobStore.Builder)builderWithSocketTimeout));

        var builderWithIdleConnectionTimeout = new AwsBlobStore.Builder()
                .withTransformerSupplier(transformerSupplier)
                .withBucket("bucket-1").withRegion("us-east-2")
                .withIdleConnectionTimeout(Duration.ofSeconds(10));
        assertTrue(AwsBlobStore.shouldConfigureHttpClient((AwsBlobStore.Builder)builderWithIdleConnectionTimeout));

        var builderWithNoOverrides = new AwsBlobStore.Builder()
                .withTransformerSupplier(transformerSupplier)
                .withBucket("bucket-1").withRegion("us-east-2");
        assertFalse(AwsBlobStore.shouldConfigureHttpClient((AwsBlobStore.Builder)builderWithNoOverrides));
    }

    @Test
    void testExceptionHandling() {
        AwsServiceException awsServiceException = AwsServiceException.builder()
                .awsErrorDetails(
                        AwsErrorDetails.builder()
                                .errorCode("IncompleteSignature")
                                .build())
                .build();
        Class<?> cls = aws.getException(awsServiceException);
        assertEquals(cls, InvalidArgumentException.class);

        SdkClientException sdkClientException = SdkClientException.builder().build();
        cls = aws.getException(sdkClientException);
        assertEquals(cls, InvalidArgumentException.class);

        cls = aws.getException(new IOException("Channel is closed"));
        assertEquals(cls, UnknownException.class);
    }

    private UploadRequest buildTestUploadRequest() {
        Map<String, String> metadata = Map.of("key-1", "value-1");
        Map<String, String> tags = Map.of("tag-1", "tag-value-1");
        return new UploadRequest.Builder()
                .withKey("object-1")
                .withContentLength(1024)
                .withMetadata(metadata)
                .withTags(tags)
                .build();
    }

    private PutObjectResponse buildMockPutObjectResponse() {
        PutObjectResponse mockResponse = mock(PutObjectResponse.class);
        doReturn("version-1").when(mockResponse).versionId();
        doReturn("etag").when(mockResponse).eTag();
        doReturn(1024L).when(mockResponse).size();
        return mockResponse;
    }

    @Test
    void testDoUploadInputStream() {
        doReturn(buildMockPutObjectResponse()).when(mockS3Client).putObject((PutObjectRequest) any(), (RequestBody) any());
        verifyUploadTestResults(aws.doUpload(buildTestUploadRequest(), mock(InputStream.class)));
    }

    @Test
    void testDoUploadByteArray() {
        doReturn(buildMockPutObjectResponse()).when(mockS3Client).putObject((PutObjectRequest) any(), (RequestBody) any());
        verifyUploadTestResults(aws.doUpload(buildTestUploadRequest(), new byte[1024]));
    }

    @Test
    void testDoUploadFile() throws IOException {
        doReturn(buildMockPutObjectResponse()).when(mockS3Client).putObject((PutObjectRequest) any(), (RequestBody) any());
        Path path = null;
        try {
            path = Files.createTempFile("tempFile", ".txt");
            try(BufferedWriter writer = Files.newBufferedWriter(path)) {
                writer.write(new char[1024]);
            }
            verifyUploadTestResults(aws.doUpload(buildTestUploadRequest(), path.toFile()));
        } finally {
            // Clean up temp file even if test fails
            if (path != null) {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    Assertions.fail();
                }
            }
        }
    }

    @Test
    void testDoUploadPath() throws IOException {
        doReturn(buildMockPutObjectResponse()).when(mockS3Client).putObject((PutObjectRequest) any(), (RequestBody) any());
        Path path = null;
        try {
            path = Files.createTempFile("tempFile", ".txt");
            try(BufferedWriter writer = Files.newBufferedWriter(path)) {
                writer.write(new char[1024]);
            }
            verifyUploadTestResults(aws.doUpload(buildTestUploadRequest(), path));
        } finally {
            // Clean up temp file even if test fails
            if (path != null) {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    Assertions.fail();
                }
            }
        }
    }

    void verifyUploadTestResults(UploadResponse uploadResponse) {

        // Verify the parameters passed into the SDK
        ArgumentCaptor<PutObjectRequest> putObjectRequestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> requestBodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(mockS3Client, times(1)).putObject(putObjectRequestCaptor.capture(), requestBodyCaptor.capture());
        PutObjectRequest actualPutObjectRequest = putObjectRequestCaptor.getValue();
        assertEquals("bucket-1", actualPutObjectRequest.bucket());
        assertEquals("object-1", actualPutObjectRequest.key());
        assertEquals("value-1", actualPutObjectRequest.metadata().get("key-1"));
        assertEquals("tag-1=tag-value-1", actualPutObjectRequest.tagging());
        RequestBody actualRequestBody = requestBodyCaptor.getValue();
        assertTrue(actualRequestBody.optionalContentLength().isPresent());
        assertEquals(1024L, actualRequestBody.optionalContentLength().get());

        // Verify the mapping of the response into the UploadResponse object
        assertEquals("object-1", uploadResponse.getKey());
        assertEquals("version-1", uploadResponse.getVersionId());
        assertEquals("etag", uploadResponse.getETag());
    }

    private DownloadRequest buildTestDownloadRequest() {
        return new DownloadRequest.Builder().withKey("object-1").withVersionId("version-1").withRange(10L, 110L).build();
    }

    private void setupMockGetObjectResponse(Instant now, Boolean getBytes) {
        Map<String, String> metadataMap = Map.of("key1", "value1", "key2", "value2");
        GetObjectResponse getObjectResponse = mock(GetObjectResponse.class);
        doReturn("version-1").when(getObjectResponse).versionId();
        doReturn("etag1").when(getObjectResponse).eTag();
        doReturn(now).when(getObjectResponse).lastModified();
        doReturn(metadataMap).when(getObjectResponse).metadata();
        doReturn(100L).when(getObjectResponse).contentLength();

        ResponseBytes<GetObjectResponse> responseBytes = mock(ResponseBytes.class);
        doReturn("downloadedData".getBytes()).when(responseBytes).asByteArray();
        doReturn(getObjectResponse).when(responseBytes).response();

        if (getBytes) {
            // For toBytes transformer, return ResponseBytes
            doReturn(responseBytes).when(mockS3Client).getObject(any(GetObjectRequest.class), ArgumentMatchers.<ResponseTransformer<GetObjectResponse, ?>>any());
        } else {
            // For other transformers (toOutputStream, toFile, etc.), return GetObjectResponse
            doReturn(getObjectResponse).when(mockS3Client).getObject(any(GetObjectRequest.class), ArgumentMatchers.<ResponseTransformer<GetObjectResponse, ?>>any());
        }

        ResponseInputStream<GetObjectResponse> responseInputStream = mock(ResponseInputStream.class);
        try {
            doReturn("downloadedDataFromStream".getBytes()).when(responseInputStream).readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        doReturn(responseInputStream).when(mockS3Client).getObject(any(GetObjectRequest.class));
        doReturn(getObjectResponse).when(responseInputStream).response();
    }

    @Test
    void testDoDownloadOutputStream() {
        OutputStream mockContent = mock(OutputStream.class);
        Instant now = Instant.now();
        setupMockGetObjectResponse(now, false);
        DownloadResponse response = aws.doDownload(buildTestDownloadRequest(), mockContent);
        verifyDownloadTestResults(response, now, true);
    }

    @Test
    void testDoDownloadByteArrayWrapper() {
        ByteArray byteArray = new ByteArray();
        Instant now = Instant.now();
        setupMockGetObjectResponse(now, true);
        verifyDownloadTestResults(aws.doDownload(buildTestDownloadRequest(), byteArray), now, true);
        assertEquals("downloadedData", new String(byteArray.getBytes()));
    }

    @Test
    void testDoDownloadFile() {
        Instant now = Instant.now();
        setupMockGetObjectResponse(now, false);
        Path path = Path.of("tempFile.txt");
        try {
            Files.deleteIfExists(path);
            verifyDownloadTestResults(aws.doDownload(buildTestDownloadRequest(), path.toFile()), now, true);
        } catch (IOException e) {
            Assertions.fail();
        } finally {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                Assertions.fail();
            }
        }
    }

    @Test
    void testDoDownloadPath() {
        Instant now = Instant.now();
        setupMockGetObjectResponse(now, false);
        Path path = Path.of("tempPath.txt");
        try {
            Files.deleteIfExists(path);
            verifyDownloadTestResults(aws.doDownload(buildTestDownloadRequest(), path), now, true);
        } catch (IOException e) {
            Assertions.fail();
        } finally {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                Assertions.fail();
            }
        }
    }

    @Test
    void testDoDownloadInputStream() {
        Instant now = Instant.now();
        setupMockGetObjectResponse(now, false);
        DownloadResponse response = aws.download(buildTestDownloadRequest());
        verifyDownloadTestResults(response, now, false);
        try {
            assertArrayEquals("downloadedDataFromStream".getBytes(), response.getInputStream().readAllBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void verifyDownloadTestResults(DownloadResponse response, Instant now, Boolean responseTransfer) {

        ArgumentCaptor<GetObjectRequest> getObjectRequestCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        if (responseTransfer) {
            verify(mockS3Client, times(1)).getObject(getObjectRequestCaptor.capture(), (ResponseTransformer<GetObjectResponse, Object>) any());
        } else {
            verify(mockS3Client, times(1)).getObject(getObjectRequestCaptor.capture());
        }
        GetObjectRequest actualGetObjectRequest = getObjectRequestCaptor.getValue();
        assertEquals("bucket-1", actualGetObjectRequest.bucket());
        assertEquals("object-1", actualGetObjectRequest.key());
        assertEquals("version-1", actualGetObjectRequest.versionId());
        assertEquals("bytes=10-110", actualGetObjectRequest.range());

        // Verify the response data is properly mapped into the DownloadResponse object
        assertEquals("object-1", response.getKey());
        assertEquals("object-1", response.getMetadata().getKey());
        assertEquals("version-1", response.getMetadata().getVersionId());
        assertEquals("etag1", response.getMetadata().getETag());
        assertEquals(now, response.getMetadata().getLastModified());
        assertEquals(Map.of("key1", "value1", "key2", "value2"), response.getMetadata().getMetadata());
        assertEquals(100, response.getMetadata().getObjectSize());
    }

    @Test
    void testDoDelete() {
        aws.doDelete("object-1", "version-1");

        ArgumentCaptor<DeleteObjectRequest> deleteObjectRequestCaptor = ArgumentCaptor.forClass(DeleteObjectRequest.class);
        verify(mockS3Client, times(1)).deleteObject(deleteObjectRequestCaptor.capture());
        DeleteObjectRequest actualDeleteObjectRequest = deleteObjectRequestCaptor.getValue();
        assertEquals("object-1", actualDeleteObjectRequest.key());
        assertEquals("bucket-1", actualDeleteObjectRequest.bucket());
    }

    @Test
    void testDoBulkDelete() {
        List<BlobIdentifier> objects = List.of(new BlobIdentifier("object-1","version-1"),
                new BlobIdentifier("object-2","version-2"),
                new BlobIdentifier("object-3","version-3"));
        aws.doDelete(objects);

        ArgumentCaptor<DeleteObjectsRequest> deleteObjectsRequestCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(mockS3Client, times(1)).deleteObjects(deleteObjectsRequestCaptor.capture());
        DeleteObjectsRequest actualDeleteObjectsRequest = deleteObjectsRequestCaptor.getValue();
        assertEquals("bucket-1", actualDeleteObjectsRequest.bucket());

        Map<String, String> objectsMap = objects.stream().collect(Collectors.toMap(BlobIdentifier::getKey, BlobIdentifier::getVersionId));

        List<ObjectIdentifier> identifiers = actualDeleteObjectsRequest.delete().objects();
        for(ObjectIdentifier objectIdentifier : identifiers){
            assertEquals(objectsMap.get(objectIdentifier.key()), objectIdentifier.versionId());
        }
    }

    @Test
    void testDoCopy() {

        Instant now = Instant.now();
        CopyObjectResult mockResult = mock(CopyObjectResult.class);
        doReturn("eTag-1").when(mockResult).eTag();
        doReturn(now).when(mockResult).lastModified();

        CopyObjectResponse mockResponse = mock(CopyObjectResponse.class);
        doReturn(mockResult).when(mockResponse).copyObjectResult();
        doReturn("copyVersion-1").when(mockResponse).versionId();

        when(mockS3Client.copyObject((CopyObjectRequest) any())).thenReturn(mockResponse);

        CopyRequest copyRequest = CopyRequest.builder()
                .srcKey("src-object-1")
                .srcVersionId("version-1")
                .destBucket("dest-bucket-1")
                .destKey("dest-object-1")
                .build();

        CopyResponse copyResponse = aws.doCopy(copyRequest);

        assertEquals("dest-object-1", copyResponse.getKey());
        assertEquals("copyVersion-1", copyResponse.getVersionId());
        assertEquals("eTag-1", copyResponse.getETag());
        assertEquals(now, copyResponse.getLastModified());

        ArgumentCaptor<CopyObjectRequest> copyObjectRequestCaptor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        verify(mockS3Client, times(1)).copyObject(copyObjectRequestCaptor.capture());
        CopyObjectRequest actualCopyObjectRequest = copyObjectRequestCaptor.getValue();
        assertEquals("bucket-1", actualCopyObjectRequest.sourceBucket());
        assertEquals("src-object-1", actualCopyObjectRequest.sourceKey());
        assertEquals("version-1", actualCopyObjectRequest.sourceVersionId());
        assertEquals("dest-bucket-1", actualCopyObjectRequest.destinationBucket());
        assertEquals("dest-object-1", actualCopyObjectRequest.destinationKey());
    }

    @Test
    void testDoGetMetadata() {
        Instant now = Instant.now();
        Map<String, String> metadataMap = Map.of("key1", "value1", "key2", "value2");
        HeadObjectResponse mockResponse = mock(HeadObjectResponse.class);
        when(mockResponse.versionId()).thenReturn("v1");
        when(mockResponse.eTag()).thenReturn("\"5d41402abc4b2a76b9719d911017c592\"");
        when(mockResponse.contentLength()).thenReturn(1024L);
        when(mockResponse.metadata()).thenReturn(metadataMap);
        when(mockResponse.lastModified()).thenReturn(now);
        when(mockS3Client.headObject((HeadObjectRequest) any())).thenReturn(mockResponse);

        BlobMetadata metadata = aws.doGetMetadata("object-1", "v1");
        ArgumentCaptor<HeadObjectRequest> headObjectRequestCaptor = ArgumentCaptor.forClass(HeadObjectRequest.class);
        verify(mockS3Client, times(1)).headObject(headObjectRequestCaptor.capture());
        HeadObjectRequest actualHeadObjectRequest = headObjectRequestCaptor.getValue();
        assertEquals("object-1", actualHeadObjectRequest.key());
        assertEquals("v1", actualHeadObjectRequest.versionId());
        assertEquals("bucket-1", actualHeadObjectRequest.bucket());
        assertEquals("object-1", metadata.getKey());
        assertEquals("v1", metadata.getVersionId());
        assertEquals("\"5d41402abc4b2a76b9719d911017c592\"", metadata.getETag());
        assertEquals(1024L, metadata.getObjectSize());
        assertEquals(metadataMap, metadata.getMetadata());
        assertEquals(now, metadata.getLastModified());
        byte[] expectedMd5 = {93, 65, 64, 42, -68, 75, 42, 118, -71, 113, -99, -111, 16, 23, -59, -110};
        assertArrayEquals(expectedMd5, metadata.getMd5());
    }

    @Test
    void testBadETag() {
        HeadObjectResponse mockResponse = mock(HeadObjectResponse.class);
        when(mockS3Client.headObject((HeadObjectRequest) any())).thenReturn(mockResponse);

        // eTag is null
        when(mockResponse.eTag()).thenReturn(null);
        BlobMetadata metadata = aws.doGetMetadata("object-1", "v1");
        assertArrayEquals(new byte[0], metadata.getMd5());

        // eTag's length < 2
        when(mockResponse.eTag()).thenReturn("b");
        metadata = aws.doGetMetadata("object-1", "v1");
        assertArrayEquals(new byte[0], metadata.getMd5());

        // eTag does not begin with double-quote
        when(mockResponse.eTag()).thenReturn("5d41402abc4b2a76b9719d911017c592\"");
        metadata = aws.doGetMetadata("object-1", "v1");
        assertArrayEquals(new byte[0], metadata.getMd5());

        // eTag does not end with double-quote
        when(mockResponse.eTag()).thenReturn("\"5d41402abc4b2a76b9719d911017c592");
        metadata = aws.doGetMetadata("object-1", "v1");
        assertArrayEquals(new byte[0], metadata.getMd5());
    }

    @Test
    void testDoListEmpty() {
        ListBlobsRequest request = new ListBlobsRequest.Builder().build();
        ListObjectsV2Response mockResponse = mock(ListObjectsV2Response.class);
        when(mockS3Client.listObjectsV2((ListObjectsV2Request) any())).thenReturn(mockResponse);

        Iterator<BlobInfo> iterator = aws.doList(request);
        assertThrows(NoSuchElementException.class, () -> {
            iterator.next();
        });
    }

    @Test
    void testDoList() {
        ListBlobsRequest request = new ListBlobsRequest.Builder().withPrefix("abc").withDelimiter("/").build();
        ListObjectsV2Response mockResponse = mock(ListObjectsV2Response.class);
        when(mockS3Client.listObjectsV2((ListObjectsV2Request) any())).thenReturn(mockResponse);
        populateList(mockResponse);

        Iterator<BlobInfo> iterator = aws.doList(request);
        assertNotNull(iterator);

        int count = 1;
        while(iterator.hasNext()) {
           BlobInfo blobInfo = iterator.next();
           int current = count++;
           assertEquals("key-" + current, blobInfo.getKey());
           assertEquals(current, blobInfo.getObjectSize());
        }
    }

    private void populateList(ListObjectsV2Response mockResponse) {
        List<S3Object> list = IntStream
                .range(1, 100)
                .mapToObj(this::mockObject)
                .collect(Collectors.toList());
        when(mockResponse.contents()).thenReturn(list);
    }

    @Test
    void testDoListPage() {
        ListBlobsPageRequest request = ListBlobsPageRequest
                .builder()
                .withPrefix("abc")
                .withDelimiter("/")
                .withPaginationToken("next-token")
                .withMaxResults(50)
                .build();
        
        ListObjectsV2Response mockResponse = mock(ListObjectsV2Response.class);
        when(mockS3Client.listObjectsV2((ListObjectsV2Request) any())).thenReturn(mockResponse);
        populateList(mockResponse);
        when(mockResponse.isTruncated()).thenReturn(true);
        when(mockResponse.nextContinuationToken()).thenReturn("next-page-token");

        ListBlobsPageResponse response = aws.listPage(request);

        // Verify the request is mapped to the SDK
        ArgumentCaptor<ListObjectsV2Request> requestCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        verify(mockS3Client, times(1)).listObjectsV2(requestCaptor.capture());
        ListObjectsV2Request actualRequest = requestCaptor.getValue();
        assertEquals("bucket-1", actualRequest.bucket());
        assertEquals("abc", actualRequest.prefix());
        assertEquals("/", actualRequest.delimiter());
        assertEquals("next-token", actualRequest.continuationToken());
        assertEquals(50, actualRequest.maxKeys());

        // Verify the response is mapped back properly
        assertNotNull(response);
        assertEquals(99, response.getBlobs().size()); // 1 to 99
        assertEquals(true, response.isTruncated());
        assertEquals("next-page-token", response.getNextPageToken());
        
        // Verify first and last blob
        assertEquals("key-1", response.getBlobs().get(0).getKey());
        assertEquals(1, response.getBlobs().get(0).getObjectSize());
        assertEquals("key-99", response.getBlobs().get(98).getKey());
        assertEquals(99, response.getBlobs().get(98).getObjectSize());
    }

    @Test
    void testDoListPageEmpty() {
        ListBlobsPageRequest request = ListBlobsPageRequest.builder().build();
        ListObjectsV2Response mockResponse = mock(ListObjectsV2Response.class);
        when(mockS3Client.listObjectsV2((ListObjectsV2Request) any())).thenReturn(mockResponse);
        when(mockResponse.contents()).thenReturn(List.of());
        when(mockResponse.isTruncated()).thenReturn(false);
        when(mockResponse.nextContinuationToken()).thenReturn(null);

        ListBlobsPageResponse response = aws.listPage(request);

        assertNotNull(response);
        assertEquals(0, response.getBlobs().size());
        assertFalse(response.isTruncated());
        assertNull(response.getNextPageToken());
    }

    @Test
    void testDoInitiateMultipartUpload() {
        CreateMultipartUploadResponse mockResponse = mock(CreateMultipartUploadResponse.class);
        doReturn("bucket-1").when(mockResponse).bucket();
        doReturn("object-1").when(mockResponse).key();
        doReturn("mpu-id").when(mockResponse).uploadId();
        when(mockS3Client.createMultipartUpload((CreateMultipartUploadRequest) any())).thenReturn(mockResponse);
        Map<String, String> metadata = Map.of("key-1", "value-1");
        MultipartUploadRequest request = new MultipartUploadRequest.Builder().withKey("object-1").withMetadata(metadata).build();

        MultipartUpload response = aws.initiateMultipartUpload(request);

        // Verify the request is mapped to the SDK
        ArgumentCaptor<CreateMultipartUploadRequest> requestCaptor = ArgumentCaptor.forClass(CreateMultipartUploadRequest.class);
        verify(mockS3Client, times(1)).createMultipartUpload(requestCaptor.capture());
        CreateMultipartUploadRequest actualRequest = requestCaptor.getValue();
        assertEquals("object-1", actualRequest.key());
        assertEquals("bucket-1", actualRequest.bucket());
        assertEquals(metadata, actualRequest.metadata());

        // Verify the response is mapped back properly
        assertEquals("object-1", response.getKey());
        assertEquals("bucket-1", response.getBucket());
        assertEquals("mpu-id", response.getId());
    }

    @Test
    void testDoInitiateMultipartUploadWithKms() {
        CreateMultipartUploadResponse mockResponse = mock(CreateMultipartUploadResponse.class);
        doReturn("bucket-1").when(mockResponse).bucket();
        doReturn("object-1").when(mockResponse).key();
        doReturn("mpu-id").when(mockResponse).uploadId();
        when(mockS3Client.createMultipartUpload((CreateMultipartUploadRequest) any())).thenReturn(mockResponse);
        Map<String, String> metadata = Map.of("key-1", "value-1");
        String kmsKeyId = "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012";
        MultipartUploadRequest request = new MultipartUploadRequest.Builder()
                .withKey("object-1")
                .withMetadata(metadata)
                .withKmsKeyId(kmsKeyId)
                .build();

        MultipartUpload response = aws.initiateMultipartUpload(request);

        // Verify the request is mapped to the SDK with KMS encryption
        ArgumentCaptor<CreateMultipartUploadRequest> requestCaptor = ArgumentCaptor.forClass(CreateMultipartUploadRequest.class);
        verify(mockS3Client, times(1)).createMultipartUpload(requestCaptor.capture());
        CreateMultipartUploadRequest actualRequest = requestCaptor.getValue();
        assertEquals("object-1", actualRequest.key());
        assertEquals("bucket-1", actualRequest.bucket());
        assertEquals(metadata, actualRequest.metadata());
        assertEquals(ServerSideEncryption.AWS_KMS, actualRequest.serverSideEncryption());
        assertEquals(kmsKeyId, actualRequest.ssekmsKeyId());

        // Verify the response is mapped back properly with KMS key
        assertEquals("object-1", response.getKey());
        assertEquals("bucket-1", response.getBucket());
        assertEquals("mpu-id", response.getId());
        assertEquals(kmsKeyId, response.getKmsKeyId());
    }

    @Test
    void testDoUploadMultipartPart() {
        UploadPartResponse mockResponse = mock(UploadPartResponse.class);
        doReturn("etag").when(mockResponse).eTag();
        doReturn(mockResponse).when(mockS3Client).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .build();
        byte[] content = "This is test data".getBytes(StandardCharsets.UTF_8);
        MultipartPart multipartPart = new MultipartPart(1, content);

        var response = aws.uploadMultipartPart(multipartUpload, multipartPart);

        // Verify the request is mapped to the SDK
        ArgumentCaptor<UploadPartRequest> requestCaptor = ArgumentCaptor.forClass(UploadPartRequest.class);
        ArgumentCaptor<RequestBody> requestBodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(mockS3Client, times(1)).uploadPart(requestCaptor.capture(), requestBodyCaptor.capture());
        UploadPartRequest actualRequest = requestCaptor.getValue();
        assertEquals("object-1", actualRequest.key());
        assertEquals("bucket-1", actualRequest.bucket());
        assertEquals("mpu-id", actualRequest.uploadId());
        assertEquals(1, actualRequest.partNumber());

        // Verify the response is mapped back properly
        assertEquals(1, response.getPartNumber());
        assertEquals("etag", response.getEtag());
        assertEquals(content.length, response.getSizeInBytes());
    }

    @Test
    void testDoCompleteMultipartUpload() {
        CompleteMultipartUploadResponse mockResponse = mock(CompleteMultipartUploadResponse.class);
        doReturn("bucket-1").when(mockResponse).bucket();
        doReturn("object-1").when(mockResponse).key();
        doReturn("complete-etag").when(mockResponse).eTag();
        doReturn(mockResponse).when(mockS3Client).completeMultipartUpload((CompleteMultipartUploadRequest) any());
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .build();
        List<com.salesforce.multicloudj.blob.driver.UploadPartResponse> listOfParts = List.of(new com.salesforce.multicloudj.blob.driver.UploadPartResponse(1, "etag", 0));

        MultipartUploadResponse response = aws.completeMultipartUpload(multipartUpload, listOfParts);

        // Verify the request is mapped to the SDK
        ArgumentCaptor<CompleteMultipartUploadRequest> requestCaptor = ArgumentCaptor.forClass(CompleteMultipartUploadRequest.class);
        verify(mockS3Client, times(1)).completeMultipartUpload(requestCaptor.capture());
        CompleteMultipartUploadRequest actualRequest = requestCaptor.getValue();
        assertEquals("object-1", actualRequest.key());
        assertEquals("bucket-1", actualRequest.bucket());
        assertEquals("mpu-id", actualRequest.uploadId());
        List<CompletedPart> parts = actualRequest.multipartUpload().parts();
        assertEquals(1, parts.size());
        assertEquals(1, parts.get(0).partNumber());
        assertEquals("etag", parts.get(0).eTag());

        // Verify the response is mapped back properly
        assertEquals("complete-etag", response.getEtag());
    }

    @Test
    void testDoListMultipartUpload() {
        ListPartsResponse mockResponse = mock(ListPartsResponse.class);
        doReturn("bucket-1").when(mockResponse).bucket();
        doReturn("object-1").when(mockResponse).key();
        doReturn("mpu-id").when(mockResponse).uploadId();
        List<Part> parts = List.of(
                Part.builder().partNumber(1).eTag("etag1").size(3000L).build(),
                Part.builder().partNumber(2).eTag("etag2").size(2000L).build(),
                Part.builder().partNumber(3).eTag("etag3").size(1000L).build());
        doReturn(parts).when(mockResponse).parts();
        doReturn(mockResponse).when(mockS3Client).listParts((ListPartsRequest) any());
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .build();

        var response = aws.listMultipartUpload(multipartUpload);

        // Verify the request is mapped to the SDK
        ArgumentCaptor<ListPartsRequest> requestCaptor = ArgumentCaptor.forClass(ListPartsRequest.class);
        verify(mockS3Client, times(1)).listParts(requestCaptor.capture());
        ListPartsRequest actualRequest = requestCaptor.getValue();
        assertEquals("object-1", actualRequest.key());
        assertEquals("bucket-1", actualRequest.bucket());
        assertEquals("mpu-id", actualRequest.uploadId());

        // Verify the response is mapped back properly
        assertEquals(1, response.get(0).getPartNumber());
        assertEquals("etag1", response.get(0).getEtag());
        assertEquals(3000L, response.get(0).getSizeInBytes());
        assertEquals(2, response.get(1).getPartNumber());
        assertEquals("etag2", response.get(1).getEtag());
        assertEquals(2000L, response.get(1).getSizeInBytes());
        assertEquals(3, response.get(2).getPartNumber());
        assertEquals("etag3", response.get(2).getEtag());
        assertEquals(1000L, response.get(2).getSizeInBytes());
    }

    @Test
    void testDoAbortMultipartUpload() {
        AbortMultipartUploadResponse mockResponse = mock(AbortMultipartUploadResponse.class);
        doReturn(mockResponse).when(mockS3Client).abortMultipartUpload((AbortMultipartUploadRequest) any());
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .build();

        aws.abortMultipartUpload(multipartUpload);

        // Verify the request is mapped to the SDK
        ArgumentCaptor<AbortMultipartUploadRequest> requestCaptor = ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
        verify(mockS3Client, times(1)).abortMultipartUpload(requestCaptor.capture());
        AbortMultipartUploadRequest actualRequest = requestCaptor.getValue();
        assertEquals("object-1", actualRequest.key());
        assertEquals("bucket-1", actualRequest.bucket());
        assertEquals("mpu-id", actualRequest.uploadId());
    }

    @Test
    void testDoGetTags() {
        GetObjectTaggingResponse mockResponse = mock(GetObjectTaggingResponse.class);
        List<Tag> tags = List.of(Tag.builder().key("key1").value("value1").build(), Tag.builder().key("key2").value("value2").build());
        doReturn(tags).when(mockResponse).tagSet();
        doReturn(mockResponse).when(mockS3Client).getObjectTagging((GetObjectTaggingRequest)any());

        Map<String,String> tagsResult = aws.getTags("object-1");

        ArgumentCaptor<GetObjectTaggingRequest> getObjectTaggingRequestCaptor = ArgumentCaptor.forClass(GetObjectTaggingRequest.class);
        verify(mockS3Client, times(1)).getObjectTagging(getObjectTaggingRequestCaptor.capture());
        GetObjectTaggingRequest actualGetObjectTaggingRequest = getObjectTaggingRequestCaptor.getValue();
        assertEquals("object-1", actualGetObjectTaggingRequest.key());
        assertEquals("bucket-1", actualGetObjectTaggingRequest.bucket());
        assertEquals(Map.of("key1", "value1", "key2", "value2"), tagsResult);
    }

    @Test
    void testDoSetTags() {
        PutObjectTaggingResponse mockResponse = mock(PutObjectTaggingResponse.class);
        doReturn(mockResponse).when(mockS3Client).putObjectTagging((PutObjectTaggingRequest)any());

        Map<String, String> tags = Map.of("key1", "value1", "key2", "value2");
        aws.setTags("object-1", tags);

        ArgumentCaptor<PutObjectTaggingRequest> putObjectTaggingRequestCaptor = ArgumentCaptor.forClass(PutObjectTaggingRequest.class);
        verify(mockS3Client, times(1)).putObjectTagging(putObjectTaggingRequestCaptor.capture());
        PutObjectTaggingRequest actualPutObjectTaggingRequest = putObjectTaggingRequestCaptor.getValue();
        assertEquals("object-1", actualPutObjectTaggingRequest.key());
        assertEquals("bucket-1", actualPutObjectTaggingRequest.bucket());
        List<Tag> actualTags = actualPutObjectTaggingRequest.tagging().tagSet();
        assertTrue(actualTags.contains(Tag.builder().key("key1").value("value1").build()));
        assertTrue(actualTags.contains(Tag.builder().key("key2").value("value2").build()));
    }

    @Test
    void testDoGeneratePresignedUploadUrl() throws MalformedURLException {
        AwsBlobStore spyAws = spy(aws);
        S3Presigner mockPresigner = mock(S3Presigner.class);
        doReturn(mockPresigner).when(spyAws).getPresigner();

        PresignedPutObjectRequest mockPresignedPutObjectRequest = mock(PresignedPutObjectRequest.class);
        URL url = new URL("http://localhost:8080");
        doReturn(url).when(mockPresignedPutObjectRequest).url();
        doReturn(mockPresignedPutObjectRequest).when(mockPresigner).presignPutObject(any(PutObjectPresignRequest.class));

        PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.UPLOAD)
                .key("object-1")
                .duration(Duration.ofHours(4))
                .build();

        URL actualUrl = spyAws.doGeneratePresignedUrl(presignedUrlRequest);
        assertEquals(url, actualUrl);
    }

    @Test
    void testDoGeneratePresignedDownloadUrl() throws MalformedURLException {
        AwsBlobStore spyAws = spy(aws);
        S3Presigner mockPresigner = mock(S3Presigner.class);
        doReturn(mockPresigner).when(spyAws).getPresigner();

        PresignedGetObjectRequest mockPresignedGetObjectRequest = mock(PresignedGetObjectRequest.class);
        URL url = new URL("http://localhost:8080");
        doReturn(url).when(mockPresignedGetObjectRequest).url();
        doReturn(mockPresignedGetObjectRequest).when(mockPresigner).presignGetObject(any(GetObjectPresignRequest.class));

        PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.DOWNLOAD)
                .key("object-1")
                .duration(Duration.ofHours(4))
                .build();

        URL actualUrl = spyAws.doGeneratePresignedUrl(presignedUrlRequest);
        assertEquals(url, actualUrl);
    }

    @Test
    void testDoDoesObjectExist() {
        boolean result = aws.doDoesObjectExist("object-1", "version-1");

        ArgumentCaptor<HeadObjectRequest> requestCaptor = ArgumentCaptor.forClass(HeadObjectRequest.class);
        verify(mockS3Client, times(1)).headObject(requestCaptor.capture());
        HeadObjectRequest actualRequest = requestCaptor.getValue();
        assertEquals("object-1", actualRequest.key());
        assertEquals("bucket-1", actualRequest.bucket());
        assertEquals("version-1", actualRequest.versionId());
        assertTrue(result);

        // Verify the error state
        S3Exception mockException = mock(S3Exception.class);
        doReturn(404).when(mockException).statusCode();
        doThrow(mockException).when(mockS3Client).headObject(any(HeadObjectRequest.class));

        result = aws.doDoesObjectExist("object-1", "version-1");
        assertFalse(result);
    }

    private S3Object mockObject(int index) {
        S3Object mockS3 = mock(S3Object.class);
        when(mockS3.key()).thenReturn("key-" + index);
        when(mockS3.size()).thenReturn((long) index);
        return mockS3;
    }

    @Test
    void testBuildS3ClientWithRetryConfig() {
        // Test with exponential retry config
        RetryConfig exponentialConfig = RetryConfig.builder()
                .mode(RetryConfig.Mode.EXPONENTIAL)
                .maxAttempts(3)
                .initialDelayMillis(100L)
                .multiplier(2.0)
                .maxDelayMillis(5000L)
                .attemptTimeout(5000L)
                .totalTimeout(30000L)
                .build();

        var store = new AwsBlobStore.Builder()
                .withTransformerSupplier(transformerSupplier)
                .withBucket("bucket-1")
                .withRegion("us-east-2")
                .withRetryConfig(exponentialConfig)
                .build();

        assertNotNull(store);
        assertEquals("bucket-1", store.getBucket());
    }

    @Test
    void testBuildS3ClientWithFixedRetryConfig() {
        // Test with fixed retry config
        RetryConfig fixedConfig = RetryConfig.builder()
                .mode(RetryConfig.Mode.FIXED)
                .maxAttempts(5)
                .fixedDelayMillis(1000L)
                .build();

        var store = new AwsBlobStore.Builder()
                .withTransformerSupplier(transformerSupplier)
                .withBucket("bucket-1")
                .withRegion("us-east-2")
                .withRetryConfig(fixedConfig)
                .build();

        assertNotNull(store);
        assertEquals("bucket-1", store.getBucket());
    }

    @Test
    void testBuildS3ClientWithRetryConfigWithNullMaxAttempts() {
        // Test with null maxAttempts (should use AWS default)
        RetryConfig config = RetryConfig.builder()
                .mode(RetryConfig.Mode.EXPONENTIAL)
                .maxAttempts(null)
                .initialDelayMillis(100L)
                .maxDelayMillis(5000L)
                .build();

        var store = new AwsBlobStore.Builder()
                .withTransformerSupplier(transformerSupplier)
                .withBucket("bucket-1")
                .withRegion("us-east-2")
                .withRetryConfig(config)
                .build();

        assertNotNull(store);
        assertEquals("bucket-1", store.getBucket());
    }

    @Test
    void testBuildS3ClientWithRetryConfigWithAttemptTimeout() {
        // Test with attempt timeout only
        RetryConfig config = RetryConfig.builder()
                .mode(RetryConfig.Mode.EXPONENTIAL)
                .maxAttempts(3)
                .initialDelayMillis(100L)
                .maxDelayMillis(5000L)
                .attemptTimeout(5000L)
                .build();

        var store = new AwsBlobStore.Builder()
                .withTransformerSupplier(transformerSupplier)
                .withBucket("bucket-1")
                .withRegion("us-east-2")
                .withRetryConfig(config)
                .build();

        assertNotNull(store);
        assertEquals("bucket-1", store.getBucket());
    }

    @Test
    void testBuildS3ClientWithRetryConfigWithTotalTimeout() {
        // Test with total timeout only
        RetryConfig config = RetryConfig.builder()
                .mode(RetryConfig.Mode.EXPONENTIAL)
                .maxAttempts(3)
                .initialDelayMillis(100L)
                .maxDelayMillis(5000L)
                .totalTimeout(30000L)
                .build();

        var store = new AwsBlobStore.Builder()
                .withTransformerSupplier(transformerSupplier)
                .withBucket("bucket-1")
                .withRegion("us-east-2")
                .withRetryConfig(config)
                .build();

        assertNotNull(store);
        assertEquals("bucket-1", store.getBucket());
    }

    @Test
    void testBuildS3ClientWithoutRetryConfig() {
        // Test without retry config (default behavior)
        var store = new AwsBlobStore.Builder()
                .withTransformerSupplier(transformerSupplier)
                .withBucket("bucket-1")
                .withRegion("us-east-2")
                .build();

        assertNotNull(store);
        assertEquals("bucket-1", store.getBucket());
    }
}
