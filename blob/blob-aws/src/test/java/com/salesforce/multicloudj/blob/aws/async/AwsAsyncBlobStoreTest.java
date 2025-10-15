package com.salesforce.multicloudj.blob.aws.async;

import com.salesforce.multicloudj.blob.aws.AwsTransformerSupplier;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.BlobStoreValidator;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsBatch;
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
import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.internal.async.ByteArrayAsyncResponseTransformer;
import software.amazon.awssdk.core.internal.async.InputStreamResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
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
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
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
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryDownload;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryUpload;
import software.amazon.awssdk.transfer.s3.model.DirectoryDownload;
import software.amazon.awssdk.transfer.s3.model.DirectoryUpload;
import software.amazon.awssdk.transfer.s3.model.DownloadDirectoryRequest;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FailedFileDownload;
import software.amazon.awssdk.transfer.s3.model.FailedFileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadDirectoryRequest;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

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
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AwsAsyncBlobStoreTest {

    private static final String BUCKET = "bucket-1";
    private static final String REGION = "us-east-2";

    private MockedStatic<S3AsyncClient> s3Client;
    private S3AsyncClient mockS3Client;
    private S3TransferManager mockS3TransferManager;
    private AwsAsyncBlobStore aws;
    private final BlobStoreValidator validator = new BlobStoreValidator();
    private final AwsTransformerSupplier transformerSupplier = new AwsTransformerSupplier();

    @BeforeEach
    void setup() {
        S3AsyncClientBuilder mockBuilder = mock(S3AsyncClientBuilder.class);
        when(mockBuilder.region(any())).thenReturn(mockBuilder);
        S3CrtAsyncClientBuilder mockCrtBuilder = mock(S3CrtAsyncClientBuilder.class);
        when(mockCrtBuilder.region(any())).thenReturn(mockCrtBuilder);

        s3Client = mockStatic(S3AsyncClient.class);
        s3Client.when(S3AsyncClient::builder).thenReturn(mockBuilder);

        s3Client.when(S3AsyncClient::crtBuilder).thenReturn(mockCrtBuilder);
        mockS3Client = mock(S3AsyncClient.class);
        when(mockBuilder.build()).thenReturn(mockS3Client);
        when(mockCrtBuilder.build()).thenReturn(mockS3Client);

        when(mockBuilder.credentialsProvider(any())).thenReturn(mockBuilder);
        mockS3TransferManager = mock(S3TransferManager.class);
        StsCredentials sessionCreds = new StsCredentials("key-1", "secret-1", "token-1");
        CredentialsOverrider credsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(sessionCreds)
                .build();

        aws = new AwsAsyncBlobStore(
                BUCKET,
                REGION,
                credsOverrider,
                validator,
                mockS3Client,
                mockS3TransferManager,
                transformerSupplier
        );
    }

    @AfterEach
    void testDown() {
        if (s3Client != null) {
            s3Client.close();
        }
    }

    <T> CompletableFuture<T> future(T value) {
        return CompletableFuture.completedFuture(value);
    }

    CompletableFuture<Void> futureVoid() {
        return future(null);
    }

    @Test
    void testProxyProviderId() {
        AwsAsyncBlobStoreProvider provider = new AwsAsyncBlobStoreProvider();
        assertEquals(AwsConstants.PROVIDER_ID, provider.getProviderId());
    }

    @Test
    void testProviderBuilder() {
        AwsAsyncBlobStoreProvider provider = new AwsAsyncBlobStoreProvider();
        AwsAsyncBlobStoreProvider.Builder builder = provider.builder();
        assertInstanceOf(AwsAsyncBlobStore.Builder.class, builder);

        var store = builder
                .withExecutorService(mock(ExecutorService.class))
                .withBucket(BUCKET)
                .withRegion(REGION)
                .withEndpoint(URI.create("https://endpoint.example.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:443"))
                .withSocketTimeout(Duration.ofMinutes(1))
                .withIdleConnectionTimeout(Duration.ofMinutes(5))
                .withMaxConnections(100)
                .withExecutorService(ForkJoinPool.commonPool())
                .build();

        assertNotNull(store);
    }

    @Test
    void testCrtClientConfiguration() {
        // Test CRT client configuration with parallel downloads enabled
        AwsAsyncBlobStoreProvider provider = new AwsAsyncBlobStoreProvider();
        AwsAsyncBlobStoreProvider.Builder builder = provider.builder();

        var store = builder
                .withBucket(BUCKET)
                .withRegion(REGION)
                .withParallelDownloadsEnabled(true)             // This should trigger CRT client
                .withTargetThroughputInGbps(20.0)               // CRT-specific
                .withMaxNativeMemoryLimitInBytes(2L * 1024L * 1024L * 1024L) // CRT-specific
                .withPartBufferSize(8 * 1024 * 1024L)           // Common config
                .withThresholdBytes(10 * 1024 * 1024L)          // Common config
                .withExecutorService(ForkJoinPool.commonPool()) // Common config
                .withEndpoint(URI.create("https://crt-endpoint.example.com")) // Common config
                .withProxyEndpoint(URI.create("https://crt-proxy.example.com:443")) // Common config
                .build();

        assertNotNull(store);
        assertEquals(AwsConstants.PROVIDER_ID, store.getProviderId());
        assertEquals(BUCKET, store.getBucket());
        assertEquals(REGION, store.getRegion());
    }

    @Test
    void testCrtClientWithNewConfigurations() {
        // Test CRT client with new configuration options
        AwsAsyncBlobStoreProvider provider = new AwsAsyncBlobStoreProvider();
        AwsAsyncBlobStoreProvider.Builder builder = provider.builder();

        var store = builder
                .withBucket(BUCKET)
                .withRegion(REGION)
                .withParallelDownloadsEnabled(true)             // This should trigger CRT client
                .withInitialReadBufferSizeInBytes(16 * 1024L)   // New CRT-specific config
                .withMaxConcurrency(50)                         // New CRT-specific config
                .withTargetThroughputInGbps(20.0)
                .withMaxNativeMemoryLimitInBytes(2L * 1024L * 1024L * 1024L)
                .build();

        assertNotNull(store);
        assertEquals(AwsConstants.PROVIDER_ID, store.getProviderId());
        assertEquals(BUCKET, store.getBucket());
        assertEquals(REGION, store.getRegion());
    }

    @Test
    void testStandardClientConfiguration() {
        // Test standard client configuration with parallel downloads disabled
        AwsAsyncBlobStoreProvider provider = new AwsAsyncBlobStoreProvider();
        AwsAsyncBlobStoreProvider.Builder builder = provider.builder();

        var store = builder
                .withBucket(BUCKET)
                .withRegion(REGION)
                .withParallelUploadsEnabled(true)
                .withThresholdBytes(5 * 1024 * 1024L)
                .withPartBufferSize(1024 * 1024L)
                .withExecutorService(ForkJoinPool.commonPool())
                .withEndpoint(URI.create("https://standard-endpoint.example.com"))
                .withProxyEndpoint(URI.create("https://standard-proxy.example.com:443"))
                .build();

        assertNotNull(store);
        assertEquals(AwsConstants.PROVIDER_ID, store.getProviderId());
        assertEquals(BUCKET, store.getBucket());
        assertEquals(REGION, store.getRegion());
    }

    @Test
    void testAllConfigurationOptions() {
        // Test all configuration options together
        AwsAsyncBlobStoreProvider provider = new AwsAsyncBlobStoreProvider();
        AwsAsyncBlobStoreProvider.Builder builder = provider.builder();

        ExecutorService executorService = ForkJoinPool.commonPool();
        URI endpoint = URI.create("https://all-config-endpoint.example.com");
        URI proxyEndpoint = URI.create("https://all-config-proxy.example.com:443");
        Long thresholdBytes = 10 * 1024 * 1024L; // 10MB
        Long partBufferSize = 2 * 1024 * 1024L; // 2MB
        Double targetThroughputInGbps = 25.0;
        Long maxNativeMemoryLimitInBytes = 2L * 1024L * 1024L * 1024L; // 2GB
        Long initialReadBufferSizeInBytes = 16 * 1024L; // 16KB
        Integer maxConcurrency = 50;
        Integer maxConnections = 100;
        Duration socketTimeout = Duration.ofMinutes(1);
        Duration idleConnectionTimeout = Duration.ofMinutes(5);
        Integer transferManagerThreadPoolSize = 10;
        Integer transferDirectoryMaxConcurrency = 20;

        var store = builder
                .withBucket(BUCKET)
                .withRegion(REGION)
                .withThresholdBytes(thresholdBytes)
                .withPartBufferSize(partBufferSize)
                .withParallelUploadsEnabled(true)
                .withParallelDownloadsEnabled(true)
                .withTargetThroughputInGbps(targetThroughputInGbps)
                .withMaxNativeMemoryLimitInBytes(maxNativeMemoryLimitInBytes)
                .withInitialReadBufferSizeInBytes(initialReadBufferSizeInBytes)
                .withMaxConcurrency(maxConcurrency)
                .withMaxConnections(maxConnections)
                .withSocketTimeout(socketTimeout)
                .withIdleConnectionTimeout(idleConnectionTimeout)
                .withTransferManagerThreadPoolSize(transferManagerThreadPoolSize)
                .withTransferDirectoryMaxConcurrency(transferDirectoryMaxConcurrency)
                .withExecutorService(executorService)
                .withEndpoint(endpoint)
                .withProxyEndpoint(proxyEndpoint)
                .build();

        assertNotNull(store);
        assertEquals(AwsConstants.PROVIDER_ID, store.getProviderId());
        assertEquals(BUCKET, store.getBucket());
        assertEquals(REGION, store.getRegion());
    }

    @Test
    void testHttpClientConfiguration() {
        // Test HTTP client configuration with standard async client
        AwsAsyncBlobStoreProvider provider = new AwsAsyncBlobStoreProvider();
        AwsAsyncBlobStoreProvider.Builder builder = provider.builder();

        Integer maxConnections = 100;
        Duration socketTimeout = Duration.ofSeconds(30);
        Duration idleConnectionTimeout = Duration.ofMinutes(2);
        URI proxyEndpoint = URI.create("https://http-proxy.example.com:8080");

        var store = builder
                .withBucket(BUCKET)
                .withRegion(REGION)
                .withMaxConnections(maxConnections)
                .withSocketTimeout(socketTimeout)
                .withIdleConnectionTimeout(idleConnectionTimeout)
                .withProxyEndpoint(proxyEndpoint)
                .build();

        assertNotNull(store);
        assertEquals(AwsConstants.PROVIDER_ID, store.getProviderId());
        assertEquals(BUCKET, store.getBucket());
        assertEquals(REGION, store.getRegion());
    }

    @Test
    void testTransferManagerConfiguration() {
        // Test transfer manager specific configurations
        AwsAsyncBlobStoreProvider provider = new AwsAsyncBlobStoreProvider();
        AwsAsyncBlobStoreProvider.Builder builder = provider.builder();

        Integer transferManagerThreadPoolSize = 15;
        Integer transferDirectoryMaxConcurrency = 25;

        var store = builder
                .withBucket(BUCKET)
                .withRegion(REGION)
                .withTransferManagerThreadPoolSize(transferManagerThreadPoolSize)
                .withTransferDirectoryMaxConcurrency(transferDirectoryMaxConcurrency)
                .build();

        assertNotNull(store);
        assertEquals(AwsConstants.PROVIDER_ID, store.getProviderId());
        assertEquals(BUCKET, store.getBucket());
        assertEquals(REGION, store.getRegion());
    }

    @Test
    void testProviderId() {
        assertEquals(AwsConstants.PROVIDER_ID, aws.getProviderId());
    }

    @Test
    void testExceptionHandling() {
        AwsErrorDetails errorDetails = AwsErrorDetails
                .builder()
                .errorCode("IncompleteSignature")
                .build();
        AwsServiceException awsServiceException = AwsServiceException
                .builder()
                .awsErrorDetails(errorDetails)
                .build();
        Class<?> cls = aws.getException(awsServiceException);
        assertEquals(cls, InvalidArgumentException.class);

        SdkClientException sdkClientException = SdkClientException.builder().build();
        cls = aws.getException(sdkClientException);
        assertEquals(cls, InvalidArgumentException.class);

        cls = aws.getException(new IOException("Channel is closed"));
        assertEquals(cls, UnknownException.class);
    }

    private UploadRequest generateTestUploadRequest() {
        Map<String, String> metadata = Map.of("key-1", "value-1");
        Map<String, String> tags = Map.of("tag-1", "value-1");
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
    void testDoUploadInputStream() throws ExecutionException, InterruptedException {
        doReturn(CompletableFuture.completedFuture(buildMockPutObjectResponse())).when(mockS3Client).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));
        verifyUploadTestResults(aws.doUpload(generateTestUploadRequest(), mock(InputStream.class)).get());
    }

    @Test
    void testDoUploadByteArray() throws ExecutionException, InterruptedException {
        doReturn(CompletableFuture.completedFuture(buildMockPutObjectResponse())).when(mockS3Client).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));
        verifyUploadTestResults(aws.doUpload(generateTestUploadRequest(), new byte[1024]).get());
    }

    @Test
    void testDoUploadFile() throws ExecutionException, InterruptedException, IOException {
        doReturn(CompletableFuture.completedFuture(buildMockPutObjectResponse())).when(mockS3Client).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));
        Path path = null;
        try {
            path = Files.createTempFile("tempFile", ".txt");
            try(BufferedWriter writer = Files.newBufferedWriter(path)) {
                writer.write(new char[1024]);
            }
            verifyUploadTestResults(aws.doUpload(generateTestUploadRequest(), path.toFile()).get());
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
    void testDoUploadPath() throws ExecutionException, InterruptedException, IOException {
        doReturn(CompletableFuture.completedFuture(buildMockPutObjectResponse())).when(mockS3Client).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));
        Path path = null;
        try {
            path = Files.createTempFile("tempFile", ".txt");
            try(BufferedWriter writer = Files.newBufferedWriter(path)) {
                writer.write(new char[1024]);
            }
            verifyUploadTestResults(aws.doUpload(generateTestUploadRequest(), path).get());
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
        ArgumentCaptor<AsyncRequestBody> bodyCaptor = ArgumentCaptor.forClass(AsyncRequestBody.class);
        verify(mockS3Client, times(1)).putObject(putObjectRequestCaptor.capture(), bodyCaptor.capture());
        PutObjectRequest actualPutObjectRequest = putObjectRequestCaptor.getValue();
        assertEquals("object-1", actualPutObjectRequest.key());
        assertEquals("bucket-1", actualPutObjectRequest.bucket());
        assertEquals("value-1", actualPutObjectRequest.metadata().get("key-1"));
        assertTrue(actualPutObjectRequest.tagging().contains("tag-1=value-1"));
        AsyncRequestBody actualRequestBody = bodyCaptor.getValue();
        assertTrue(actualRequestBody.contentLength().isPresent());
        assertEquals(1024L, actualRequestBody.contentLength().get());

        // Verify the mapping of the response into the UploadResponse object
        assertEquals("object-1", uploadResponse.getKey());
        assertEquals("version-1", uploadResponse.getVersionId());
        assertEquals("etag", uploadResponse.getETag());
    }

    private DownloadRequest generateTestDownloadRequest() {
        return new DownloadRequest.Builder().withKey("object-1").withVersionId("version-1").build();
    }

    private void setupMockGetObjectResponse(Instant now) throws IOException {
        GetObjectResponse getObjectResponse = mock(GetObjectResponse.class);
        doReturn("version-1").when(getObjectResponse).versionId();
        doReturn("etag").when(getObjectResponse).eTag();
        doReturn(now).when(getObjectResponse).lastModified();
        Map<String,String> metadata = Map.of("key1", "value1", "key2", "value2");
        doReturn(metadata).when(getObjectResponse).metadata();
        doReturn(1024L).when(getObjectResponse).contentLength();

        ResponseBytes<GetObjectResponse> responseBytes = mock(ResponseBytes.class);
        doReturn("downloadedData".getBytes()).when(responseBytes).asByteArray();
        doReturn(getObjectResponse).when(responseBytes).response();

        ResponseInputStream responseInputStream = mock(ResponseInputStream.class);
        doReturn(getObjectResponse).when(responseInputStream).response();

        doReturn(future(getObjectResponse)).when(mockS3Client).getObject(any(GetObjectRequest.class), (AsyncResponseTransformer<GetObjectResponse, Object>) any());
        doReturn(future(getObjectResponse)).when(mockS3Client).getObject(any(GetObjectRequest.class), any(Path.class));
        doReturn(future(responseBytes)).when(mockS3Client).getObject(any(GetObjectRequest.class), any(ByteArrayAsyncResponseTransformer.class));
        doReturn(future(responseInputStream)).when(mockS3Client).getObject(any(GetObjectRequest.class), any(InputStreamResponseTransformer.class));
    }

    @Test
    void testDoDownloadOutputStream() throws ExecutionException, InterruptedException, IOException {
        Instant now = Instant.now();
        setupMockGetObjectResponse(now);
        DownloadResponse response = aws.doDownload(generateTestDownloadRequest(), mock(OutputStream.class)).get();

        ArgumentCaptor<GetObjectRequest> getObjectRequestCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(mockS3Client, times(1)).getObject(getObjectRequestCaptor.capture(), any(AsyncResponseTransformer.class));
        verifyDownloadTestResults(response, getObjectRequestCaptor, now);
    }

    @Test
    void testDoDownloadByteArrayWrapper() throws ExecutionException, InterruptedException, IOException {
        Instant now = Instant.now();
        setupMockGetObjectResponse(now);
        ByteArray byteArray = new ByteArray();
        DownloadResponse response = aws.doDownload(generateTestDownloadRequest(), byteArray).get();
        assertEquals("downloadedData", new String(byteArray.getBytes()));

        ArgumentCaptor<GetObjectRequest> getObjectRequestCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(mockS3Client, times(1)).getObject(getObjectRequestCaptor.capture(), any(ByteArrayAsyncResponseTransformer.class));
        verifyDownloadTestResults(response, getObjectRequestCaptor, now);
    }

    @Test
    void testDoDownloadFile() throws ExecutionException, InterruptedException, IOException {
        Instant now = Instant.now();
        setupMockGetObjectResponse(now);
        Path path = Path.of("tempFile.txt");
        try {
            Files.deleteIfExists(path);
            DownloadResponse response = aws.doDownload(generateTestDownloadRequest(), path.toFile()).get();
            ArgumentCaptor<GetObjectRequest> getObjectRequestCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
            verify(mockS3Client, times(1)).getObject(getObjectRequestCaptor.capture(), any(AsyncResponseTransformer.class));
            verifyDownloadTestResults(response, getObjectRequestCaptor, now);
        } finally {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                Assertions.fail();
            }
        }
    }

    @Test
    void testDoDownloadPath() throws ExecutionException, InterruptedException, IOException {
        Instant now = Instant.now();
        setupMockGetObjectResponse(now);
        Path path = Path.of("tempPath.txt");
        try {
            Files.deleteIfExists(path);
            DownloadResponse response = aws.doDownload(generateTestDownloadRequest(), path).get();
            ArgumentCaptor<GetObjectRequest> getObjectRequestCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
            verify(mockS3Client, times(1)).getObject(getObjectRequestCaptor.capture(), any(Path.class));
            verifyDownloadTestResults(response, getObjectRequestCaptor, now);
        } finally {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                Assertions.fail();
            }
        }
    }

    @Test
    void testDoDownloadInputStream() throws ExecutionException, InterruptedException, IOException {
        Instant now = Instant.now();
        setupMockGetObjectResponse(now);

        DownloadResponse response = aws.doDownload(generateTestDownloadRequest()).get();
        ArgumentCaptor<GetObjectRequest> getObjectRequestCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(mockS3Client, times(1)).getObject(getObjectRequestCaptor.capture(), any(InputStreamResponseTransformer.class));
        verifyDownloadTestResults(response, getObjectRequestCaptor, now);

    }

    void verifyDownloadTestResults(DownloadResponse response, ArgumentCaptor<GetObjectRequest> getObjectRequestCaptor, Instant now) {
        GetObjectRequest actualGetObjectRequest = getObjectRequestCaptor.getValue();
        assertEquals("bucket-1", actualGetObjectRequest.bucket());
        assertEquals("object-1", actualGetObjectRequest.key());
        assertEquals("version-1", actualGetObjectRequest.versionId());

        assertEquals("object-1", response.getKey());
        assertEquals("object-1", response.getMetadata().getKey());
        assertEquals("version-1", response.getMetadata().getVersionId());
        assertEquals("etag", response.getMetadata().getETag());
        assertEquals(now, response.getMetadata().getLastModified());
        assertEquals(Map.of("key1", "value1", "key2", "value2"), response.getMetadata().getMetadata());
        assertEquals(1024L, response.getMetadata().getObjectSize());
    }

    @Test
    void testDoDelete() throws ExecutionException, InterruptedException {
        DeleteObjectResponse response = mock(DeleteObjectResponse.class);
        when(mockS3Client.deleteObject(any(DeleteObjectRequest.class))).thenReturn(future(response));
        aws.doDelete("object-1", "version-1").get();

        ArgumentCaptor<DeleteObjectRequest> deleteObjectRequestCaptor = ArgumentCaptor.forClass(DeleteObjectRequest.class);
        verify(mockS3Client, times(1)).deleteObject(deleteObjectRequestCaptor.capture());
        DeleteObjectRequest actualDeleteObjectRequest = deleteObjectRequestCaptor.getValue();
        assertEquals("object-1", actualDeleteObjectRequest.key());
        assertEquals("version-1", actualDeleteObjectRequest.versionId());
        assertEquals("bucket-1", actualDeleteObjectRequest.bucket());
    }

    @Test
    void testDoBulkDelete() {
        Collection<BlobIdentifier> objects = List.of(new BlobIdentifier("object-1", "version-1"),
                new BlobIdentifier("object-2", "version-2"),
                new BlobIdentifier("object-3", "version-3"));
        List<String> keys = objects.stream().map(BlobIdentifier::getKey).collect(Collectors.toList());
        DeleteObjectsResponse response = mock(DeleteObjectsResponse.class);
        when(mockS3Client.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(future(response));
        aws.doDelete(objects);

        ArgumentCaptor<DeleteObjectsRequest> deleteObjectsRequestCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(mockS3Client, times(1)).deleteObjects(deleteObjectsRequestCaptor.capture());
        DeleteObjectsRequest actualDeleteObjectsRequest = deleteObjectsRequestCaptor.getValue();
        assertEquals("bucket-1", actualDeleteObjectsRequest.bucket());
        List<String> identifiers = actualDeleteObjectsRequest
                .delete()
                .objects()
                .stream()
                .map(ObjectIdentifier::key)
                .collect(Collectors.toList());
        assertTrue(identifiers.containsAll(keys));
    }

    @Test
    void testDoCopy() throws ExecutionException, InterruptedException {
        Instant now = Instant.now();
        CopyObjectResult mockResult = mock(CopyObjectResult.class);
        doReturn("eTag-1").when(mockResult).eTag();
        doReturn(now).when(mockResult).lastModified();

        CopyObjectResponse mockResponse = mock(CopyObjectResponse.class);
        doReturn(mockResult).when(mockResponse).copyObjectResult();
        doReturn("copyVersion-1").when(mockResponse).versionId();

        when(mockS3Client.copyObject((CopyObjectRequest) any())).thenReturn(future(mockResponse));

        CopyRequest request = CopyRequest
                .builder()
                .srcKey("src-object-1")
                .srcVersionId("version-1")
                .destBucket("dest-bucket-1")
                .destKey("dest-object-1")
                .build();
        CopyResponse copyResponse = aws.doCopy(request).get();

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
    void testDoGetMetadata() throws ExecutionException, InterruptedException {

        Instant now = Instant.now();
        Map<String, String> metadataMap = Map.of("key1", "value1", "key2", "value2");

        HeadObjectResponse mockResponse = mock(HeadObjectResponse.class);
        doReturn("version-1").when(mockResponse).versionId();
        doReturn(1024L).when(mockResponse).contentLength();
        doReturn("etag").when(mockResponse).eTag();
        doReturn(now).when(mockResponse).lastModified();
        doReturn(metadataMap).when(mockResponse).metadata();
        doReturn(future(mockResponse)).when(mockS3Client).headObject((HeadObjectRequest) any());

        BlobMetadata metadata = aws.doGetMetadata("object-1", "version-1").get();

        ArgumentCaptor<HeadObjectRequest> headObjectRequestCaptor = ArgumentCaptor.forClass(HeadObjectRequest.class);
        verify(mockS3Client, times(1)).headObject(headObjectRequestCaptor.capture());
        HeadObjectRequest actualHeadObjectRequest = headObjectRequestCaptor.getValue();
        assertEquals("object-1", actualHeadObjectRequest.key());
        assertEquals("version-1", actualHeadObjectRequest.versionId());
        assertEquals("bucket-1", actualHeadObjectRequest.bucket());

        assertEquals("object-1", metadata.getKey());
        assertEquals("version-1", metadata.getVersionId());
        assertEquals(1024L, metadata.getObjectSize());
        assertEquals("etag", metadata.getETag());
        assertEquals(now, metadata.getLastModified());
        assertEquals(metadataMap, metadata.getMetadata());
    }

    @Test
    void testDoList() throws ExecutionException, InterruptedException {
        ListBlobsRequest request = new ListBlobsRequest.Builder().withPrefix("abc").withDelimiter("/").build();
        ListObjectsV2Publisher publisher = mock(ListObjectsV2Publisher.class);
        when(mockS3Client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(publisher);
        when(publisher.subscribe(any(Consumer.class))).thenReturn(futureVoid());

        Consumer<ListBlobsBatch> consumer = batch -> {};
        aws.doList(request, consumer).get();

        ListObjectsV2Request awsRequest = transformerSupplier.get(BUCKET).toRequest(request);
        verify(mockS3Client).listObjectsV2Paginator(awsRequest);
        verify(publisher).subscribe(any(ConsumerWrapper.class));
    }

    @Test
    void testDoListPage() throws ExecutionException, InterruptedException {
        ListBlobsPageRequest request = ListBlobsPageRequest
                .builder()
                .withPrefix("abc")
                .withDelimiter("/")
                .withPaginationToken("next-token")
                .withMaxResults(50)
                .build();

        ListObjectsV2Response mockResponse = mock(ListObjectsV2Response.class);
        when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(future(mockResponse));

        // Mock response contents
        List<S3Object> s3Objects = List.of(
                S3Object.builder().key("key-1").size(100L).lastModified(Instant.now()).build(),
                S3Object.builder().key("key-2").size(200L).lastModified(Instant.now()).build(),
                S3Object.builder().key("key-3").size(300L).lastModified(Instant.now()).build()
        );
        when(mockResponse.contents()).thenReturn(s3Objects);
        when(mockResponse.isTruncated()).thenReturn(true);
        when(mockResponse.nextContinuationToken()).thenReturn("next-page-token");

        ListBlobsPageResponse response = aws.doListPage(request).get();

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
        assertEquals(3, response.getBlobs().size());
        assertTrue(response.isTruncated());
        assertEquals("next-page-token", response.getNextPageToken());

        // Verify blob details
        assertEquals("key-1", response.getBlobs().get(0).getKey());
        assertEquals(100L, response.getBlobs().get(0).getObjectSize());
        assertEquals("key-2", response.getBlobs().get(1).getKey());
        assertEquals(200L, response.getBlobs().get(1).getObjectSize());
        assertEquals("key-3", response.getBlobs().get(2).getKey());
        assertEquals(300L, response.getBlobs().get(2).getObjectSize());
    }

    @Test
    void testDoListPageEmpty() throws ExecutionException, InterruptedException {
        ListBlobsPageRequest request = ListBlobsPageRequest.builder().build();
        ListObjectsV2Response mockResponse = mock(ListObjectsV2Response.class);
        when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(future(mockResponse));
        when(mockResponse.contents()).thenReturn(List.of());
        when(mockResponse.isTruncated()).thenReturn(false);
        when(mockResponse.nextContinuationToken()).thenReturn(null);

        ListBlobsPageResponse response = aws.doListPage(request).get();

        assertNotNull(response);
        assertEquals(0, response.getBlobs().size());
        assertFalse(response.isTruncated());
        assertNull(response.getNextPageToken());
    }

    @Test
    void testDoInitiateMultipartUpload() throws ExecutionException, InterruptedException {

        CreateMultipartUploadResponse mockResponse = mock(CreateMultipartUploadResponse.class);
        doReturn("bucket-1").when(mockResponse).bucket();
        doReturn("object-1").when(mockResponse).key();
        doReturn("mpu-id").when(mockResponse).uploadId();
        doReturn(future(mockResponse)).when(mockS3Client).createMultipartUpload((CreateMultipartUploadRequest) any());
        Map<String, String> metadata = Map.of("key-1", "value-1");
        MultipartUploadRequest request = new MultipartUploadRequest.Builder().withKey("object-1").withMetadata(metadata).build();

        MultipartUpload response = aws.initiateMultipartUpload(request).get();

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
        assertEquals(metadata, response.getMetadata());
    }

    @Test
    void testDoUploadMultipartPart() throws ExecutionException, InterruptedException {
        UploadPartResponse mockResponse = mock(UploadPartResponse.class);
        doReturn("etag").when(mockResponse).eTag();
        doReturn(future(mockResponse)).when(mockS3Client).uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class));
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .build();
        byte[] content = "This is test data".getBytes(StandardCharsets.UTF_8);
        MultipartPart multipartPart = new MultipartPart(1, content);

        var response = aws.uploadMultipartPart(multipartUpload, multipartPart).get();

        // Verify the request is mapped to the SDK
        ArgumentCaptor<UploadPartRequest> requestCaptor = ArgumentCaptor.forClass(UploadPartRequest.class);
        ArgumentCaptor<AsyncRequestBody> requestBodyCaptor = ArgumentCaptor.forClass(AsyncRequestBody.class);
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
    void testDoCompleteMultipartUpload() throws ExecutionException, InterruptedException {
        CompleteMultipartUploadResponse mockResponse = mock(CompleteMultipartUploadResponse.class);
        doReturn("bucket-1").when(mockResponse).bucket();
        doReturn("object-1").when(mockResponse).key();
        doReturn("complete-etag").when(mockResponse).eTag();
        doReturn(future(mockResponse)).when(mockS3Client).completeMultipartUpload((CompleteMultipartUploadRequest) any());
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .build();
        List<com.salesforce.multicloudj.blob.driver.UploadPartResponse> listOfParts = List.of(new com.salesforce.multicloudj.blob.driver.UploadPartResponse(1, "etag", 0));

        MultipartUploadResponse response = aws.completeMultipartUpload(multipartUpload, listOfParts).get();

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
    void testDoListMultipartUpload() throws ExecutionException, InterruptedException {
        ListPartsResponse mockResponse = mock(ListPartsResponse.class);
        doReturn("bucket-1").when(mockResponse).bucket();
        doReturn("object-1").when(mockResponse).key();
        doReturn("mpu-id").when(mockResponse).uploadId();
        List<Part> parts = List.of(
                Part.builder().partNumber(1).eTag("etag1").size(3000L).build(),
                Part.builder().partNumber(2).eTag("etag2").size(2000L).build(),
                Part.builder().partNumber(3).eTag("etag3").size(1000L).build());
        doReturn(parts).when(mockResponse).parts();
        doReturn(future(mockResponse)).when(mockS3Client).listParts((ListPartsRequest) any());
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .build();

        var response = aws.listMultipartUpload(multipartUpload).get();

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
    void testDoAbortMultipartUpload() throws ExecutionException, InterruptedException {
        AbortMultipartUploadResponse mockResponse = mock(AbortMultipartUploadResponse.class);
        doReturn(future(mockResponse)).when(mockS3Client).abortMultipartUpload((AbortMultipartUploadRequest) any());
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .build();

        aws.abortMultipartUpload(multipartUpload).get();

        // Verify the request is mapped to the SDK
        ArgumentCaptor<AbortMultipartUploadRequest> requestCaptor = ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
        verify(mockS3Client, times(1)).abortMultipartUpload(requestCaptor.capture());
        AbortMultipartUploadRequest actualRequest = requestCaptor.getValue();
        assertEquals("object-1", actualRequest.key());
        assertEquals("bucket-1", actualRequest.bucket());
        assertEquals("mpu-id", actualRequest.uploadId());
    }

    @Test
    void testDoGetTags() throws ExecutionException, InterruptedException {
        GetObjectTaggingResponse mockResponse = mock(GetObjectTaggingResponse.class);
        List<Tag> tags = List.of(Tag.builder().key("key1").value("value1").build(), Tag.builder().key("key2").value("value2").build());
        doReturn(tags).when(mockResponse).tagSet();
        doReturn(future(mockResponse)).when(mockS3Client).getObjectTagging((GetObjectTaggingRequest)any());

        Map<String,String> tagsResult = aws.getTags("object-1").get();

        ArgumentCaptor<GetObjectTaggingRequest> getObjectTaggingRequestCaptor = ArgumentCaptor.forClass(GetObjectTaggingRequest.class);
        verify(mockS3Client, times(1)).getObjectTagging(getObjectTaggingRequestCaptor.capture());
        GetObjectTaggingRequest actualGetObjectTaggingRequest = getObjectTaggingRequestCaptor.getValue();
        assertEquals("object-1", actualGetObjectTaggingRequest.key());
        assertEquals("bucket-1", actualGetObjectTaggingRequest.bucket());
        assertEquals(Map.of("key1", "value1", "key2", "value2"), tagsResult);
    }

    @Test
    void testDoSetTags() throws ExecutionException, InterruptedException {
        PutObjectTaggingResponse mockResponse = mock(PutObjectTaggingResponse.class);
        doReturn(future(mockResponse)).when(mockS3Client).putObjectTagging((PutObjectTaggingRequest)any());

        Map<String, String> tags = Map.of("key1", "value1", "key2", "value2");
        aws.setTags("object-1", tags).get();

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
    void testDoGeneratePresignedUploadUrl() throws MalformedURLException, ExecutionException, InterruptedException {
        AwsAsyncBlobStore spyAws = spy(aws);
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

        URL actualUrl = spyAws.doGeneratePresignedUrl(presignedUrlRequest).get();
        assertEquals(url, actualUrl);
    }

    @Test
    void testDoGeneratePresignedDownloadUrl() throws MalformedURLException, ExecutionException, InterruptedException {
        AwsAsyncBlobStore spyAws = spy(aws);
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

        URL actualUrl = spyAws.doGeneratePresignedUrl(presignedUrlRequest).get();
        assertEquals(url, actualUrl);
    }

    @Test
    void testDoDoesObjectExist() throws ExecutionException, InterruptedException {

        HeadObjectResponse mockResponse = mock(HeadObjectResponse.class);
        doReturn("version-1").when(mockResponse).versionId();
        doReturn(future(mockResponse)).when(mockS3Client).headObject((HeadObjectRequest) any());

        boolean result = aws.doDoesObjectExist("object-1", "version-1").get();

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
        doReturn(CompletableFuture.failedFuture(mockException)).when(mockS3Client).headObject(any(HeadObjectRequest.class));
        result = aws.doDoesObjectExist("object-1", "version-1").get();
        assertFalse(result);

        // Verify the unexpected error state
        doReturn(CompletableFuture.failedFuture(mock(RuntimeException.class))).when(mockS3Client).headObject(any(HeadObjectRequest.class));
        var exceptionalResult = aws.doDoesObjectExist("object-1", "version-1");
        assertTrue(exceptionalResult.isCompletedExceptionally());
        assertInstanceOf(SubstrateSdkException.class, assertThrows(ExecutionException.class, exceptionalResult::get).getCause());
    }

    @Test
    void doDownloadDirectory() throws ExecutionException, InterruptedException {

        DirectoryDownload awsResponseFuture = mock(DirectoryDownload.class);
        CompletedDirectoryDownload awsResponse = mock(CompletedDirectoryDownload.class);
        doReturn(awsResponseFuture).when(mockS3TransferManager).downloadDirectory(any(DownloadDirectoryRequest.class));
        doReturn(future(awsResponse)).when(awsResponseFuture).completionFuture();

        Exception failedDownloadException = new RuntimeException("Fake exception!");
        Path failedDownloadPath = Paths.get("files/business/taxes.csv");
        DownloadFileRequest downloadFileRequest = DownloadFileRequest.builder()
                .destination(failedDownloadPath)
                .getObjectRequest(mock(GetObjectRequest.class))
                .build();
        List<FailedFileDownload> failedTransfers = List.of(FailedFileDownload.builder()
                .request(downloadFileRequest)
                .exception(failedDownloadException)
                .build());
        doReturn(failedTransfers).when(awsResponse).failedTransfers();

        String destination = "/home/documents";
        DirectoryDownloadRequest downloadRequest = DirectoryDownloadRequest.builder()
                .prefixToDownload("files/")
                .prefixesToExclude(List.of("files/personal", "files/images"))
                .localDestinationDirectory(destination)
                .build();

        // Perform the request
        DirectoryDownloadResponse response = aws.doDownloadDirectory(downloadRequest).get();

        // Verify the wiring
        ArgumentCaptor<DownloadDirectoryRequest> requestCaptor = ArgumentCaptor.forClass(DownloadDirectoryRequest.class);
        verify(mockS3TransferManager, times(1)).downloadDirectory(requestCaptor.capture());
        var actualCapturedValue = requestCaptor.getValue();
        assertEquals(BUCKET, actualCapturedValue.bucket());
        assertEquals(destination, actualCapturedValue.destination().toString());

        // Verify the results
        assertEquals(1, response.getFailedTransfers().size());
        assertEquals(failedDownloadException, response.getFailedTransfers().get(0).getException());
        assertEquals(failedDownloadPath, response.getFailedTransfers().get(0).getDestination());
    }

    @Test
    void doUploadDirectory() throws ExecutionException, InterruptedException {
        DirectoryUpload awsResponseFuture = mock(DirectoryUpload.class);
        CompletedDirectoryUpload awsResponse = mock(CompletedDirectoryUpload.class);
        doReturn(awsResponseFuture).when(mockS3TransferManager).uploadDirectory(any(UploadDirectoryRequest.class));
        doReturn(future(awsResponse)).when(awsResponseFuture).completionFuture();

        Exception failedUploadException = new RuntimeException("Fake exception!");
        Path failedUploadPath = Paths.get("/home/documents/files/business/taxes.csv");
        UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
                .source(failedUploadPath)
                .putObjectRequest(mock(PutObjectRequest.class))
                .build();
        List<FailedFileUpload> failedTransfers = List.of(FailedFileUpload.builder()
                .request(uploadFileRequest)
                .exception(failedUploadException)
                .build());
        doReturn(failedTransfers).when(awsResponse).failedTransfers();

        String source = "/home/documents";
        DirectoryUploadRequest uploadRequest = DirectoryUploadRequest.builder()
                .localSourceDirectory(source)
                .prefix("files/")
                .includeSubFolders(true)
                .build();

        // Perform the request
        DirectoryUploadResponse response = aws.doUploadDirectory(uploadRequest).get();

        // Verify the wiring
        ArgumentCaptor<UploadDirectoryRequest> requestCaptor = ArgumentCaptor.forClass(UploadDirectoryRequest.class);
        verify(mockS3TransferManager, times(1)).uploadDirectory(requestCaptor.capture());
        var actualCapturedValue = requestCaptor.getValue();
        assertEquals(BUCKET, actualCapturedValue.bucket());
        assertEquals(source, actualCapturedValue.source().toString());

        // Verify the results
        assertEquals(1, response.getFailedTransfers().size());
        assertEquals(failedUploadException, response.getFailedTransfers().get(0).getException());
        assertEquals(failedUploadPath, response.getFailedTransfers().get(0).getSource());
    }

    @Test
    void doDeleteDirectory() throws ExecutionException, InterruptedException {

        // Arrange it so calling doList() returns these blobs via the async publisher
        ListObjectsV2Publisher publisher = mock(ListObjectsV2Publisher.class);
        S3Object object1 = S3Object.builder().key("file1.txt").size(123L).build();
        S3Object object2 = S3Object.builder().key("file2.txt").size(456L).build();
        S3Object object3 = S3Object.builder().key("file3.txt").size(789L).build();
        doAnswer(invocation -> {
            Consumer consumer = invocation.getArgument(0);
            consumer.accept(ListObjectsV2Response.builder().contents(object1, object2).build());
            consumer.accept(ListObjectsV2Response.builder().contents(object3).build());
            return futureVoid();
        }).when(publisher).subscribe(any(Consumer.class));
        DeleteObjectsResponse response = mock(DeleteObjectsResponse.class);
        when(mockS3Client.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(future(response));
        when(mockS3Client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(publisher);

        // Perform the request
        aws.doDeleteDirectory("files").get();

        // Verify the wiring
        verify(mockS3Client).listObjectsV2Paginator(any(ListObjectsV2Request.class));
        verify(publisher).subscribe(any(ConsumerWrapper.class));
        ArgumentCaptor<DeleteObjectsRequest> requestCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(mockS3Client, times(2)).deleteObjects(requestCaptor.capture());

        // Verify every object was deleted
        List<DeleteObjectsRequest> actualDeleteRequests = requestCaptor.getAllValues();
        assertEquals(2, actualDeleteRequests.size());
        List<ObjectIdentifier> firstObjectsDeleted = actualDeleteRequests.get(0).delete().objects();
        assertEquals(2, firstObjectsDeleted.size());
        assertEquals(object1.key(), firstObjectsDeleted.get(0).key());
        assertEquals(object2.key(), firstObjectsDeleted.get(1).key());
        List<ObjectIdentifier> secondObjectsDeleted = actualDeleteRequests.get(1).delete().objects();
        assertEquals(1, secondObjectsDeleted.size());
        assertEquals(object3.key(), secondObjectsDeleted.get(0).key());
    }
}
