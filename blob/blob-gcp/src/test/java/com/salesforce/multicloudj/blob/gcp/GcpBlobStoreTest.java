package com.salesforce.multicloudj.blob.gcp;

import com.google.api.gax.paging.Page;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.io.ByteStreams;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
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
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.provider.Provider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GcpBlobStoreTest {

    private static final String TEST_BUCKET = "test-bucket";
    private static final String TEST_KEY = "test-key";
    private static final String TEST_VERSION_ID = "12345";
    private static final String TEST_ETAG = "test-etag";
    private static final Long TEST_GENERATION = 12345L;
    private static final Long TEST_SIZE = 1024L;
    private static final byte[] TEST_CONTENT = "test content".getBytes();

    @Mock
    private Storage mockStorage;

    @Mock
    private GcpTransformer mockTransformer;

    @Mock
    private GcpTransformerSupplier mockTransformerSupplier;

    @Mock
    private Blob mockBlob;

    @Mock
    private BlobId mockBlobId;

    @Mock
    private BlobInfo mockBlobInfo;

    @Mock
    private WriteChannel mockWriteChannel;

    @Mock
    private ReadChannel mockReadChannel;

    @Mock
    private Storage.CopyRequest mockCopyRequest;

    @Mock
    private CopyWriter mockCopyWriter;

    @TempDir
    Path tempDir;

    private GcpBlobStore gcpBlobStore;

    @BeforeEach
    void setUp() {
        GcpBlobStore.Builder builder = (GcpBlobStore.Builder) new GcpBlobStore.Builder()
                .withStorage(mockStorage)
                .withTransformerSupplier(mockTransformerSupplier)
                .withBucket(TEST_BUCKET);

        when(mockTransformerSupplier.get(TEST_BUCKET)).thenReturn(mockTransformer);
        gcpBlobStore = new GcpBlobStore(builder, mockStorage);
    }

    @Test
    void testBuilder() {
        Provider.Builder providerBuilder = gcpBlobStore.builder();
        assertNotNull(providerBuilder);
        assertInstanceOf(GcpBlobStore.Builder.class, providerBuilder);
    }

    @Test
    void testDoUpload_WithInputStream() throws IOException {
        try (MockedStatic<ByteStreams> mockedStatic = Mockito.mockStatic(ByteStreams.class)) {
            // Given
            UploadRequest uploadRequest = UploadRequest.builder()
                    .withKey(TEST_KEY)
                    .build();

            UploadResponse expectedResponse = UploadResponse.builder()
                    .key(TEST_KEY)
                    .versionId(TEST_VERSION_ID)
                    .eTag(TEST_ETAG)
                    .build();

            when(mockTransformer.toBlobInfo(uploadRequest)).thenReturn(mockBlobInfo);
            when(mockStorage.writer(mockBlobInfo)).thenReturn(mockWriteChannel);
            when(mockStorage.get(TEST_BUCKET, TEST_KEY)).thenReturn(mockBlob);
            when(mockTransformer.toUploadResponse(mockBlob)).thenReturn(expectedResponse);

            // When
            UploadResponse response = gcpBlobStore.doUpload(uploadRequest, new ByteArrayInputStream(TEST_CONTENT));

            // Then
            assertEquals(expectedResponse, response);
            verify(mockStorage).writer(mockBlobInfo);
            verify(mockStorage).get(TEST_BUCKET, TEST_KEY);
            verify(mockTransformer).toUploadResponse(mockBlob);
        }
    }

    @Test
    void testDoUpload_WithInputStream_ThrowsException() throws IOException {
        try (MockedStatic<ByteStreams> mockedStatic = Mockito.mockStatic(ByteStreams.class)) {
            // Given
            UploadRequest uploadRequest = UploadRequest.builder()
                    .withKey(TEST_KEY)
                    .build();

            when(mockTransformer.toBlobInfo(uploadRequest)).thenReturn(mockBlobInfo);
            when(mockStorage.writer(mockBlobInfo)).thenReturn(mockWriteChannel);
            mockedStatic.when(() -> ByteStreams.copy(any(InputStream.class), any(OutputStream.class)))
                    .thenThrow(new IOException("Test exception"));

            // When & Then
            SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
                gcpBlobStore.doUpload(uploadRequest, new ByteArrayInputStream(TEST_CONTENT));
            });
            assertEquals("Request failed while uploading from input stream", exception.getMessage());
        }
    }

    @Test
    void testDoUpload_WithByteArray() {
        // Given
        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .build();

        UploadResponse expectedResponse = UploadResponse.builder()
                .key(TEST_KEY)
                .versionId(TEST_VERSION_ID)
                .eTag(TEST_ETAG)
                .build();

        when(mockTransformer.toBlobInfo(uploadRequest)).thenReturn(mockBlobInfo);
        when(mockStorage.create(mockBlobInfo, TEST_CONTENT)).thenReturn(mockBlob);
        when(mockTransformer.toUploadResponse(mockBlob)).thenReturn(expectedResponse);

        // When
        UploadResponse response = gcpBlobStore.doUpload(uploadRequest, TEST_CONTENT);

        // Then
        assertEquals(expectedResponse, response);
        verify(mockStorage).create(mockBlobInfo, TEST_CONTENT);
        verify(mockTransformer).toUploadResponse(mockBlob);
    }

    @Test
    void testDoUpload_WithFile() throws IOException {
        // Given
        Path testFile = tempDir.resolve("test.txt");
        Files.write(testFile, TEST_CONTENT);

        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .build();

        UploadResponse expectedResponse = UploadResponse.builder()
                .key(TEST_KEY)
                .versionId(TEST_VERSION_ID)
                .eTag(TEST_ETAG)
                .build();

        when(mockTransformer.toBlobInfo(uploadRequest)).thenReturn(mockBlobInfo);
        when(mockStorage.createFrom(mockBlobInfo, testFile)).thenReturn(mockBlob);
        when(mockTransformer.toUploadResponse(mockBlob)).thenReturn(expectedResponse);

        // When
        UploadResponse response = gcpBlobStore.doUpload(uploadRequest, testFile.toFile());

        // Then
        assertEquals(expectedResponse, response);
        verify(mockStorage).createFrom(mockBlobInfo, testFile);
        verify(mockTransformer).toUploadResponse(mockBlob);
    }

    @Test
    void testDoUpload_WithPath() throws IOException {
        // Given
        Path testFile = tempDir.resolve("test.txt");
        Files.write(testFile, TEST_CONTENT);

        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .build();

        UploadResponse expectedResponse = UploadResponse.builder()
                .key(TEST_KEY)
                .versionId(TEST_VERSION_ID)
                .eTag(TEST_ETAG)
                .build();

        when(mockTransformer.toBlobInfo(uploadRequest)).thenReturn(mockBlobInfo);
        when(mockStorage.createFrom(mockBlobInfo, testFile)).thenReturn(mockBlob);
        when(mockTransformer.toUploadResponse(mockBlob)).thenReturn(expectedResponse);

        // When
        UploadResponse response = gcpBlobStore.doUpload(uploadRequest, testFile);

        // Then
        assertEquals(expectedResponse, response);
        verify(mockStorage).createFrom(mockBlobInfo, testFile);
        verify(mockTransformer).toUploadResponse(mockBlob);
    }

    @Test
    void testDoUpload_WithPath_ThrowsException() throws IOException {
        // Given
        Path testFile = tempDir.resolve("test.txt");
        Files.write(testFile, TEST_CONTENT);

        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(TEST_KEY)
                .build();

        when(mockTransformer.toBlobInfo(uploadRequest)).thenReturn(mockBlobInfo);
        when(mockStorage.createFrom(mockBlobInfo, testFile)).thenThrow(new IOException("Test exception"));

        // When & Then
        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            gcpBlobStore.doUpload(uploadRequest, testFile);
        });
        assertEquals("Request failed while uploading from path", exception.getMessage());
    }

    @Test
    void testDoDownload_WithOutputStream() throws IOException {
        try (MockedStatic<ByteStreams> mockedStatic = Mockito.mockStatic(ByteStreams.class)) {
            // Given
            DownloadRequest downloadRequest = DownloadRequest.builder()
                    .withKey(TEST_KEY)
                    .build();

            DownloadResponse expectedResponse = DownloadResponse.builder()
                    .key(TEST_KEY)
                    .build();

            when(mockTransformer.toBlobId(downloadRequest)).thenReturn(mockBlobId);
            when(mockStorage.reader(mockBlobId)).thenReturn(mockReadChannel);
            when(mockStorage.get(mockBlobId)).thenReturn(mockBlob);
            when(mockTransformer.toDownloadResponse(mockBlob)).thenReturn(expectedResponse);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            // When
            DownloadResponse response = gcpBlobStore.doDownload(downloadRequest, outputStream);

            // Then
            assertEquals(expectedResponse, response);
            verify(mockStorage).reader(mockBlobId);
            verify(mockStorage).get(mockBlobId);
            verify(mockTransformer).toDownloadResponse(mockBlob);
        }
    }

    @Test
    void testDoDownload_WithOutputStream_WithRange() throws IOException {
        try (MockedStatic<ByteStreams> mockedStatic = Mockito.mockStatic(ByteStreams.class)) {
            // Given
            DownloadRequest downloadRequest = DownloadRequest.builder()
                    .withKey(TEST_KEY)
                    .withRange(10L, 20L)
                    .build();

            DownloadResponse expectedResponse = DownloadResponse.builder()
                    .key(TEST_KEY)
                    .build();

            when(mockTransformer.toBlobId(downloadRequest)).thenReturn(mockBlobId);
            when(mockStorage.reader(mockBlobId)).thenReturn(mockReadChannel);
            when(mockStorage.get(mockBlobId)).thenReturn(mockBlob);
            when(mockTransformer.toDownloadResponse(mockBlob)).thenReturn(expectedResponse);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            // When
            DownloadResponse response = gcpBlobStore.doDownload(downloadRequest, outputStream);

            // Then
            assertEquals(expectedResponse, response);
            verify(mockReadChannel).seek(10L);
            verify(mockReadChannel).limit(20L);
        }
    }

    @Test
    void testDoDownload_WithOutputStream_ThrowsException() throws IOException {
        try (MockedStatic<ByteStreams> mockedStatic = Mockito.mockStatic(ByteStreams.class)) {
            // Given
            DownloadRequest downloadRequest = DownloadRequest.builder()
                    .withKey(TEST_KEY)
                    .build();

            when(mockTransformer.toBlobId(downloadRequest)).thenReturn(mockBlobId);
            when(mockStorage.reader(mockBlobId)).thenReturn(mockReadChannel);
            mockedStatic.when(() -> ByteStreams.copy(any(InputStream.class), any(OutputStream.class)))
                    .thenThrow(new IOException("Test exception"));

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            // When & Then
            SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
                gcpBlobStore.doDownload(downloadRequest, outputStream);
            });
            assertEquals("Request failed during download", exception.getMessage());
        }
    }

    @Test
    void testDoDownload_WithByteArray() throws IOException {
        try (MockedStatic<ByteStreams> mockedStatic = Mockito.mockStatic(ByteStreams.class)) {
            // Given
            DownloadRequest downloadRequest = DownloadRequest.builder()
                    .withKey(TEST_KEY)
                    .build();

            DownloadResponse expectedResponse = DownloadResponse.builder()
                    .key(TEST_KEY)
                    .build();

            when(mockTransformer.toBlobId(downloadRequest)).thenReturn(mockBlobId);
            when(mockStorage.reader(mockBlobId)).thenReturn(mockReadChannel);
            when(mockStorage.get(mockBlobId)).thenReturn(mockBlob);
            when(mockTransformer.toDownloadResponse(mockBlob)).thenReturn(expectedResponse);

            ByteArray byteArray = new ByteArray();

            // When
            DownloadResponse response = gcpBlobStore.doDownload(downloadRequest, byteArray);

            // Then
            assertEquals(expectedResponse, response);
            assertArrayEquals(new byte[0], byteArray.getBytes()); // Empty because mock doesn't write data
        }
    }

    @Test
    void testDoDownload_WithFile() throws IOException {
        try (MockedStatic<ByteStreams> mockedStatic = Mockito.mockStatic(ByteStreams.class)) {
            // Given
            Path testFile = tempDir.resolve("download.txt");
            DownloadRequest downloadRequest = DownloadRequest.builder()
                    .withKey(TEST_KEY)
                    .build();

            DownloadResponse expectedResponse = DownloadResponse.builder()
                    .key(TEST_KEY)
                    .build();

            when(mockTransformer.toBlobId(downloadRequest)).thenReturn(mockBlobId);
            when(mockStorage.reader(mockBlobId)).thenReturn(mockReadChannel);
            when(mockStorage.get(mockBlobId)).thenReturn(mockBlob);
            when(mockTransformer.toDownloadResponse(mockBlob)).thenReturn(expectedResponse);

            // When
            DownloadResponse response = gcpBlobStore.doDownload(downloadRequest, testFile.toFile());

            // Then
            assertEquals(expectedResponse, response);
        }
    }

    @Test
    void testDoDownload_WithPath() throws IOException {
        try (MockedStatic<ByteStreams> mockedStatic = Mockito.mockStatic(ByteStreams.class)) {
            // Given
            Path testFile = tempDir.resolve("download.txt");
            DownloadRequest downloadRequest = DownloadRequest.builder()
                    .withKey(TEST_KEY)
                    .build();

            DownloadResponse expectedResponse = DownloadResponse.builder()
                    .key(TEST_KEY)
                    .build();

            when(mockTransformer.toBlobId(downloadRequest)).thenReturn(mockBlobId);
            when(mockStorage.reader(mockBlobId)).thenReturn(mockReadChannel);
            when(mockStorage.get(mockBlobId)).thenReturn(mockBlob);
            when(mockTransformer.toDownloadResponse(mockBlob)).thenReturn(expectedResponse);

            // When
            DownloadResponse response = gcpBlobStore.doDownload(downloadRequest, testFile);

            // Then
            assertEquals(expectedResponse, response);
        }
    }

    @Test
    void testDoDownload_WithPath_ThrowsException() throws IOException {
        try (MockedStatic<ByteStreams> mockedStatic = Mockito.mockStatic(ByteStreams.class)) {
            // Given
            Path testFile = tempDir.resolve("download.txt");
            DownloadRequest downloadRequest = DownloadRequest.builder()
                    .withKey(TEST_KEY)
                    .build();

            when(mockTransformer.toBlobId(downloadRequest)).thenReturn(mockBlobId);
            when(mockStorage.reader(mockBlobId)).thenReturn(mockReadChannel);
            mockedStatic.when(() -> ByteStreams.copy(any(InputStream.class), any(OutputStream.class)))
                    .thenThrow(new IOException("Test exception"));

            // When & Then
            SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
                gcpBlobStore.doDownload(downloadRequest, testFile);
            });
            assertEquals("Request failed during download", exception.getMessage());
        }
    }

    @Test
    void testDoDelete_WithKeyAndVersionId() {
        // Given
        when(mockTransformer.toBlobId(TEST_KEY, TEST_VERSION_ID)).thenReturn(mockBlobId);

        // When
        gcpBlobStore.doDelete(TEST_KEY, TEST_VERSION_ID);

        // Then
        verify(mockStorage).delete(mockBlobId);
    }

    @Test
    void testDoDelete_WithKeyAndNullVersionId() {
        // Given
        when(mockTransformer.toBlobId(TEST_KEY, null)).thenReturn(mockBlobId);

        // When
        gcpBlobStore.doDelete(TEST_KEY, null);

        // Then
        verify(mockStorage).delete(mockBlobId);
    }

    @Test
    void testDoDelete_WithCollection() {
        // Given
        BlobIdentifier blobId1 = new BlobIdentifier("key1", "version1");
        BlobIdentifier blobId2 = new BlobIdentifier("key2", "version2");
        Collection<BlobIdentifier> objects = Arrays.asList(blobId1, blobId2);

        BlobId mockBlobId1 = mock(BlobId.class);
        BlobId mockBlobId2 = mock(BlobId.class);

        when(mockTransformer.toBlobId("key1", "version1")).thenReturn(mockBlobId1);
        when(mockTransformer.toBlobId("key2", "version2")).thenReturn(mockBlobId2);

        // When
        gcpBlobStore.doDelete(objects);

        // Then
        verify(mockStorage).delete(Arrays.asList(mockBlobId1, mockBlobId2));
    }

    @Test
    void testDoCopy() {
        // Given
        CopyRequest copyRequest = CopyRequest.builder()
                .srcKey("source-key")
                .destKey("dest-key")
                .build();

        CopyResponse expectedResponse = CopyResponse.builder()
                .key(TEST_KEY)
                .versionId(TEST_VERSION_ID)
                .eTag(TEST_ETAG)
                .build();

        when(mockTransformer.toCopyRequest(copyRequest)).thenReturn(mockCopyRequest);
        when(mockStorage.copy(mockCopyRequest)).thenReturn(mockCopyWriter);
        when(mockCopyWriter.getResult()).thenReturn(mockBlob);
        when(mockTransformer.toCopyResponse(mockBlob)).thenReturn(expectedResponse);

        // When
        CopyResponse response = gcpBlobStore.doCopy(copyRequest);

        // Then
        assertEquals(expectedResponse, response);
        verify(mockStorage).copy(mockCopyRequest);
        verify(mockTransformer).toCopyResponse(mockBlob);
    }

    @Test
    void testDoGetMetadata() {
        // Given
        BlobMetadata expectedMetadata = BlobMetadata.builder()
                .key(TEST_KEY)
                .versionId(TEST_VERSION_ID)
                .eTag(TEST_ETAG)
                .build();

        when(mockTransformer.toBlobId(TEST_KEY, TEST_VERSION_ID)).thenReturn(mockBlobId);
        when(mockStorage.get(mockBlobId)).thenReturn(mockBlob);
        when(mockTransformer.toBlobMetadata(mockBlob)).thenReturn(expectedMetadata);

        // When
        BlobMetadata metadata = gcpBlobStore.doGetMetadata(TEST_KEY, TEST_VERSION_ID);

        // Then
        assertEquals(expectedMetadata, metadata);
        verify(mockStorage).get(mockBlobId);
        verify(mockTransformer).toBlobMetadata(mockBlob);
    }

    @Test
    void testDoList_WithPrefix() {
        // Given
        ListBlobsRequest request = ListBlobsRequest.builder()
                .withPrefix("test-prefix")
                .build();

        List<com.google.cloud.storage.Blob> mockBlobs = Arrays.asList(mockBlob, mockBlob);
        Page mockPage = mock(Page.class);
        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class))).thenReturn(mockPage);
        when(mockPage.iterateAll()).thenReturn(mockBlobs);
        when(mockBlob.getName()).thenReturn("test-key-1", "test-key-2");
        when(mockBlob.getSize()).thenReturn(1024L, 2048L);

        // When
        var iterator = gcpBlobStore.doList(request);

        // Then
        assertTrue(iterator.hasNext());
        var blobInfo1 = iterator.next();
        assertEquals("test-key-1", blobInfo1.getKey());
        assertEquals(1024L, blobInfo1.getObjectSize());

        assertTrue(iterator.hasNext());
        var blobInfo2 = iterator.next();
        assertEquals("test-key-2", blobInfo2.getKey());
        assertEquals(2048L, blobInfo2.getObjectSize());

        assertFalse(iterator.hasNext());
    }

    @Test
    void testDoList_WithPrefixAndDelimiter() {
        // Given
        ListBlobsRequest request = ListBlobsRequest.builder()
                .withPrefix("test-prefix")
                .withDelimiter("/")
                .build();

        Page mockPage = mock(Page.class);
        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class))).thenReturn(mockPage);
        when(mockPage.iterateAll()).thenReturn(Collections.emptyList());

        // When
        var iterator = gcpBlobStore.doList(request);

        // Then
        assertFalse(iterator.hasNext());
    }

    @Test
    void testDoGetTags() {
        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            gcpBlobStore.doGetTags(TEST_KEY);
        });
        assertEquals("Tags are not supported by GCP", exception.getMessage());
    }

    @Test
    void testDoSetTags() {

        Map<String, String> tags = Map.of("tag1","value1","tag2","value2");
        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            gcpBlobStore.doSetTags(TEST_KEY, tags);
        });
        assertEquals("Tags are not supported by GCP", exception.getMessage());
    }

    @Test
    void testDoDoesObjectExist_WithVersionId() {
        // Given
        when(mockTransformer.toBlobId(TEST_KEY, TEST_VERSION_ID)).thenReturn(mockBlobId);
        when(mockStorage.get(mockBlobId)).thenReturn(mockBlob);

        // When
        boolean exists = gcpBlobStore.doDoesObjectExist(TEST_KEY, TEST_VERSION_ID);

        // Then
        assertTrue(exists);
        verify(mockStorage).get(mockBlobId);
    }

    @Test
    void testDoDoesObjectExist_WithNullVersionId() {
        // Given
        when(mockTransformer.toBlobId(TEST_KEY, null)).thenReturn(mockBlobId);
        when(mockStorage.get(mockBlobId)).thenReturn(null);

        // When
        boolean exists = gcpBlobStore.doDoesObjectExist(TEST_KEY, null);

        // Then
        assertFalse(exists);
        verify(mockStorage).get(mockBlobId);
    }

    @Test
    void testGetException_WithSubstrateSdkException() {
        // Given
        SubstrateSdkException testException = new SubstrateSdkException("Test");

        // When
        Class<? extends SubstrateSdkException> exceptionClass = gcpBlobStore.getException(testException);

        // Then
        assertEquals(SubstrateSdkException.class, exceptionClass);
    }

    @Test
    void testGetException_WithApiException() {
        // Given
        StatusCode mockStatusCode = mock(StatusCode.class);
        when(mockStatusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);

        ApiException apiException = mock(ApiException.class);
        when(apiException.getStatusCode()).thenReturn(mockStatusCode);

        // When
        Class<? extends SubstrateSdkException> exceptionClass = gcpBlobStore.getException(apiException);

        // Then
        assertNotNull(exceptionClass);
    }

    @Test
    void testGetException_WithStorageException() {
        // Given
        StorageException storageException = mock(StorageException.class);
        when(storageException.getCode()).thenReturn(404);

        // When
        Class<? extends SubstrateSdkException> exceptionClass = gcpBlobStore.getException(storageException);

        // Then
        assertNotNull(exceptionClass);
    }

    @Test
    void testGetException_WithUnknownException() {
        // Given
        RuntimeException runtimeException = new RuntimeException("Test");

        // When
        Class<? extends SubstrateSdkException> exceptionClass = gcpBlobStore.getException(runtimeException);

        // Then
        assertEquals(UnknownException.class, exceptionClass);
    }

    @Test
    void testDoInitiateMultipartUpload() {
        MultipartUploadRequest mpu = new MultipartUploadRequest.Builder()
                .withKey(TEST_KEY)
                .withMetadata(Map.of("key1","value1","key2","value2"))
                .build();
        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            gcpBlobStore.doInitiateMultipartUpload(mpu);
        });
        assertEquals("MultipartUploads are not supported by GCP", exception.getMessage());
    }

    @Test
    void testDoUploadMultipartPart() {
        MultipartUpload mpu = new MultipartUpload(TEST_BUCKET, TEST_KEY, "id");
        MultipartPart mpp = new MultipartPart(1, "TestData".getBytes(StandardCharsets.UTF_8));
        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            gcpBlobStore.doUploadMultipartPart(mpu, mpp);
        });
        assertEquals("MultipartUploads are not supported by GCP", exception.getMessage());
    }

    @Test
    void testDoCompleteMultipartUpload() {
        MultipartUpload mpu = new MultipartUpload(TEST_BUCKET, TEST_KEY, "id");
        List<UploadPartResponse> parts = List.of(new UploadPartResponse(1,"etag", 100));
        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            gcpBlobStore.doCompleteMultipartUpload(mpu, parts);
        });
        assertEquals("MultipartUploads are not supported by GCP", exception.getMessage());
    }

    @Test
    void testDoListMultipartUpload() {
        MultipartUpload mpu = new MultipartUpload(TEST_BUCKET, TEST_KEY, "id");
        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            gcpBlobStore.doListMultipartUpload(mpu);
        });
        assertEquals("MultipartUploads are not supported by GCP", exception.getMessage());
    }

    @Test
    void testDoAbortMultipartUpload() {
        MultipartUpload mpu = new MultipartUpload(TEST_BUCKET, TEST_KEY, "id");
        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            gcpBlobStore.doAbortMultipartUpload(mpu);
        });
        assertEquals("MultipartUploads are not supported by GCP", exception.getMessage());
    }

    @Test
    void testDoGeneratePresignedUrl_Upload() throws Exception {
        // Given
        Duration duration = Duration.ofHours(4);
        Map<String, String> metadata = Map.of("some-key", "some-value");
        PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.UPLOAD)
                .key(TEST_KEY)
                .duration(duration)
                .metadata(metadata)
                .build();

        URL expectedUrl = new URL("https://signed-url-for-upload.example.com");
        
        when(mockTransformer.toBlobInfo(presignedUrlRequest)).thenReturn(mockBlobInfo);
        when(mockStorage.signUrl(eq(mockBlobInfo),
                any(Long.class),
                eq(TimeUnit.MILLISECONDS),
                any(Storage.SignUrlOption.class),
                any(Storage.SignUrlOption.class)))
                .thenReturn(expectedUrl);

        // When
        URL actualUrl = gcpBlobStore.doGeneratePresignedUrl(presignedUrlRequest);

        // Then
        assertEquals(expectedUrl, actualUrl);
        verify(mockTransformer).toBlobInfo(presignedUrlRequest);
        verify(mockStorage).signUrl(eq(mockBlobInfo),
                eq(duration.toMillis()),
                eq(TimeUnit.MILLISECONDS),
                any(Storage.SignUrlOption.class),
                any(Storage.SignUrlOption.class));
    }

    @Test
    void testDoGeneratePresignedUrl_Download() throws Exception {
        // Given
        Duration duration = Duration.ofMinutes(30);
        PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.DOWNLOAD)
                .key(TEST_KEY)
                .duration(duration)
                .build();

        URL expectedUrl = new URL("https://signed-url-for-download.example.com");
        
        when(mockTransformer.toBlobInfo(presignedUrlRequest)).thenReturn(mockBlobInfo);
        when(mockStorage.signUrl(eq(mockBlobInfo),
                any(Long.class),
                eq(TimeUnit.MILLISECONDS),
                any(Storage.SignUrlOption.class),
                any(Storage.SignUrlOption.class)))
                .thenReturn(expectedUrl);

        // When
        URL actualUrl = gcpBlobStore.doGeneratePresignedUrl(presignedUrlRequest);

        // Then
        assertEquals(expectedUrl, actualUrl);
        verify(mockTransformer).toBlobInfo(presignedUrlRequest);
        verify(mockStorage).signUrl(eq(mockBlobInfo),
                eq(duration.toMillis()),
                eq(TimeUnit.MILLISECONDS),
                any(Storage.SignUrlOption.class),
                any(Storage.SignUrlOption.class));
    }

    @Test
    void testDoGeneratePresignedUrl_WithLongDuration() throws Exception {
        // Given
        Duration duration = Duration.ofDays(7); // Test with a longer duration
        PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.UPLOAD)
                .key("long-term-key")
                .duration(duration)
                .build();

        URL expectedUrl = new URL("https://long-term-signed-url.example.com");
        
        when(mockTransformer.toBlobInfo(presignedUrlRequest)).thenReturn(mockBlobInfo);
        when(mockStorage.signUrl(eq(mockBlobInfo),
                any(Long.class),
                eq(TimeUnit.MILLISECONDS),
                any(Storage.SignUrlOption.class),
                any(Storage.SignUrlOption.class)))
                .thenReturn(expectedUrl);

        // When
        URL actualUrl = gcpBlobStore.doGeneratePresignedUrl(presignedUrlRequest);

        // Then
        assertEquals(expectedUrl, actualUrl);
        assertEquals(duration.toMillis(), Duration.ofDays(7).toMillis()); // Verify duration calculation
        verify(mockStorage).signUrl(eq(mockBlobInfo),
                eq(duration.toMillis()),
                eq(TimeUnit.MILLISECONDS),
                any(Storage.SignUrlOption.class),
                any(Storage.SignUrlOption.class));
    }
}
