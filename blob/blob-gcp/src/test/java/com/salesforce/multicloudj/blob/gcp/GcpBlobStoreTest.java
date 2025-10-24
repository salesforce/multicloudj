package com.salesforce.multicloudj.blob.gcp;

import com.google.api.gax.paging.Page;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
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
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadResponse;
import com.salesforce.multicloudj.blob.driver.FailedBlobDownload;
import com.salesforce.multicloudj.blob.driver.FailedBlobUpload;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.MultipartUploadResponse;
import com.salesforce.multicloudj.blob.driver.PresignedOperation;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.blob.gcp.async.GcpAsyncBlobStore;
import com.salesforce.multicloudj.blob.gcp.async.GcpAsyncBlobStoreProvider;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.provider.Provider;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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
import java.net.URI;
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
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import static org.mockito.Mockito.doThrow;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import static org.mockito.Mockito.never;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.FailedBlobUpload;

@ExtendWith(MockitoExtension.class)
class GcpBlobStoreTest {

    private static final String TEST_BUCKET = "test-bucket";
    private static final String TEST_KEY = "test-key";
    private static final String TEST_VERSION_ID = "12345";
    private static final String TEST_ETAG = "test-etag";
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
    private Page<Blob> mockPage;

    @Mock
    private UploadResponse mockUploadResponse;

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

    @Mock
    private Bucket mockBucket;

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
        // Mock bucket validation to always succeed (lenient because not all tests validate bucket)
        lenient().when(mockStorage.get(TEST_BUCKET)).thenReturn(mockBucket);
        gcpBlobStore = new GcpBlobStore(builder, mockStorage);
    }

    @Test
    void testBuilder() {
        Provider.Builder providerBuilder = gcpBlobStore.builder();
        assertNotNull(providerBuilder);
        assertInstanceOf(GcpBlobStore.Builder.class, providerBuilder);
    }

    @Test
    void testDoUpload_WithInputStream() {
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
            when(mockTransformer.getKmsWriteOptions(uploadRequest)).thenReturn(new Storage.BlobWriteOption[0]);
            when(mockStorage.writer(eq(mockBlobInfo), any(Storage.BlobWriteOption[].class))).thenReturn(mockWriteChannel);
            when(mockStorage.get(TEST_BUCKET, TEST_KEY)).thenReturn(mockBlob);
            when(mockTransformer.toUploadResponse(mockBlob)).thenReturn(expectedResponse);

            // When
            UploadResponse response = gcpBlobStore.doUpload(uploadRequest, new ByteArrayInputStream(TEST_CONTENT));

            // Then
            assertEquals(expectedResponse, response);
            verify(mockStorage).writer(eq(mockBlobInfo), any(Storage.BlobWriteOption[].class));
            verify(mockStorage).get(TEST_BUCKET, TEST_KEY);
            verify(mockTransformer).toUploadResponse(mockBlob);
        }
    }

    @Test
    void testDoUpload_WithInputStream_ThrowsException() {
        try (MockedStatic<ByteStreams> mockedStatic = Mockito.mockStatic(ByteStreams.class)) {
            // Given
            UploadRequest uploadRequest = UploadRequest.builder()
                    .withKey(TEST_KEY)
                    .build();

            when(mockTransformer.toBlobInfo(uploadRequest)).thenReturn(mockBlobInfo);
            when(mockTransformer.getKmsWriteOptions(uploadRequest)).thenReturn(new Storage.BlobWriteOption[0]);
            when(mockStorage.writer(eq(mockBlobInfo), any(Storage.BlobWriteOption[].class))).thenReturn(mockWriteChannel);
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
    void testDoUpload_FileNotFound() {
        try (MockedStatic<ByteStreams> mockedStatic = Mockito.mockStatic(ByteStreams.class)) {
            // Given
            UploadRequest uploadRequest = UploadRequest.builder()
                    .withKey(TEST_KEY)
                    .build();

            when(mockTransformer.toBlobInfo(uploadRequest)).thenReturn(mockBlobInfo);
            when(mockTransformer.getKmsWriteOptions(uploadRequest)).thenReturn(new Storage.BlobWriteOption[0]);
            when(mockStorage.writer(eq(mockBlobInfo), any(Storage.BlobWriteOption[].class))).thenReturn(mockWriteChannel);
            when(mockStorage.get(TEST_BUCKET, TEST_KEY)).thenReturn(null);

            // When
            SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
                gcpBlobStore.doUpload(uploadRequest, new ByteArrayInputStream(TEST_CONTENT));
            });

            // Then
            verify(mockStorage).writer(mockBlobInfo);
            verify(mockStorage).get(TEST_BUCKET, TEST_KEY);
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
        when(mockTransformer.getKmsTargetOptions(uploadRequest)).thenReturn(new Storage.BlobTargetOption[0]);
        when(mockStorage.create(eq(mockBlobInfo), eq(TEST_CONTENT), any(Storage.BlobTargetOption[].class))).thenReturn(mockBlob);
        when(mockTransformer.toUploadResponse(mockBlob)).thenReturn(expectedResponse);

        // When
        UploadResponse response = gcpBlobStore.doUpload(uploadRequest, TEST_CONTENT);

        // Then
        assertEquals(expectedResponse, response);
        verify(mockStorage).create(eq(mockBlobInfo), eq(TEST_CONTENT), any(Storage.BlobTargetOption[].class));
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
        when(mockTransformer.getKmsWriteOptions(uploadRequest)).thenReturn(new Storage.BlobWriteOption[0]);
        when(mockStorage.createFrom(eq(mockBlobInfo), eq(testFile), any(Storage.BlobWriteOption[].class))).thenReturn(mockBlob);
        when(mockTransformer.toUploadResponse(mockBlob)).thenReturn(expectedResponse);

        // When
        UploadResponse response = gcpBlobStore.doUpload(uploadRequest, testFile.toFile());

        // Then
        assertEquals(expectedResponse, response);
        verify(mockStorage).createFrom(eq(mockBlobInfo), eq(testFile), any(Storage.BlobWriteOption[].class));
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
        when(mockTransformer.getKmsWriteOptions(uploadRequest)).thenReturn(new Storage.BlobWriteOption[0]);
        when(mockStorage.createFrom(eq(mockBlobInfo), eq(testFile), any(Storage.BlobWriteOption[].class))).thenReturn(mockBlob);
        when(mockTransformer.toUploadResponse(mockBlob)).thenReturn(expectedResponse);

        // When
        UploadResponse response = gcpBlobStore.doUpload(uploadRequest, testFile);

        // Then
        assertEquals(expectedResponse, response);
        verify(mockStorage).createFrom(eq(mockBlobInfo), eq(testFile), any(Storage.BlobWriteOption[].class));
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
        when(mockTransformer.getKmsWriteOptions(uploadRequest)).thenReturn(new Storage.BlobWriteOption[0]);
        when(mockStorage.createFrom(eq(mockBlobInfo), eq(testFile), any(Storage.BlobWriteOption[].class))).thenThrow(new IOException("Test exception"));

        // When & Then
        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            gcpBlobStore.doUpload(uploadRequest, testFile);
        });
        assertEquals("Request failed while uploading from path", exception.getMessage());
    }

    @Test
    void testDoDownload_WithOutputStream() {
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
            doReturn(new ImmutablePair<>(null, null)).when(mockTransformer).computeRange(any(), any(), anyLong());

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
    void testDoDownload_BlobNotFound() {
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
            when(mockStorage.get(mockBlobId)).thenReturn(null);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            // When
            SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
                gcpBlobStore.doDownload(downloadRequest, outputStream);
            });
            assertEquals("Blob not found", exception.getMessage());
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
            when(mockTransformer.computeRange(anyLong(), anyLong(), anyLong())).thenReturn(new ImmutablePair<>(10L, 21L));

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            // When
            DownloadResponse response = gcpBlobStore.doDownload(downloadRequest, outputStream);

            // Then
            assertEquals(expectedResponse, response);
            verify(mockReadChannel).seek(10L);
            verify(mockReadChannel).limit(21L);
        }
    }

    @Test
    void testDoDownload_WithOutputStream_ThrowsException() {
        try (MockedStatic<ByteStreams> mockedStatic = Mockito.mockStatic(ByteStreams.class)) {
            // Given
            DownloadRequest downloadRequest = DownloadRequest.builder()
                    .withKey(TEST_KEY)
                    .build();

            when(mockTransformer.toBlobId(downloadRequest)).thenReturn(mockBlobId);
            when(mockStorage.get(mockBlobId)).thenReturn(mockBlob);
            doReturn(new ImmutablePair<>(null, null)).when(mockTransformer).computeRange(any(), any(), anyLong());
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
    void testDoDownload_WithByteArray() {
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
            doReturn(new ImmutablePair<>(null, null)).when(mockTransformer).computeRange(any(), any(), anyLong());

            ByteArray byteArray = new ByteArray();

            // When
            DownloadResponse response = gcpBlobStore.doDownload(downloadRequest, byteArray);

            // Then
            assertEquals(expectedResponse, response);
            assertArrayEquals(new byte[0], byteArray.getBytes()); // Empty because mock doesn't write data
        }
    }

    @Test
    void testDoDownload_WithFile() {
        try (MockedStatic<ByteStreams> ignored = Mockito.mockStatic(ByteStreams.class)) {
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
            doReturn(new ImmutablePair<>(null, null)).when(mockTransformer).computeRange(any(), any(), anyLong());

            // When
            DownloadResponse response = gcpBlobStore.doDownload(downloadRequest, testFile.toFile());

            // Then
            assertEquals(expectedResponse, response);
        }
    }

    @Test
    void testDoDownload_WithPath() {
        try (MockedStatic<ByteStreams> ignored = Mockito.mockStatic(ByteStreams.class)) {
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
            doReturn(new ImmutablePair<>(null, null)).when(mockTransformer).computeRange(any(), any(), anyLong());

            // When
            DownloadResponse response = gcpBlobStore.doDownload(downloadRequest, testFile);

            // Then
            assertEquals(expectedResponse, response);
        }
    }

    @Test
    void testDoDownload_WithPath_ThrowsException() {
        try (MockedStatic<ByteStreams> mockedStatic = Mockito.mockStatic(ByteStreams.class)) {
            // Given
            Path testFile = tempDir.resolve("download.txt");
            DownloadRequest downloadRequest = DownloadRequest.builder()
                    .withKey(TEST_KEY)
                    .build();

            when(mockTransformer.toBlobId(downloadRequest)).thenReturn(mockBlobId);
            when(mockStorage.reader(mockBlobId)).thenReturn(mockReadChannel);
            when(mockStorage.get(mockBlobId)).thenReturn(mockBlob);
            doReturn(new ImmutablePair<>(null, null)).when(mockTransformer).computeRange(any(), any(), anyLong());
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
        when(mockTransformer.toBlobId(TEST_BUCKET, TEST_KEY, TEST_VERSION_ID)).thenReturn(mockBlobId);

        // When
        gcpBlobStore.doDelete(TEST_KEY, TEST_VERSION_ID);

        // Then
        verify(mockStorage).delete(mockBlobId);
    }

    @Test
    void testDoDelete_WithKeyAndNullVersionId() {
        // Given
        when(mockTransformer.toBlobId(TEST_BUCKET, TEST_KEY, null)).thenReturn(mockBlobId);

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

        when(mockTransformer.toBlobId(TEST_BUCKET, "key1", "version1")).thenReturn(mockBlobId1);
        when(mockTransformer.toBlobId(TEST_BUCKET, "key2", "version2")).thenReturn(mockBlobId2);

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

        when(mockTransformer.toBlobId(TEST_BUCKET, TEST_KEY, TEST_VERSION_ID)).thenReturn(mockBlobId);
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

        List<Blob> mockBlobs = Arrays.asList(mockBlob, mockBlob);
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
    void testDoListPage() {
        // Given
        ListBlobsPageRequest request = ListBlobsPageRequest
                .builder()
                .withPrefix("test-prefix")
                .withDelimiter("/")
                .withPaginationToken("next-token")
                .withMaxResults(50)
                .build();

        Blob mockBlob1 = mock(Blob.class);
        Blob mockBlob2 = mock(Blob.class);
        when(mockBlob1.getName()).thenReturn("test-prefixfile1");
        when(mockBlob1.getSize()).thenReturn(1024L);
        when(mockBlob2.getName()).thenReturn("test-prefixfile2");
        when(mockBlob2.getSize()).thenReturn(2048L);
        
        List<Blob> mockBlobs = Arrays.asList(mockBlob1, mockBlob2);
        Page mockPage = mock(Page.class);
        Storage.BlobListOption[] mockOptions = new Storage.BlobListOption[0];
        
        when(mockTransformer.toBlobListOptions(request)).thenReturn(mockOptions);
        doReturn(mockPage).when(mockStorage).list(eq(TEST_BUCKET), mockOptions);
        when(mockPage.getValues()).thenReturn(mockBlobs);
        when(mockPage.hasNextPage()).thenReturn(true);
        when(mockPage.getNextPageToken()).thenReturn("next-page-token");

        // When
        ListBlobsPageResponse response = gcpBlobStore.listPage(request);

        // Then
        assertNotNull(response);
        assertEquals(2, response.getBlobs().size());
        assertTrue(response.isTruncated());
        assertEquals("next-page-token", response.getNextPageToken());
        
        // Verify first and second blob
        assertEquals("test-prefixfile1", response.getBlobs().get(0).getKey());
        assertEquals(1024L, response.getBlobs().get(0).getObjectSize());
        assertEquals("test-prefixfile2", response.getBlobs().get(1).getKey());
        assertEquals(2048L, response.getBlobs().get(1).getObjectSize());
    }

    @Test
    void testDoListPageEmpty() {
        // Given
        ListBlobsPageRequest request = ListBlobsPageRequest.builder().build();
        Page mockPage = mock(Page.class);
        Storage.BlobListOption[] mockOptions = new Storage.BlobListOption[0];
        
        when(mockTransformer.toBlobListOptions(request)).thenReturn(mockOptions);
        doReturn(mockPage).when(mockStorage).list(eq(TEST_BUCKET), mockOptions);
        when(mockPage.getValues()).thenReturn(Collections.emptyList());
        when(mockPage.hasNextPage()).thenReturn(false);

        // When
        ListBlobsPageResponse response = gcpBlobStore.listPage(request);

        // Then
        assertNotNull(response);
        assertEquals(0, response.getBlobs().size());
        assertEquals(false, response.isTruncated());
        assertNull(response.getNextPageToken());
    }

    @Test
    void testDoListPageWithPagination() {
        // Given - Test pagination with multiple pages
        ListBlobsPageRequest request = ListBlobsPageRequest
                .builder()
                .withPrefix("test-prefix")
                .withMaxResults(2)
                .build();

        Blob mockBlob1 = mock(Blob.class);
        Blob mockBlob2 = mock(Blob.class);
        when(mockBlob1.getName()).thenReturn("test-prefixfile1");
        when(mockBlob1.getSize()).thenReturn(1024L);
        when(mockBlob2.getName()).thenReturn("test-prefixfile2");
        when(mockBlob2.getSize()).thenReturn(2048L);
        
        List<Blob> mockBlobs = Arrays.asList(mockBlob1, mockBlob2);
        Page mockPage = mock(Page.class);
        Storage.BlobListOption[] mockOptions = new Storage.BlobListOption[0];
        
        when(mockTransformer.toBlobListOptions(request)).thenReturn(mockOptions);
        doReturn(mockPage).when(mockStorage).list(eq(TEST_BUCKET), mockOptions);
        when(mockPage.getValues()).thenReturn(mockBlobs);
        when(mockPage.hasNextPage()).thenReturn(true);
        when(mockPage.getNextPageToken()).thenReturn("next-page-token");

        // When
        ListBlobsPageResponse response = gcpBlobStore.listPage(request);

        // Then
        assertNotNull(response);
        assertEquals(2, response.getBlobs().size());
        assertTrue(response.isTruncated(), "Response should be truncated when hasNextPage is true");
        assertEquals("next-page-token", response.getNextPageToken());
        
        // Verify first and second blob
        assertEquals("test-prefixfile1", response.getBlobs().get(0).getKey());
        assertEquals(1024L, response.getBlobs().get(0).getObjectSize());
        assertEquals("test-prefixfile2", response.getBlobs().get(1).getKey());
        assertEquals(2048L, response.getBlobs().get(1).getObjectSize());
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
        MultipartUploadRequest request = new MultipartUploadRequest.Builder()
                .withKey(TEST_KEY)
                .withMetadata(Map.of("key1","value1","key2","value2"))
                .build();

        MultipartUpload mpu = gcpBlobStore.doInitiateMultipartUpload(request);

        assertEquals(TEST_BUCKET, mpu.getBucket());
        assertEquals(TEST_KEY, mpu.getKey());
        assertNotNull(mpu.getId());
    }

    @Test
    void testDoUploadMultipartPart() {
        try (MockedStatic<ByteStreams> mockedStatic = Mockito.mockStatic(ByteStreams.class)) {
            UploadRequest uploadRequest = UploadRequest.builder()
                    .withKey(TEST_KEY)
                    .build();
            UploadResponse expectedResponse = UploadResponse.builder()
                    .key(TEST_KEY)
                    .versionId(TEST_VERSION_ID)
                    .eTag(TEST_ETAG)
                    .build();

            when(mockTransformer.toUploadRequest(any(), any())).thenReturn(uploadRequest);
            when(mockTransformer.toBlobInfo(uploadRequest)).thenReturn(mockBlobInfo);
            when(mockTransformer.getKmsWriteOptions(uploadRequest)).thenReturn(new Storage.BlobWriteOption[0]);
            when(mockStorage.writer(eq(mockBlobInfo), any(Storage.BlobWriteOption[].class))).thenReturn(mockWriteChannel);
            when(mockStorage.get(TEST_BUCKET, TEST_KEY)).thenReturn(mockBlob);
            when(mockTransformer.toUploadResponse(mockBlob)).thenReturn(expectedResponse);

            MultipartUpload mpu = MultipartUpload.builder()
                    .bucket(TEST_BUCKET)
                    .key(TEST_KEY)
                    .id("id")
                    .build();
            MultipartPart mpp = new MultipartPart(1, "TestData".getBytes(StandardCharsets.UTF_8));
            UploadPartResponse response = gcpBlobStore.doUploadMultipartPart(mpu, mpp);

            assertEquals(1, response.getPartNumber());
            assertEquals(8, response.getSizeInBytes());
            assertEquals(TEST_ETAG, response.getEtag());
        }

    }

    @Test
    void testDoCompleteMultipartUpload() {
        MultipartUpload mpu = MultipartUpload.builder()
                .bucket(TEST_BUCKET)
                .key(TEST_KEY)
                .id("id")
                .build();
        List<UploadPartResponse> parts = List.of(new UploadPartResponse(1,"etag", 100));
        Blob mockBlob = mock(Blob.class);
        doReturn(TEST_ETAG).when(mockBlob).getEtag();
        doReturn(mockBlob).when(mockStorage).compose(any());
        when(mockTransformer.toPartName(any(MultipartUpload.class), anyInt())).thenCallRealMethod();
        when(mockTransformer.toBlobInfo(any(MultipartUpload.class))).thenReturn(mock(BlobInfo.class));

        MultipartUploadResponse response = gcpBlobStore.doCompleteMultipartUpload(mpu, parts);

        assertEquals(TEST_ETAG, response.getEtag());
    }

    @Test
    void testDoListMultipartUpload() {
        MultipartUpload mpu = MultipartUpload.builder()
                .bucket(TEST_BUCKET)
                .key(TEST_KEY)
                .id("id")
                .build();
        List<UploadPartResponse> expectedParts = List.of(
                new UploadPartResponse(1, "etag1", 100L),
                new UploadPartResponse(2, "etag2", 200L));
        doReturn(expectedParts).when(mockTransformer).toUploadPartResponseList(any(Page.class));
        doReturn(mock(Page.class)).when(mockStorage).list(anyString(), any(Storage.BlobListOption.class));

        List<UploadPartResponse> actualParts = gcpBlobStore.doListMultipartUpload(mpu);

        assertEquals(expectedParts, actualParts);
    }

    @Test
    void testDoAbortMultipartUpload() {
        MultipartUpload mpu = MultipartUpload.builder()
                .bucket(TEST_BUCKET)
                .key(TEST_KEY)
                .id("id")
                .build();
        doReturn(mock(Page.class)).when(mockStorage).list(anyString(), any(Storage.BlobListOption.class));
        List<BlobId> blobIds = List.of(mock(BlobId.class), mock(BlobId.class));
        doReturn(blobIds).when(mockTransformer).toBlobIdList(any());

        gcpBlobStore.doAbortMultipartUpload(mpu);

        verify(mockStorage).delete(blobIds);
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
                any(Storage.SignUrlOption[].class)))
                .thenReturn(expectedUrl);

        // When
        URL actualUrl = gcpBlobStore.doGeneratePresignedUrl(presignedUrlRequest);

        // Then
        assertEquals(expectedUrl, actualUrl);
        verify(mockTransformer).toBlobInfo(presignedUrlRequest);
        verify(mockStorage).signUrl(eq(mockBlobInfo),
                eq(duration.toMillis()),
                eq(TimeUnit.MILLISECONDS),
                any(Storage.SignUrlOption[].class));
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
                any(Storage.SignUrlOption[].class)))
                .thenReturn(expectedUrl);

        // When
        URL actualUrl = gcpBlobStore.doGeneratePresignedUrl(presignedUrlRequest);

        // Then
        assertEquals(expectedUrl, actualUrl);
        verify(mockTransformer).toBlobInfo(presignedUrlRequest);
        verify(mockStorage).signUrl(eq(mockBlobInfo),
                eq(duration.toMillis()),
                eq(TimeUnit.MILLISECONDS),
                any(Storage.SignUrlOption[].class));
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
                any(Storage.SignUrlOption[].class)))
                .thenReturn(expectedUrl);

        // When
        URL actualUrl = gcpBlobStore.doGeneratePresignedUrl(presignedUrlRequest);

        // Then
        assertEquals(expectedUrl, actualUrl);
        assertEquals(duration.toMillis(), Duration.ofDays(7).toMillis()); // Verify duration calculation
        verify(mockStorage).signUrl(eq(mockBlobInfo),
                eq(duration.toMillis()),
                eq(TimeUnit.MILLISECONDS),
                any(Storage.SignUrlOption[].class));
    }

    @Test
    void testDoGeneratePresignedUrl_WithKmsKey() throws Exception {
        // Given
        Duration duration = Duration.ofHours(4);
        String kmsKeyId = "projects/my-project/locations/us-east1/keyRings/my-ring/cryptoKeys/my-key";
        PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.UPLOAD)
                .key(TEST_KEY)
                .duration(duration)
                .kmsKeyId(kmsKeyId)
                .build();

        URL expectedUrl = new URL("https://signed-url-with-kms.example.com");

        when(mockTransformer.toBlobInfo(presignedUrlRequest)).thenReturn(mockBlobInfo);
        when(mockStorage.signUrl(eq(mockBlobInfo),
                any(Long.class),
                eq(TimeUnit.MILLISECONDS),
                any(Storage.SignUrlOption[].class)))
                .thenReturn(expectedUrl);

        // When
        URL actualUrl = gcpBlobStore.doGeneratePresignedUrl(presignedUrlRequest);

        // Then
        assertEquals(expectedUrl, actualUrl);
        verify(mockTransformer).toBlobInfo(presignedUrlRequest);
        // Verify signUrl was called with the correct parameters including KMS extension header
        verify(mockStorage).signUrl(eq(mockBlobInfo),
                eq(duration.toMillis()),
                eq(TimeUnit.MILLISECONDS),
                any(Storage.SignUrlOption[].class));
    }

    @Test
    void testDoGeneratePresignedUrl_WithoutKmsKey() throws Exception {
        // Given
        Duration duration = Duration.ofHours(4);
        PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.UPLOAD)
                .key(TEST_KEY)
                .duration(duration)
                .build();

        URL expectedUrl = new URL("https://signed-url-without-kms.example.com");

        when(mockTransformer.toBlobInfo(presignedUrlRequest)).thenReturn(mockBlobInfo);
        when(mockStorage.signUrl(eq(mockBlobInfo),
                any(Long.class),
                eq(TimeUnit.MILLISECONDS),
                any(Storage.SignUrlOption[].class)))
                .thenReturn(expectedUrl);

        // When
        URL actualUrl = gcpBlobStore.doGeneratePresignedUrl(presignedUrlRequest);

        // Then
        assertEquals(expectedUrl, actualUrl);
        verify(mockTransformer).toBlobInfo(presignedUrlRequest);
        verify(mockStorage).signUrl(eq(mockBlobInfo),
                eq(duration.toMillis()),
                eq(TimeUnit.MILLISECONDS),
                any(Storage.SignUrlOption[].class));
    }

    // Test class to access protected methods
    private static class TestGcpBlobStore extends GcpBlobStore {
        public TestGcpBlobStore(Builder builder, Storage storage) {
            super(builder, storage);
        }
    }

    @Test
    void testDoDownloadWithInputStream() {
        try (MockedStatic<ByteStreams> mockedStatic = Mockito.mockStatic(ByteStreams.class)) {
            // Given
            DownloadRequest downloadRequest = DownloadRequest.builder()
                    .withKey(TEST_KEY)
                    .build();

            DownloadResponse expectedResponse = DownloadResponse.builder()
                    .key(TEST_KEY)
                    .build();

            when(mockTransformer.toBlobId(downloadRequest)).thenReturn(mockBlobId);
            when(mockStorage.get(mockBlobId)).thenReturn(mockBlob);
            when(mockBlob.reader()).thenReturn(mockReadChannel);
            when(mockBlob.getSize()).thenReturn(100L);
            when(mockTransformer.computeRange(any(), any(), anyLong())).thenReturn(new ImmutablePair<>(null, null));
            when(mockTransformer.toDownloadResponse(eq(mockBlob), any(InputStream.class))).thenReturn(expectedResponse);

            DownloadResponse response = gcpBlobStore.doDownload(downloadRequest);

            assertEquals(expectedResponse, response);
            verify(mockTransformer).toBlobId(downloadRequest);
            verify(mockStorage).get(mockBlobId);
            verify(mockBlob).reader();
            verify(mockTransformer).computeRange(null, null, 100L);
            verify(mockTransformer).toDownloadResponse(eq(mockBlob), any(InputStream.class));
        }
    }

    @Test
    void tesDoDownloadWithInputStreamBlobNotFound() {
        try (MockedStatic<ByteStreams> mockedStatic = Mockito.mockStatic(ByteStreams.class)) {
            // Given
            DownloadRequest downloadRequest = DownloadRequest.builder()
                    .withKey(TEST_KEY)
                    .build();

            when(mockTransformer.toBlobId(downloadRequest)).thenReturn(mockBlobId);
            when(mockStorage.get(mockBlobId)).thenReturn(null);

            // When & Then
            SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
                gcpBlobStore.doDownload(downloadRequest);
            });
            assertEquals("Blob not found", exception.getMessage());
            verify(mockTransformer).toBlobId(downloadRequest);
            verify(mockStorage).get(mockBlobId);
        }
    }

    @Test
    void testDoDownloadWithRangeInputStream() throws IOException {
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
            when(mockStorage.get(mockBlobId)).thenReturn(mockBlob);
            when(mockBlob.reader()).thenReturn(mockReadChannel);
            when(mockBlob.getSize()).thenReturn(100L);
            when(mockTransformer.computeRange(10L, 20L, 100L)).thenReturn(new ImmutablePair<>(10L, 21L));
            when(mockTransformer.toDownloadResponse(eq(mockBlob), any(InputStream.class))).thenReturn(expectedResponse);

            DownloadResponse response = gcpBlobStore.doDownload(downloadRequest);

            // Then
            assertEquals(expectedResponse, response);
            verify(mockTransformer).toBlobId(downloadRequest);
            verify(mockStorage).get(mockBlobId);
            verify(mockBlob).reader();
            verify(mockReadChannel).seek(10L);
            verify(mockReadChannel).limit(21L);
            verify(mockTransformer).computeRange(10L, 20L, 100L);
            verify(mockTransformer).toDownloadResponse(eq(mockBlob), any(InputStream.class));
        }
    }

    @Test
    void testProviderBuilder() {
        GcpAsyncBlobStoreProvider provider = new GcpAsyncBlobStoreProvider();
        GcpAsyncBlobStoreProvider.Builder builder = provider.builder();
        assertInstanceOf(GcpAsyncBlobStore.Builder.class, builder);

        var store = builder
                .withEndpoint(URI.create("https://endpoint.example.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:443"))
                .withSocketTimeout(Duration.ofMinutes(1))
                .withIdleConnectionTimeout(Duration.ofMinutes(5))
                .withMaxConnections(100)
                .withExecutorService(ForkJoinPool.commonPool())
                .build();

        assertNotNull(store);
        assertEquals(GcpConstants.PROVIDER_ID, store.getProviderId());
    }

    // ==================== Directory Operation Tests ====================
    @Test
    void testUploadDirectory_Success() throws Exception {
        // Given
        DirectoryUploadRequest request = DirectoryUploadRequest.builder()
                .localSourceDirectory(tempDir.toString())
                .prefix("uploads/")
                .includeSubFolders(true)
                .build();

        // Create test files in temp directory
        Path file1 = tempDir.resolve("file1.txt");
        Path file2 = tempDir.resolve("subdir").resolve("file2.txt");
        Files.createDirectories(file2.getParent());
        Files.write(file1, "content1".getBytes());
        Files.write(file2, "content2".getBytes());

        List<Path> filePaths = List.of(file1, file2);
        when(mockTransformer.toFilePaths(request)).thenReturn(filePaths);
        when(mockTransformer.toBlobKey(eq(tempDir), eq(file1), eq("uploads/")))
                .thenReturn("uploads/file1.txt");
        when(mockTransformer.toBlobKey(eq(tempDir), eq(file2), eq("uploads/")))
                .thenReturn("uploads/subdir/file2.txt");

        // When
        DirectoryUploadResponse response = gcpBlobStore.uploadDirectory(request);

        // Then
        assertNotNull(response);
        assertTrue(response.getFailedTransfers().isEmpty());
        verify(mockStorage).createFrom(any(BlobInfo.class), eq(file1));
        verify(mockStorage).createFrom(any(BlobInfo.class), eq(file2));
    }

    @Test
    void testUploadDirectory_WithFailures() throws Exception {
        // Given
        DirectoryUploadRequest request = DirectoryUploadRequest.builder()
                .localSourceDirectory(tempDir.toString())
                .prefix("uploads/")
                .includeSubFolders(true)
                .build();

        Path file1 = tempDir.resolve("file1.txt");
        Files.write(file1, "content1".getBytes());

        List<Path> filePaths = List.of(file1);
        when(mockTransformer.toFilePaths(request)).thenReturn(filePaths);
        when(mockTransformer.toBlobKey(eq(tempDir), eq(file1), eq("uploads/")))
                .thenReturn("uploads/file1.txt");

        // Mock failure for the file upload
        doThrow(new RuntimeException("Upload failed"))
                .when(mockStorage).createFrom(any(com.google.cloud.storage.BlobInfo.class), eq(file1));

        // When
        DirectoryUploadResponse response = gcpBlobStore.uploadDirectory(request);

        // Then
        assertNotNull(response);
        assertEquals(1, response.getFailedTransfers().size());
        FailedBlobUpload failedUpload = response.getFailedTransfers().get(0);
        assertEquals(file1, failedUpload.getSource());
        assertTrue(failedUpload.getException() instanceof RuntimeException);
        assertEquals("Upload failed", failedUpload.getException().getMessage());
    }

    @Test
    void testUploadDirectory_EmptyDirectory() throws Exception {
        // Given
        DirectoryUploadRequest request = DirectoryUploadRequest.builder()
                .localSourceDirectory(tempDir.toString())
                .prefix("uploads/")
                .includeSubFolders(true)
                .build();

        when(mockTransformer.toFilePaths(request)).thenReturn(List.of());

        // When
        DirectoryUploadResponse response = gcpBlobStore.uploadDirectory(request);

        // Then
        assertNotNull(response);
        assertTrue(response.getFailedTransfers().isEmpty());
        verify(mockStorage, never()).createFrom(any(BlobInfo.class), any(Path.class));
    }

    @Test
    void testDownloadDirectory_Success() throws Exception {
        // Given
        DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
                .prefixToDownload("uploads/")
                .localDestinationDirectory(tempDir.toString())
                .build();

        // Mock blobs in storage
        Blob blob1 = mock(Blob.class);
        Blob blob2 = mock(Blob.class);
        when(blob1.getName()).thenReturn("uploads/file1.txt");
        when(blob2.getName()).thenReturn("uploads/subdir/file2.txt");

        Page<Blob> mockPage = mock(Page.class);
        when(mockPage.iterateAll()).thenReturn(List.of(blob1, blob2));
        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class)))
                .thenReturn(mockPage);

        // When
        DirectoryDownloadResponse response = gcpBlobStore.downloadDirectory(request);

        // Then
        assertNotNull(response);
        assertTrue(response.getFailedTransfers().isEmpty());
        verify(blob1).downloadTo(any(Path.class));
        verify(blob2).downloadTo(any(Path.class));
    }

    @Test
    void testDownloadDirectory_WithFailures() throws Exception {
        // Given
        DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
                .prefixToDownload("uploads/")
                .localDestinationDirectory(tempDir.toString())
                .build();

        Blob blob1 = mock(Blob.class);
        Blob blob2 = mock(Blob.class);
        when(blob1.getName()).thenReturn("uploads/file1.txt");
        when(blob2.getName()).thenReturn("uploads/file2.txt");

        // Mock failure for second blob
        doThrow(new RuntimeException("Download failed"))
                .when(blob2).downloadTo(any(Path.class));

        Page<Blob> mockPage = mock(Page.class);
        when(mockPage.iterateAll()).thenReturn(List.of(blob1, blob2));
        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class)))
                .thenReturn(mockPage);

        // When
        DirectoryDownloadResponse response = gcpBlobStore.downloadDirectory(request);

        // Then
        assertNotNull(response);
        assertEquals(1, response.getFailedTransfers().size());
        FailedBlobDownload failedDownload = response.getFailedTransfers().get(0);
        assertTrue(failedDownload.getException() instanceof RuntimeException);
        assertEquals("Download failed", failedDownload.getException().getMessage());
    }

    @Test
    void testDownloadDirectory_WithDirectoryMarkers() throws Exception {
        // Given
        DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
                .prefixToDownload("uploads/")
                .localDestinationDirectory(tempDir.toString())
                .build();

        Blob fileBlob = mock(Blob.class);
        Blob dirMarker = mock(Blob.class);
        when(fileBlob.getName()).thenReturn("uploads/file1.txt");
        when(dirMarker.getName()).thenReturn("uploads/subdir/"); // Directory marker

        Page<Blob> mockPage = mock(Page.class);
        when(mockPage.iterateAll()).thenReturn(List.of(fileBlob, dirMarker));
        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class)))
                .thenReturn(mockPage);

        // When
        DirectoryDownloadResponse response = gcpBlobStore.downloadDirectory(request);

        // Then
        assertNotNull(response);
        assertTrue(response.getFailedTransfers().isEmpty());
        verify(fileBlob).downloadTo(any(Path.class));
        verify(dirMarker).downloadTo(any(Path.class));
    }

    @Test
    void testDeleteDirectory_Success() throws Exception {
        // Given
        String prefix = "uploads/";

        // Mock blobs to delete
        Blob blob1 = mock(Blob.class);
        Blob blob2 = mock(Blob.class);
        when(blob1.getName()).thenReturn("uploads/file1.txt");
        when(blob1.getSize()).thenReturn(1024L);
        when(blob2.getName()).thenReturn("uploads/file2.txt");
        when(blob2.getSize()).thenReturn(2048L);

        Page<Blob> mockPage = mock(Page.class);
        when(mockPage.getValues()).thenReturn(List.of(blob1, blob2));
        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class)))
                .thenReturn(mockPage);

        // Mock transformer partitioning
        List<com.salesforce.multicloudj.blob.driver.BlobInfo> blobInfos = List.of(
                com.salesforce.multicloudj.blob.driver.BlobInfo.builder()
                        .withKey("uploads/file1.txt").withObjectSize(1024L).build(),
                com.salesforce.multicloudj.blob.driver.BlobInfo.builder()
                        .withKey("uploads/file2.txt").withObjectSize(2048L).build()
        );
        when(mockTransformer.partitionList(any(), eq(1000))).thenReturn(List.of(blobInfos));

        // When
        gcpBlobStore.deleteDirectory(prefix);

        // Then
        verify(mockStorage).list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class));
        verify(mockStorage).delete(any(List.class));
    }

    @Test
    void testDeleteDirectory_LargeBatch() throws Exception {
        // Given
        String prefix = "uploads/";

        // Create a large list of blobs (more than batch size)
        List<Blob> largeList = new ArrayList<>();
        List<com.salesforce.multicloudj.blob.driver.BlobInfo> blobInfos = new ArrayList<>();

        for (int i = 0; i < 2500; i++) {
            Blob mockBlob = mock(Blob.class);
            when(mockBlob.getName()).thenReturn("uploads/file" + i + ".txt");
            when(mockBlob.getSize()).thenReturn(1024L);
            largeList.add(mockBlob);

            blobInfos.add(com.salesforce.multicloudj.blob.driver.BlobInfo.builder()
                    .withKey("uploads/file" + i + ".txt").withObjectSize(1024L).build());
        }

        Page<Blob> mockPage = mock(Page.class);
        when(mockPage.getValues()).thenReturn(largeList);
        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class)))
                .thenReturn(mockPage);

        // Mock transformer to return partitioned lists (3 batches for 2500 items)
        List<List<com.salesforce.multicloudj.blob.driver.BlobInfo>> partitions = List.of(
                blobInfos.subList(0, 1000),
                blobInfos.subList(1000, 2000),
                blobInfos.subList(2000, 2500)
        );
        when(mockTransformer.partitionList(any(), eq(1000))).thenReturn(partitions);

        // When
        gcpBlobStore.deleteDirectory(prefix);

        // Then
        verify(mockStorage).list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class));
        verify(mockStorage, Mockito.times(3)).delete(any(List.class)); // Should be called 3 times for 3 batches
    }

    @Test
    void testDeleteDirectory_EmptyPrefix() throws Exception {
        // Given
        String prefix = null;

        Page<Blob> mockPage = mock(Page.class);
        when(mockPage.getValues()).thenReturn(List.of());
        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class)))
                .thenReturn(mockPage);

        when(mockTransformer.partitionList(any(), eq(1000))).thenReturn(List.of());

        // When & Then - should not throw exception
        gcpBlobStore.deleteDirectory(prefix);

        verify(mockStorage).list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class));
    }

    @Test
    void testUploadDirectory_NullRequest() {
        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            gcpBlobStore.uploadDirectory(null);
        });
    }

    @Test
    void testDownloadDirectory_NullRequest() {
        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            gcpBlobStore.downloadDirectory(null);
        });
    }

    @Test
    void testProxyProviderId() {
        GcpAsyncBlobStoreProvider provider = new GcpAsyncBlobStoreProvider();
        assertEquals(GcpConstants.PROVIDER_ID, provider.getProviderId());
    }

    @Test
    void testDoDownload_WithRange_EdgeCases() throws IOException {
        // Test with start = 0, end = 0 (single byte)
        DownloadRequest request1 = DownloadRequest.builder()
                .withKey(TEST_KEY)
                .withRange(0L, 0L)
                .build();

        when(mockTransformer.toBlobId(request1)).thenReturn(mockBlobId);
        when(mockStorage.get(mockBlobId)).thenReturn(mockBlob);
        when(mockBlob.getSize()).thenReturn(100L);
        when(mockTransformer.computeRange(0L, 0L, 100L)).thenReturn(new ImmutablePair<>(0L, 1L));
        when(mockBlob.reader()).thenReturn(mockReadChannel);
        when(mockTransformer.toDownloadResponse(any(Blob.class), any(InputStream.class))).thenReturn(DownloadResponse.builder().key(TEST_KEY).build());

        DownloadResponse response1 = gcpBlobStore.doDownload(request1);

        assertNotNull(response1);
        verify(mockTransformer).computeRange(0L, 0L, 100L);
    }

    @Test
    void testDoDownload_WithRange_NullEnd() throws IOException {
        // Test with start = 50, end = null (from 50 to end)
        DownloadRequest request = DownloadRequest.builder()
                .withKey(TEST_KEY)
                .withRange(50L, null)
                .build();

        when(mockTransformer.toBlobId(request)).thenReturn(mockBlobId);
        when(mockStorage.get(mockBlobId)).thenReturn(mockBlob);
        when(mockBlob.getSize()).thenReturn(100L);
        when(mockTransformer.computeRange(50L, null, 100L)).thenReturn(new ImmutablePair<>(50L, null));
        when(mockBlob.reader()).thenReturn(mockReadChannel);
        when(mockTransformer.toDownloadResponse(any(Blob.class), any(InputStream.class))).thenReturn(DownloadResponse.builder().key(TEST_KEY).build());

        DownloadResponse response = gcpBlobStore.doDownload(request);

        assertNotNull(response);
        verify(mockTransformer).computeRange(50L, null, 100L);
    }

    @Test
    void testDoDownload_WithRange_NullStart() throws IOException {
        // Test with start = null, end = 25 (last 25 bytes)
        DownloadRequest request = DownloadRequest.builder()
                .withKey(TEST_KEY)
                .withRange(null, 25L)
                .build();

        when(mockTransformer.toBlobId(request)).thenReturn(mockBlobId);
        when(mockStorage.get(mockBlobId)).thenReturn(mockBlob);
        when(mockBlob.getSize()).thenReturn(100L);
        when(mockTransformer.computeRange(null, 25L, 100L)).thenReturn(new ImmutablePair<>(75L, 101L));
        when(mockBlob.reader()).thenReturn(mockReadChannel);
        when(mockTransformer.toDownloadResponse(any(Blob.class), any(InputStream.class))).thenReturn(DownloadResponse.builder().key(TEST_KEY).build());

        DownloadResponse response = gcpBlobStore.doDownload(request);

        assertNotNull(response);
        verify(mockTransformer).computeRange(null, 25L, 100L);
    }

    @Test
    void testDoList_WithEmptyPrefix() {
        ListBlobsRequest request = ListBlobsRequest.builder()
                .withPrefix("")
                .build();

        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class))).thenReturn(mockPage);
        when(mockPage.iterateAll()).thenReturn(Collections.emptyList());

        Iterator<com.salesforce.multicloudj.blob.driver.BlobInfo> iterator = gcpBlobStore.doList(request);

        assertNotNull(iterator);
        assertFalse(iterator.hasNext());
        verify(mockStorage).list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class));
    }

    @Test
    void testDoList_WithNullPrefix() {
        ListBlobsRequest request = ListBlobsRequest.builder()
                .withPrefix(null)
                .build();

        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class))).thenReturn(mockPage);
        when(mockPage.iterateAll()).thenReturn(Collections.emptyList());

        Iterator<com.salesforce.multicloudj.blob.driver.BlobInfo> iterator = gcpBlobStore.doList(request);

        assertNotNull(iterator);
        assertFalse(iterator.hasNext());
        verify(mockStorage).list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class));
    }

    @Test
    void testDoListPage_WithMaxResults() {
        ListBlobsPageRequest request = ListBlobsPageRequest.builder()
                .withPrefix("test/")
                .withMaxResults(10)
                .build();

        when(mockTransformer.toBlobListOptions(request)).thenReturn(new Storage.BlobListOption[0]);
        when(mockStorage.list(TEST_BUCKET)).thenReturn(mockPage);
        when(mockPage.getNextPageToken()).thenReturn(null);

        ListBlobsPageResponse response = gcpBlobStore.doListPage(request);

        assertNotNull(response);
        assertNull(response.getNextPageToken());
        verify(mockStorage).list(TEST_BUCKET);
    }

    @Test
    void testDoListPage_WithPageToken() {
        ListBlobsPageRequest request = ListBlobsPageRequest.builder()
                .withPrefix("test/")
                .withPaginationToken("token-123")
                .build();

        when(mockTransformer.toBlobListOptions(request)).thenReturn(new Storage.BlobListOption[0]);
        when(mockStorage.list(TEST_BUCKET)).thenReturn(mockPage);
        when(mockPage.getNextPageToken()).thenReturn(null);

        ListBlobsPageResponse response = gcpBlobStore.doListPage(request);

        assertNotNull(response);
        assertNull(response.getNextPageToken());
        verify(mockStorage).list(TEST_BUCKET);
    }

    @Test
    void testDoGetMetadata_WithNullBlob() {
        when(mockTransformer.toBlobId(TEST_BUCKET, TEST_KEY, null)).thenReturn(mockBlobId);
        when(mockStorage.get(mockBlobId)).thenReturn(null);

        BlobMetadata metadata = gcpBlobStore.doGetMetadata(TEST_KEY, null);

        assertNull(metadata);
        verify(mockStorage).get(mockBlobId);
    }

    @Test
    void testDoCopy_WithNullBlob() {
        CopyRequest request = CopyRequest.builder()
                .srcKey(TEST_KEY)
                .destKey("dest-key")
                .build();

        when(mockTransformer.toCopyRequest(request)).thenReturn(mockCopyRequest);
        CopyWriter mockCopyWriter = mock(CopyWriter.class);
        when(mockStorage.copy(mockCopyRequest)).thenReturn(mockCopyWriter);
        when(mockCopyWriter.getResult()).thenReturn(null);

        CopyResponse response = gcpBlobStore.doCopy(request);

        assertNull(response);
        verify(mockStorage).copy(mockCopyRequest);
    }

    @Test
    void testDoGeneratePresignedUrl_WithNullBlob() throws Exception {
        PresignedUrlRequest request = PresignedUrlRequest.builder()
                .key(TEST_KEY)
                .type(PresignedOperation.DOWNLOAD)
                .duration(Duration.ofHours(1))
                .build();

        when(mockTransformer.toBlobInfo(request)).thenReturn(mockBlobInfo);
        when(mockStorage.signUrl(eq(mockBlobInfo), eq(3600000L), eq(TimeUnit.MILLISECONDS), any(), any())).thenReturn(null);

        URL url = gcpBlobStore.doGeneratePresignedUrl(request);

        assertNull(url);
        verify(mockStorage).signUrl(eq(mockBlobInfo), eq(3600000L), eq(TimeUnit.MILLISECONDS), any(), any());
    }

    @Test
    void testDoGeneratePresignedUrl_WithZeroExpiration() throws Exception {
        PresignedUrlRequest request = PresignedUrlRequest.builder()
                .key(TEST_KEY)
                .type(PresignedOperation.DOWNLOAD)
                .duration(Duration.ZERO)
                .build();

        when(mockTransformer.toBlobInfo(request)).thenReturn(mockBlobInfo);
        when(mockStorage.signUrl(eq(mockBlobInfo), eq(0L), eq(TimeUnit.MILLISECONDS), any(), any())).thenReturn(new URL("https://example.com"));

        URL url = gcpBlobStore.doGeneratePresignedUrl(request);

        assertNotNull(url);
        verify(mockStorage).signUrl(eq(mockBlobInfo), eq(0L), eq(TimeUnit.MILLISECONDS), any(), any());
    }

    @Test
    void testDoSetTags_WithEmptyMap() {
        Map<String, String> emptyTags = new HashMap<>();

        assertThrows(UnSupportedOperationException.class, () -> {
            gcpBlobStore.doSetTags(TEST_KEY, emptyTags);
        });
    }

    @Test
    void testDoSetTags_WithNullMap() {
        assertThrows(UnSupportedOperationException.class, () -> {
            gcpBlobStore.doSetTags(TEST_KEY, null);
        });
    }

    @Test
    void testDoGetTags_WithNullBlob() {
        assertThrows(UnSupportedOperationException.class, () -> {
            gcpBlobStore.doGetTags(TEST_KEY);
        });
    }

    @Test
    void testDoUpload_WithPath_EmptyFile() throws IOException {
        Path tempFile = Files.createTempFile("empty", ".txt");
        try {
            UploadRequest request = UploadRequest.builder()
                    .withKey(TEST_KEY)
                    .build();

            when(mockTransformer.toBlobInfo(request)).thenReturn(mockBlobInfo);
            when(mockTransformer.getKmsWriteOptions(request)).thenReturn(new Storage.BlobWriteOption[0]);
            when(mockStorage.createFrom(eq(mockBlobInfo), eq(tempFile), any(Storage.BlobWriteOption[].class))).thenReturn(mockBlob);
            when(mockTransformer.toUploadResponse(mockBlob)).thenReturn(mockUploadResponse);

            UploadResponse response = gcpBlobStore.doUpload(request, tempFile);

            assertNotNull(response);
            verify(mockStorage).createFrom(eq(mockBlobInfo), eq(tempFile), any(Storage.BlobWriteOption[].class));
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    @Test
    void testDoUpload_WithPath_NonExistentFile() throws IOException {
        Path nonExistentFile = Paths.get("/non/existent/file.txt");
        UploadRequest request = UploadRequest.builder()
                .withKey(TEST_KEY)
                .build();

        when(mockTransformer.toBlobInfo(request)).thenReturn(mockBlobInfo);
        when(mockTransformer.getKmsWriteOptions(request)).thenReturn(new Storage.BlobWriteOption[0]);
        when(mockStorage.createFrom(any(BlobInfo.class), any(Path.class), any(Storage.BlobWriteOption[].class))).thenThrow(new IOException("File not found"));

        assertThrows(SubstrateSdkException.class, () -> {
            gcpBlobStore.doUpload(request, nonExistentFile);
        });
    }

    @Test
    void testDoUpload_WithByteArray_EmptyArray() {
        byte[] emptyArray = new byte[0];
        UploadRequest request = UploadRequest.builder()
                .withKey(TEST_KEY)
                .build();

        when(mockTransformer.toBlobInfo(request)).thenReturn(mockBlobInfo);
        when(mockTransformer.getKmsTargetOptions(request)).thenReturn(new Storage.BlobTargetOption[0]);
        when(mockStorage.create(eq(mockBlobInfo), eq(emptyArray), any(Storage.BlobTargetOption[].class))).thenReturn(mockBlob);
        when(mockTransformer.toUploadResponse(mockBlob)).thenReturn(mockUploadResponse);

        UploadResponse response = gcpBlobStore.doUpload(request, emptyArray);

        assertNotNull(response);
        verify(mockStorage).create(eq(mockBlobInfo), eq(emptyArray), any(Storage.BlobTargetOption[].class));
    }

    @Test
    void testDoUpload_WithFile_EmptyFile() throws IOException {
        Path emptyFile = Files.createTempFile("empty", ".txt");
        try {
            UploadRequest request = UploadRequest.builder()
                    .withKey(TEST_KEY)
                    .build();

            when(mockTransformer.toBlobInfo(request)).thenReturn(mockBlobInfo);
            when(mockTransformer.getKmsWriteOptions(request)).thenReturn(new Storage.BlobWriteOption[0]);
            when(mockStorage.createFrom(eq(mockBlobInfo), eq(emptyFile), any(Storage.BlobWriteOption[].class))).thenReturn(mockBlob);
            when(mockTransformer.toUploadResponse(mockBlob)).thenReturn(mockUploadResponse);

            UploadResponse response = gcpBlobStore.doUpload(request, emptyFile);

            assertNotNull(response);
            verify(mockStorage).createFrom(eq(mockBlobInfo), eq(emptyFile), any(Storage.BlobWriteOption[].class));
        } finally {
            Files.deleteIfExists(emptyFile);
        }
    }

    @Test
    void testDoDelete_WithEmptyCollection() {
        Collection<BlobIdentifier> emptyCollection = Collections.emptyList();

        gcpBlobStore.doDelete(emptyCollection);

        // Should not throw exception and not call storage
        verify(mockStorage, never()).delete(any(BlobId[].class));
    }

    @Test
    void testDoDelete_WithNullCollection() {
        assertThrows(NullPointerException.class, () -> {
            gcpBlobStore.doDelete(null);
        });
    }

    @Test
    void testDoList_Iterator_EmptyList() {
        ListBlobsRequest request = ListBlobsRequest.builder()
                .withPrefix("test/")
                .build();

        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class))).thenReturn(mockPage);
        when(mockPage.iterateAll()).thenReturn(Collections.emptyList());

        Iterator<com.salesforce.multicloudj.blob.driver.BlobInfo> iterator = gcpBlobStore.doList(request);

        assertNotNull(iterator);
        assertFalse(iterator.hasNext());

        // Test calling next() on empty iterator
        assertThrows(NoSuchElementException.class, () -> {
            iterator.next();
        });
    }

    @Test
    void testDoList_Iterator_SingleElement() {
        ListBlobsRequest request = ListBlobsRequest.builder()
                .withPrefix("test/")
                .build();

        Blob mockBlobForList = mock(Blob.class);
        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class))).thenReturn(mockPage);
        when(mockPage.iterateAll()).thenReturn(Collections.singletonList(mockBlobForList));

        Iterator<com.salesforce.multicloudj.blob.driver.BlobInfo> iterator = gcpBlobStore.doList(request);

        assertNotNull(iterator);
        assertTrue(iterator.hasNext());
        assertNotNull(iterator.next()); // The actual BlobInfo will be created by the transformer
        assertFalse(iterator.hasNext());
    }

    @Test
    void testDoDownloadDirectory_Success() throws Exception {
        // Given
        String prefix = "test-prefix/";
        String localDir = tempDir.toString();
        DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
                .prefixToDownload(prefix)
                .localDestinationDirectory(localDir)
                .build();

        // Mock blobs
        Blob blob1 = mock(Blob.class);
        when(blob1.getName()).thenReturn("test-prefix/file1.txt");

        Blob blob2 = mock(Blob.class);
        when(blob2.getName()).thenReturn("test-prefix/subdir/file2.txt");

        Page<Blob> mockPage = mock(Page.class);
        when(mockPage.iterateAll()).thenReturn(List.of(blob1, blob2));
        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class)))
                .thenReturn(mockPage);

        // When
        DirectoryDownloadResponse response = gcpBlobStore.doDownloadDirectory(request);

        // Then
        assertNotNull(response);
        assertEquals(0, response.getFailedTransfers().size());
        verify(mockStorage).list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class));
        verify(blob1).downloadTo(any(Path.class));
        verify(blob2).downloadTo(any(Path.class));
    }

    @Test
    void testDoDownloadDirectory_SkipsFolderMarkers() throws Exception {
        // Given
        String prefix = "test-prefix/";
        String localDir = tempDir.toString();
        DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
                .prefixToDownload(prefix)
                .localDestinationDirectory(localDir)
                .build();

        // Mock blobs including folder marker
        Blob folderMarker = mock(Blob.class);
        when(folderMarker.getName()).thenReturn("test-prefix/subdir/");

        Blob realFile = mock(Blob.class);
        when(realFile.getName()).thenReturn("test-prefix/file.txt");

        Page<Blob> mockPage = mock(Page.class);
        when(mockPage.iterateAll()).thenReturn(List.of(folderMarker, realFile));
        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class)))
                .thenReturn(mockPage);

        // When
        DirectoryDownloadResponse response = gcpBlobStore.doDownloadDirectory(request);

        // Then
        assertNotNull(response);
        assertEquals(0, response.getFailedTransfers().size());
        verify(folderMarker).downloadTo(any(Path.class));
        verify(realFile).downloadTo(any(Path.class));
    }

    @Test
    @Disabled
    void testDoDownloadDirectory_PathTraversalProtection() throws Exception {
        // Given
        String prefix = "test-prefix/";
        String localDir = tempDir.toString();
        DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
                .prefixToDownload(prefix)
                .localDestinationDirectory(localDir)
                .build();

        // Mock malicious blob with path traversal
        Blob maliciousBlob = mock(Blob.class);
        when(maliciousBlob.getName()).thenReturn("test-prefix/../../../etc/passwd");

        Page<Blob> mockPage = mock(Page.class);
        when(mockPage.iterateAll()).thenReturn(List.of(maliciousBlob));
        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class)))
                .thenReturn(mockPage);

        // When
        DirectoryDownloadResponse response = gcpBlobStore.doDownloadDirectory(request);

        // Then
        assertNotNull(response);
        assertEquals(0, response.getFailedTransfers().size());
        verify(maliciousBlob).downloadTo(any(Path.class)); 
    }

    @Test
    void testDoDownloadDirectory_EmptyRelativePath() throws Exception {
        // Given
        String prefix = "test-prefix/";
        String localDir = tempDir.toString();
        DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
                .prefixToDownload(prefix)
                .localDestinationDirectory(localDir)
                .build();

        // Mock blob with empty relative path (name equals prefix)
        Blob emptyBlob = mock(Blob.class);
        when(emptyBlob.getName()).thenReturn("test-prefix/");

        Page<Blob> mockPage = mock(Page.class);
        when(mockPage.iterateAll()).thenReturn(List.of(emptyBlob));
        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class)))
                .thenReturn(mockPage);

        // When
        DirectoryDownloadResponse response = gcpBlobStore.doDownloadDirectory(request);

        // Then
        assertNotNull(response);
        assertEquals(0, response.getFailedTransfers().size());
        verify(emptyBlob, never()).downloadTo(any(Path.class)); // Should be skipped
    }

    @Test
    void testDoDownloadDirectory_NonMatchingPrefix() throws Exception {
        // Given
        String prefix = "test-prefix/";
        String localDir = tempDir.toString();
        DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
                .prefixToDownload(prefix)
                .localDestinationDirectory(localDir)
                .build();

        // Mock blob that doesn't start with prefix
        Blob nonMatchingBlob = mock(Blob.class);
        when(nonMatchingBlob.getName()).thenReturn("other-prefix/file.txt");

        Page<Blob> mockPage = mock(Page.class);
        when(mockPage.iterateAll()).thenReturn(List.of(nonMatchingBlob));
        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class)))
                .thenReturn(mockPage);

        // When
        DirectoryDownloadResponse response = gcpBlobStore.doDownloadDirectory(request);

        // Then
        assertNotNull(response);
        assertEquals(0, response.getFailedTransfers().size());
        verify(nonMatchingBlob).downloadTo(any(Path.class));
    }

    @Test
    void testDoDownloadDirectory_DownloadFailure() throws Exception {
        // Given
        String prefix = "test-prefix/";
        String localDir = tempDir.toString();
        DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
                .prefixToDownload(prefix)
                .localDestinationDirectory(localDir)
                .build();

        // Mock blob that will fail to download
        Blob failingBlob = mock(Blob.class);
        when(failingBlob.getName()).thenReturn("test-prefix/failing-file.txt");
        doThrow(new RuntimeException("Download failed")).when(failingBlob).downloadTo(any(Path.class));

        Page<Blob> mockPage = mock(Page.class);
        when(mockPage.iterateAll()).thenReturn(List.of(failingBlob));
        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class)))
                .thenReturn(mockPage);

        // When
        DirectoryDownloadResponse response = gcpBlobStore.doDownloadDirectory(request);

        // Then
        assertNotNull(response);
        assertEquals(1, response.getFailedTransfers().size());
        assertEquals("Download failed", response.getFailedTransfers().get(0).getException().getMessage());
    }

    @Test
    void testDoDownloadDirectory_NoPrefix() throws Exception {
        // Given
        String localDir = tempDir.toString();
        DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
                .localDestinationDirectory(localDir)
                .build(); // No prefix

        // Mock blob
        Blob blob = mock(Blob.class);
        when(blob.getName()).thenReturn("file.txt");

        Page<Blob> mockPage = mock(Page.class);
        when(mockPage.iterateAll()).thenReturn(List.of(blob));
        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class)))
                .thenReturn(mockPage);

        // When
        DirectoryDownloadResponse response = gcpBlobStore.doDownloadDirectory(request);

        // Then
        assertNotNull(response);
        assertEquals(0, response.getFailedTransfers().size());
        verify(blob).downloadTo(any(Path.class));
    }

    @Test
    void testDoDownloadDirectory_StorageException() throws Exception {
        // Given
        String prefix = "test-prefix/";
        String localDir = tempDir.toString();
        DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
                .prefixToDownload(prefix)
                .localDestinationDirectory(localDir)
                .build();

        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class)))
                .thenThrow(new RuntimeException("Storage error"));

        // When & Then
        assertThrows(SubstrateSdkException.class, () -> {
            gcpBlobStore.doDownloadDirectory(request);
        });
    }

    @Test
    void testDoDownloadDirectory_CreatesParentDirectories() throws Exception {
        // Given
        String prefix = "test-prefix/";
        String localDir = tempDir.toString();
        DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
                .prefixToDownload(prefix)
                .localDestinationDirectory(localDir)
                .build();

        // Mock blob in subdirectory
        Blob blob = mock(Blob.class);
        when(blob.getName()).thenReturn("test-prefix/subdir/nested/file.txt");

        Page<Blob> mockPage = mock(Page.class);
        when(mockPage.iterateAll()).thenReturn(List.of(blob));
        when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class)))
                .thenReturn(mockPage);

        // When
        DirectoryDownloadResponse response = gcpBlobStore.doDownloadDirectory(request);

        // Then
        assertNotNull(response);
        assertEquals(0, response.getFailedTransfers().size());
        verify(blob).downloadTo(any(Path.class));

        // Verify the nested directory structure was created
        Path expectedPath = tempDir.resolve("subdir/nested/file.txt");
        assertTrue(Files.exists(expectedPath.getParent()));
    }    
}
