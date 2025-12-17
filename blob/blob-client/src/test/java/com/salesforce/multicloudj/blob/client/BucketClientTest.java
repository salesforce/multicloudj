package com.salesforce.multicloudj.blob.client;

import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.PresignedOperation;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BucketClientTest {

    private AbstractBlobStore mockBlobStore;
    private StsCredentials creds;
    private BucketClient client;

    private MockedStatic<ProviderSupplier> providerSupplier;

    @BeforeEach
    void setup() {
        mockBlobStore = mock(AbstractBlobStore.class);
        doReturn(UnAuthorizedException.class).when(mockBlobStore).getException(any());
        providerSupplier = mockStatic(ProviderSupplier.class);
        AbstractBlobStore.Builder mockBuilder = mock(AbstractBlobStore.Builder.class);
        when(mockBuilder.build()).thenReturn(mockBlobStore);
        providerSupplier.when(() -> ProviderSupplier.findProviderBuilder("test")).thenReturn(mockBuilder);
        creds = new StsCredentials("keyId", "keySecret", "token");
        CredentialsOverrider credsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION).withSessionCredentials(creds).build();
        client = BucketClient.builder("test")
                .withBucket("bucket-1")
                .withRegion("us-west-1")
                .withCredentialsOverrider(credsOverrider)
                .withEndpoint(URI.create("https://blob.endpoint.com"))
                .withProxyEndpoint(URI.create("https://proxy.endpoint.com"))
                .withMaxConnections(100)
                .withSocketTimeout(Duration.ofSeconds(60))
                .withIdleConnectionTimeout(Duration.ofMinutes(10))
                .build();
    }

    @AfterEach
    void teardown() {
        if (providerSupplier != null) {
            providerSupplier.close();
        }
    }

    private UploadResponse getTestUploadResponse() {
        return UploadResponse.builder()
                .key("object-1")
                .versionId("version-1")
                .eTag("eTag-1")
                .build();
    }

    @Test
    void testUploadInputStream() {
        UploadResponse expectedResponse = getTestUploadResponse();
        when(mockBlobStore.upload(any(), any(InputStream.class))).thenReturn(expectedResponse);

        var content = mock(InputStream.class);
        UploadRequest request = new UploadRequest.Builder().withKey("object-1").build();
        UploadResponse actualResponse = client.upload(request, content);
        verify(mockBlobStore, times(1)).upload(eq(request), eq(content));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    void testUploadByteArray() {
        UploadResponse expectedResponse = getTestUploadResponse();
        when(mockBlobStore.upload(any(), any(byte[].class))).thenReturn(expectedResponse);

        byte[] content = "test data".getBytes();
        UploadRequest request = new UploadRequest.Builder().withKey("object-1").build();
        UploadResponse actualResponse = client.upload(request, content);
        verify(mockBlobStore, times(1)).upload(eq(request), eq(content));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    void testUploadFile() {
        UploadResponse expectedResponse = getTestUploadResponse();
        when(mockBlobStore.upload(any(), any(File.class))).thenReturn(expectedResponse);

        File content = new File("fake.txt");
        UploadRequest request = new UploadRequest.Builder().withKey("object-1").build();
        UploadResponse actualResponse = client.upload(request, content);
        verify(mockBlobStore, times(1)).upload(eq(request), eq(content));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    void testUploadPath() {
        UploadResponse expectedResponse = getTestUploadResponse();
        when(mockBlobStore.upload(any(), any(Path.class))).thenReturn(expectedResponse);

        Path content = Paths.get("fake.txt");
        UploadRequest request = new UploadRequest.Builder().withKey("object-1").build();
        UploadResponse actualResponse = client.upload(request, content);
        verify(mockBlobStore, times(1)).upload(eq(request), eq(content));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    void testUploadThrowsException() throws IOException {
        when(mockBlobStore.upload(any(), any(InputStream.class))).thenThrow(RuntimeException.class);
        when(mockBlobStore.upload(any(), any(byte[].class))).thenThrow(RuntimeException.class);
        when(mockBlobStore.upload(any(), any(File.class))).thenThrow(RuntimeException.class);
        when(mockBlobStore.upload(any(), any(Path.class))).thenThrow(RuntimeException.class);
        UploadRequest request = mock(UploadRequest.class);

        try(InputStream inputStream = mock(InputStream.class)) {
            assertThrows(UnAuthorizedException.class, () -> {
                client.upload(request, inputStream);
            });
        }
        assertThrows(UnAuthorizedException.class, () -> {
            client.upload(request, "Test data".getBytes());
        });
        assertThrows(UnAuthorizedException.class, () -> {
            client.upload(request, new File("testfile.txt"));
        });
        assertThrows(UnAuthorizedException.class, () -> {
            client.upload(request, Paths.get("testfile.txt"));
        });
    }

    @Test
    void testDownloadOutputStream() {
        OutputStream mockContent = mock(OutputStream.class);
        DownloadRequest request = new DownloadRequest.Builder().withKey("object-1").build();
        client.download(request, mockContent);
        verify(mockBlobStore, times(1)).download(eq(request), eq(mockContent));
    }

    @Test
    void testDownloadByteArray() {
        ByteArray byteArray = new ByteArray();
        DownloadRequest request = new DownloadRequest.Builder().withKey("object-1").build();
        client.download(request, byteArray);
        verify(mockBlobStore, times(1)).download(eq(request), eq(byteArray));
    }

    @Test
    void testDownloadFile() {
        File file = new File("fake.txt");
        DownloadRequest request = new DownloadRequest.Builder().withKey("object-1").build();
        client.download(request, file);
        verify(mockBlobStore, times(1)).download(eq(request), eq(file));
    }

    @Test
    void testDownloadPath() {
        Path path = Paths.get("fake.txt");
        DownloadRequest request = new DownloadRequest.Builder().withKey("object-1").build();
        client.download(request, path);
        verify(mockBlobStore, times(1)).download(eq(request), eq(path));
    }

    @Test
    void testDownloadInputStream() {
        DownloadRequest request = new DownloadRequest.Builder().withKey("object-1").build();
        client.download(request);
        verify(mockBlobStore, times(1)).download(eq(request));
    }

    @Test
    void testDownloadThrowsException() throws IOException {
        doThrow(RuntimeException.class).when(mockBlobStore).download(any(), any(OutputStream.class));
        doThrow(RuntimeException.class).when(mockBlobStore).download(any(), any(ByteArray.class));
        doThrow(RuntimeException.class).when(mockBlobStore).download(any(), any(File.class));
        doThrow(RuntimeException.class).when(mockBlobStore).download(any(), any(Path.class));
        doThrow(RuntimeException.class).when(mockBlobStore).download(any());

        DownloadRequest request = mock(DownloadRequest.class);
        try(OutputStream outputStream = mock(OutputStream.class)) {
            assertThrows(UnAuthorizedException.class, () -> {
                client.download(request, outputStream);
            });
        }
        assertThrows(UnAuthorizedException.class, () -> {
            client.download(request, new ByteArray());
        });
        assertThrows(UnAuthorizedException.class, () -> {
            client.download(request, new File("testfile.txt"));
        });
        assertThrows(UnAuthorizedException.class, () -> {
            client.download(request, Paths.get("testfile.txt"));
        });
        assertThrows(UnAuthorizedException.class, () -> {
            client.download(request);
        });
    }

    @Test
    void testDelete() {
        client.delete("object-1", "version-1");
        verify(mockBlobStore, times(1)).delete(eq("object-1"), eq("version-1"));
    }

    @Test
    void testBulkDelete() {
        List<BlobIdentifier> objects = List.of(new BlobIdentifier("object-1","version-1"),
                new BlobIdentifier("object-2","version-2"),
                new BlobIdentifier("object-3","version-3"));
        client.delete(objects);
        verify(mockBlobStore, times(1)).delete(eq(objects));
    }

    @Test
    void testDeleteThrowsException() {
        doThrow(RuntimeException.class).when(mockBlobStore).delete(anyString(), anyString());
        doThrow(RuntimeException.class).when(mockBlobStore).delete(any());

        assertThrows(UnAuthorizedException.class, () -> {
            client.delete("object-1", "version-1");
        });
        assertThrows(UnAuthorizedException.class, () -> {
            client.delete(Collections.emptyList());
        });
    }

    @Test
    void testCopy() {
        String destKey = "dest-object-1";
        CopyResponse expectedResponse = CopyResponse.builder()
                .key(destKey)
                .versionId("dest-version-1")
                .eTag("eTag-1")
                .lastModified(Instant.now())
                .build();
        when(mockBlobStore.copy(any())).thenReturn(expectedResponse);

        CopyRequest request = CopyRequest.builder()
                .srcKey("src-object-1")
                .srcVersionId("version-1")
                .destBucket("dest-bucket-1")
                .destKey(destKey)
                .build();

        CopyResponse actualResponse = client.copy(request);
        verify(mockBlobStore, times(1)).copy(eq(request));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    void testCopyThrowsException() {
        when(mockBlobStore.copy(any())).thenThrow(RuntimeException.class);

        CopyRequest request = CopyRequest.builder()
                .srcKey("src-object-1")
                .srcVersionId("version-1")
                .destBucket("dest-bucket-1")
                .destKey("dest-object-1")
                .build();

        assertThrows(UnAuthorizedException.class, () -> {
            client.copy(request);
        });
    }

    @Test
    void testGetMetadata() {
        Instant now = Instant.now();
        BlobMetadata expectedBlobInfo = BlobMetadata.builder()
                .key("object-1")
                .versionId("v1")
                .eTag("eTag-1")
                .objectSize(10)
                .metadata(Map.of("key-1", "value-1"))
                .lastModified(now)
                .build();
        when(mockBlobStore.getMetadata(any(), any())).thenReturn(expectedBlobInfo);
        BlobMetadata actualBlobMetadata = client.getMetadata("object-1", "v1");
        verify(mockBlobStore, times(1)).getMetadata(eq("object-1"), eq("v1"));
        assertEquals("object-1", actualBlobMetadata.getKey());
        assertEquals("v1", actualBlobMetadata.getVersionId());
        assertEquals("eTag-1", actualBlobMetadata.getETag());
        assertEquals(10, actualBlobMetadata.getObjectSize());
        assertEquals("value-1", actualBlobMetadata.getMetadata().get("key-1"));
        assertEquals(now, actualBlobMetadata.getLastModified());
    }

    @Test
    void testGetMetadataThrowsException() {
        when(mockBlobStore.getMetadata(any(), any())).thenThrow(RuntimeException.class);

        assertThrows(UnAuthorizedException.class, () -> {
            client.getMetadata("object-1", "version-1");
        });
    }

    @Test
    void testList() {
        ListBlobsRequest request = new ListBlobsRequest.Builder().build();
        client.list(request);
        verify(mockBlobStore, times(1)).list(request);
    }

    @Test
    void testListThrowsException() {
        ListBlobsRequest request = new ListBlobsRequest.Builder().build();
        when(mockBlobStore.list(request)).thenThrow(RuntimeException.class);

        assertThrows(UnAuthorizedException.class, () -> {
            client.list(request);
        });
    }

    @Test
    void testInitiateMultipartUpload() {
        MultipartUploadRequest request = new MultipartUploadRequest.Builder().withKey("object-1").build();
        client.initiateMultipartUpload(request);
        verify(mockBlobStore, times(1)).initiateMultipartUpload(request);
    }

    @Test
    void testInitiateMultipartUploadException() {
        MultipartUploadRequest request = new MultipartUploadRequest.Builder().withKey("object-1").build();
        when(mockBlobStore.initiateMultipartUpload(request)).thenThrow(RuntimeException.class);

        assertThrows(UnAuthorizedException.class, () -> {
            client.initiateMultipartUpload(request);
        });
    }

    @Test
    void testUploadMultipartPart() {
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .build();
        MultipartPart multipartPart = new MultipartPart(1, null, 0);
        client.uploadMultipartPart(multipartUpload, multipartPart);
        verify(mockBlobStore, times(1)).uploadMultipartPart(multipartUpload, multipartPart);
    }

    @Test
    void testUploadMultipartPartException() {
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .build();
        MultipartPart multipartPart = new MultipartPart(1, null, 0);
        when(mockBlobStore.uploadMultipartPart(multipartUpload, multipartPart)).thenThrow(RuntimeException.class);

        assertThrows(UnAuthorizedException.class, () -> {
            client.uploadMultipartPart(multipartUpload, multipartPart);
        });
    }

    @Test
    void testCompleteMultipartUpload() {
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .build();
        List<UploadPartResponse> listOfParts = List.of(new UploadPartResponse(1, "etag", 0));
        client.completeMultipartUpload(multipartUpload, listOfParts);
        verify(mockBlobStore, times(1)).completeMultipartUpload(multipartUpload, listOfParts);
    }

    @Test
    void testCompleteMultipartUploadException() {
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .build();
        List<UploadPartResponse> listOfParts = List.of(new UploadPartResponse(1, "etag", 0));
        when(mockBlobStore.completeMultipartUpload(multipartUpload, listOfParts)).thenThrow(RuntimeException.class);

        assertThrows(UnAuthorizedException.class, () -> {
            client.completeMultipartUpload(multipartUpload, listOfParts);
        });
    }

    @Test
    void testListMultipartUpload() {
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .build();
        client.listMultipartUpload(multipartUpload);
        verify(mockBlobStore, times(1)).listMultipartUpload(multipartUpload);
    }

    @Test
    void testListMultipartUploadException() {
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .build();
        when(mockBlobStore.listMultipartUpload(multipartUpload)).thenThrow(RuntimeException.class);

        assertThrows(UnAuthorizedException.class, () -> {
            client.listMultipartUpload(multipartUpload);
        });
    }

    @Test
    void testAbortMultipartUpload() {
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .build();
        client.abortMultipartUpload(multipartUpload);
        verify(mockBlobStore, times(1)).abortMultipartUpload(multipartUpload);
    }

    @Test
    void testAbortMultipartUploadException() {
        MultipartUpload multipartUpload = MultipartUpload.builder()
                .bucket("bucket-1")
                .key("object-1")
                .id("mpu-id")
                .build();
        doThrow(RuntimeException.class).when(mockBlobStore).abortMultipartUpload(eq(multipartUpload));

        assertThrows(UnAuthorizedException.class, () -> {
            client.abortMultipartUpload(multipartUpload);
        });
    }

    @Test
    void testGetTags() {
        client.getTags("object-1");
        verify(mockBlobStore, times(1)).getTags("object-1");
    }

    @Test
    void testGetTagsException() {
        doThrow(RuntimeException.class).when(mockBlobStore).getTags("object-1");

        assertThrows(UnAuthorizedException.class, () -> {
            client.getTags("object-1");
        });
    }

    @Test
    void testSetTags() {
        Map<String, String> tags = Map.of("key1", "value1", "key2", "value2");
        client.setTags("object-1", tags);
        verify(mockBlobStore, times(1)).setTags("object-1", tags);
    }

    @Test
    void testSetTagsException() {
        Map<String, String> tags = Map.of("key1", "value1", "key2", "value2");
        doThrow(RuntimeException.class).when(mockBlobStore).setTags("object-1", tags);

        assertThrows(UnAuthorizedException.class, () -> {
            client.setTags("object-1", tags);
        });
    }

    @Test
    void testGeneratePresignedUrl() {
        PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.DOWNLOAD)
                .key("object-1")
                .duration(Duration.ofMinutes(10))
                .build();

        doThrow(RuntimeException.class).when(mockBlobStore).generatePresignedUrl(presignedUrlRequest);

        assertThrows(UnAuthorizedException.class, () -> {
            client.generatePresignedUrl(presignedUrlRequest);
        });
    }

    @Test
    void testDoesObjectExist() {
        client.doesObjectExist("object-1", "version-1");
        verify(mockBlobStore, times(1)).doesObjectExist("object-1", "version-1");

        doThrow(RuntimeException.class).when(mockBlobStore).doesObjectExist(any(), any());
        assertThrows(UnAuthorizedException.class, () -> {
            client.doesObjectExist("object-1", "version-1");
        });
    }

    @Test
    void testDoesBucketExist_ReturnsTrue() {
        when(mockBlobStore.doesBucketExist()).thenReturn(true);
        boolean result = client.doesBucketExist();
        verify(mockBlobStore, times(1)).doesBucketExist();
        assertTrue(result);
    }

    @Test
    void testDoesBucketExist_ReturnsFalse() {
        when(mockBlobStore.doesBucketExist()).thenReturn(false);
        boolean result = client.doesBucketExist();
        verify(mockBlobStore, times(1)).doesBucketExist();
        assertFalse(result);
    }

    @Test
    void testDoesBucketExist_ThrowsException() {
        doThrow(RuntimeException.class).when(mockBlobStore).doesBucketExist();
        assertThrows(UnAuthorizedException.class, () -> {
            client.doesBucketExist();
        });
        verify(mockBlobStore, times(1)).doesBucketExist();
    }

    @Test
    void testBucketClientBuilderWithRetryConfig() {
        RetryConfig retryConfig = RetryConfig.builder()
                .maxAttempts(5)
                .attemptTimeout(3000L)
                .totalTimeout(10000L)
                .build();

        AbstractBlobStore.Builder mockBuilder2 = mock(AbstractBlobStore.Builder.class);
        when(mockBuilder2.withBucket(any())).thenReturn(mockBuilder2);
        when(mockBuilder2.withRegion(any())).thenReturn(mockBuilder2);
        when(mockBuilder2.withRetryConfig(any())).thenReturn(mockBuilder2);
        when(mockBuilder2.build()).thenReturn(mockBlobStore);

        providerSupplier.when(() -> ProviderSupplier.findProviderBuilder("test2"))
                .thenReturn(mockBuilder2);

        BucketClient testClient = BucketClient.builder("test2")
                .withBucket("test-bucket")
                .withRegion("us-east-1")
                .withRetryConfig(retryConfig)
                .build();

        verify(mockBuilder2, times(1)).withRetryConfig(retryConfig);
        assertNotNull(testClient);
    }
}
