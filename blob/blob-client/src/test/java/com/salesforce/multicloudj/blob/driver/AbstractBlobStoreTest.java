package com.salesforce.multicloudj.blob.driver;


import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AbstractBlobStoreTest {

    private AbstractBlobStore<TestBlobStore> mockBlobStore;
    private BlobStoreValidator validator;

    @BeforeEach
    void setup() {
        validator = spy(new BlobStoreValidator());
        var store = new TestBlobStore.Builder()
                .withBucket("some-bucket")
                .withRegion("us-west-2")
                .withValidator(validator)
                .build();
        mockBlobStore = spy(store);
        doCallRealMethod().when(mockBlobStore).upload(any(), any(InputStream.class));
        doCallRealMethod().when(mockBlobStore).upload(any(), any(byte[].class));
        doCallRealMethod().when(mockBlobStore).upload(any(), any(File.class));
        doCallRealMethod().when(mockBlobStore).upload(any(), any(Path.class));
        doCallRealMethod().when(mockBlobStore).download(any(), any(OutputStream.class));
        doCallRealMethod().when(mockBlobStore).download(any(), any(File.class));
        doCallRealMethod().when(mockBlobStore).download(any(), any(Path.class));
        doCallRealMethod().when(mockBlobStore).download(any(), any(ByteArray.class));
        doCallRealMethod().when(mockBlobStore).delete(anyString(), anyString());
        doCallRealMethod().when(mockBlobStore).delete(anyCollection());
        doCallRealMethod().when(mockBlobStore).copy(any());
        doCallRealMethod().when(mockBlobStore).getMetadata(any(), any());
        doCallRealMethod().when(mockBlobStore).list(any());
        doCallRealMethod().when(mockBlobStore).initiateMultipartUpload(any());
        doCallRealMethod().when(mockBlobStore).uploadMultipartPart(any(), any());
        doCallRealMethod().when(mockBlobStore).completeMultipartUpload(any(), any());
        doCallRealMethod().when(mockBlobStore).listMultipartUpload(any());
        doCallRealMethod().when(mockBlobStore).abortMultipartUpload(any());
        doCallRealMethod().when(mockBlobStore).getTags(any());
        doCallRealMethod().when(mockBlobStore).setTags(any(), any());
        doCallRealMethod().when(mockBlobStore).generatePresignedUrl(any());
        doCallRealMethod().when(mockBlobStore).doesObjectExist(any(), any());
        doReturn("bucket-1").when(mockBlobStore).getBucket();
    }

    @Test
    void testBuilder() {
        StsCredentials sessionCreds = new StsCredentials("key-1", "secret-1", "token-1");
        CredentialsOverrider credsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION).withSessionCredentials(sessionCreds).build();
        AbstractBlobStore.Builder<TestBlobStore> builder = new TestBlobStore.Builder();
        AbstractBlobStore<TestBlobStore> blobStore = builder
                .withBucket("bucket-1")
                .withRegion("us-west-2")
                .withCredentialsOverrider(credsOverrider)
                .withExecutorService(ForkJoinPool.commonPool())
                .build();

        assertEquals("bucket-1", blobStore.bucket);
        assertEquals("us-west-2", blobStore.region);
        assertEquals("key-1", sessionCreds.getAccessKeyId());
        assertEquals("secret-1", sessionCreds.getAccessKeySecret());
        assertEquals("token-1", sessionCreds.getSecurityToken());
        assertNotNull(blobStore.validator);
    }

    private UploadRequest getTestUploadRequest() {
        Map<String, String> metadata = Map.of("key-1", "value-1");
        Map<String, String> tags = Map.of("tag-1", "tag-value-1");
        return new UploadRequest.Builder()
                .withKey("object-1")
                .withMetadata(metadata)
                .withTags(tags)
                .withContentLength(1024L)
                .build();
    }

    @Test
    void testDoUploadInputStream() {
        InputStream mockContent = mock(InputStream.class);
        ArgumentCaptor<UploadRequest> requestCaptor = ArgumentCaptor.forClass(UploadRequest.class);
        ArgumentCaptor<InputStream> inputStreamCaptor = ArgumentCaptor.forClass(InputStream.class);
        mockBlobStore.upload(getTestUploadRequest(), mockContent);
        verify(validator, times(1)).validate(any(UploadRequest.class));
        verify(validator, times(1)).validateKey(any());
        verify(mockBlobStore, times(1)).doUpload(requestCaptor.capture(), inputStreamCaptor.capture());

        UploadRequest actualUploadRequest = requestCaptor.getValue();
        InputStream inputStream = inputStreamCaptor.getValue();
        assertEquals("object-1", actualUploadRequest.getKey());
        assertEquals(mockContent, inputStream);
        assertEquals(1024, actualUploadRequest.getContentLength());
        assertEquals("value-1", actualUploadRequest.getMetadata().get("key-1"));
        assertEquals("tag-value-1", actualUploadRequest.getTags().get("tag-1"));
    }

    @Test
    void testDoUploadByteArray() {
        byte[] content = "This is unit test content".getBytes();
        ArgumentCaptor<UploadRequest> requestCaptor = ArgumentCaptor.forClass(UploadRequest.class);
        ArgumentCaptor<byte[]> contentCaptor = ArgumentCaptor.forClass(byte[].class);
        mockBlobStore.upload(getTestUploadRequest(), content);
        verify(validator, times(1)).validate(any(UploadRequest.class));
        verify(validator, times(1)).validateKey(any());
        verify(mockBlobStore, times(1)).doUpload(requestCaptor.capture(), contentCaptor.capture());

        UploadRequest actualUploadRequest = requestCaptor.getValue();
        byte[] actualContent = contentCaptor.getValue();
        assertEquals("object-1", actualUploadRequest.getKey());
        assertEquals(content, actualContent);
        assertEquals(1024, actualUploadRequest.getContentLength());
        assertEquals("value-1", actualUploadRequest.getMetadata().get("key-1"));
        assertEquals("tag-value-1", actualUploadRequest.getTags().get("tag-1"));
    }

    @Test
    void testDoUploadFile() {
        File file = new File("tmp/fake.txt");
        ArgumentCaptor<UploadRequest> requestCaptor = ArgumentCaptor.forClass(UploadRequest.class);
        ArgumentCaptor<File> contentCaptor = ArgumentCaptor.forClass(File.class);
        mockBlobStore.upload(getTestUploadRequest(), file);
        verify(validator, times(1)).validate(any(UploadRequest.class));
        verify(validator, times(1)).validateKey(any());
        verify(mockBlobStore, times(1)).doUpload(requestCaptor.capture(), contentCaptor.capture());

        UploadRequest actualUploadRequest = requestCaptor.getValue();
        File actualContent = contentCaptor.getValue();
        assertEquals("object-1", actualUploadRequest.getKey());
        assertEquals(file, actualContent);
        assertEquals(1024, actualUploadRequest.getContentLength());
        assertEquals("value-1", actualUploadRequest.getMetadata().get("key-1"));
        assertEquals("tag-value-1", actualUploadRequest.getTags().get("tag-1"));
    }

    @Test
    void testDoUploadPath() {
        Path path = Paths.get("tmp/fake.txt");
        ArgumentCaptor<UploadRequest> requestCaptor = ArgumentCaptor.forClass(UploadRequest.class);
        ArgumentCaptor<Path> contentCaptor = ArgumentCaptor.forClass(Path.class);
        mockBlobStore.upload(getTestUploadRequest(), path);
        verify(validator, times(1)).validate(any(UploadRequest.class));
        verify(validator, times(1)).validateKey(any());
        verify(mockBlobStore, times(1)).doUpload(requestCaptor.capture(), contentCaptor.capture());

        UploadRequest actualUploadRequest = requestCaptor.getValue();
        Path actualContent = contentCaptor.getValue();
        assertEquals("object-1", actualUploadRequest.getKey());
        assertEquals(path, actualContent);
        assertEquals(1024, actualUploadRequest.getContentLength());
        assertEquals("value-1", actualUploadRequest.getMetadata().get("key-1"));
        assertEquals("tag-value-1", actualUploadRequest.getTags().get("tag-1"));
    }

    @Test
    void testDoDownloadOutputStream() {
        OutputStream contentOutput = mock(OutputStream.class);
        DownloadRequest request = new DownloadRequest.Builder().withKey("object-1").withVersionId("v1").build();

        mockBlobStore.download(request, contentOutput);

        ArgumentCaptor<DownloadRequest> requestCaptor = ArgumentCaptor.forClass(DownloadRequest.class);
        ArgumentCaptor<OutputStream> contentCaptor = ArgumentCaptor.forClass(OutputStream.class);
        verify(validator, times(1)).validate(any(DownloadRequest.class));
        verify(validator, times(1)).validateKey(any());
        verify(validator, times(1)).validateRange(any(), any());
        verify(mockBlobStore, times(1)).doDownload(requestCaptor.capture(), contentCaptor.capture());

        DownloadRequest actualDownloadRequest = requestCaptor.getValue();
        OutputStream actualContent = contentCaptor.getValue();
        assertEquals("object-1", actualDownloadRequest.getKey());
        assertEquals("v1", actualDownloadRequest.getVersionId());
        assertEquals(contentOutput, actualContent);
    }

    @Test
    void testDoDownloadByteArrayWrapper() {
        ByteArray contentOutput = new ByteArray();
        DownloadRequest request = new DownloadRequest.Builder().withKey("object-1").withVersionId("v1").build();

        mockBlobStore.download(request, contentOutput);

        ArgumentCaptor<DownloadRequest> requestCaptor = ArgumentCaptor.forClass(DownloadRequest.class);
        ArgumentCaptor<ByteArray> contentCaptor = ArgumentCaptor.forClass(ByteArray.class);
        verify(validator, times(1)).validate(any(DownloadRequest.class));
        verify(validator, times(1)).validateKey(any());
        verify(validator, times(1)).validateRange(any(), any());
        verify(mockBlobStore, times(1)).doDownload(requestCaptor.capture(), contentCaptor.capture());

        DownloadRequest actualDownloadRequest = requestCaptor.getValue();
        ByteArray actualContent = contentCaptor.getValue();
        assertEquals("object-1", actualDownloadRequest.getKey());
        assertEquals("v1", actualDownloadRequest.getVersionId());
        assertEquals(contentOutput, actualContent);
    }

    @Test
    void testDoDownloadFile() {
        File contentOutput = new File("tmp/fake.txt");
        DownloadRequest request = new DownloadRequest.Builder().withKey("object-1").withVersionId("v1").build();

        mockBlobStore.download(request, contentOutput);

        ArgumentCaptor<DownloadRequest> requestCaptor = ArgumentCaptor.forClass(DownloadRequest.class);
        ArgumentCaptor<File> contentCaptor = ArgumentCaptor.forClass(File.class);
        verify(validator, times(1)).validate(any(DownloadRequest.class));
        verify(validator, times(1)).validateKey(any());
        verify(validator, times(1)).validateRange(any(), any());
        verify(mockBlobStore, times(1)).doDownload(requestCaptor.capture(), contentCaptor.capture());

        DownloadRequest actualDownloadRequest = requestCaptor.getValue();
        File actualContent = contentCaptor.getValue();
        assertEquals("object-1", actualDownloadRequest.getKey());
        assertEquals("v1", actualDownloadRequest.getVersionId());
        assertEquals(contentOutput, actualContent);
    }

    @Test
    void testDoDownloadPath() {
        Path contentOutput = Paths.get("tmp/fake.txt");
        DownloadRequest request = new DownloadRequest.Builder().withKey("object-1").withVersionId("v1").build();

        mockBlobStore.download(request, contentOutput);

        ArgumentCaptor<DownloadRequest> requestCaptor = ArgumentCaptor.forClass(DownloadRequest.class);
        ArgumentCaptor<Path> contentCaptor = ArgumentCaptor.forClass(Path.class);
        verify(validator, times(1)).validate(any(DownloadRequest.class));
        verify(validator, times(1)).validateKey(any());
        verify(validator, times(1)).validateRange(any(), any());
        verify(mockBlobStore, times(1)).doDownload(requestCaptor.capture(), contentCaptor.capture());

        DownloadRequest actualDownloadRequest = requestCaptor.getValue();
        Path actualContent = contentCaptor.getValue();
        assertEquals("object-1", actualDownloadRequest.getKey());
        assertEquals("v1", actualDownloadRequest.getVersionId());
        assertEquals(contentOutput, actualContent);
    }

    @Test
    void testDoDeleteFile() {
        mockBlobStore.delete("object-1", "version-1");
        verify(validator, times(1)).requireNotBlank(any(), any());
        verify(mockBlobStore, times(1)).doDelete(eq("object-1"), eq("version-1"));
    }

    @Test
    void testDoDeleteFiles() {
        List<BlobIdentifier> objects = List.of(new BlobIdentifier("object-1","version-1"),
                new BlobIdentifier("object-2","version-2"),
                new BlobIdentifier("object-3","version-3"));
        mockBlobStore.delete(objects);
        verify(validator, times(1)).requireNotEmpty(anyCollection(), any());
        verify(mockBlobStore, times(1)).doDelete(eq(objects));
    }

    @Test
    void testDoCopy() {
        CopyRequest copyRequest = CopyRequest.builder()
                .srcKey("src-object-1")
                .srcVersionId("version-1")
                .destBucket("dest-bucket-1")
                .destKey("Dest-object-1")
                .build();
        mockBlobStore.copy(copyRequest);
        verify(validator, times(3)).requireNotBlank(any(), any());
        verify(mockBlobStore, times(1)).doCopy(eq(copyRequest));
    }

    @Test
    void testDoGetMetadata() {
        mockBlobStore.getMetadata("object-1", "version-1");
        verify(validator, times(1)).requireNotBlank(any(), any());
        verify(mockBlobStore, times(1)).doGetMetadata(eq("object-1"), eq("version-1"));
    }

    @Test
    void testDoList() {
        ListBlobsRequest request = new ListBlobsRequest.Builder().build();
        mockBlobStore.list(request);
        verify(mockBlobStore, times(1)).doList(request);
    }

    @Test
    void testDoInitiateMultipartUpload() {
        MultipartUploadRequest request = new MultipartUploadRequest.Builder().withKey("object-1").build();
        mockBlobStore.initiateMultipartUpload(request);
        verify(mockBlobStore, times(1)).doInitiateMultipartUpload(request);
    }

    @Test
    void testDoUploadMultipartPart() {
        MultipartUpload multipartUpload = new MultipartUpload("bucket-1", "object-1", "mpu-id");
        MultipartPart multipartPart = new MultipartPart(1, null, 0);
        mockBlobStore.uploadMultipartPart(multipartUpload, multipartPart);
        verify(mockBlobStore, times(1)).doUploadMultipartPart(multipartUpload, multipartPart);
        verify(validator, times(1)).requireEqualsIgnoreCase(any(), any(), any());
    }

    @Test
    void testDoCompleteMultipartUpload() {
        MultipartUpload multipartUpload = new MultipartUpload("bucket-1", "object-1", "mpu-id");
        List<UploadPartResponse> listOfParts = List.of(new UploadPartResponse(1, "etag", 0));
        mockBlobStore.completeMultipartUpload(multipartUpload, listOfParts);
        verify(mockBlobStore, times(1)).doCompleteMultipartUpload(multipartUpload, listOfParts);
        verify(validator, times(1)).requireEqualsIgnoreCase(any(), any(), any());
    }

    @Test
    void testDoListMultipartUpload() {
        MultipartUpload multipartUpload = new MultipartUpload("bucket-1", "object-1", "mpu-id");
        mockBlobStore.listMultipartUpload(multipartUpload);
        verify(mockBlobStore, times(1)).doListMultipartUpload(multipartUpload);
        verify(validator, times(1)).requireEqualsIgnoreCase(any(), any(), any());
    }

    @Test
    void testDoAbortMultipartUpload() {
        MultipartUpload multipartUpload = new MultipartUpload("bucket-1", "object-1", "mpu-id");
        mockBlobStore.abortMultipartUpload(multipartUpload);
        verify(mockBlobStore, times(1)).doAbortMultipartUpload(multipartUpload);
        verify(validator, times(1)).requireEqualsIgnoreCase(any(), any(), any());
    }

    @Test
    void testDoGetTags() {
        mockBlobStore.getTags("object-1");
        verify(validator, times(1)).validateKey(any());
        verify(mockBlobStore, times(1)).doGetTags("object-1");
    }

    @Test
    void testDoSetTags() {
        Map<String, String> tags = Map.of("tag-1", "tag-value-1");
        mockBlobStore.setTags("object-1", tags);
        verify(mockBlobStore, times(1)).doSetTags("object-1", tags);
    }

    @Test
    void testGeneratePresignedUploadUrl() {
        PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.UPLOAD)
                .key("object-1")
                .duration(Duration.ofMinutes(10))
                .build();

        mockBlobStore.generatePresignedUrl(presignedUrlRequest);
        verify(mockBlobStore, times(1)).doGeneratePresignedUrl(presignedUrlRequest);
        verify(validator, times(1)).validate(any(PresignedUrlRequest.class));
    }

    @Test
    void testGeneratePresignedDownloadUrl() {
        PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.DOWNLOAD)
                .key("object-1")
                .duration(Duration.ofMinutes(10))
                .build();

        mockBlobStore.generatePresignedUrl(presignedUrlRequest);
        verify(mockBlobStore, times(1)).doGeneratePresignedUrl(presignedUrlRequest);
        verify(validator, times(1)).validate(any(PresignedUrlRequest.class));
    }

    @Test
    void testDoesObjectExist() {
        mockBlobStore.doesObjectExist("object-1", "version-1");
        verify(validator, times(1)).validateKey(any());
        verify(mockBlobStore, times(1)).doDoesObjectExist("object-1", "version-1");
    }
}
