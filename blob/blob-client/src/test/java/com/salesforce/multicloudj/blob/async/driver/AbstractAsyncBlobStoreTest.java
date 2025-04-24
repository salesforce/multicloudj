package com.salesforce.multicloudj.blob.async.driver;

import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobStoreValidator;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsBatch;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.PresignedOperation;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AbstractAsyncBlobStoreTest {

    private TestAsyncBlobStore mockBlobStore;
    private BlobStoreValidator validator;

    @BeforeEach
    void setup() {
        validator = spy(new BlobStoreValidator());
        var store = TestAsyncBlobStore
                .builder()
                .withBucket("some-bucket")
                .withRegion("us-west-2")
                .withValidator(validator)
                .build();
        mockBlobStore = spy(store);
    }

    @Test
    void testBuilder() {
        StsCredentials sessionCreds = new StsCredentials("key-1", "secret-1", "token-1");
        CredentialsOverrider credsOverrider = new CredentialsOverrider
                .Builder(CredentialsType.SESSION)
                .withSessionCredentials(sessionCreds)
                .build();
        AsyncBlobStoreProvider.Builder builder = new TestAsyncBlobStore.Builder();
        AsyncBlobStore blobStore = builder
                .withBucket("bucket-1")
                .withRegion("us-west-2")
                .withCredentialsOverrider(credsOverrider)
                .build();

        assertEquals("bucket-1", blobStore.getBucket());
        assertEquals("us-west-2", blobStore.getRegion());
        assertEquals("key-1", sessionCreds.getAccessKeyId());
        assertEquals("secret-1", sessionCreds.getAccessKeySecret());
        assertEquals("token-1", sessionCreds.getSecurityToken());
    }

    @Test
    void testDoUploadFileInputStream() {
        byte[] content = "Unit test blob data".getBytes();
        InputStream inputStream = new ByteArrayInputStream(content);
        UploadRequest request = getTestUploadRequest();
        mockBlobStore.upload(request, inputStream);

        verify(validator, times(1)).validate(any(UploadRequest.class));
        verify(validator, times(1)).validateKey(any());
        ArgumentCaptor<UploadRequest> requestCaptor = ArgumentCaptor.forClass(UploadRequest.class);
        ArgumentCaptor<InputStream> contentCaptor = ArgumentCaptor.forClass(InputStream.class);
        verify(mockBlobStore, times(1)).doUpload(requestCaptor.capture(), contentCaptor.capture());

        UploadRequest actualUploadRequest = requestCaptor.getValue();
        assertEquals("object-1", actualUploadRequest.getKey());
        assertEquals(1024L, actualUploadRequest.getContentLength());
        assertEquals("value-1", actualUploadRequest.getMetadata().get("key-1"));
        assertEquals(inputStream, contentCaptor.getValue());
    }

    @Test
    void testDoUploadFileByteArray() {
        byte[] content = new byte[1024];
        UploadRequest request = getTestUploadRequest();
        mockBlobStore.upload(request, content);

        verify(validator, times(1)).validate(any(UploadRequest.class));
        verify(validator, times(1)).validateKey(any());
        ArgumentCaptor<UploadRequest> requestCaptor = ArgumentCaptor.forClass(UploadRequest.class);
        ArgumentCaptor<byte[]> contentCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(mockBlobStore, times(1)).doUpload(requestCaptor.capture(), contentCaptor.capture());

        UploadRequest actualUploadRequest = requestCaptor.getValue();
        assertEquals("object-1", actualUploadRequest.getKey());
        assertEquals(1024L, actualUploadRequest.getContentLength());
        assertEquals("value-1", actualUploadRequest.getMetadata().get("key-1"));
        assertEquals(content, contentCaptor.getValue());
    }

    @Test
    void testDoUploadFile() {
        File file = new File("test.txt");
        UploadRequest request = getTestUploadRequest();
        mockBlobStore.upload(request, file);

        verify(validator, times(1)).validate(any(UploadRequest.class));
        verify(validator, times(1)).validateKey(any());
        ArgumentCaptor<UploadRequest> requestCaptor = ArgumentCaptor.forClass(UploadRequest.class);
        ArgumentCaptor<File> contentCaptor = ArgumentCaptor.forClass(File.class);
        verify(mockBlobStore, times(1)).doUpload(requestCaptor.capture(), contentCaptor.capture());

        UploadRequest actualUploadRequest = requestCaptor.getValue();
        assertEquals("object-1", actualUploadRequest.getKey());
        assertEquals(1024L, actualUploadRequest.getContentLength());
        assertEquals("value-1", actualUploadRequest.getMetadata().get("key-1"));
        assertEquals(file, contentCaptor.getValue());
    }

    @Test
    void testDoUploadPath() {
        File file = new File("test.txt");
        UploadRequest request = getTestUploadRequest();
        Path path = file.toPath();
        mockBlobStore.upload(request, path);

        verify(validator, times(1)).validate(any(UploadRequest.class));
        verify(validator, times(1)).validateKey(any());
        ArgumentCaptor<UploadRequest> requestCaptor = ArgumentCaptor.forClass(UploadRequest.class);
        ArgumentCaptor<Path> contentCaptor = ArgumentCaptor.forClass(Path.class);
        verify(mockBlobStore, times(1)).doUpload(requestCaptor.capture(), contentCaptor.capture());

        UploadRequest actualUploadRequest = requestCaptor.getValue();
        assertEquals("object-1", actualUploadRequest.getKey());
        assertEquals(1024L, actualUploadRequest.getContentLength());
        assertEquals("value-1", actualUploadRequest.getMetadata().get("key-1"));
        assertEquals(path, contentCaptor.getValue());
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
    void testDoDownloadOutputStream() {
        OutputStream outputStream = mock(OutputStream.class);
        DownloadRequest request = getTestDownloadRequest();
        mockBlobStore.download(request, outputStream);

        verify(validator, times(1)).validate(any(DownloadRequest.class));
        verify(validator, times(1)).validateKey(any());
        verify(validator, times(1)).validateRange(any(), any());
        ArgumentCaptor<DownloadRequest> requestCaptor = ArgumentCaptor.forClass(DownloadRequest.class);
        ArgumentCaptor<OutputStream> contentCaptor = ArgumentCaptor.forClass(OutputStream.class);
        verify(mockBlobStore, times(1)).doDownload(requestCaptor.capture(), contentCaptor.capture());

        DownloadRequest actualRequest = requestCaptor.getValue();
        assertEquals("object-1", actualRequest.getKey());
        assertEquals("version-1", actualRequest.getVersionId());
        assertEquals(outputStream, contentCaptor.getValue());
    }

    @Test
    void testDoDownloadByteArrayWrapper() {
        ByteArray byteArray = new ByteArray();
        DownloadRequest request = getTestDownloadRequest();
        mockBlobStore.download(request, byteArray);

        verify(validator, times(1)).validate(any(DownloadRequest.class));
        verify(validator, times(1)).validateKey(any());
        verify(validator, times(1)).validateRange(any(), any());
        ArgumentCaptor<DownloadRequest> requestCaptor = ArgumentCaptor.forClass(DownloadRequest.class);
        ArgumentCaptor<ByteArray> contentCaptor = ArgumentCaptor.forClass(ByteArray.class);
        verify(mockBlobStore, times(1)).doDownload(requestCaptor.capture(), contentCaptor.capture());

        DownloadRequest actualRequest = requestCaptor.getValue();
        assertEquals("object-1", actualRequest.getKey());
        assertEquals("version-1", actualRequest.getVersionId());
        assertEquals(byteArray, contentCaptor.getValue());
    }

    @Test
    void testDoDownloadFile() {
        File file = new File("testFile.txt");
        DownloadRequest request = getTestDownloadRequest();
        mockBlobStore.download(request, file);

        verify(validator, times(1)).validate(any(DownloadRequest.class));
        verify(validator, times(1)).validateKey(any());
        verify(validator, times(1)).validateRange(any(), any());
        ArgumentCaptor<DownloadRequest> requestCaptor = ArgumentCaptor.forClass(DownloadRequest.class);
        ArgumentCaptor<File> contentCaptor = ArgumentCaptor.forClass(File.class);
        verify(mockBlobStore, times(1)).doDownload(requestCaptor.capture(), contentCaptor.capture());

        DownloadRequest actualRequest = requestCaptor.getValue();
        assertEquals("object-1", actualRequest.getKey());
        assertEquals("version-1", actualRequest.getVersionId());
        assertEquals(file, contentCaptor.getValue());
    }

    @Test
    void testDoDownloadPath() {
        Path path = Path.of("testPath.txt");
        DownloadRequest request = getTestDownloadRequest();
        mockBlobStore.download(request, path);

        verify(validator, times(1)).validate(any(DownloadRequest.class));
        verify(validator, times(1)).validateKey(any());
        verify(validator, times(1)).validateRange(any(), any());
        ArgumentCaptor<DownloadRequest> requestCaptor = ArgumentCaptor.forClass(DownloadRequest.class);
        ArgumentCaptor<Path> contentCaptor = ArgumentCaptor.forClass(Path.class);
        verify(mockBlobStore, times(1)).doDownload(requestCaptor.capture(), contentCaptor.capture());

        DownloadRequest actualRequest = requestCaptor.getValue();
        assertEquals("object-1", actualRequest.getKey());
        assertEquals("version-1", actualRequest.getVersionId());
        assertEquals(path, contentCaptor.getValue());
    }

    private DownloadRequest getTestDownloadRequest() {
        return new DownloadRequest.Builder().withKey("object-1").withVersionId("version-1").build();
    }

    @Test
    void testDoDeleteFile() {
        mockBlobStore.delete("object-1", "version-1");
        verify(validator, times(1)).validateKey(any());
        verify(mockBlobStore, times(1)).doDelete(eq("object-1"), eq("version-1"));
    }

    @Test
    void testDoDeleteFiles() {
        Collection<BlobIdentifier> objects = List.of(new BlobIdentifier("object-1", "version-1"),
                new BlobIdentifier("object-2", "version-2"),
                new BlobIdentifier("object-3", "version-3"));
        mockBlobStore.delete(objects);
        verify(validator, times(1)).validateBlobIdentifiers(eq(objects));
        verify(mockBlobStore, times(1)).doDelete(eq(objects));
    }

    @Test
    void testDoCopy() {
        CopyRequest request = CopyRequest.builder()
                .destBucket("dest-bucket-1")
                .srcKey("src-object-1")
                .srcVersionId("version-1")
                .destKey("dest-object-1")
                .build();
        mockBlobStore.copy(request);
        verify(validator).validateKey("src-object-1");
        verify(validator).validateKey("dest-object-1");
        verify(validator).validateBucket("dest-bucket-1");
        verify(mockBlobStore, times(1)).doCopy(request);
    }

    @Test
    void testDoGetMetadata() {
        mockBlobStore.getMetadata("object-1", "version-1");
        verify(validator, times(1)).validateKey(any());
        verify(mockBlobStore, times(1)).doGetMetadata(eq("object-1"), eq("version-1"));
    }

    @Test
    void testDoList() {
        ListBlobsRequest request = new ListBlobsRequest.Builder().build();
        Consumer<ListBlobsBatch> consumer = batch -> {};
        mockBlobStore.list(request, consumer);
        verify(mockBlobStore, times(1)).doList(request, consumer);
    }

    @Test
    void testDoInitiateMultipartUpload() {
        MultipartUploadRequest request = new MultipartUploadRequest.Builder().withKey("object-1").build();
        mockBlobStore.initiateMultipartUpload(request);
        verify(mockBlobStore, times(1)).doInitiateMultipartUpload(request);
    }

    @Test
    void testDoUploadMultipartPart() {
        MultipartUpload multipartUpload = new MultipartUpload("some-bucket", "object-1", "mpu-id");
        MultipartPart multipartPart = new MultipartPart(1, null, 0);
        mockBlobStore.uploadMultipartPart(multipartUpload, multipartPart);
        verify(mockBlobStore, times(1)).doUploadMultipartPart(multipartUpload, multipartPart);
        verify(validator, times(1)).requireEqualsIgnoreCase(any(), any(), any());
    }

    @Test
    void testDoCompleteMultipartUpload() {
        MultipartUpload multipartUpload = new MultipartUpload("some-bucket", "object-1", "mpu-id");
        List<UploadPartResponse> listOfParts = List.of(new UploadPartResponse(1, "etag", 0));
        mockBlobStore.completeMultipartUpload(multipartUpload, listOfParts);
        verify(mockBlobStore, times(1)).doCompleteMultipartUpload(multipartUpload, listOfParts);
        verify(validator, times(1)).requireEqualsIgnoreCase(any(), any(), any());
    }

    @Test
    void testDoListMultipartUpload() {
        MultipartUpload multipartUpload = new MultipartUpload("some-bucket", "object-1", "mpu-id");
        mockBlobStore.listMultipartUpload(multipartUpload);
        verify(mockBlobStore, times(1)).doListMultipartUpload(multipartUpload);
        verify(validator, times(1)).requireEqualsIgnoreCase(any(), any(), any());
    }

    @Test
    void testDoAbortMultipartUpload() {
        MultipartUpload multipartUpload = new MultipartUpload("some-bucket", "object-1", "mpu-id");
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
}
