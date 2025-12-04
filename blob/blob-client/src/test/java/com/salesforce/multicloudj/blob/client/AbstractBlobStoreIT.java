package com.salesforce.multicloudj.blob.client;

import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
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
import com.salesforce.multicloudj.blob.driver.PresignedOperation;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractBlobStoreIT {
    private static final Logger logger = LoggerFactory.getLogger(AbstractBlobStoreIT.class);

    // Define the Harness interface
    public interface Harness extends AutoCloseable {

        // Method to create a blob driver
        AbstractBlobStore createBlobStore(boolean useValidBucket, boolean useValidCredentials, boolean useVersionedBucket);

        // provide the BlobClient endpoint in provider
        String getEndpoint();

        // provide the provider ID
        String getProviderId();

        // Returns the header to use for this metadata header
        String getMetadataHeader(String key);

        // Returns the header to use for tagging for this substrate
        String getTaggingHeader();

        // Wiremock server need the https port, if
        // we make it constant at abstract class, we won't be able
        // to run tests in parallel. Each provider can provide the
        // randomly selected port number.
        int getPort();

        // Returns the KMS key ID for encryption tests (provider-specific)
        String getKmsKeyId();
    }

    protected abstract Harness createHarness();

    private Harness harness;

    private static final String GCP_PROVIDER_ID = "gcp";

    /**
     * Initializes the WireMock server before all tests.
     */
    @BeforeAll
    public void initializeWireMockServer() {
        harness = createHarness();
        TestsUtil.startWireMockServer("src/test/resources", harness.getPort());
    }

    /**
     * Shuts down the WireMock server after all tests.
     */
    @AfterAll
    public void shutdownWireMockServer() throws Exception {
        TestsUtil.stopWireMockServer();
        harness.close();
    }

    /**
     * Initialize the harness and
     */
    @BeforeEach
    public void setupTestEnvironment() {
        TestsUtil.startWireMockRecording(harness.getEndpoint());
    }

    /**
     * Cleans up the test environment after each test.
     */
    @AfterEach
    public void cleanupTestEnvironment() {
        TestsUtil.stopWireMockRecording();
    }

    @Test
    public void testNonexistentBucket() {

        // Create the blobstore driver for the bucket that doesn't exist
        AbstractBlobStore blobStore = harness.createBlobStore(false, true, false);
        BucketClient bucketClient = new BucketClient(blobStore);

        // And run the tests given the non-existent bucket
        runOperationsThatShouldFail("testNonexistentBucket", bucketClient);
        if (!GCP_PROVIDER_ID.equals(harness.getProviderId())) {
        runOperationsThatShouldNotFail("testNonexistentBucket", bucketClient);
    }
    }

    @Test
    public void testInvalidCredentials() {
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        // Create the blobstore driver for a bucket that exists, but use invalid credentialsOverrider
        AbstractBlobStore blobStore = harness.createBlobStore(true, false, false);
        BucketClient bucketClient = new BucketClient(blobStore);

        // And run the tests given the invalid credentialsOverrider
        runOperationsThatShouldFail("testInvalidCredentials", bucketClient);
        if (!GCP_PROVIDER_ID.equals(harness.getProviderId())) {
        runOperationsThatShouldNotFail("testInvalidCredentials", bucketClient);
    }
    }

    private void runOperationsThatShouldFail(String testName, BucketClient bucketClient) {

        // Now try various operations to ensure they all fail
        String key = "conformance-tests/blob-for-failing/" + testName;
        boolean writeFailed = false;
        String blobData = "This is test data";
        byte[] utf8BlobBytes = blobData.getBytes(StandardCharsets.UTF_8);

        // Read operation
        boolean readFailed = false;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            bucketClient.download(new DownloadRequest.Builder().withKey(key).build(), outputStream);
        } catch (Throwable t) {
            readFailed = true;
        }
        Assertions.assertTrue(readFailed, testName + ": The download operation did not fail");

        // Write operation
        try (InputStream inputStream = new ByteArrayInputStream(utf8BlobBytes)) {
            UploadRequest request = new UploadRequest.Builder()
                    .withKey(key)
                    .withContentLength(utf8BlobBytes.length)
                    .build();
            bucketClient.upload(request, inputStream);
        } catch (Throwable t) {
            writeFailed = true;
        }
        Assertions.assertTrue(writeFailed, testName + ": The upload operation did not fail");

        // Delete operation
        boolean deleteFailed = false;
        try {
            bucketClient.delete(key, null);
        } catch (Throwable t) {
            deleteFailed = true;
        }
        Assertions.assertTrue(deleteFailed, testName + ": The delete operation did not fail");

        // Bulk delete operation
        boolean bulkDeleteFailed = false;
        try {
            bucketClient.delete(List.of(new BlobIdentifier(key, null)));
        } catch (Throwable t) {
            bulkDeleteFailed = true;
        }
        Assertions.assertTrue(bulkDeleteFailed, testName + ": The bulk delete operation did not fail");

        // List operation
        boolean listFailed = false;
        try {
            ListBlobsRequest request = new ListBlobsRequest.Builder()
                    .withPrefix("prefix")
                    .build();
            bucketClient.list(request);
        } catch (Throwable t) {
            listFailed = true;
        }
        Assertions.assertTrue(listFailed, testName + ": The list operation did not fail");

        // Metadata operation
        boolean metadataFailed = false;
        try {
            bucketClient.getMetadata(key, null);
        } catch (Throwable t) {
            metadataFailed = true;
        }
        Assertions.assertTrue(metadataFailed, testName + ": The metadata operation did not fail");

        // Multipart upload operations
        boolean multipartUploadFailed = false;
        try {
            MultipartUploadRequest request = new MultipartUploadRequest.Builder().withKey(key + "multipart1").build();
            bucketClient.initiateMultipartUpload(request);
        } catch (Throwable t) {
            multipartUploadFailed = true;
        }
        Assertions.assertTrue(multipartUploadFailed, testName + ": The initiateMultipartUpload operation did not fail");

        multipartUploadFailed = false;
        try {
            MultipartUpload mpu = MultipartUpload.builder()
                    .bucket(bucketClient.getBucket())
                    .key(key + "multipart2")
                    .id("multipart2")
                    .build();
            MultipartPart multipartPart = new MultipartPart(1, utf8BlobBytes);
            bucketClient.uploadMultipartPart(mpu, multipartPart);
        } catch (Throwable t) {
            multipartUploadFailed = true;
        }
        Assertions.assertTrue(multipartUploadFailed, testName + ": The uploadMultipartPart operation did not fail");

        multipartUploadFailed = false;
        try {
            MultipartUpload request = MultipartUpload.builder()
                    .bucket(bucketClient.getBucket())
                    .key(key + "multipart3")
                    .id("multipart3")
                    .build();
            List<UploadPartResponse> listOfParts = List.of(new UploadPartResponse(1, "etag", utf8BlobBytes.length));
            bucketClient.completeMultipartUpload(request, listOfParts);
        } catch (Throwable t) {
            multipartUploadFailed = true;
        }
        Assertions.assertTrue(multipartUploadFailed, testName + ": The completeMultipartUpload operation did not fail");

        multipartUploadFailed = false;
        try {
            MultipartUpload request = MultipartUpload.builder()
                    .bucket(bucketClient.getBucket())
                    .key(key + "multipart4")
                    .id("multipart4")
                    .build();
            bucketClient.listMultipartUpload(request);
        } catch (Throwable t) {
            multipartUploadFailed = true;
        }
        Assertions.assertTrue(multipartUploadFailed, testName + ": The listMultipartUpload operation did not fail");

        multipartUploadFailed = false;
        try {
            MultipartUpload request = MultipartUpload.builder()
                    .bucket(bucketClient.getBucket())
                    .key(key + "multipart5")
                    .id("multipart5")
                    .build();
            bucketClient.abortMultipartUpload(request);
        } catch (Throwable t) {
            multipartUploadFailed = true;
        }
        Assertions.assertTrue(multipartUploadFailed, testName + ": The abortMultipartUpload operation did not fail");

        boolean taggingRequestFailed = false;
        try {
            bucketClient.getTags(key);
        } catch (Throwable t) {
            taggingRequestFailed = true;
        }
        Assertions.assertTrue(taggingRequestFailed, testName + ": The getTags operation did not fail");

        taggingRequestFailed = false;
        try {
            bucketClient.setTags(key, Map.of("tagfail1","value1", "tagfail2","value2"));
        } catch (Throwable t) {
            taggingRequestFailed = true;
        }
        Assertions.assertTrue(taggingRequestFailed, testName + ": The setTags operation did not fail");
    }

    private void runOperationsThatShouldNotFail(String testName, BucketClient bucketClient) {
        // Now try various operations to ensure they do not fail
        // These are operations are client-side, and thus do not validate bucket existence or credential validity
        String key = "conformance-tests/blob-for-not-failing/" + testName;

        PresignedUrlRequest presignedUploadRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.UPLOAD)
                .key(key)
                .duration(Duration.ofHours(24))
                .build();
        bucketClient.generatePresignedUrl(presignedUploadRequest);

        PresignedUrlRequest presignedDownloadRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.DOWNLOAD)
                .key(key)
                .duration(Duration.ofHours(24))
                .build();
        bucketClient.generatePresignedUrl(presignedDownloadRequest);
    }

    enum UploadType {
        InputStream,
        ByteArray,
        File,
        Path;
    }

    @Test
    public void testUpload_nullKey() {
        runUploadTests("testUpload_nullKey", null, "This is test data".getBytes(), true);
    }

    @Test
    public void testUpload_emptyKey() {
        runUploadTests("testUpload_emptyKey", "", "This is test data".getBytes(), true);
    }

    @Test
    public void testUpload_emptyContent() {
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        runUploadTests("testUpload_emptyContent",  "conformance-tests/upload/emptyContent", new byte[]{}, false);
    }

    @Test
    public void testUpload_happyPath() {
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        runUploadTests("testUpload_happyPath", "conformance-tests/upload/happyPath", "This is test data".getBytes(), false);
    }

    private void runUploadTests(String testName, String key, byte[] content, boolean wantError) {
        runUploadTest(testName, false, UploadType.InputStream, key, content, wantError);
        runUploadTest(testName, false, UploadType.ByteArray, key, content, wantError);
        runUploadTest(testName, false, UploadType.File, key, content, wantError);
        runUploadTest(testName, false, UploadType.Path, key, content, wantError);
        runUploadTest(testName, true, UploadType.InputStream, key, content, wantError);
        runUploadTest(testName, true, UploadType.ByteArray, key, content, wantError);
        runUploadTest(testName, true, UploadType.File, key, content, wantError);
        runUploadTest(testName, true, UploadType.Path, key, content, wantError);
    }

    private void runUploadTest(String testName, boolean useVersionedBucket, UploadType uploadType, String key, byte[] content, boolean wantError) {

        String suffix = "_" + (useVersionedBucket ? "versioned_" : "" ) + uploadType;
        testName += suffix;
        if(!StringUtils.isEmpty(key)){
            key += suffix;
        }
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, useVersionedBucket);
        BucketClient bucketClient = new BucketClient(blobStore);
        UploadRequest request = new UploadRequest.Builder()
                .withKey(key)
                .withContentLength(content.length)
                .build();
        try {
            boolean writeFailed = false;
            UploadResponse response = null;

            // Do the upload
            try {
                switch (uploadType) {
                    case InputStream:
                        try (InputStream inputStream = new ByteArrayInputStream(content)) {
                            response = bucketClient.upload(request, inputStream);
                        }
                        break;
                    case ByteArray:
                        response = bucketClient.upload(request, content);
                        break;
                    case File:
                        Path path = Files.createTempFile("tempFile", ".txt");
                        Files.write(path, content);
                        response = bucketClient.upload(request, path.toFile());
                        break;
                    case Path:
                        Path path2 = Files.createTempFile("tempFile", ".txt");
                        Files.write(path2, content);
                        response = bucketClient.upload(request, path2);
                        break;
                }
            }
            catch (Throwable t) {
                Assertions.assertTrue(wantError, testName + ": Unexpected error " + t.getMessage());
                return;
            }
            Assertions.assertNotNull(response, testName + ": No response was returned!");
            Assertions.assertNotNull(response.getETag(), testName + ": No eTag was returned!");
            Assertions.assertEquals(wantError, writeFailed, testName + ": Did not receive the expected error response");

            // Read the blob out so we can verify information
            boolean readFailed = false;
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try (outputStream) {
                bucketClient.download(new DownloadRequest.Builder().withKey(key).build(), outputStream);
            } catch (Throwable t) {
                readFailed = true;
            }
            if (!readFailed) {
                Assertions.assertEquals(content.length, outputStream.toByteArray().length, testName + ": Content-Length did not match");
                Assertions.assertArrayEquals(content, outputStream.toByteArray(), testName + ": Bytes arrays did not match");
            }
        } finally {
            // Now delete the blob that was created
            safeDeleteBlobs(bucketClient, key);
        }
    }

    enum DownloadType {
        InputStream,
        ByteArray,
        File,
        Path;
    }

    @Test
    public void testDownload_nullKey() throws IOException {
        runDownloadTests("read from null key fails",
                "conformance-tests/download_null_key",
                null,
                true);
    }

    @Test
    public void testDownload_emptyKey() throws IOException {
        runDownloadTests("read from empty key fails",
                "conformance-tests/download_empty_key",
                "",
                true);
    }

    @Test
    public void testDownload_happy() throws IOException {
        runDownloadTests("happy path read",
                "conformance-tests/download_happy",
                "conformance-tests/download_happy",
                false);
    }

    @Test
    public void testVersionedDownload_happy() throws IOException {
        runVersionedDownloadTests("happy versioned download",
                "conformance-tests/versioned_download_happy",
                "conformance-tests/versioned_download_happy",
                true,
                true,
                false);
    }

    @Test
    public void testVersionedDownload_noVersionId() throws IOException {
        runVersionedDownloadTests("no versionId download",
                "conformance-tests/versioned_download_no_versionId",
                "conformance-tests/versioned_download_no_versionId",
                false,
                false,
                false);
    }

    @Test
    public void testVersionedDownload_badVersionId() throws IOException {
        runVersionedDownloadTests("bad versionId download",
                "conformance-tests/versioned_download_bad_versionId",
                "conformance-tests/versioned_download_bad_versionId",
                true,
                false,
                true);
    }

    // Helper function for executing tests against both versions and unversioned buckets
    private void runDownloadTests(String testName,
                                                   String uploadKey,
                                                   String downloadKey,
                                                   boolean wantError) throws IOException {
        runDownloadTest(testName, uploadKey+"_unversioned", downloadKey+"_unversioned", false, DownloadType.InputStream, true, true, wantError);
        runDownloadTest(testName, uploadKey+"_unversioned", downloadKey+"_unversioned", false, DownloadType.ByteArray, true, true, wantError);
        runDownloadTest(testName, uploadKey+"_unversioned", downloadKey+"_unversioned", false, DownloadType.File, true, true, wantError);
        runDownloadTest(testName, uploadKey+"_unversioned", downloadKey+"_unversioned", false, DownloadType.Path, true, true, wantError);
        runVersionedDownloadTests(testName, uploadKey, downloadKey, true, true, wantError);
    }

    // Helper function for executing tests against both versioned buckets
    private void runVersionedDownloadTests(String testName,
                                           String uploadKey,
                                           String downloadKey,
                                           boolean downloadUsingVersionId,
                                           boolean useCorrectVersionId,
                                  boolean wantError) throws IOException {
        runDownloadTest(testName, uploadKey+"_versioned", downloadKey+"_versioned", true, DownloadType.InputStream, downloadUsingVersionId, useCorrectVersionId, wantError);
        runDownloadTest(testName, uploadKey+"_versioned", downloadKey+"_versioned", true, DownloadType.ByteArray, downloadUsingVersionId, useCorrectVersionId, wantError);
        runDownloadTest(testName, uploadKey+"_versioned", downloadKey+"_versioned", true, DownloadType.File, downloadUsingVersionId, useCorrectVersionId, wantError);
        runDownloadTest(testName, uploadKey+"_versioned", downloadKey+"_versioned", true, DownloadType.Path, downloadUsingVersionId, useCorrectVersionId, wantError);
    }

    private void runDownloadTest(String testName,
                                 String uploadKey,
                                 String downloadKey,
                                 boolean useVersionedBucket,
                                 DownloadType downloadType,
                                 boolean downloadUsingVersionId,
                                 boolean useCorrectVersionId,
                                 boolean wantError) throws IOException {
        // Test data
        String blobData = "This is test data";
        byte[] blobBytes = blobData.getBytes(StandardCharsets.UTF_8);

        // Create the BucketClient
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, useVersionedBucket);
        BucketClient bucketClient = new BucketClient(blobStore);

        // Upload a blob so we can read from it
        UploadResponse uploadResponse;
        try (InputStream inputStream = new ByteArrayInputStream(blobBytes)) {
            UploadRequest request = new UploadRequest.Builder()
                    .withKey(uploadKey)
                    .withContentLength(blobBytes.length)
                    .build();
            uploadResponse = bucketClient.upload(request, inputStream);

            // Verify the upload worked properly
            Assertions.assertNotNull(uploadResponse.getKey(), testName + ": key was missing");
            Assertions.assertNotNull(uploadResponse.getETag(), testName + ": etag was missing");
            if(useVersionedBucket) {
                Assertions.assertNotNull(uploadResponse.getVersionId(), testName + ": versionId was missing");
            }
        }

        // Run the test
        try {
            DownloadRequest.Builder requestBuilder = new DownloadRequest.Builder().withKey(downloadKey);
            if(downloadUsingVersionId) {
                requestBuilder.withVersionId(useCorrectVersionId ? uploadResponse.getVersionId() : "fakeVersionId");
            }
            DownloadRequest request = requestBuilder.build();
            DownloadResponse response;
            byte[] content;
            try {
                Pair<DownloadResponse, byte[]> result = readContent(bucketClient, request, downloadType);
                response = result.getLeft();
                content = result.getRight();
                Assertions.assertEquals(blobBytes.length, content.length, testName + ": Content-Length did not match");
                Assertions.assertArrayEquals(blobBytes, content, testName + ": Bytes arrays did not match");
            } catch (SubstrateSdkException e) {
                Assertions.assertTrue(wantError, testName + ": Did not expect error. " + e.getMessage());
                return;
            }
            Assertions.assertFalse(wantError);
            Assertions.assertEquals(downloadKey, response.getKey(), testName + ": key did not match");
            Assertions.assertEquals(downloadKey, response.getMetadata().getKey(), testName + ": metadata key did not match");
            Assertions.assertEquals(blobBytes.length, response.getMetadata().getObjectSize(), testName + ": objectSize did not match");
            Assertions.assertEquals(uploadResponse.getVersionId(), response.getMetadata().getVersionId(), testName + ": versionId did not match");
            Assertions.assertNotNull(response.getMetadata().getETag(), testName + ": etag was missing");
            Assertions.assertNotNull(response.getMetadata().getLastModified(), testName + ": lastModified was missing");
        } finally {
            // Delete our blob to clean up the test
            safeDeleteBlobs(bucketClient, uploadKey);
        }
    }

    @Test
    public void testRangedRead() throws IOException {
        String key = "conformance-tests/testRangedRead";
        runRangedReadDownloadTest(key+"_unversioned", false, DownloadType.InputStream);
        runRangedReadDownloadTest(key+"_unversioned", false, DownloadType.ByteArray);
        runRangedReadDownloadTest(key+"_unversioned", false, DownloadType.File);
        runRangedReadDownloadTest(key+"_unversioned", false, DownloadType.Path);
        runRangedReadDownloadTest(key+"_versioned", true, DownloadType.InputStream);
        runRangedReadDownloadTest(key+"_versioned", true, DownloadType.ByteArray);
        runRangedReadDownloadTest(key+"_versioned", true, DownloadType.File);
        runRangedReadDownloadTest(key+"_versioned", true, DownloadType.Path);
    }

    private void runRangedReadDownloadTest(String key, boolean useVersionedBucket, DownloadType downloadType) throws IOException {

        String blobData = "This is test data for the ranged read test file";
        byte[] blobBytes = blobData.getBytes();

        // Create the BucketClient
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, useVersionedBucket);
        BucketClient bucketClient = new BucketClient(blobStore);

        // Upload a blob so we can read from it
        UploadResponse uploadResponse;
        try (InputStream inputStream = new ByteArrayInputStream(blobBytes)) {
            UploadRequest request = new UploadRequest.Builder()
                    .withKey(key)
                    .withContentLength(blobBytes.length)
                    .build();
            uploadResponse = bucketClient.upload(request, inputStream);
        }

        try {
            DownloadRequest.Builder requestBuilder = new DownloadRequest.Builder()
                    .withKey(key)
                    .withVersionId(uploadResponse.getVersionId());

            // Try downloading the first 10 bytes
            Pair<DownloadResponse, byte[]> result = readContent(bucketClient, requestBuilder.withRange(0L, 9L).build(), downloadType);
            byte[] content = result.getRight();
            Assertions.assertEquals(10, content.length);
            Assertions.assertArrayEquals(Arrays.copyOfRange(blobBytes, 0, 10), content);

            // Try downloading a middle 20 bytes
            result = readContent(bucketClient, requestBuilder.withRange(10L, 29L).build(), downloadType);
            content = result.getRight();
            Assertions.assertEquals(20, content.length);
            Assertions.assertArrayEquals(Arrays.copyOfRange(blobBytes, 10, 30), content);

            // Try downloading from byte 10 onward
            result = readContent(bucketClient, requestBuilder.withRange(10L, null).build(), downloadType);
            content = result.getRight();
            Assertions.assertEquals(blobBytes.length-10, content.length);
            Assertions.assertArrayEquals(Arrays.copyOfRange(blobBytes, 10, blobBytes.length), content);

            // Try downloading the last 10 bytes
            result = readContent(bucketClient, requestBuilder.withRange(null, 10L).build(), downloadType);
            content = result.getRight();
            Assertions.assertEquals(10, content.length);
            Assertions.assertArrayEquals(Arrays.copyOfRange(blobBytes, blobBytes.length-10, blobBytes.length), content);

            // Try downloading a single byte
            result = readContent(bucketClient, requestBuilder.withRange(10L, 10L).build(), downloadType);
            content = result.getRight();
            Assertions.assertEquals(1, content.length);
            Assertions.assertArrayEquals(Arrays.copyOfRange(blobBytes, 10, 11), content);

            // Ask for bytes out of range (0 to length+10). This exceeds the total size, but still works
            result = readContent(bucketClient, requestBuilder.withRange(0L, blobBytes.length+10L).build(), downloadType);
            content = result.getRight();
            Assertions.assertEquals(blobBytes.length, content.length);
            Assertions.assertArrayEquals(blobBytes, content);

            // Ask for the last length+10 bytes. This exceeds the total size, but still works
            result = readContent(bucketClient, requestBuilder.withRange(null, blobBytes.length+10L).build(), downloadType);
            content = result.getRight();
            Assertions.assertEquals(blobBytes.length, content.length);
            Assertions.assertArrayEquals(blobBytes, content);

            // Ask for everything but the first length+10 bytes (this should fail)
            boolean hasError = false;
            try{
                readContent(bucketClient, requestBuilder.withRange(blobBytes.length+10L, null).build(), downloadType);
            }
            catch (SubstrateSdkException e) {
                hasError = true;
            }
            Assertions.assertTrue(hasError);
        } finally {
            // Delete our blob to clean up the test
            safeDeleteBlobs(bucketClient, key);
        }
    }

    /**
     * Helper function for downloading content using the overloaded download() types
     */
    private Pair<DownloadResponse, byte[]> readContent(BucketClient bucketClient, DownloadRequest request, DownloadType downloadType) throws IOException {
        byte[] content = null;
        DownloadResponse response = null;
        switch (downloadType) {
            case InputStream:
                response = bucketClient.download(request);
                if (response.getInputStream() != null) {
                    try (InputStream inputStream = response.getInputStream();
                         ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                        byte[] buffer = new byte[1024];
                        int bytesRead;
                        while ((bytesRead = inputStream.read(buffer)) != -1) {
                            outputStream.write(buffer, 0, bytesRead);
                        }
                        content = outputStream.toByteArray();
                    }
                }
                break;
            case ByteArray:
                ByteArray byteArray = new ByteArray();
                response = bucketClient.download(request, byteArray);
                content = byteArray.getBytes();
                break;
            case File:
                Path path = Files.createTempFile("tempFile", ".txt");
                File file = path.toFile();
                file.delete();
                response = bucketClient.download(request, file);
                content = Files.readAllBytes(path);
                break;
            case Path:
                Path path2 = Files.createTempFile("tempPath", ".txt");
                path2.toFile().delete();
                response = bucketClient.download(request, path2);
                content = Files.readAllBytes(path2);
                break;
        }
        return new ImmutablePair<>(response, content);
    }

    // Note: This tests delete for non-versioned buckets
    @Test
    public void testDelete() throws IOException {

        // Create the BucketClient
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, false);
        BucketClient bucketClient = new BucketClient(blobStore);

        // Try deleting a blob that doesn't exist
        try {
            bucketClient.delete("blob-that-doesnt-exist", null);
        } catch (Throwable t) {
            Assertions.fail("testDelete: Should not fail when deleting a non-existent blob", t);
        }

        // Upload a blob so we can delete it
        String key = "conformance-tests/blob-for-deleting";
        String blobData = "This is test data";
        byte[] blobBytes = blobData.getBytes(StandardCharsets.UTF_8);
        try (InputStream inputStream = new ByteArrayInputStream(blobBytes)) {
            UploadRequest request = new UploadRequest.Builder()
                    .withKey(key)
                    .withContentLength(blobBytes.length)
                    .build();
            bucketClient.upload(request, inputStream);
        }

        // Now delete that blob
        try {
            bucketClient.delete(key, null);
        } catch (Throwable t) {
            Assertions.fail("testDelete: Should not fail when deleting a blob");
        }

        // Try reading the blob (which shouldn't work because it's deleted)
        boolean readFailed = false;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            bucketClient.download(new DownloadRequest.Builder().withKey(key).build(), outputStream);
        } catch (Throwable t) {
            readFailed = true;
        }
        Assertions.assertTrue(readFailed, "testDelete: Should have failed when downloading a blob that's been deleted");

        // Subsequent deletes shouldn't fail either (this is equivalent to deleting a blob that doesn't exist)
        try {
            bucketClient.delete(key, null);
        } catch (Throwable t) {
            Assertions.fail("testDelete: Should not fail when deleting an already-deleted blob");
        }
    }

    // Note: This tests bulk delete for non-versioned buckets
    @Test
    public void testBulkDelete() throws IOException {
        class TestConfig {
            final String testName;
            final Collection<String> keysToCreate;
            final Collection<String> keysToDelete;
            final boolean wantError;

            public TestConfig(String testName, Collection<String> keysToCreate, Collection<String> keysToDelete, boolean wantError) {
                this.testName = testName;
                this.keysToCreate = keysToCreate;
                this.keysToDelete = keysToDelete;
                this.wantError = wantError;
            }
        }

        // Test data
        String keyPrefix = "conformance-tests/blob-for-bulk-delete_";
        Set<String> keysToDelete = new HashSet<>();

        // Create the BucketClient
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, false);
        BucketClient bucketClient = new BucketClient(blobStore);

        // Prepare the tests
        List<TestConfig> testConfigs = Arrays.asList(
                new TestConfig("empty collection", new ArrayList<>(), new ArrayList<>(), true),
                new TestConfig("delete non-existing blob", new ArrayList<>(), List.of(keyPrefix + "nonexisting"), false),
                new TestConfig("happy path", List.of(keyPrefix + "happy1", keyPrefix + "happy2", keyPrefix + "happy3"), List.of(keyPrefix + "happy1", keyPrefix + "happy2", keyPrefix + "happy3"), false),
                new TestConfig("happy path with nonexisting", List.of(keyPrefix + "happy4", keyPrefix + "happy5", keyPrefix + "happy6"), List.of(keyPrefix + "happy4", keyPrefix + "happy5", keyPrefix + "happy6", keyPrefix + "nonexisting2"), false),
                new TestConfig("duplicate deletion", List.of(keyPrefix + "happy7", keyPrefix + "happy8"), List.of(keyPrefix + "happy7", keyPrefix + "happy7", keyPrefix + "happy8"), false));

        // Now run the tests
        try {
            for (TestConfig testConfig : testConfigs) {

                keysToDelete.addAll(testConfig.keysToCreate);   // Clean up at the end of the test run regardless of outcome

                // Upload the desired test blobs
                for (String keyToCreate : testConfig.keysToCreate) {
                    byte[] blobBytes = ("Bulk delete blob for " + keyToCreate).getBytes(StandardCharsets.UTF_8);
                    try (InputStream inputStream = new ByteArrayInputStream(blobBytes)) {
                        UploadRequest request = new UploadRequest.Builder()
                                .withKey(keyToCreate)
                                .withContentLength(blobBytes.length)
                                .build();
                        bucketClient.upload(request, inputStream);
                    } catch (Throwable t) {
                        Assertions.fail(testConfig.testName + ": The test wasn't supposed to fail while uploading test data", t);
                    }
                }

                // Now delete the requested blobs
                List<BlobIdentifier> objectsToDelete = testConfig.keysToDelete.stream()
                        .map(key -> new BlobIdentifier(key, null))
                        .collect(Collectors.toList());
                boolean failed = false;
                try {
                    bucketClient.delete(objectsToDelete);
                } catch (Throwable t) {

                    // If we expected an error, validate that here
                    failed = true;
                    Assertions.assertTrue(testConfig.wantError, testConfig.testName + ": Unexpected error " + t.getMessage());
                }

                // Verify we got the expected error state
                Assertions.assertEquals(testConfig.wantError, failed, testConfig.testName + ": Did not generate expected error state");
                if (failed) {
                    continue;
                }

                // Validate the blobs are actually deleted
                for (String keyToDelete : testConfig.keysToDelete) {

                    // Try reading the blob (which shouldn't work because it's deleted)
                    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                        bucketClient.download(new DownloadRequest.Builder().withKey(keyToDelete).build(), outputStream);
                    } catch (Throwable t) {
                        continue;
                    }
                    Assertions.fail(testConfig.testName + ": Should have failed when downloading a blob that's been deleted");
                }
            }
        } finally {
            // Delete our blob to clean up the test
            safeDeleteBlobs(bucketClient, keysToDelete.toArray(new String[0]));
        }
    }

    @Test
    public void testVersionedDelete_fileDoesNotExist() throws IOException {

        // Create the BucketClient
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, true);
        BucketClient bucketClient = new BucketClient(blobStore);

        // Try deleting a blob that doesn't exist
        try {
            bucketClient.delete("versioned-blob-that-doesnt-exist", null);
        } catch (Throwable t) {
            Assertions.fail("Should not fail when deleting a non-existent blob", t);
        }
    }

    @Test
    public void testVersionedDelete() throws IOException {
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        // Create the BucketClient
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, true);
        BucketClient bucketClient = new BucketClient(blobStore);
        String key = "conformance-tests/delete/happyPath";

        // Upload a blob twice so we can delete versions of it
        try {
            byte[] blobBytes1 = "This is test data".getBytes(StandardCharsets.UTF_8);
            byte[] blobBytes2= "This is the second test data".getBytes(StandardCharsets.UTF_8);
            UploadResponse uploadResponse1;
            UploadResponse uploadResponse2;
            try (InputStream inputStream1 = new ByteArrayInputStream(blobBytes1);
                 InputStream inputStream2 = new ByteArrayInputStream(blobBytes2)) {
                UploadRequest request1 = new UploadRequest.Builder()
                        .withKey(key)
                        .withContentLength(blobBytes1.length)
                        .build();
                uploadResponse1 = bucketClient.upload(request1, inputStream1);
                UploadRequest request2 = new UploadRequest.Builder()
                        .withKey(key)
                        .withContentLength(blobBytes2.length)
                        .build();
                uploadResponse2 = bucketClient.upload(request2, inputStream2);
            }

            // Delete the first version of the blob
            bucketClient.delete(key, uploadResponse1.getVersionId());

            // Try reading the first blob (which shouldn't work because it's deleted)
            boolean readFailed = false;
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                bucketClient.download(new DownloadRequest.Builder().withKey(key).withVersionId(uploadResponse1.getVersionId()).build(), outputStream);
            } catch (Throwable t) {
                readFailed = true;
            }
            Assertions.assertTrue(readFailed, "Should have failed when downloading a blob that's been deleted");

            // Download the second blob (the most recent version) to verify it's still downloadable
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            DownloadResponse downloadResponse = bucketClient.download(new DownloadRequest.Builder().withKey(key).withVersionId(uploadResponse2.getVersionId()).build(), outputStream);
            Assertions.assertEquals(key, downloadResponse.getKey(), "Key should have matched");
            Assertions.assertEquals(uploadResponse2.getVersionId(), downloadResponse.getMetadata().getVersionId(), "VersionId should have matched");
            Assertions.assertEquals(blobBytes2.length, downloadResponse.getMetadata().getObjectSize(), "Object size should have matched");
            Assertions.assertEquals(blobBytes2.length, outputStream.size(), "Object size should have matched");

            // Delete the second version of the blob now. This will leave no versions left
            bucketClient.delete(key, uploadResponse2.getVersionId());

            // Subsequent deletes shouldn't fail either (this is equivalent to deleting a blob that doesn't exist)
            bucketClient.delete(key, uploadResponse1.getVersionId());
            bucketClient.delete(key, uploadResponse2.getVersionId());
        }
        finally {
            // Delete our blobs to clean up the test
            safeDeleteBlobs(bucketClient, key);
        }
    }

    @Test
    public void testBulkVersionedDelete_emptyCollection() throws IOException {
        runBulkVersionedDeleteTest("testBulkVersionedDelete_emptyCollection",
                new ArrayList<>(),
                new ArrayList<>(),
                true);
    }

    @Test
    public void testBulkVersionedDelete_nonExistingBlob() throws IOException {
        runBulkVersionedDeleteTest("testBulkVersionedDelete_nonExistingBlob",
                new ArrayList<>(),
                List.of("nonexisting"),
                false);
    }

    @Test
    public void testBulkVersionedDelete_happyPath() throws IOException {
        runBulkVersionedDeleteTest("testBulkVersionedDelete_happyPath",
                List.of("happy1", "happy2", "happy3"),
                List.of("happy1", "happy2", "happy3"),
                false);
    }

    @Test
    public void testBulkVersionedDelete_happyPathWithNonExisting() throws IOException {
        runBulkVersionedDeleteTest("testBulkVersionedDelete_happyPathWithNonExisting",
                List.of("happy4", "happy5", "happy6"),
                List.of("happy4", "happy5", "happy6", "nonexisting2"),
                false);
    }

    @Test
    public void testBulkVersionedDelete_duplicateDeletion() throws IOException {
        runBulkVersionedDeleteTest("testBulkVersionedDelete_duplicateDeletion",
                List.of("happy7", "happy8"),
                List.of("happy7", "happy7", "happy8"),
                false);
    }

    // Note: This tests bulk delete for versioned buckets
    public void runBulkVersionedDeleteTest(String testName, Collection<String> keysToCreate, Collection<String> keysToDelete, boolean wantError) throws IOException {

        // Create the BucketClient
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, true);
        BucketClient bucketClient = new BucketClient(blobStore);
        String keyPrefix = "conformance-tests/bulkDeleteByVersion/";

        keysToCreate = keysToCreate.stream().map(key -> keyPrefix + key).collect(Collectors.toList());
        keysToDelete = keysToDelete.stream().map(key -> keyPrefix + key).collect(Collectors.toList());
        Set<String> keysToCleanup = new HashSet<>(keysToCreate);   // Clean up at the end of the test run regardless of outcome

        try {
            // Upload the desired test blobs
            Map<String, String> uploadedKeyVersionMap = new HashMap<>();
            for (String keyToCreate : keysToCreate) {
                byte[] blobBytes = ("Bulk versioned delete blob for " + keyToCreate).getBytes(StandardCharsets.UTF_8);
                try (InputStream inputStream = new ByteArrayInputStream(blobBytes)) {
                    UploadRequest request = new UploadRequest.Builder()
                            .withKey(keyToCreate)
                            .withContentLength(blobBytes.length)
                            .build();
                    UploadResponse uploadResponse = bucketClient.upload(request, inputStream);
                    uploadedKeyVersionMap.put(keyToCreate, uploadResponse.getVersionId());
                } catch (Throwable t) {
                    Assertions.fail(testName + ": The test wasn't supposed to fail while uploading test data", t);
                }
            }
            // Compile the key/version list of files we uploaded
            List<BlobIdentifier> objects = keysToDelete.stream()
                    .map(keyToDelete -> new BlobIdentifier(keyToDelete, uploadedKeyVersionMap.getOrDefault(keyToDelete, null)))
                    .collect(Collectors.toList());

            // Now delete the requested blobs
            boolean failed = false;
            try {
                bucketClient.delete(objects);
            } catch (Throwable t) {
                failed = true;
                Assertions.assertTrue(wantError, testName + ": Unexpected error " + t.getMessage());
            }

            // Verify we got the expected error state
            Assertions.assertEquals(wantError, failed, testName + ": Did not generate expected error state");
            if (failed) {
                return;
            }

            // Validate the blobs are actually deleted
            for (String keyToDelete : keysToDelete) {

                // Try reading the blob (which shouldn't work because it's deleted)
                try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                    bucketClient.download(new DownloadRequest.Builder().withKey(keyToDelete).build(), outputStream);
                } catch (Throwable t) {
                    continue;
                }
                Assertions.fail(testName + ": Should have failed when downloading a blob that's been deleted");
            }
        } finally {
            // Delete our blob to clean up the test
            safeDeleteBlobs(bucketClient, keysToCleanup.toArray(new String[0]));
        }
    }

    @Test
    public void testCopy() throws IOException {

        String key = "conformance-tests/blob-for-copying";
        String destKey = "conformance-tests/copied-blob";
        String blobToClobber = "conformance-tests/clobbered-blob";

        // Create the BucketClient
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, false);
        BucketClient bucketClient = new BucketClient(blobStore);
        try {

            // Try copying from a non-existent source
            boolean copyFailed = false;
            try {
                CopyRequest copyRequest = CopyRequest.builder()
                        .srcKey("blob-that-doesnt-exist")
                        .destBucket(bucketClient.blobStore.getBucket())
                        .destKey(destKey)
                        .build();
                bucketClient.copy(copyRequest);
            } catch (Throwable t) {
                copyFailed = true;
            }
            Assertions.assertTrue(copyFailed, "testCopy: Should have failed when copying a non-existent blob");

            // Upload a blob so we can have something to copy
            byte[] blobBytes = "Please copy this data!".getBytes(StandardCharsets.UTF_8);
            try (InputStream inputStream = new ByteArrayInputStream(blobBytes)) {
                UploadRequest request = new UploadRequest.Builder()
                        .withKey(key)
                        .withContentLength(blobBytes.length)
                        .withMetadata(Map.of("key1", "value1", "key2", "value2"))
                        .build();
                bucketClient.upload(request, inputStream);
            }

            // Try copying to a non-existent bucket
            copyFailed = false;
            try {
                CopyRequest copyRequest = CopyRequest.builder()
                        .srcKey(key)
                        .destBucket("bucketThatDoesntExist")
                        .destKey(destKey)
                        .build();
                bucketClient.copy(copyRequest);
            } catch (Throwable t) {
                copyFailed = true;
            }
            Assertions.assertTrue(copyFailed, "testCopy: Should have failed when copying to a non-existent bucket");

            // Do a happy-path copy
            try {
                CopyRequest copyRequest = CopyRequest.builder()
                        .srcKey(key)
                        .destBucket(bucketClient.blobStore.getBucket())
                        .destKey(destKey)
                        .build();
                CopyResponse copyResponse = bucketClient.copy(copyRequest);
                Assertions.assertEquals(destKey, copyResponse.getKey());
                Assertions.assertNotNull(copyResponse.getLastModified());
                Assertions.assertNotNull(copyResponse.getETag());
            } catch (Throwable t) {
                Assertions.fail("testCopy: Failed to copy blob " + t.getMessage());
            }
            verifyBlobCopy(bucketClient, key, null, destKey);

            // Now test using the copy operation to overwrite an existing blob
            // Create a blob that we'll overwrite
            byte[] clobberBlobBytes = "Clobber this blob!".getBytes(StandardCharsets.UTF_8);
            try (InputStream inputStream = new ByteArrayInputStream(clobberBlobBytes)) {
                UploadRequest request = new UploadRequest.Builder()
                        .withKey(blobToClobber)
                        .withContentLength(clobberBlobBytes.length)
                        .withMetadata(Map.of("key3", "value3", "key4", "value4"))
                        .build();
                bucketClient.upload(request, inputStream);
            }

            // Copy to overwrite that blob
            try {
                CopyRequest copyRequest = CopyRequest.builder()
                        .srcKey(key)
                        .destBucket(bucketClient.blobStore.getBucket())
                        .destKey(blobToClobber)
                        .build();
                CopyResponse copyResponse = bucketClient.copy(copyRequest);
                Assertions.assertEquals(blobToClobber, copyResponse.getKey());
                Assertions.assertNotNull(copyResponse.getLastModified());
                Assertions.assertNotNull(copyResponse.getETag());
            } catch (Throwable t) {
                Assertions.fail("testCopy: Failed to copy blob " + t.getMessage());
            }
            verifyBlobCopy(bucketClient, key, null, blobToClobber);
        } finally {
            // Delete our blobs to clean up the test
            safeDeleteBlobs(bucketClient, key, destKey, blobToClobber);
        }
    }

    @Test
    public void testVersionedCopy() throws IOException {

        String key = "conformance-tests/versionedCopy/blob";
        String destKeyV1 = "conformance-tests/versionedCopy/copied-blob-v1";
        String destKeyV2 = "conformance-tests/versionedCopy/copied-blob-v2";
        String destKeyLatest = "conformance-tests/versionedCopy/copied-blob-latest";

        // Create the BucketClient
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, true);
        BucketClient bucketClient = new BucketClient(blobStore);
        try {
            // Upload a blob so we can have something to copy
            byte[] v1BlobBytes = "Version 1 of blob".getBytes(StandardCharsets.UTF_8);
            UploadResponse uploadResponseV1;
            try (InputStream inputStream = new ByteArrayInputStream(v1BlobBytes)) {
                UploadRequest request = new UploadRequest.Builder()
                        .withKey(key)
                        .withContentLength(v1BlobBytes.length)
                        .build();
                uploadResponseV1 = bucketClient.upload(request, inputStream);
            }

            // Upload a second version
            byte[] v2BlobBytes = "This is the second version of the blob".getBytes(StandardCharsets.UTF_8);
            UploadResponse uploadResponseV2;
            try (InputStream inputStream = new ByteArrayInputStream(v2BlobBytes)) {
                UploadRequest request = new UploadRequest.Builder()
                        .withKey(key)
                        .withContentLength(v2BlobBytes.length)
                        .build();
                uploadResponseV2 = bucketClient.upload(request, inputStream);
            }

            // Do happy-path copies of both, and verify
            CopyRequest copyRequestV1 = CopyRequest.builder()
                    .srcKey(key)
                    .srcVersionId(uploadResponseV1.getVersionId())
                    .destBucket(bucketClient.blobStore.getBucket())
                    .destKey(destKeyV1)
                    .build();
            CopyResponse copyResponse1 = bucketClient.copy(copyRequestV1);
            Assertions.assertEquals(destKeyV1, copyResponse1.getKey());
            Assertions.assertNotNull(copyResponse1.getVersionId());
            Assertions.assertNotNull(copyResponse1.getLastModified());
            Assertions.assertNotNull(copyResponse1.getETag());
            verifyBlobCopy(bucketClient, key, uploadResponseV1.getVersionId(), destKeyV1);

            // Try copying the second version
            CopyRequest copyRequestV2 = CopyRequest.builder()
                    .srcKey(key)
                    .srcVersionId(uploadResponseV2.getVersionId())
                    .destBucket(bucketClient.blobStore.getBucket())
                    .destKey(destKeyV2)
                    .build();
            CopyResponse copyResponse2 = bucketClient.copy(copyRequestV2);
            Assertions.assertEquals(destKeyV2, copyResponse2.getKey());
            Assertions.assertNotNull(copyResponse2.getVersionId());
            Assertions.assertNotNull(copyResponse2.getLastModified());
            Assertions.assertNotNull(copyResponse2.getETag());
            verifyBlobCopy(bucketClient, key, uploadResponseV2.getVersionId(), destKeyV2);

            // Try copying without specifying the version (this should use the latest)
            CopyRequest copyRequestLatest = CopyRequest.builder()
                    .srcKey(key)
                    .destBucket(bucketClient.blobStore.getBucket())
                    .destKey(destKeyLatest)
                    .build();
            CopyResponse copyResponse3 = bucketClient.copy(copyRequestLatest);
            Assertions.assertEquals(destKeyLatest, copyResponse3.getKey());
            Assertions.assertNotNull(copyResponse3.getVersionId());
            Assertions.assertNotNull(copyResponse3.getLastModified());
            Assertions.assertNotNull(copyResponse3.getETag());
            verifyBlobCopy(bucketClient, key, uploadResponseV2.getVersionId(), destKeyLatest);
        } finally {
            // Delete our blobs to clean up the test
            safeDeleteBlobs(bucketClient, key, destKeyV1, destKeyV2, destKeyLatest);
        }
    }

    private void verifyBlobCopy(BucketClient bucketClient, String originalKey, String originalVersionId, String destKey) throws IOException {

        // Verify the copied contents are the same
        try (ByteArrayOutputStream originalOutputStream = new ByteArrayOutputStream();
             ByteArrayOutputStream destOutputStream = new ByteArrayOutputStream()) {

            bucketClient.download(new DownloadRequest.Builder().withKey(originalKey).withVersionId(originalVersionId).build(), originalOutputStream);
            bucketClient.download(new DownloadRequest.Builder().withKey(destKey).build(), destOutputStream);

            Assertions.assertEquals(originalOutputStream.toByteArray().length, destOutputStream.toByteArray().length, "testCopy: Content-Length did not match");
            Assertions.assertArrayEquals(originalOutputStream.toByteArray(), destOutputStream.toByteArray(), "testCopy: Bytes arrays did not match");
        }

        // Verify the copied metadata is the same
        BlobMetadata originalMetadata = bucketClient.getMetadata(originalKey, originalVersionId);
        Assertions.assertNotNull(originalMetadata);

        BlobMetadata copiedMetadata = bucketClient.getMetadata(destKey, null);
        Assertions.assertNotNull(copiedMetadata);
        Assertions.assertEquals(originalMetadata.getObjectSize(), copiedMetadata.getObjectSize());
        Assertions.assertEquals(originalMetadata.getMetadata(), copiedMetadata.getMetadata(), "testCopy: The metadata of the copied object does not match the original");
    }

    @Test
    public void testList() throws IOException {
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        // Create the BucketClient
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, false);
        BucketClient bucketClient = new BucketClient(blobStore);

        // Upload a blob to the bucket
        String baseKey = "conformance-tests/blob-for-list";
        String prefixKey = baseKey + "/prefix";
        String[] keys = new String[]{baseKey, prefixKey + "-1", prefixKey + "-2", prefixKey + "_3"};
        byte[] blobBytes = "Default content for this blob".getBytes(StandardCharsets.UTF_8);
        try {
            // Load some blobs into the bucket for this test
            for (String key : keys) {
                try (InputStream inputStream = new ByteArrayInputStream(blobBytes)) {
                    UploadRequest request = new UploadRequest.Builder()
                            .withKey(key)
                            .withContentLength(blobBytes.length)
                            .build();
                    bucketClient.upload(request, inputStream);
                }
            }

            // Call the list() function to verify it detects our blobs
            ListBlobsRequest request = new ListBlobsRequest.Builder().build();
            Iterator<BlobInfo> iter = bucketClient.list(request);
            Assertions.assertNotNull(iter);
            Set<String> observedKeys = new HashSet<>();
            while (iter.hasNext()) {
                BlobInfo blobInfo = iter.next();
                observedKeys.add(blobInfo.getKey());
            }
            for (String key : keys) {
                if (!observedKeys.contains(key)) {
                    Assertions.fail("testList: Was unable to find key=" + key);
                }
            }

            // Now verify the prefix functionality is working
            request = new ListBlobsRequest.Builder()
                    .withPrefix(prefixKey)
                    .build();
            iter = bucketClient.list(request);
            Assertions.assertNotNull(iter);
            observedKeys = new HashSet<>();
            while (iter.hasNext()) {
                BlobInfo blobInfo = iter.next();
                observedKeys.add(blobInfo.getKey());
            }
            for (String key : keys) {
                if (!key.startsWith(prefixKey)) {
                    continue;
                }
                if (!observedKeys.contains(key)) {
                    Assertions.fail("testList: Was unable to find key=" + key);
                }
            }

            // Now verify the delimiter functionality
            request = new ListBlobsRequest.Builder()
                    .withPrefix(prefixKey)
                    .withDelimiter("-")     // Filter out every key that has a "-"
                    .build();
            iter = bucketClient.list(request);
            Assertions.assertNotNull(iter);
            observedKeys = new HashSet<>();
            while (iter.hasNext()) {
                BlobInfo blobInfo = iter.next();
                observedKeys.add(blobInfo.getKey());
                Assertions.assertEquals(1, observedKeys.size(), "testList: Did not return expected number of keys");
                Assertions.assertTrue(observedKeys.contains(prefixKey + "_3"));
            }
        }
        // Clean up
        finally {
            safeDeleteBlobs(bucketClient, keys);
        }
    }

    @Test
    public void testListPage() throws IOException {

        // Create the BucketClient
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, false);
        BucketClient bucketClient = new BucketClient(blobStore);

        // Upload multiple blobs to the bucket to test pagination
        String baseKey = "conformance-tests/blob-for-list-page";
        String prefixKey = baseKey + "/prefix";
        String[] keys = new String[]{
            baseKey, 
            prefixKey + "-1", 
            prefixKey + "-2", 
            prefixKey + "_3",
            prefixKey + "-4",
            prefixKey + "-5",
            prefixKey + "_6"
        };
        byte[] blobBytes = "Default content for this blob".getBytes(StandardCharsets.UTF_8);
        
        try {
            // Load some blobs into the bucket for this test
            for (String key : keys) {
                try (InputStream inputStream = new ByteArrayInputStream(blobBytes)) {
                    UploadRequest request = new UploadRequest.Builder()
                            .withKey(key)
                            .withContentLength(blobBytes.length)
                            .build();
                    bucketClient.upload(request, inputStream);
                }
            }

            // Test 1: Basic listPage with small maxResults to force pagination
            ListBlobsPageRequest request = ListBlobsPageRequest.builder()
                    .withPrefix(baseKey)
                    .withMaxResults(3)
                    .build();
            
            ListBlobsPageResponse firstPage = bucketClient.listPage(request);
            Assertions.assertNotNull(firstPage);
            Assertions.assertNotNull(firstPage.getBlobs());
            
            // Should have at most 3 items due to maxResults
             Assertions.assertTrue(firstPage.getBlobs().size() <= 3,
                 "testListPage: First page should have at most 3 items");
            
            // If we have more than 3 items total, it should be truncated
            Assertions.assertTrue(firstPage.isTruncated(),
                    "testListPage: Should be truncated when more items exist");
            Assertions.assertNotNull(firstPage.getNextPageToken(),
                "testListPage: Should have next page token when truncated");

            // Test 2: Continue to next page if available
            ListBlobsPageRequest nextPageRequest = ListBlobsPageRequest.builder()
                    .withPrefix(baseKey)
                    .withMaxResults(3)
                    .withPaginationToken(firstPage.getNextPageToken())
                    .build();

            ListBlobsPageResponse secondPage = bucketClient.listPage(nextPageRequest);
            Assertions.assertNotNull(secondPage);
            Assertions.assertNotNull(secondPage.getBlobs());

            // Verify we got different results
            Set<String> firstPageKeys = firstPage.getBlobs().stream()
                    .map(BlobInfo::getKey)
                    .collect(Collectors.toSet());
            Set<String> secondPageKeys = secondPage.getBlobs().stream()
                    .map(BlobInfo::getKey)
                    .collect(Collectors.toSet());

            // Pages should not overlap
            Set<String> intersection = new HashSet<>(firstPageKeys);
            intersection.retainAll(secondPageKeys);
            Assertions.assertTrue(intersection.isEmpty(),
                "testListPage: Pages should not have overlapping keys");


            // Test 3: Test prefix functionality with pagination
            ListBlobsPageRequest prefixRequest = ListBlobsPageRequest.builder()
                    .withPrefix(prefixKey)
                    .withMaxResults(2)
                    .build();

            ListBlobsPageResponse prefixPage = bucketClient.listPage(prefixRequest);
            Assertions.assertNotNull(prefixPage);

            // All returned keys should start with the prefix
            for (BlobInfo blobInfo : prefixPage.getBlobs()) {
                Assertions.assertTrue(blobInfo.getKey().startsWith(prefixKey),
                        "testListPage: All keys should start with prefix: " + blobInfo.getKey());
            }

            // Test 4: Test delimiter functionality with pagination
            ListBlobsPageRequest delimiterRequest = ListBlobsPageRequest.builder()
                    .withPrefix(prefixKey)
                    .withDelimiter("-")
                    .withMaxResults(2)
                    .build();

            ListBlobsPageResponse delimiterPage = bucketClient.listPage(delimiterRequest);
            Assertions.assertNotNull(delimiterPage);

            // Test 5: Manual pagination loop to collect all items
            Set<String> allKeys = new HashSet<>();
            String nextToken = null;
            int pageCount = 0;
            
            do {
                ListBlobsPageRequest pageRequest = ListBlobsPageRequest.builder()
                        .withPrefix(baseKey)
                        .withMaxResults(2)
                        .withPaginationToken(nextToken)
                        .build();

                ListBlobsPageResponse page = bucketClient.listPage(pageRequest);
                Assertions.assertNotNull(page);
                
                page.getBlobs().forEach(blob -> allKeys.add(blob.getKey()));
                nextToken = page.getNextPageToken();
                pageCount++;
                
                // Safety check to prevent infinite loops
                Assertions.assertTrue(pageCount <= 10, 
                    "testListPage: Pagination loop exceeded maximum expected pages");
                
            } while (nextToken != null);
            
            // Verify we collected all expected keys
            for (String key : keys) {
                Assertions.assertTrue(allKeys.contains(key), 
                    "testListPage: Missing expected key: " + key);
            }
        }
        // Clean up
        finally {
            safeDeleteBlobs(bucketClient, keys);
        }
    }

    @Test
    public void testGetMetadata() throws IOException {

        class TestConfig {
            final String testName;
            final String key;
            final byte[] content;
            final Map<String, String> metadata;
            final Map<String, String> expectedMetadata;
            final boolean wantError;

            public TestConfig(String testName, String key, byte[] content, Map<String, String> metadata, Map<String, String> expectedMetadata, boolean wantError) {
                this.testName = testName;
                this.key = key;
                this.content = content;
                this.metadata = metadata;
                this.expectedMetadata = expectedMetadata;
                this.wantError = wantError;
            }
        }

        // Test data
        String key = "conformance-tests/blob-for-metadata";
        byte[] blobBytes = "Metadata blob for testing some large amount".getBytes(StandardCharsets.UTF_8);

        // Create the BucketClient
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, false);
        BucketClient bucketClient = new BucketClient(blobStore);

        // Prepare the tests
        List<TestConfig> testConfigs = new ArrayList<>();
        testConfigs.add(new TestConfig("empty metadata map", key, blobBytes, Map.of(), Map.of(), false));
        testConfigs.add(new TestConfig("null key fails", null, blobBytes, Map.of(), Map.of(), true));
        testConfigs.add(new TestConfig("empty key fails", "", blobBytes, Map.of(), Map.of(), true));
        testConfigs.add(new TestConfig("populated metadata map", key + testConfigs.size(), blobBytes, Map.of("abc", "foo", "def", "bar"), Map.of("abc", "foo", "def", "bar"), false));

        // Now run the tests
        try {
            for (TestConfig testConfig : testConfigs) {

                // Upload a blob with the attached metadata
                boolean failed = false;
                UploadResponse uploadResponse = null;
                try (InputStream inputStream = new ByteArrayInputStream(testConfig.content)) {
                    UploadRequest request = new UploadRequest.Builder()
                            .withKey(testConfig.key)
                            .withContentLength(testConfig.content.length)
                            .withMetadata(testConfig.metadata)
                            .build();
                    uploadResponse = bucketClient.upload(request, inputStream);
                } catch (Throwable t) {
                    failed = true;
                }
                Assertions.assertEquals(testConfig.wantError, failed, testConfig.testName + ": Did not generate expected error state");
                if (failed) {
                    continue;
                }

                // Then read the metadata
                BlobMetadata blobMetadata = bucketClient.getMetadata(testConfig.key, null);

                // Validate the results
                Assertions.assertNotNull(blobMetadata);
                Assertions.assertEquals(uploadResponse.getETag(), blobMetadata.getETag(), testConfig.testName + ": The metadata etag does not match the original");
                Assertions.assertEquals(testConfig.expectedMetadata, blobMetadata.getMetadata(), testConfig.testName + ": The metadata does not match the original");
                Assertions.assertNotNull(blobMetadata.getLastModified());
            }
        } finally {
            // Delete our blob to clean up the test
            safeDeleteBlobs(bucketClient, key);
        }
    }

    @Test
    public void testGetVersionedMetadata() throws IOException {

        String key = "conformance-tests/metadata/versioned-blob";
        byte[] blobBytes = "Versioned metadata blob".getBytes(StandardCharsets.UTF_8);
        Map<String, String> metadata1 = Map.of("key1", "value1", "key2", "value2");

        // Create the BucketClient
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, true);
        BucketClient bucketClient = new BucketClient(blobStore);

        try {
            // Upload a blob with the attached metadata
            UploadResponse uploadResponse1;
            try (InputStream inputStream = new ByteArrayInputStream(blobBytes)) {
                UploadRequest request = new UploadRequest.Builder()
                        .withKey(key)
                        .withContentLength(blobBytes.length)
                        .withMetadata(metadata1)
                        .build();
                uploadResponse1 = bucketClient.upload(request, inputStream);
            }

            // Upload the blob a second time, with different metadata
            UploadResponse uploadResponse2;
            byte[] blobBytes2 = "Versioned metadata blob - version 2".getBytes(StandardCharsets.UTF_8);
            Map<String, String> metadata2 = Map.of("key3", "value3", "key4", "value4");
            try (InputStream inputStream = new ByteArrayInputStream(blobBytes2)) {
                UploadRequest request = new UploadRequest.Builder()
                        .withKey(key)
                        .withContentLength(blobBytes2.length)
                        .withMetadata(metadata2)
                        .build();
                uploadResponse2 = bucketClient.upload(request, inputStream);
            }

            // Now verify the metadata from v1
            BlobMetadata v1Metadata = bucketClient.getMetadata(key, uploadResponse1.getVersionId());
            Assertions.assertNotNull(v1Metadata);
            Assertions.assertEquals(v1Metadata.getKey(), uploadResponse1.getKey());
            Assertions.assertEquals(v1Metadata.getVersionId(), uploadResponse1.getVersionId());
            Assertions.assertEquals(v1Metadata.getMetadata(), metadata1);

            // Now verify the metadata from v2
            BlobMetadata v2Metadata = bucketClient.getMetadata(key, uploadResponse2.getVersionId());
            Assertions.assertNotNull(v2Metadata);
            Assertions.assertEquals(v2Metadata.getKey(), uploadResponse2.getKey());
            Assertions.assertEquals(v2Metadata.getVersionId(), uploadResponse2.getVersionId());
            Assertions.assertEquals(v2Metadata.getMetadata(), metadata2);
        } finally {
            // Delete our blob to clean up the test
            safeDeleteBlobs(bucketClient, key);
        }
    }

    private static final String DEFAULT_MULTIPART_KEY_PREFIX = "conformance-tests/multipart-";
    private static final byte[] multipartBytes1 = new byte[5 * 1024 * 1024];
    private static final byte[] multipartBytes2 = new byte[5 * 1024 * 1024];
    private static final byte[] multipartBytes3 = new byte[5 * 1024 * 1024];
    private static final byte[] multipartBytes4 = new byte[5 * 1024 * 1024];
    static {
        Arrays.fill(multipartBytes1,(byte)'1');
        Arrays.fill(multipartBytes2,(byte)'2');
        Arrays.fill(multipartBytes3,(byte)'3');
        Arrays.fill(multipartBytes4,(byte)'4');
    }

    static class MultipartUploadTestPart {
        final int partNumber;
        final byte[] content;

        MultipartUploadTestPart(int partNumber, byte[] content){
            this.partNumber = partNumber;
            this.content = content;
        }
    }

    static class MultipartUploadPartResult {
        final int partNumber;
        final boolean useRealEtag;

        MultipartUploadPartResult(int partNumber, boolean useRealEtag){
            this.partNumber = partNumber;
            this.useRealEtag = useRealEtag;
        }
    }

    static class MultipartUploadTestConfig {
        final String testName;
        final String key;
        final Map<String,String> metadata;
        final List<MultipartUploadTestPart> partsToUpload;
        final List<MultipartUploadPartResult> partsToComplete;
        final boolean abortUpload;
        final boolean wantCompletionError;
        final String kmsKeyId;
        final Map<String, String> tags;

        public MultipartUploadTestConfig(String testName,
                          String key,
                          Map<String,String> metadata,
                          List<MultipartUploadTestPart> partsToUpload,
                          List<MultipartUploadPartResult> partsToComplete,
                          boolean abortUpload,
                          boolean wantCompletionError) {
            this(testName, key, metadata, partsToUpload, partsToComplete, abortUpload, wantCompletionError, null, null);
        }

        public MultipartUploadTestConfig(String testName,
                          String key,
                          Map<String,String> metadata,
                          List<MultipartUploadTestPart> partsToUpload,
                          List<MultipartUploadPartResult> partsToComplete,
                          boolean abortUpload,
                          boolean wantCompletionError,
                          String kmsKeyId) {
            this(testName, key, metadata, partsToUpload, partsToComplete, abortUpload, wantCompletionError, kmsKeyId, null);
        }

        public MultipartUploadTestConfig(String testName,
                          String key,
                          Map<String,String> metadata,
                          List<MultipartUploadTestPart> partsToUpload,
                          List<MultipartUploadPartResult> partsToComplete,
                          boolean abortUpload,
                          boolean wantCompletionError,
                          String kmsKeyId,
                          Map<String, String> tags) {
            this.testName = testName;
            this.key = key;
            this.metadata = metadata;
            this.partsToUpload = partsToUpload;
            this.partsToComplete = partsToComplete;
            this.abortUpload = abortUpload;
            this.wantCompletionError = wantCompletionError;
            this.kmsKeyId = kmsKeyId;
            this.tags = tags;
        }
    }

    private void runMultipartUploadTest(MultipartUploadTestConfig testConfig) throws IOException {
        logger.info("=== runMultipartUploadTest: {} ===", testConfig.testName);
        logger.info("Key: {}", testConfig.key);
        logger.info("Metadata: {}", testConfig.metadata);
        logger.info("Tags: {}", testConfig.tags);
        logger.info("KMS Key ID: {}", testConfig.kmsKeyId);
        logger.info("Parts to upload: {}", testConfig.partsToUpload.size());
        
        // Create the BucketClient
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, false);
        BucketClient bucketClient = new BucketClient(blobStore);

        // Now run the tests
        MultipartUpload mpu = null;
        try {

            // Initiate the multipartUpload
            MultipartUploadRequest.Builder requestBuilder = new MultipartUploadRequest.Builder()
                    .withKey(testConfig.key)
                    .withMetadata(testConfig.metadata);
            if (testConfig.kmsKeyId != null) {
                requestBuilder.withKmsKeyId(testConfig.kmsKeyId);
            }
            if (testConfig.tags != null && !testConfig.tags.isEmpty()) {
                requestBuilder.withTags(testConfig.tags);
            }
            MultipartUploadRequest multipartUploadRequest = requestBuilder.build();
            logger.info("Initiating multipart upload - Key: {}, Tags: {}", testConfig.key, testConfig.tags);
            mpu = bucketClient.initiateMultipartUpload(multipartUploadRequest);
            logger.info("Multipart upload initiated - Upload ID: {}, Bucket: {}, Key: {}", 
                    mpu.getId(), mpu.getBucket(), mpu.getKey());

            // Upload the individual parts
            Map<Integer, UploadPartResponse> uploadedParts = new HashMap<>();
            for(MultipartUploadTestPart testPart : testConfig.partsToUpload) {
                logger.info("Uploading part {} - Size: {} bytes, Upload ID: {}", 
                        testPart.partNumber, testPart.content.length, mpu.getId());
                UploadPartResponse partResponse = bucketClient.uploadMultipartPart(mpu, new MultipartPart(testPart.partNumber, testPart.content));
                logger.info("Part {} uploaded successfully - ETag: {}, Size: {} bytes", 
                        partResponse.getPartNumber(), partResponse.getEtag(), partResponse.getSizeInBytes());
                uploadedParts.put(partResponse.getPartNumber(), partResponse);
            }

            // List the parts and verify they're all accounted for
            List<UploadPartResponse> partResponses = bucketClient.listMultipartUpload(mpu);
            Assertions.assertEquals(uploadedParts.size(), partResponses.size(), testConfig.testName + ": listMultipartUpload() returned unexpected number of parts");
            for(UploadPartResponse partResponse : partResponses) {
                UploadPartResponse partThatWasUploaded = uploadedParts.get(partResponse.getPartNumber());
                Assertions.assertNotNull(partThatWasUploaded, testConfig.testName + ": listMultipartUpload() reported a part we hadn't uploaded partNumber=" + partResponse.getPartNumber());
                Assertions.assertEquals(partThatWasUploaded.getEtag(), partResponse.getEtag(), testConfig.testName + ": listMultipartUpload() etags do not match partNumber=" + partResponse.getPartNumber());
                uploadedParts.remove(partResponse.getPartNumber());
            }
            Assertions.assertTrue(uploadedParts.isEmpty(), testConfig.testName + ": listMultipartUpload() Some parts we uploaded were not reported via listMultipartUpload(): " + uploadedParts);

            // Abort the upload if desired
            if(testConfig.abortUpload){
                bucketClient.abortMultipartUpload(mpu);
                return;
            }
            // Determine what set of parts we'll use when declaring the upload complete
            Map<Integer, UploadPartResponse> mappingOfUploadPartResponses = partResponses.stream().collect(Collectors.toMap(UploadPartResponse::getPartNumber, Function.identity()));
            List<UploadPartResponse> partResponsesToComplete = new ArrayList<>();
            for(MultipartUploadPartResult partResult : testConfig.partsToComplete){
                UploadPartResponse foundResult = mappingOfUploadPartResponses.get(partResult.partNumber);
                String eTagToUse = "fakeEtag";
                long sizeToUse = 0;
                if(foundResult != null){
                    eTagToUse =  partResult.useRealEtag ? foundResult.getEtag() : foundResult.getEtag()+"fake";
                    sizeToUse = foundResult.getSizeInBytes();
                }
                partResponsesToComplete.add(new UploadPartResponse(partResult.partNumber, eTagToUse, sizeToUse));
            }

            // Complete the multipartUpload
            boolean completionFailed = false;
            try {
                logger.info("Completing multipart upload - Upload ID: {}, Key: {}, Parts: {}", 
                        mpu.getId(), mpu.getKey(), partResponsesToComplete.size());
                bucketClient.completeMultipartUpload(mpu, partResponsesToComplete);
                logger.info("Multipart upload completed successfully - Key: {}", mpu.getKey());
            }
            catch(Throwable t){
                logger.error("Multipart upload completion failed - Upload ID: {}, Key: {}, Error: {}", 
                        mpu.getId(), mpu.getKey(), t.getMessage(), t);
                Assertions.assertTrue(testConfig.wantCompletionError, testConfig.testName + ": completeMultipartUpload() produced unexpected error " + t.getMessage());
                completionFailed = true;
            }
            Assertions.assertEquals(testConfig.wantCompletionError, completionFailed, testConfig.testName + ": completeMultipartUpload() did not fail as expected");
            if(completionFailed){
                return;
            }

            BlobMetadata blobMetadata = bucketClient.getMetadata(testConfig.key, null);
            Map<String, String> actualMetadata = blobMetadata.getMetadata();
            
            // For GCP, tags are stored as metadata with "gcp-tag-" prefix, so we need to filter them out
            // when comparing metadata
            if (GCP_PROVIDER_ID.equals(harness.getProviderId()) && testConfig.tags != null && !testConfig.tags.isEmpty()) {
                String tagPrefix = "gcp-tag-";
                actualMetadata = actualMetadata.entrySet().stream()
                        .filter(entry -> !entry.getKey().startsWith(tagPrefix))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            }
            
            Assertions.assertEquals(testConfig.metadata, actualMetadata, testConfig.testName + ": Downloaded metadata did not match");

            // Verify tags if they were provided
            if (testConfig.tags != null && !testConfig.tags.isEmpty()) {
                Map<String, String> tagResults = bucketClient.getTags(testConfig.key);
                Assertions.assertEquals(testConfig.tags, tagResults, testConfig.testName + ": Tags did not match what was uploaded");
            }
        }
        finally {
            // Now delete all blobs that were created
            safeDeleteBlobs(bucketClient, testConfig.key);
            try {
                bucketClient.abortMultipartUpload(mpu);
            }
            catch(Throwable t){
                // Ignore
            }
        }
    }

    @Test
    public void testMultipartUpload_singlePart() throws IOException {
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        runMultipartUploadTest(new MultipartUploadTestConfig(
                "single part", DEFAULT_MULTIPART_KEY_PREFIX + "singlePart",
                Map.of("123", "456"),
                List.of(new MultipartUploadTestPart(1, multipartBytes1)),
                List.of(new MultipartUploadPartResult(1, true)),
                false, false));
    }

    @Test
    public void testMultipartUpload_multipleParts() throws IOException {
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        runMultipartUploadTest(new MultipartUploadTestConfig(
                "multiple parts", DEFAULT_MULTIPART_KEY_PREFIX + "multipleParts",
                Map.of("234", "456"),
                List.of(
                        new MultipartUploadTestPart(1, multipartBytes1),
                        new MultipartUploadTestPart(2, multipartBytes2),
                        new MultipartUploadTestPart(3, multipartBytes3),
                        new MultipartUploadTestPart(4, multipartBytes4)),
                List.of(
                        new MultipartUploadPartResult(1, true),
                        new MultipartUploadPartResult(2, true),
                        new MultipartUploadPartResult(3, true),
                        new MultipartUploadPartResult(4, true)),
                false, false));
    }

    @Test
    public void testMultipartUpload_unorderedMultipleParts() throws IOException {
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        runMultipartUploadTest(new MultipartUploadTestConfig(
                "unordered multiple parts", DEFAULT_MULTIPART_KEY_PREFIX + "unorderedMultipleParts",
                Map.of("345", "456"),
                List.of(
                        new MultipartUploadTestPart(1, multipartBytes1),
                        new MultipartUploadTestPart(2, multipartBytes2)),
                List.of(
                        new MultipartUploadPartResult(2, true),
                        new MultipartUploadPartResult(1, true)),
                false, false));
    }

    @Test
    public void testMultipartUpload_skippingNumbers() throws IOException {
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        runMultipartUploadTest(new MultipartUploadTestConfig(
                "skipping numbers", DEFAULT_MULTIPART_KEY_PREFIX + "skippingNumbers",
                Map.of("456", "456"),
                List.of(
                        new MultipartUploadTestPart(2, multipartBytes1),
                        new MultipartUploadTestPart(3, multipartBytes2),
                        new MultipartUploadTestPart(6, multipartBytes3)),
                List.of(
                        new MultipartUploadPartResult(2, true),
                        new MultipartUploadPartResult(3, true),
                        new MultipartUploadPartResult(6, true)),
                false, false));
    }

    @Test
    public void testMultipartUpload_duplicateParts() throws IOException {
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        runMultipartUploadTest(new MultipartUploadTestConfig(
                "duplicates parts", DEFAULT_MULTIPART_KEY_PREFIX + "duplicateParts",
                Map.of("567", "456"),
                List.of(
                        new MultipartUploadTestPart(2, multipartBytes1),
                        new MultipartUploadTestPart(3, multipartBytes2),
                        new MultipartUploadTestPart(2, multipartBytes3)),
                List.of(
                        new MultipartUploadPartResult(2, true),
                        new MultipartUploadPartResult(3, true)),
                false, false));
    }

    @Test
    public void testMultipartUpload_nonExistentParts() throws IOException {
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        runMultipartUploadTest(new MultipartUploadTestConfig(
                "non-existent parts", DEFAULT_MULTIPART_KEY_PREFIX + "nonExistentParts",
                Map.of("678", "456"),
                List.of(
                        new MultipartUploadTestPart(2, multipartBytes1)),
                List.of(
                        new MultipartUploadPartResult(2, true),
                        new MultipartUploadPartResult(3, false)),
                false, true));
    }

    @Test
    public void testMultipartUpload_badETag() throws IOException {
    Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        runMultipartUploadTest(new MultipartUploadTestConfig(
                "bad etag", DEFAULT_MULTIPART_KEY_PREFIX + "badETag",
                Map.of("789", "456"),
                List.of(
                        new MultipartUploadTestPart(2, multipartBytes1),
                        new MultipartUploadTestPart(3, multipartBytes2),
                        new MultipartUploadTestPart(4, multipartBytes3)),
                List.of(
                        new MultipartUploadPartResult(2, true),
                        new MultipartUploadPartResult(3, false),
                        new MultipartUploadPartResult(4, true)),
                false, true));
    }

    @Test
    public void testMultipartUpload_invalidMultipartUpload(){
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, false);
        BucketClient bucketClient = new BucketClient(blobStore);

        // Call each operation with an invalid MPU
        MultipartUpload invalidMPU = MultipartUpload.builder()
                .bucket("fakeBucket")
                .key(DEFAULT_MULTIPART_KEY_PREFIX+"invalidKey")
                .id("invalidUploadId")
                .build();

        boolean multipartUploadFailed = false;
        try {
            MultipartPart multipartPart = new MultipartPart(1, multipartBytes1);
            bucketClient.uploadMultipartPart(invalidMPU, multipartPart);
        } catch (Throwable t) {
            multipartUploadFailed = true;
        }
        Assertions.assertTrue(multipartUploadFailed, "The uploadMultipartPart() operation did not fail");

        multipartUploadFailed = false;
        try {
            List<UploadPartResponse> listOfParts = List.of(new UploadPartResponse(1, "etag", multipartBytes1.length));
            bucketClient.completeMultipartUpload(invalidMPU, listOfParts);
        } catch (Throwable t) {
            multipartUploadFailed = true;
        }
        Assertions.assertTrue(multipartUploadFailed, "The completeMultipartUpload() operation did not fail");

        multipartUploadFailed = false;
        try {
            bucketClient.listMultipartUpload(invalidMPU);
        } catch (Throwable t) {
            multipartUploadFailed = true;
        }
        Assertions.assertTrue(multipartUploadFailed, "The listMultipartUpload() operation did not fail");

        multipartUploadFailed = false;
        try {
            bucketClient.abortMultipartUpload(invalidMPU);
        } catch (Throwable t) {
            multipartUploadFailed = true;
        }
        Assertions.assertTrue(multipartUploadFailed, "The abortMultipartUpload() operation did not fail");
    }

    @Test
    public void testMultipartUpload_multipleMultipartUploadsForSameKey(){
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, false);
        BucketClient bucketClient = new BucketClient(blobStore);

        String key = DEFAULT_MULTIPART_KEY_PREFIX + "multipleMPU";
        MultipartUploadRequest multipartUploadRequest = new MultipartUploadRequest.Builder()
                .withKey(key)
                .build();

        List<MultipartUpload> mpuList = new ArrayList<>();
        try {
            boolean multipartUploadFailed = false;
            try {
                mpuList.add(bucketClient.initiateMultipartUpload(multipartUploadRequest));
                mpuList.add(bucketClient.initiateMultipartUpload(multipartUploadRequest));
            } catch (Throwable t) {
                multipartUploadFailed = true;
            }
            Assertions.assertFalse(multipartUploadFailed, "Duplicate initiateMultipartUpload() operations did not fail");
        }
        finally{
            safeDeleteBlobs(bucketClient, key);
            for(MultipartUpload mpu : mpuList){
                try {
                    bucketClient.abortMultipartUpload(mpu);
                }
                catch(Throwable t){
                    // Ignore
                }
            }
        }
    }

    @Test
    public void testMultipartUpload_completeAnAbortedUpload(){
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, false);
        BucketClient bucketClient = new BucketClient(blobStore);

        String key = DEFAULT_MULTIPART_KEY_PREFIX + "completeAnAborted";
        MultipartUploadRequest multipartUploadRequest = new MultipartUploadRequest.Builder()
                .withKey(key)
                .build();

        MultipartUpload mpu = null;
        try {
            // Initiate a valid MPU, and upload a part
            mpu = bucketClient.initiateMultipartUpload(multipartUploadRequest);
            UploadPartResponse part1 = bucketClient.uploadMultipartPart(mpu, new MultipartPart(1, multipartBytes1));

            // Now abort it
            bucketClient.abortMultipartUpload(mpu);

            // Now try to complete it
            List<UploadPartResponse> listOfParts = List.of(part1);
            boolean multipartUploadFailed = false;
            try {
                bucketClient.completeMultipartUpload(mpu, listOfParts);
            }
            catch(Throwable t){
                multipartUploadFailed = true;
            }
            Assertions.assertTrue(multipartUploadFailed, "Attempting to complete an aborted multipartUpload should have failed");
        }
        finally{
            safeDeleteBlobs(bucketClient, key);
            try {
                bucketClient.abortMultipartUpload(mpu);
            }
            catch(Throwable t){
                // Ignore
            }
        }
    }

    @Test
    public void testMultipartUpload_withKms() throws IOException {
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        String kmsKeyId = harness.getKmsKeyId();
        Assumptions.assumeTrue(kmsKeyId != null && !kmsKeyId.isEmpty(), "KMS key ID not configured");

        runMultipartUploadTest(new MultipartUploadTestConfig(
                "multipart with KMS", DEFAULT_MULTIPART_KEY_PREFIX + "withKms",
                Map.of("encryption", "kms"),
                List.of(
                        new MultipartUploadTestPart(1, multipartBytes1),
                        new MultipartUploadTestPart(2, multipartBytes2)),
                List.of(
                        new MultipartUploadPartResult(1, true),
                        new MultipartUploadPartResult(2, true)),
                false, false, kmsKeyId));
    }

    @Test
    public void testMultipartUpload_withTags() throws IOException {
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        String expectedKey = DEFAULT_MULTIPART_KEY_PREFIX + "withTags";
        Map<String, String> tags = Map.of("tag1", "value1");
        logger.info("=== Starting testMultipartUpload_withTags ===");
        logger.info("Expected S3 key: {}", expectedKey);
        logger.info("Tags to be applied: {}", tags);
        logger.info("Metadata: key1=value1");
        logger.info("Parts to upload: 2 parts (part 1: {} bytes, part 2: {} bytes)", 
                multipartBytes1.length, multipartBytes2.length);
        runMultipartUploadTest(new MultipartUploadTestConfig(
                "multipart with tags", expectedKey,
                Map.of("key1", "value1"),
                List.of(
                        new MultipartUploadTestPart(1, multipartBytes1),
                        new MultipartUploadTestPart(2, multipartBytes2)),
                List.of(
                        new MultipartUploadPartResult(1, true),
                        new MultipartUploadPartResult(2, true)),
                false, false, null, tags));
        logger.info("=== Completed testMultipartUpload_withTags ===");
    }

    @Test
    public void testTagging() throws IOException {

        AbstractBlobStore blobStore = harness.createBlobStore(true, true, false);
        BucketClient bucketClient = new BucketClient(blobStore);

        // Now run the tests
        String key = "conformance-tests/blob-for-tagging";
        try {
            String defaultTestData = "This is tagging test data";
            byte[] utf8BlobBytes = defaultTestData.getBytes(StandardCharsets.UTF_8);
            Map<String, String> tags = Map.of("tag1", "value1");

            // Upload the file with the tags
            try (InputStream inputStream = new ByteArrayInputStream(utf8BlobBytes)) {
                UploadRequest request = new UploadRequest.Builder()
                        .withKey(key)
                        .withContentLength(utf8BlobBytes.length)
                        .withTags(tags)
                        .build();
                bucketClient.upload(request, inputStream);
            }

            // Verify the tags are applied to the file
            Map<String, String> tagResults = bucketClient.getTags(key);
            Assertions.assertEquals(tags, tagResults, "testTagging: Tags did not match what was uploaded");

            // Try overwriting the tags
            Map<String, String> tags2 = Map.of("tag3", "value3");
            bucketClient.setTags(key, tags2);
            tagResults = bucketClient.getTags(key);
            Assertions.assertEquals(tags2, tagResults, "testTagging: Tags did not match what was overwriting");

            // Try writing tags to a blob that doesn't exist
            boolean failed = false;
            try{
                Map<String, String> tags3 = Map.of("tag5", "value5");
                bucketClient.setTags(key+"-fake", tags3);
            }
            catch(Throwable t){
                failed = true;
            }
            Assertions.assertTrue(failed, "testTagging: Succeeded in writing tags to non-existent blob");
        } finally {
            // Now delete all blobs that were created
            safeDeleteBlobs(bucketClient, key);
            safeDeleteBlobs(bucketClient, key+"-fake");
        }
    }

    private static final String PRESIGNED_BLOB_UPLOAD_PREFIX = "conformance-tests/presignedUploadUrls/";
    private static final String PRESIGNED_BLOB_DOWNLOAD_PREFIX = "conformance-tests/presignedDownloadUrls/";

    //@Test
    public void testGeneratePresignedUploadUrl_happyPathWithNoMetadataOrTags() throws IOException {
        String key = PRESIGNED_BLOB_UPLOAD_PREFIX + "happyPathWithNoMetadataOrTags";
        runPresignedUploadTest(key, Duration.ofHours(10), null, null, null, null, null);
    }

    //@Test
    public void testGeneratePresignedUploadUrl_happyPathWithMetadataButWithNoTags() throws IOException {
        String key = PRESIGNED_BLOB_UPLOAD_PREFIX + "happyPathWithMetadataButWithNoTags";
        Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
        runPresignedUploadTest(key, Duration.ofHours(10), null, metadata, metadata, null, null);
    }

    //@Test
    public void testGeneratePresignedUploadUrl_happyPathWithNoMetadataButWithTags() throws IOException {
        String key = PRESIGNED_BLOB_UPLOAD_PREFIX + "happyPathWithNoMetadataButWithTags";
        Map<String, String> tags = Map.of("tag1", "tagValue1", "tag2", "tagValue2");
        runPresignedUploadTest(key, Duration.ofHours(10), null, null, null, tags, tags);
    }

    //@Test
    public void testGeneratePresignedUploadUrl_happyPathWithBothMetadataAndTags() throws IOException {
        String key = PRESIGNED_BLOB_UPLOAD_PREFIX + "happyPathWithBothMetadataAndTags";
        Map<String, String> metadata = Map.of("key3", "value3", "key4", "value4");
        Map<String, String> tags = Map.of("tag3", "tagValue3", "tag4", "tagValue4");
        runPresignedUploadTest(key, Duration.ofHours(10), null, metadata, metadata, tags, tags);
    }

    @Test
    public void testGeneratePresignedUploadUrl_negativeDuration() {
        String key = PRESIGNED_BLOB_UPLOAD_PREFIX + "negativeDuration";
        Assertions.assertThrows(InvalidArgumentException.class, () -> runPresignedUploadTest(key, Duration.ofHours(-10), null, null, null, null, null));
    }

    @Test
    public void testGeneratePresignedUploadUrl_zeroDuration() {
        String key = PRESIGNED_BLOB_UPLOAD_PREFIX + "zeroDuration";
        Assertions.assertThrows(InvalidArgumentException.class, () -> runPresignedUploadTest(key, Duration.ofHours(0), null, null, null, null, null));
    }

    @Test
    public void testGeneratePresignedUploadUrl_expiredUrl() {
        String key = PRESIGNED_BLOB_UPLOAD_PREFIX + "expiredUrl";
        Assertions.assertThrows(Exception.class, () -> runPresignedUploadTest(key, Duration.ofSeconds(1), 2L, null, null,  null, null));
    }

    // We initiate the presignedUrl expecting certain metadata headers to be there,
    // but when we actually upload the file we don't specify those headers
    @Test
    public void testGeneratePresignedUploadUrl_missingMetadataOnUpload() {
        String key = PRESIGNED_BLOB_UPLOAD_PREFIX + "missingMetadataOnUpload";
        Map<String, String> metadata = Map.of("key5", "value5", "key6", "value6");
        Assertions.assertThrows(Exception.class, () ->
            runPresignedUploadTest(key, Duration.ofHours(10),null,  metadata, null, null, null)
        );
    }

    // We initiate the presignedUrl expecting certain tags to be there,
    // but when we actually upload the file we don't specify those tags
    @Test
    public void testGeneratePresignedUploadUrl_missingTagsOnUpload() {
        String key = PRESIGNED_BLOB_UPLOAD_PREFIX + "missingTagsOnUpload";
        Map<String, String> tags = Map.of("tag5", "tagValue5", "tag6", "tagValue6");
        Assertions.assertThrows(Exception.class, () ->
            runPresignedUploadTest(key, Duration.ofHours(10), null, null, null, tags, null)
        );
    }

    // We initiate the presignedUrl without any metadata headers,
    // but when we actually upload the file we include some
    @Test
    public void testGeneratePresignedUploadUrl_missingMetadataOnUrlGeneration() {
        String key = PRESIGNED_BLOB_UPLOAD_PREFIX + "missingMetadataOnUrlGeneration";
        Map<String, String> metadata = Map.of("key7", "value7", "key8", "value8");
        Assertions.assertThrows(Exception.class, () ->
            runPresignedUploadTest(key, Duration.ofHours(10), null, null, metadata, null, null)
        );
    }

    // We initiate the presignedUrl expecting certain tags to be there,
    // but when we actually upload the file we don't specify those tags
    @Test
    public void testGeneratePresignedUploadUrl_missingTagsOnUrlGeneration() {
        String key = PRESIGNED_BLOB_UPLOAD_PREFIX + "missingTagsOnUrlGeneration";
        Map<String, String> tags = Map.of("tag7", "tagValue7", "tag8", "tagValue8");
        Assertions.assertThrows(Exception.class, () ->
                runPresignedUploadTest(key, Duration.ofHours(10), null, null, null, null, tags)
        );
    }

    private void runPresignedUploadTest(String key,
                                        Duration duration,
                                        Long delayInSeconds,
                                        Map<String, String> metadataForUrlGeneration,
                                        Map<String, String> metadataForUpload,
                                        Map<String, String> tagsForUrlGeneration,
                                        Map<String, String> tagsForUpload) throws IOException {

        AbstractBlobStore blobStore = harness.createBlobStore(true, true, false);
        BucketClient bucketClient = new BucketClient(blobStore);
        String blobData = "This is test data";
        byte[] utf8BlobBytes = blobData.getBytes(StandardCharsets.UTF_8);
        try{
            PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                    .type(PresignedOperation.UPLOAD)
                    .key(key)
                    .duration(duration)
                    .metadata(metadataForUrlGeneration)
                    .tags(tagsForUrlGeneration)
                    .build();

            // Generate a presigned URL
            URL presignedUrl = bucketClient.generatePresignedUrl(presignedUrlRequest);

            // Optional delay
            if(delayInSeconds != null) {
                try {
                    Thread.sleep(1000 * delayInSeconds);
                } catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            // Upload the file using the presigned URL
            useHttpUrlConnectionToPut(harness, presignedUrl, utf8BlobBytes, metadataForUpload, tagsForUpload);

            // Now read the file to ensure it was uploaded properly
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                DownloadResponse downloadResponse = bucketClient.download(new DownloadRequest.Builder().withKey(key).build(), outputStream);

                // Verify the content
                Assertions.assertEquals(utf8BlobBytes.length, outputStream.toByteArray().length);
                Assertions.assertArrayEquals(utf8BlobBytes, outputStream.toByteArray());

                // Verify the DownloadResponse
                metadataForUrlGeneration = metadataForUrlGeneration==null ? new HashMap<>() : metadataForUrlGeneration;
                Assertions.assertEquals(key, downloadResponse.getKey());
                Assertions.assertEquals(key, downloadResponse.getMetadata().getKey());
                Assertions.assertEquals(utf8BlobBytes.length, downloadResponse.getMetadata().getObjectSize());
                Assertions.assertEquals(metadataForUrlGeneration, downloadResponse.getMetadata().getMetadata());
                Assertions.assertNotNull(downloadResponse.getMetadata().getETag());
                Assertions.assertNotNull(downloadResponse.getMetadata().getLastModified());

                // Check the metadata on the object
                BlobMetadata blobMetadata = bucketClient.getMetadata(key, null);
                Map<String, String> actualMetadata = blobMetadata.getMetadata();
                Assertions.assertEquals(metadataForUrlGeneration, actualMetadata);

                // Check the tags on the object
                Map<String, String> actualTags = bucketClient.getTags(key);
                tagsForUrlGeneration = tagsForUrlGeneration==null ? new HashMap<>() : tagsForUrlGeneration;
                Assertions.assertEquals(tagsForUrlGeneration, actualTags);
            }
        }
        finally{
            safeDeleteBlobs(bucketClient, key);
        }
    }

    //@Test
    void testGeneratePresignedDownloadUrl_happyPath() throws IOException {
        String key = PRESIGNED_BLOB_DOWNLOAD_PREFIX + "happyPath";
        runPresignedDownloadTest(key, true, Duration.ofHours(6), null);
    }

    @Test
    void testGeneratePresignedDownloadUrl_nonExistingFile() throws IOException {
        String key = PRESIGNED_BLOB_DOWNLOAD_PREFIX + "nonExistingFile";
        Assertions.assertThrows(Throwable.class, () ->
            runPresignedDownloadTest(key, false, Duration.ofHours(4), null)
        );
    }

    @Test
    void testGeneratePresignedDownloadUrl_negativeDuration() throws IOException {
        String key = PRESIGNED_BLOB_DOWNLOAD_PREFIX + "negativeDuration";
        Assertions.assertThrows(InvalidArgumentException.class, () ->
                runPresignedDownloadTest(key, true, Duration.ofHours(-6), null)
        );
    }

    @Test
    void testGeneratePresignedDownloadUrl_zeroDuration() throws IOException {
        String key = PRESIGNED_BLOB_DOWNLOAD_PREFIX + "zeroDuration";
        Assertions.assertThrows(InvalidArgumentException.class, () ->
                runPresignedDownloadTest(key, true, Duration.ofHours(0), null)
        );
    }

    @Test
    void testGeneratePresignedDownloadUrl_expiredUrl() throws IOException {
        String key = PRESIGNED_BLOB_DOWNLOAD_PREFIX + "expiredUrl";
        Assertions.assertThrows(Exception.class, () ->
                runPresignedDownloadTest(key, false, Duration.ofSeconds(1), 2L)
        );
    }

    private void runPresignedDownloadTest(String key,
                                        boolean uploadFile,
                                        Duration duration,
                                        Long delayInSeconds) throws IOException {

        AbstractBlobStore blobStore = harness.createBlobStore(true, true, false);
        BucketClient bucketClient = new BucketClient(blobStore);
        String blobData = "This is test data";
        byte[] utf8BlobBytes = blobData.getBytes(StandardCharsets.UTF_8);
        String secondKey = key + "2";
        try{

            // Upload the file so we can download it
            if(uploadFile) {
                try(InputStream baos = new ByteArrayInputStream(utf8BlobBytes)) {
                    bucketClient.upload(new UploadRequest.Builder()
                            .withKey(key)
                            .withContentLength(utf8BlobBytes.length)
                            .build(), baos);
                }
            }

            PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                    .type(PresignedOperation.DOWNLOAD)
                    .key(key)
                    .duration(duration)
                    .build();

            // Generate a presigned URL
            URL presignedUrl = bucketClient.generatePresignedUrl(presignedUrlRequest);

            // Optional delay
            if(delayInSeconds != null) {
                try {
                    Thread.sleep(1000 * delayInSeconds);
                } catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            // Download the file using the presigned URL
            // Note: Wiremock isn't capturing this request here, so we're uploading the content
            // using a presignedUrl as well (also not captured by wiremock), and then downloading it normally to verify
            byte[] output = useHttpUrlConnectionToGet(presignedUrl);

            // Verify the contents
            Assertions.assertArrayEquals(utf8BlobBytes, output);
        }
        finally {
            safeDeleteBlobs(bucketClient, key);
            safeDeleteBlobs(bucketClient, secondKey);
        }
    }

    @Test
    void testDoesObjectExist() throws IOException {
        runDoesObjectExistTest("conformance-tests/doesBlobExist/unversioned", false);
    }

    @Test
    void testDoesObjectExist_versioned() throws IOException {
        runDoesObjectExistTest("conformance-tests/doesBlobExist/versioned", true);
    }

    private void runDoesObjectExistTest(String key, boolean useVersionedBucket) throws IOException {
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, useVersionedBucket);
        BucketClient bucketClient = new BucketClient(blobStore);
        byte[] blobBytes1 = "This is test data".getBytes(StandardCharsets.UTF_8);
        byte[] blobBytes2= "This is the second test data".getBytes(StandardCharsets.UTF_8);
        UploadResponse uploadResponse1;
        UploadResponse uploadResponse2;

        try (InputStream inputStream1 = new ByteArrayInputStream(blobBytes1);
             InputStream inputStream2 = new ByteArrayInputStream(blobBytes2)) {
            UploadRequest request1 = new UploadRequest.Builder()
                    .withKey(key)
                    .withContentLength(blobBytes1.length)
                    .build();
            uploadResponse1 = bucketClient.upload(request1, inputStream1);
            UploadRequest request2 = new UploadRequest.Builder()
                    .withKey(key)
                    .withContentLength(blobBytes2.length)
                    .build();
            uploadResponse2 = bucketClient.upload(request2, inputStream2);

            // Check if the objects exist
            Assertions.assertTrue(bucketClient.doesObjectExist(key, useVersionedBucket ? uploadResponse1.getVersionId() : null));  // Get 1st version
            Assertions.assertTrue(bucketClient.doesObjectExist(key, useVersionedBucket ? uploadResponse2.getVersionId() : null));  // Get 2nd version
            Assertions.assertTrue(bucketClient.doesObjectExist(key, null));  // Get latest version

            // Check something that doesn't exist
            Assertions.assertFalse(bucketClient.doesObjectExist(key+"fake", null));
        }
        finally {
            safeDeleteBlobs(bucketClient, key);
        }
    }

    /**
     * Helper function for uploading to a presignedUrl
     */
    void useHttpUrlConnectionToPut(Harness harness, URL presignedUrl, byte[] blobBytes, Map<String, String> metadata, Map<String, String> tags) throws IOException {
        HttpURLConnection connection = (HttpURLConnection)presignedUrl.openConnection();
        connection.setDoOutput(true);

        // Append the metadata
        if(metadata != null) {
            metadata.forEach((k, v) -> connection.setRequestProperty(harness.getMetadataHeader(k), v));
        }

        // Append the tags
        String tagsValue = generateTagsValue(tags);
        if(tagsValue != null) {
            connection.setRequestProperty(harness.getTaggingHeader(), tagsValue);
        }

        connection.setRequestMethod("PUT");

        try(InputStream inputStream = new ByteArrayInputStream(blobBytes);
            OutputStream out = connection.getOutputStream()) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }
        int responseCode = connection.getResponseCode();
        if(responseCode != 200) {
            throw new IOException("Failed to upload using presignedUrl. responseCode=" + responseCode);
        }
    }

    /**
     * Helper function for downloading from a presignedUrl
     */
    public byte[] useHttpUrlConnectionToGet(URL presignedUrl) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        HttpURLConnection connection = (HttpURLConnection) presignedUrl.openConnection();
        connection.setRequestMethod("GET");
        try (InputStream inputStream = connection.getInputStream()) {
            int bytesRead;
            byte[] buffer = new byte[1024];
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                byteArrayOutputStream.write(buffer, 0, bytesRead);
            }
        }
        return byteArrayOutputStream.toByteArray();
    }

    /**
     * Helper function to generate the tag header value of the format tag1=value1&tag2=value2
     */
    protected String generateTagsValue(Map<String, String> tags) {
        if (tags == null || tags.isEmpty()) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> tag : tags.entrySet()) {
            if (tag.getKey() == null || tag.getKey().isEmpty() || tag.getValue() == null || tag.getValue().isEmpty()) {
                throw new IllegalArgumentException("Illegal empty/null tag (" + tag.getKey() + ", " + tag.getValue() + ")");
            }
            if (builder.length() > 0) {
                builder.append("&");
            }
            builder.append(tag.getKey());
            builder.append("=");
            builder.append(tag.getValue());
        }
        return builder.toString();
    }

    private void safeDeleteBlobs(BucketClient bucketClient, String... keys){
        for(String key : keys){
            try {
                bucketClient.delete(key, null);
            }
            catch(Throwable t){
                // Ignore
            }
        }
    }

    @Test
    public void testUploadWithKmsKey_happyPath() {
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        String key = "conformance-tests/kms/upload-happy-path";
        String kmsKeyId = harness.getKmsKeyId();
        runUploadWithKmsKeyTest(key, kmsKeyId, "Test data with KMS encryption".getBytes());
    }

    @Test
    public void testUploadWithKmsKey_nullKmsKeyId() {
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        String key = "conformance-tests/kms/upload-null-key";
        runUploadWithKmsKeyTest(key, null, "Test data without KMS".getBytes());
    }

    @Test
    public void testUploadWithKmsKey_emptyKmsKeyId() {
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        String key = "conformance-tests/kms/upload-empty-key";
        runUploadWithKmsKeyTest(key, "", "Test data with empty KMS key".getBytes());
    }

    private void runUploadWithKmsKeyTest(String key, String kmsKeyId, byte[] content) {
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, false);
        BucketClient bucketClient = new BucketClient(blobStore);

        try {
            // Upload with KMS key
            UploadResponse uploadResponse;
            try (InputStream inputStream = new ByteArrayInputStream(content)) {
                UploadRequest request = new UploadRequest.Builder()
                        .withKey(key)
                        .withContentLength(content.length)
                        .withKmsKeyId(kmsKeyId)
                        .build();
                uploadResponse = bucketClient.upload(request, inputStream);
            }

            Assertions.assertNotNull(uploadResponse, "testUploadWithKmsKey: No response returned");
            Assertions.assertNotNull(uploadResponse.getETag(), "testUploadWithKmsKey: No eTag returned");

            // Download and verify
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                DownloadRequest downloadRequest = new DownloadRequest.Builder()
                        .withKey(key)
                        .build();
                bucketClient.download(downloadRequest, outputStream);

                Assertions.assertEquals(content.length, outputStream.toByteArray().length, "testUploadWithKmsKey: Content length mismatch");
                Assertions.assertArrayEquals(content, outputStream.toByteArray(), "testUploadWithKmsKey: Content mismatch");
            }
        } catch (Exception e) {
            Assertions.fail("testUploadWithKmsKey: Test failed with exception: " + e.getMessage());
        } finally {
            safeDeleteBlobs(bucketClient, key);
        }
    }

    @Test
    public void testDownloadWithKmsKey() throws IOException {
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        String key = "conformance-tests/kms/download-happy-path";
        String kmsKeyId = harness.getKmsKeyId();
        byte[] content = "Test data for KMS download".getBytes(StandardCharsets.UTF_8);
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, false);
        BucketClient bucketClient = new BucketClient(blobStore);

        try {
            // Upload with KMS key
            try (InputStream inputStream = new ByteArrayInputStream(content)) {
                UploadRequest request = new UploadRequest.Builder()
                        .withKey(key)
                        .withContentLength(content.length)
                        .withKmsKeyId(kmsKeyId)
                        .build();
                bucketClient.upload(request, inputStream);
            }

            // Download and verify
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                DownloadRequest downloadRequest = new DownloadRequest.Builder()
                        .withKey(key)
                        .build();
                bucketClient.download(downloadRequest, outputStream);
                byte[] downloadedContent = outputStream.toByteArray();

                Assertions.assertEquals(content.length, downloadedContent.length,
                        "testDownloadWithKmsKey: Content length mismatch");
                Assertions.assertArrayEquals(content, downloadedContent,
                        "testDownloadWithKmsKey: Content mismatch");
            }
        } finally {
            safeDeleteBlobs(bucketClient, key);
        }
    }

    @Test
    public void testRangedReadWithKmsKey() throws IOException {
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        String key = "conformance-tests/kms/ranged-read";
        String kmsKeyId = harness.getKmsKeyId();
        runRangedReadWithKmsKeyTest(key, kmsKeyId);
    }

    private void runRangedReadWithKmsKeyTest(String key, String kmsKeyId) throws IOException {
        String blobData = "This is test data for the KMS ranged read test file";
        byte[] blobBytes = blobData.getBytes(StandardCharsets.UTF_8);

        AbstractBlobStore blobStore = harness.createBlobStore(true, true, false);
        BucketClient bucketClient = new BucketClient(blobStore);

        try (InputStream inputStream = new ByteArrayInputStream(blobBytes)) {
            UploadRequest request = new UploadRequest.Builder()
                    .withKey(key)
                    .withContentLength(blobBytes.length)
                    .withKmsKeyId(kmsKeyId)
                    .build();
            bucketClient.upload(request, inputStream);
        }

        try {
            DownloadRequest.Builder requestBuilder = new DownloadRequest.Builder()
                    .withKey(key)
                    .withKmsKeyId(kmsKeyId);

            // Try downloading the first 10 bytes
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                bucketClient.download(requestBuilder.withRange(0L, 9L).build(), outputStream);
                byte[] content = outputStream.toByteArray();
                Assertions.assertEquals(10, content.length);
                Assertions.assertArrayEquals(Arrays.copyOfRange(blobBytes, 0, 10), content);
            }

            // Try downloading a middle 20 bytes
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                bucketClient.download(requestBuilder.withRange(10L, 29L).build(), outputStream);
                byte[] content = outputStream.toByteArray();
                Assertions.assertEquals(20, content.length);
                Assertions.assertArrayEquals(Arrays.copyOfRange(blobBytes, 10, 30), content);
            }

            // Try downloading from byte 10 onward
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                bucketClient.download(requestBuilder.withRange(10L, null).build(), outputStream);
                byte[] content = outputStream.toByteArray();
                Assertions.assertEquals(blobBytes.length - 10, content.length);
                Assertions.assertArrayEquals(Arrays.copyOfRange(blobBytes, 10, blobBytes.length), content);
            }

            // Try downloading the last 10 bytes
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                bucketClient.download(requestBuilder.withRange(null, 10L).build(), outputStream);
                byte[] content = outputStream.toByteArray();
                Assertions.assertEquals(10, content.length);
                Assertions.assertArrayEquals(Arrays.copyOfRange(blobBytes, blobBytes.length - 10, blobBytes.length), content);
            }
        } finally {
            safeDeleteBlobs(bucketClient, key);
        }
    }

    @Test
    public void testPresignedUrlWithKmsKey_nullKmsKeyId() throws IOException {
        Assumptions.assumeFalse(GCP_PROVIDER_ID.equals(harness.getProviderId()));
        String key = "conformance-tests/kms/presigned-url-null-key";
        Map<String, String> metadata = Map.of("key2", "value2");
        byte[] content = "Test data for presigned URL without KMS".getBytes(StandardCharsets.UTF_8);

        runPresignedUrlWithKmsKeyTest(key, null, metadata, content);
    }

    private void runPresignedUrlWithKmsKeyTest(String key, String kmsKeyId,
                                               Map<String, String> metadata, byte[] content) throws IOException {
        AbstractBlobStore blobStore = harness.createBlobStore(true, true, false);
        BucketClient bucketClient = new BucketClient(blobStore);

        try {
            // Generate presigned URL with KMS key
            PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                    .type(PresignedOperation.UPLOAD)
                    .key(key)
                    .duration(Duration.ofHours(1))
                    .metadata(metadata)
                    .kmsKeyId(kmsKeyId)
                    .build();

            URL presignedUrl = bucketClient.generatePresignedUrl(presignedUrlRequest);
            Assertions.assertNotNull(presignedUrl);
        } finally {
            safeDeleteBlobs(bucketClient, key);
        }
    }
}