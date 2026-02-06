package com.salesforce.multicloudj.blob;

import com.salesforce.multicloudj.blob.async.client.AsyncBucketClient;
import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
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
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketEncryptionRequest;
import software.amazon.awssdk.services.s3.model.GetBucketEncryptionResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.ServerSideEncryptionByDefault;
import software.amazon.awssdk.services.s3.model.ServerSideEncryptionRule;

public class Main {

    // ---------- Wrangler credential issue reproduction and pre-warm tests ----------
    // Run with -Dwrangler.test.mode=credential-issue | reproduce | prewarm | upload | fix-verify | fix-verify-interrupt
    // and -Dassume.role.arn=<ARN> (or ASSUME_ROLE_ARN) for assume-role modes.
    // credential-issue = same test, same assertion (no cache message):
    //   Without fix: -Dwrangler.test.interrupt.delay.ms=30  → fails (cache message).
    //   With fix:    omit -Dwrangler.test.interrupt.delay.ms (default 400ms) → passes.
    // Requires BLOB_PROVIDER=aws and AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN.

    /** Default interrupt delay (ms). Long enough for async refresh to complete so test passes with fix. */
    private static final int DEFAULT_INTERRUPT_DELAY_MS = 400;
    /** Short delay (ms) to interrupt during sync refresh so test fails without fix. Use -Dwrangler.test.interrupt.delay.ms=30 */
    private static final int SHORT_INTERRUPT_DELAY_MS = 30;

    /**
     * Same test case to prove: before the fix it fails, after the fix it passes.
     * <p>
     * First upload with ASSUME_ROLE on a single thread; we interrupt that thread after a delay.
     * Assertion: we must NOT see "Interrupted waiting to refresh a cached value".
     * </p>
     * <ul>
     *   <li><b>Without fix</b> (sync refresh): use short delay so we interrupt during refresh.
     *       Run with {@code -Dwrangler.test.interrupt.delay.ms=30}. Test fails (cache message).</li>
     *   <li><b>With fix</b> (asyncCredentialUpdateEnabled): use default delay so refresh completes first.
     *       Omit {@code -Dwrangler.test.interrupt.delay.ms} (default 400 ms). Test passes.</li>
     * </ul>
     * Same test, same assertion; only the delay differs for the two proofs.
     */
    public static void testReproduceCredentialIssue() {
        int delayMs = getInterruptDelayMs();
        System.out.println("=== Credential issue test (same case: first upload on one thread, then interrupt) ===");
        System.out.println("Interrupt delay: " + delayMs + " ms. Assertion: no 'Interrupted waiting to refresh a cached value'.");
        if (delayMs <= 50) {
            System.out.println("(Short delay is for proving failure WITHOUT fix. With fix enabled, omit -Dwrangler.test.interrupt.delay.ms so default " + DEFAULT_INTERRUPT_DELAY_MS + " ms is used.)");
        }
        runFirstUploadSingleThreadThenInterruptAndAssertNoCacheMessage(delayMs);
    }

    private static int getInterruptDelayMs() {
        String prop = System.getProperty("wrangler.test.interrupt.delay.ms");
        if (prop != null && !prop.isEmpty()) {
            try {
                return Integer.parseInt(prop.trim());
            } catch (NumberFormatException ignored) {
                // fallback to default
            }
        }
        return DEFAULT_INTERRUPT_DELAY_MS;
    }

    /** Ms to wait after building client so credential pre-warm can complete (e.g. -Dwrangler.test.prewarm.wait.ms=2000). */
    private static int getPrewarmWaitMs() {
        String prop = System.getProperty("wrangler.test.prewarm.wait.ms");
        if (prop != null && !prop.isEmpty()) {
            try {
                return Integer.parseInt(prop.trim());
            } catch (NumberFormatException ignored) {
                // fallback to 0
            }
        }
        return 0;
    }

    /**
     * First upload with ASSUME_ROLE on a single thread, then interrupt that thread after
     * {@code interruptDelayMs}. Asserts that the exception (if any) does NOT contain
     * "Interrupted waiting to refresh a cached value".
     * <p>
     * Use short delay (30 ms) to prove failure without fix; default 400 ms to prove pass with fix.
     */
    public static void runFirstUploadSingleThreadThenInterruptAndAssertNoCacheMessage(int interruptDelayMs) {
        String provider = getProvider();
        BucketClient client = getBucketClient(provider);
        // Allow pre-warm to complete when testing eager init (e.g. -Dwrangler.test.prewarm.wait.ms=2000)
        int prewarmWaitMs = getPrewarmWaitMs();
        if (prewarmWaitMs > 0) {
            System.out.println("Waiting " + prewarmWaitMs + " ms for credential pre-warm to complete...");
            try {
                TimeUnit.MILLISECONDS.sleep(prewarmWaitMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        UploadRequest uploadRequest = new UploadRequest.Builder()
                .withKey("wrangler-credential-issue-" + System.currentTimeMillis() + ".txt")
                .build();
        byte[] content = "credential-issue test".getBytes();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread uploadThread = new Thread(() -> {
            try {
                client.upload(uploadRequest, content);
            } catch (Throwable t) {
                thrown.set(t);
            }
        }, "upload-thread");
        uploadThread.start();
        try {
            TimeUnit.MILLISECONDS.sleep(interruptDelayMs);
            uploadThread.interrupt();
            uploadThread.join(TimeUnit.SECONDS.toMillis(10));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        Throwable t = thrown.get();
        if (t != null && hasMessageInChain(t, "Interrupted waiting to refresh a cached value")) {
            getLogger().info("Credential-issue test failed (no fix or fix not effective): got cache message");
            throw new RuntimeException("Credential-issue test failed: got 'Interrupted waiting to refresh a cached value'. "
                    + "With asyncCredentialUpdateEnabled the first caller should not see this. "
                    + "Use -Dwrangler.test.interrupt.delay.ms=" + SHORT_INTERRUPT_DELAY_MS + " to prove failure without fix.", t);
        }
        System.out.println("Credential-issue test passed: no 'Interrupted waiting to refresh a cached value' (fix is effective).");
    }

    /** Overload using default delay (for fix-verify-interrupt). */
    public static void runFirstUploadSingleThreadThenInterruptAndAssertNoCacheMessage() {
        runFirstUploadSingleThreadThenInterruptAndAssertNoCacheMessage(getInterruptDelayMs());
    }

    /**
     * Reproduces the exact Wrangler error "Interrupted waiting to refresh a cached value": uses
     * ASSUME_ROLE with two threads so one thread blocks waiting on the cache; we interrupt that
     * thread. Use wrangler.test.mode=reproduce to run this (for reproduction only, not for fix validation).
     */
    public static void reproduceAssumeRoleInterrupt() {
        String provider = getProvider();
        BucketClient client = getBucketClient(provider);
        UploadRequest uploadRequest1 = new UploadRequest.Builder()
                .withKey("wrangler-repro-1-" + System.currentTimeMillis() + ".txt")
                .build();
        UploadRequest uploadRequest2 = new UploadRequest.Builder()
                .withKey("wrangler-repro-2-" + System.currentTimeMillis() + ".txt")
                .build();
        byte[] content = "reproduce interrupt".getBytes();
        AtomicReference<Throwable> thrown1 = new AtomicReference<>();
        AtomicReference<Throwable> thrown2 = new AtomicReference<>();

        Thread uploadThread1 = new Thread(() -> {
            try {
                client.upload(uploadRequest1, content);
            } catch (Throwable t) {
                thrown1.set(t);
            }
        }, "upload-thread-1");
        Thread uploadThread2 = new Thread(() -> {
            try {
                client.upload(uploadRequest2, content);
            } catch (Throwable t) {
                thrown2.set(t);
            }
        }, "upload-thread-2");

        uploadThread1.start();
        try {
            TimeUnit.MILLISECONDS.sleep(20);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        uploadThread2.start();
        try {
            TimeUnit.MILLISECONDS.sleep(80);
            uploadThread2.interrupt();
            uploadThread1.join(TimeUnit.SECONDS.toMillis(15));
            uploadThread2.join(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        Throwable t1 = thrown1.get();
        Throwable t2 = thrown2.get();
        Throwable preferred = null;
        if (t2 != null && hasMessageInChain(t2, "Interrupted waiting to refresh a cached value")) {
            preferred = t2;
        } else if (t1 != null && hasMessageInChain(t1, "Interrupted waiting to refresh a cached value")) {
            preferred = t1;
        } else if (t2 != null) {
            preferred = t2;
        } else if (t1 != null) {
            preferred = t1;
        }
        if (preferred != null) {
            getLogger().info("Reproduce test: got expected exception (thread interrupted during credential refresh): {}", preferred.getMessage());
            throw new RuntimeException("Reproduce: saw exception as expected", preferred);
        }
    }

    private static boolean hasMessageInChain(Throwable t, String substring) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c.getMessage() != null && c.getMessage().contains(substring)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Confirms the SDK fix (asyncCredentialUpdateEnabled): first upload with ASSUME_ROLE on a
     * single thread, no interrupt. With async refresh the first caller gets credentials (from
     * background refresh) and the upload should succeed. Run with -Dwrangler.test.mode=fix-verify
     * and -Dassume.role.arn=... and AWS_* env.
     */
    public static void testAssumeRoleFirstUploadSucceeds() {
        System.out.println("=== Fix verification: first upload with ASSUME_ROLE (single thread, no interrupt) ===");
        String provider = getProvider();
        BucketClient client = getBucketClient(provider);
        UploadRequest uploadRequest = new UploadRequest.Builder()
                .withKey("wrangler-fix-verify-" + System.currentTimeMillis() + ".txt")
                .build();
        UploadResponse response = client.upload(uploadRequest, "fix-verify".getBytes());
        if (response == null) {
            throw new AssertionError("Expected upload to succeed (fix verification); got null response");
        }
        System.out.println("Fix verification passed: first upload succeeded, response=" + response);
    }

    /**
     * Same test as credential-issue: single-thread first upload + interrupt, assert no cache message.
     * Run with -Dwrangler.test.mode=fix-verify-interrupt and -Dassume.role.arn=... and AWS_* env.
     */
    public static void testAssumeRoleSingleThreadInterruptNoCacheMessage() {
        System.out.println("=== Fix verification (interrupt): same as credential-issue ===");
        runFirstUploadSingleThreadThenInterruptAndAssertNoCacheMessage();
    }

    /**
     * Pre-warm credentials then upload: uses ASSUME_ROLE, warms the credential cache with a
     * cheap list call on the current thread, then uploads. Verifies the pre-warm fix.
     */
    public static void prewarmThenUpload() {
        String provider = getProvider();
        BucketClient client = getBucketClient(provider);
        getLogger().info("Pre-warming credentials (listPage with maxResults=1)...");
        client.listPage(ListBlobsPageRequest.builder().withMaxResults(1).build());
        getLogger().info("Pre-warm done. Uploading...");
        UploadRequest uploadRequest = new UploadRequest.Builder()
                .withKey("wrangler-prewarm-" + System.currentTimeMillis() + ".txt")
                .build();
        UploadResponse response = client.upload(uploadRequest, "prewarm test".getBytes());
        getLogger().info("Pre-warm test: upload succeeded, response={}", response);
    }

    /**
     * Uploads content to a specified provider using the given bucket client.
     */
    public static void upload() {
        // Get the provider for the storage service
        String provider = getProvider();

        // Create a BucketClient instance based on the provider
        BucketClient client = getBucketClient(provider);

        // Get the input stream for the file to be uploaded
        InputStream content = getInputStream();

        // Build the UploadRequest with the necessary details
        UploadRequest uploadRequest = new UploadRequest.Builder()
                .withKey("bucket-path/chameleon.jpg")
                .withMetadata(Map.of("key1", "val1"))
                .withTags(Map.of("tagKey1", "tagVal1"))
                .build();

        // Upload the file to the storage service and store the response
        UploadResponse response = client.upload(uploadRequest, content);

        // Log the upload response
        getLogger().info("received upload response {}", response);
    }

    /**
     * Downloads a blob from a specified bucket using the provided key.
     */
    public static void downloadBlob() throws FileNotFoundException {
        // Create a DownloadRequest object with the specified object key
        DownloadRequest downloadRequest = new DownloadRequest.Builder()
                .withKey("objectKey-to-download")
                .withVersionId("version-1")
                .build();

        // Get a BucketClient object using the provided provider method
        BucketClient client = getBucketClient(getProvider());

        // Create an OutputStream to write the downloaded content to a file
        OutputStream content = new FileOutputStream("exampleFile.txt");

        // Download the blob using the client, download request, and output stream
        DownloadResponse response = client.download(downloadRequest, content);

        // Log a message indicating that the download is complete
        getLogger().info("download complete");
        getLogger().info("The downloaded file has a name of {}", response.getKey());
        getLogger().info("The file has an etag of {}", response.getMetadata().getETag());
        getLogger().info("The file has a versionId of {}", response.getMetadata().getVersionId());
        getLogger().info("The file has metadata of {}", response.getMetadata().getMetadata());
        getLogger().info("The file was last modified at {}", response.getMetadata().getLastModified());
        getLogger().info("The file is {} bytes", response.getMetadata().getObjectSize());
    }

    /**
     * Deletes a single Blob from substrate-specific Blob storage by key Object name of the Blob
     */
    public static void deleteBlob() {
        // Get the BucketClient instance using the provided getBucketClient() method
        BucketClient client = getBucketClient(getProvider());

        // Delete the blob with the key "key-to-be-deleted"
        client.delete("key-to-be-deleted", "version-1");
    }

    /**
     * Deletes a collection of Blobs from a substrate-specific Blob storage specified by a list of keys/versions
     */
    public static void deleteManyBlobs() {
        BucketClient bucketClient = getBucketClient(getProvider());
        // Assume we have a list of keys/versionIds to delete
        // Note: You only need to specify a versionId if you're wanting to delete a specific version
        Collection<BlobIdentifier> blobsToDelete = Arrays.asList(new BlobIdentifier("key1",null),
                new BlobIdentifier("key2","version2"),
                new BlobIdentifier("key3","version3"));
        // Delete the blobs from the bucket
        bucketClient.delete(blobsToDelete);
        getLogger().info("Blobs deleted successfully.");
    }

    /**
     * Copies a Blob from one bucket to other bucket
     */
    public static void copyFromBucketToBucket() {
        BucketClient bucketClient = getBucketClient(getProvider());

        CopyRequest copyRequest = CopyRequest.builder()
                .srcKey("src-key")
                .srcVersionId("version-1")
                .destBucket("destination-bucket")
                .destKey("dest-key")
                .build();

        // Receive a CopyResponse of the copied Blob
        CopyResponse copyResponse = bucketClient.copy(copyRequest);

        getLogger().info("copied blob has a key of {}", copyResponse.getKey());
        getLogger().info("copied blob has a versionId of {}", copyResponse.getVersionId());
        getLogger().info("copied blob has an etag of {}", copyResponse.getETag());
        getLogger().info("copied blob has a lastModified timestamp of {}", copyResponse.getLastModified());
    }

    public static void generatePresignedUrl() {
        // Get the BucketClient instance using the getBucketClient() method
        BucketClient client = getBucketClient(getProvider());

        // Create a PresignedUrlRequest builder
        PresignedUrlRequest requestBuilder = PresignedUrlRequest.builder()

                // Set the key/name of the blob to access
                .key("blob-key")

                // Set the type of operation to be performed on the presigned URL
                .type(PresignedOperation.UPLOAD)

                // Set the metadata for the presigned URL
                .metadata(Map.of("key1", "val1"))

                // Set the duration for which the presigned URL will be valid
                .duration(Duration.ofMinutes(1))

                // Set the tags for the presigned URL
                .tags(Map.of("tagKey1", "tagVal1"))

                // Build the PresignedUrlRequest instance
                .build();

        // Generate the presigned URL using the BucketClient instance
        URL presignedUrl = client.generatePresignedUrl(requestBuilder);

        getLogger().info("Presigned URL: {}", presignedUrl.toString());
    }

    /**
     * Retrieves the metadata for the specified object for which to retrieve the metadata t
     */
    public static void getMetadataForBlob() {
        // Get the BucketClient instance using the getBucketClient method
        BucketClient bucketClient = getBucketClient(getProvider());

        // reference to the blob key
        var blobKey = "blob-key";
        var blobVersion = "version-1";

        // Use the BucketClient instance to get the metadata for the specified blob key
        BlobMetadata metadata = bucketClient.getMetadata(blobKey, blobVersion);

        getLogger().info("blob metadata is {}", metadata);
    }

    /**
     * Retrieves the list of Blob in the bucket and returns an iterator of @{BlobInfo}
     */
    public static void getBlobListInBucket() {
        // Get the BucketClient instance using the getBucketClient method
        BucketClient bucketClient = getBucketClient(getProvider());

        // Create a ListBlobsRequest to list blobs with a prefix
        ListBlobsRequest request = new ListBlobsRequest.Builder()
                .withPrefix("folder/")
                .build();

        // Iterate through the blob list using the bucket client and request
        Iterator<BlobInfo> blobIterator = bucketClient.list(request);

        // Process each blob in the list
        while (blobIterator.hasNext()) {
            BlobInfo blobInfo = blobIterator.next();
            getLogger().info("Blob key: {}", blobInfo.getKey());
            getLogger().info("Blob size: {}", blobInfo.getObjectSize());
        }
    }

    /**
     * Initiates a multipartUpload for a Blob
     */
    public static void multipartUpload() throws IOException {
        // Get the BucketClient instance
        BucketClient bucketClient = getBucketClient(getProvider());

        // Create a request with the required parameters
        MultipartUploadRequest request = createMultipartUploadRequest();

        // Upload the file in multiple parts
        MultipartUpload response = uploadFileInParts(bucketClient, request);

        // Print the uploaded file's information
        getLogger().info("Uploaded file info:");
        getLogger().info("Object ID: {}", response.getKey());
        getLogger().info("Object Location: {}", response.getBucket());
    }

    /**
     * Demonstrates a full multipart upload lifecycle: initiate, upload parts, complete.
     */
    public static void fullMultipartUploadExample() {
        BucketClient bucketClient = getBucketClient(getProvider());
        String key = "multipart-example-" + System.currentTimeMillis();

        getLogger().info("Starting full multipart upload example for key: {}", key);

        // 1. Initiate
        MultipartUploadRequest request = new MultipartUploadRequest.Builder()
                .withKey(key)
                .withMetadata(Map.of("type", "multipart-example"))
                .build();

        MultipartUpload upload = bucketClient.initiateMultipartUpload(request);
        getLogger().info("Initiated multipart upload: id={}", upload.getId());

        try {
            List<UploadPartResponse> partResponses = new ArrayList<>();

            // 2. Upload Parts (Example: 2 parts, 5MB each to meet minimum part size requirements for some providers)
            int partSize = 5 * 1024 * 1024; // 5MB
            byte[] partData = new byte[partSize];
            
            for (int i = 0; i < 2; i++) {
                int partNumber = i + 1;
                // Fill with some data
                Arrays.fill(partData, (byte) ('0' + i));
                
                MultipartPart part = new MultipartPart(partNumber, partData);

                getLogger().info("Uploading part {} ({} bytes)...", partNumber, partSize);
                UploadPartResponse response = bucketClient.uploadMultipartPart(upload, part);
                partResponses.add(response);
                getLogger().info("Uploaded part {}: etag={}", partNumber, response.getEtag());
            }

            // 3. Complete
            getLogger().info("Completing multipart upload...");
            MultipartUploadResponse response = bucketClient.completeMultipartUpload(upload, partResponses);
            getLogger().info("Completed multipart upload. Etag={}", response.getEtag());

        } catch (Exception e) {
            getLogger().error("Multipart upload failed, aborting...", e);
            try {
                bucketClient.abortMultipartUpload(upload);
                getLogger().info("Aborted multipart upload");
            } catch (Exception abortEx) {
                getLogger().error("Failed to abort multipart upload", abortEx);
            }
        }
    }

    /**
     * This method retrieves all tags associated with a specific blob.
     */
    public static void getTags() {
        // Get the BucketClient instance using the getBucketClient method
        BucketClient client = getBucketClient(getProvider());

        // Retrieve the tags for the blob with the key "blob-key"
        Map<String, String> tags = client.getTags("blob-key");

        // Check if the blob has any tags
        if (tags != null) {
            // If it has tags, log the tags information
            getLogger().info("blob tags are {}", tags);
        } else {
            // If it doesn't have any tags, log a message indicating the same
            getLogger().info("blob does not have any tags");
        }
    }

    /**
     * This method sets tags on a blob. This replaces all existing tags.
     */
    public static void setTags() {
        // Get the BucketClient instance using the getBucketClient method
        BucketClient client = getBucketClient(getProvider());

        // Create a map to store tags
        Map<String, String> tags = new HashMap<>();

        // Add tag1 and its value to the map
        tags.put("tag1", "value1");

        // Add tag2 and its value to the map
        tags.put("tag2", "value2");

        // Set the tags for the specified blob using the client instance
        client.setTags("blob-key", tags);
    }

    /**
     * Uploads a directory to blob storage
     */
    public static void uploadDirectory() {
        System.out.println("Creating AsyncBucketClient for provider: " + getProvider());

        // Get the AsyncBucketClient instance
        AsyncBucketClient asyncClient = getAsyncBucketClient(getProvider());
        System.out.println("AsyncBucketClient created successfully");

        // Check if test directory exists
        java.nio.file.Path testDir = java.nio.file.Paths.get("/tmp/test-directory");
        if (!Files.exists(testDir)) {
            System.out.println("ERROR: Test directory does not exist at " + testDir);
            return;
        }
        System.out.println("Test directory exists: " + testDir.toAbsolutePath());

        // DEBUG: List local files that will be uploaded
        List<String> relativePaths = new ArrayList<>();
        try (Stream<java.nio.file.Path> walk = Files.walk(testDir)) {
            relativePaths = walk
                    .filter(p -> !Files.isDirectory(p))
                    .map(testDir::relativize)
                    .map(p -> p.toString().replace('\\', '/'))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            System.out.println("WARN: Could not list files in test directory: " + e.getMessage());
        }
        System.out.println("[DEBUG] Local files to upload (" + relativePaths.size() + "): " + relativePaths);
        if (relativePaths.isEmpty()) {
            System.out.println("ERROR: No files found in " + testDir + " - directory upload will upload nothing!");
        }

        String prefix = "directoryTagTest/";
        String bucketName = resolveBucketName();
        System.out.println("[DEBUG] Bucket: " + bucketName + ", S3 prefix: " + prefix);
        System.out.println("[DEBUG] Expected S3 object keys: " + relativePaths.stream()
                .map(p -> prefix + p)
                .collect(Collectors.joining(", ")));

        // Create a DirectoryUploadRequest with tags (tests transfer manager tagging in directory upload)
        DirectoryUploadRequest request = DirectoryUploadRequest.builder()
                .localSourceDirectory("/tmp/test-directory")  // Change this to your test directory
                .prefix(prefix)
                .includeSubFolders(true)
                .tags(Map.of("example-tag", "directory-upload", "Environment", "test"))
                .build();

        System.out.println("DirectoryUploadRequest created: " + request);
        System.out.println("Tags applied to directory upload: " + request.getTags());

        try {
            System.out.println("Calling asyncClient.uploadDirectory()...");
            
            // Use async method and wait for completion
            CompletableFuture<DirectoryUploadResponse> future = asyncClient.uploadDirectory(request);
            System.out.println("Got CompletableFuture, waiting for completion...");
            DirectoryUploadResponse response = future.get(); // Block until completion

            // Log the response
            int failedCount = response.getFailedTransfers().size();
            System.out.println("Directory upload completed. Failed transfers: " + failedCount);
            
            if (!response.getFailedTransfers().isEmpty()) {
                System.out.println("Some files failed to upload:");
                response.getFailedTransfers().forEach(failure -> {
                    System.out.println("  Failed: " + failure.getSource() + " - " + failure.getException().getMessage());
                });
            } else {
                System.out.println("All " + relativePaths.size() + " file(s) uploaded successfully!");
            }
            System.out.println("[DEBUG] In S3 console, open bucket '" + bucketName + "' and look for folder '" + prefix + "' (or search by prefix '" + prefix + "').");
        } catch (Exception e) {
            System.out.println("Directory upload failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Downloads a directory from blob storage
     */
    public static void downloadDirectory() {
        // Get the AsyncBucketClient instance
        AsyncBucketClient asyncClient = getAsyncBucketClient(getProvider());

        // Create a DirectoryDownloadRequest
        DirectoryDownloadRequest request = DirectoryDownloadRequest.builder()
                .prefixToDownload("directoryTagTest/")
                .localDestinationDirectory("/tmp/downloaded-directory")  // Change this to your download directory
                .build();

        try {
            // Download the directory using async method
            CompletableFuture<DirectoryDownloadResponse> future = asyncClient.downloadDirectory(request);
            DirectoryDownloadResponse response = future.get(); // Block until completion
            
            // Log the response
            getLogger().info("Directory download completed");
            getLogger().info("Failed transfers: {}", response.getFailedTransfers().size());
            
            if (!response.getFailedTransfers().isEmpty()) {
                getLogger().error("Some files failed to download:");
                response.getFailedTransfers().forEach(failure -> {
                    getLogger().error("Failed: {} - {}", failure.getDestination(), failure.getException().getMessage());
                });
            }
        } catch (Exception e) {
            getLogger().error("Directory download failed: {}", e.getMessage(), e);
        }
    }

    /**
     * Deletes a directory from blob storage
     */
    public static void deleteDirectory() {
        // Get the AsyncBucketClient instance
        AsyncBucketClient asyncClient = getAsyncBucketClient(getProvider());

        try {
            // Delete the directory using async method
            CompletableFuture<Void> future = asyncClient.deleteDirectory("directoryTagTest/");
            future.get(); // Block until completion
            getLogger().info("Directory deleted successfully");
        } catch (Exception e) {
            getLogger().error("Directory delete failed: {}", e.getMessage(), e);
        }
    }


    /**
     * This method uploads a part of the multipartUpload operation
     */
    public static void uploadMultiPart() {
        // Get an instance of BucketClient
        BucketClient bucketClient = getBucketClient(getProvider());

        // Create a MultipartUpload object
        MultipartUploadRequest multipartUploadRequest = createMultipartUploadRequest();
        MultipartUpload multipartUpload = bucketClient.initiateMultipartUpload(multipartUploadRequest);

        // Create a MultipartPart object
        MultipartPart multipartPart = new MultipartPart(1, "data-part-1".getBytes());

        // Call the uploadMultipartPart method using bucketClient
        UploadPartResponse response = bucketClient.uploadMultipartPart(multipartUpload, multipartPart);

        // Process the response
        getLogger().info("Response: {}", response);
    }

    /**
     * This method Completes a multipartUpload
     */
    public static void completeMultiPartUpload() {
        BucketClient bucketClient = getBucketClient(getProvider());

        // List of UploadPartResponse objects (simulated here, in real use you'd get these from actual uploads)
        List<UploadPartResponse> uploadPartResponses = new ArrayList<>();

        // Create a MultipartUpload object
        MultipartUploadRequest multipartUploadRequest = createMultipartUploadRequest();
        MultipartUpload multipartUpload = bucketClient.initiateMultipartUpload(multipartUploadRequest);
        // Upload some parts here
        MultipartUploadResponse response = bucketClient.completeMultipartUpload(multipartUpload, uploadPartResponses);
        getLogger().info("response received is {}", response);
        getLogger().info("Etag of the response {}", response.getEtag());
    }

    private static MultipartUploadRequest createMultipartUploadRequest() {
        // Create a request with the required parameters
        return new MultipartUploadRequest.Builder()
                .withKey("object-key")
                .withMetadata(Map.of("metadata-key", "metadata-value"))
                .build();
    }

    private static MultipartUpload uploadFileInParts(BucketClient bucketClient, MultipartUploadRequest request) {
        // Upload the file in multiple parts using the BucketClient
        return bucketClient.initiateMultipartUpload(request);
    }

    private static final String DEFAULT_BUCKET_NAME = "example-test-bucket-barry";
    private static final String DEFAULT_BUCKET_REGION = "us-west-2";

    static {
        // Set bucket/region as early as possible (exec:java may not pass -D to forked JVM)
        if (System.getProperty("bucket.name") == null || System.getProperty("bucket.name").trim().isEmpty()) {
            System.setProperty("bucket.name", DEFAULT_BUCKET_NAME);
        }
        if (System.getProperty("bucket.region") == null || System.getProperty("bucket.region").trim().isEmpty()) {
            System.setProperty("bucket.region", DEFAULT_BUCKET_REGION);
        }
    }

    private static BucketClient getBucketClient(String provider) {
        // Get configuration from system properties, then env, then example defaults
        String bucketName = resolveBucketName();
        String region = resolveBucketRegion();
        String endpoint = System.getProperty("bucket.endpoint", System.getenv("BUCKET_ENDPOINT"));
        String proxyEndpoint = System.getProperty("bucket.proxy.endpoint", System.getenv("BUCKET_PROXY_ENDPOINT"));
        
        BucketClient.BlobBuilder builder = BucketClient.builder(provider)
                .withBucket(bucketName);
        
        if (region != null && !region.trim().isEmpty()) {
            builder.withRegion(region);
        }

        if (endpoint != null && !endpoint.trim().isEmpty()) {
            builder.withEndpoint(URI.create(endpoint));
        }
        
        if (proxyEndpoint != null && !proxyEndpoint.trim().isEmpty()) {
            builder.withProxyEndpoint(URI.create(proxyEndpoint));
        }
        
        // Credentials: ASSUME_ROLE (for Wrangler credential-issue/reproduce/prewarm) or SESSION from env.
        String assumeRoleArn = System.getProperty("assume.role.arn", System.getenv("ASSUME_ROLE_ARN"));
        String accessKeyId = System.getenv("ACCESS_KEY_ID");
        if (accessKeyId == null) {
            accessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
        }
        String secretAccessKey = System.getenv("SECRET_ACCESS_KEY");
        if (secretAccessKey == null) {
            secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        }
        String sessionToken = System.getenv("SESSION_TOKEN");
        if (sessionToken == null) {
            sessionToken = System.getenv("AWS_SESSION_TOKEN");
        }

        if (assumeRoleArn != null && !assumeRoleArn.trim().isEmpty()) {
            String sessionName = System.getProperty("assume.role.session.name", "wrangler-test-" + System.currentTimeMillis());
            CredentialsOverrider credsOverrider = new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE)
                    .withRole(assumeRoleArn.trim())
                    .withSessionName(sessionName)
                    .withDurationSeconds(3600)
                    .build();
            builder.withCredentialsOverrider(credsOverrider);
            getLogger().info("Using ASSUME_ROLE credentials for role: {}", assumeRoleArn);
        } else if (accessKeyId != null && secretAccessKey != null) {
            getLogger().info("Using credentials from env (ACCESS_KEY_ID/AWS_ACCESS_KEY_ID and SECRET_ACCESS_KEY/AWS_SECRET_ACCESS_KEY)");
            StsCredentials credentials = new StsCredentials(accessKeyId, secretAccessKey, sessionToken);
            CredentialsOverrider credsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                    .withSessionCredentials(credentials).build();
            builder.withCredentialsOverrider(credsOverrider);
        } else {
            getLogger().info("No credentials in env; AWS client will use default credential chain");
        }

        return builder.build();
    }


    private static String resolveBucketName() {
        String v = System.getProperty("bucket.name");
        if (v != null && !v.trim().isEmpty()) {
            return v.trim();
        }
        v = System.getenv("BUCKET_NAME");
        if (v != null && !v.trim().isEmpty()) {
            return v.trim();
        }
        return DEFAULT_BUCKET_NAME;
    }

    private static String resolveBucketRegion() {
        String v = System.getProperty("bucket.region");
        if (v != null && !v.trim().isEmpty()) {
            return v.trim();
        }
        v = System.getenv("BUCKET_REGION");
        if (v != null && !v.trim().isEmpty()) {
            return v.trim();
        }
        return DEFAULT_BUCKET_REGION;
    }

    private static AsyncBucketClient getAsyncBucketClient(String provider) {
        String bucketName = resolveBucketName();
        String region = resolveBucketRegion();
        // Ensure non-null bucket so SDK never sees "Parameter 'Bucket' must not be null"
        if (bucketName == null || bucketName.trim().isEmpty()) {
            bucketName = DEFAULT_BUCKET_NAME;
        }
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        AsyncBucketClient.Builder builder = AsyncBucketClient.builder(provider)
                .withBucket(bucketName)
                .withExecutorService(executorService);
        
        if (region != null && !region.trim().isEmpty()) {
            builder.withRegion(region);
        }
        
        return builder.build();
    }

    private static String getProvider() {
        // Use BLOB_PROVIDER env var if set (e.g. "aws", "gcp", "ali"), otherwise default to "gcp"
        String env = System.getenv("BLOB_PROVIDER");
        return (env != null && !env.trim().isEmpty()) ? env.trim() : "gcp";
    }

    private static InputStream getInputStream() {
        return new ByteArrayInputStream("Upload Content".getBytes());
    }

    private static Logger getLogger() {
        return LoggerFactory.getLogger("Main");
    }

    /**
     * Creates a test directory with sample files for testing
     */
    public static void createTestDirectory() {
        try {
            java.nio.file.Path testDir = java.nio.file.Paths.get("/tmp/test-directory");
            java.nio.file.Files.createDirectories(testDir);
            
            // Create some test files
            java.nio.file.Files.write(testDir.resolve("file1.txt"), "Hello from file1".getBytes());
            java.nio.file.Files.write(testDir.resolve("file2.txt"), "Hello from file2".getBytes());
            
            // Create a subdirectory
            java.nio.file.Path subDir = testDir.resolve("subdir");
            java.nio.file.Files.createDirectories(subDir);
            java.nio.file.Files.write(subDir.resolve("file3.txt"), "Hello from subdir/file3".getBytes());
            
            getLogger().info("Test directory created at: {}", testDir);
        } catch (Exception e) {
            getLogger().error("Failed to create test directory: {}", e.getMessage(), e);
        }
    }

    /**
     * Investigates the bucket's default encryption configuration (AWS only).
     * Explains why "upload without kmsKeyId" succeeds or why the client might see 403.
     */
    public static void investigateBucketEncryption() {
        String provider = getProvider();
        if (!"aws".equals(provider)) {
            System.out.println("Bucket encryption investigation is only implemented for provider=aws (current: " + provider + ").");
            return;
        }
        String bucketName = System.getProperty("bucket.name", System.getenv("BUCKET_NAME") != null ? System.getenv("BUCKET_NAME") : "palsfdc");
        String regionStr = System.getProperty("bucket.region", System.getenv("BUCKET_REGION"));
        String endpointStr = System.getProperty("bucket.endpoint", System.getenv("BUCKET_ENDPOINT"));
        String accessKeyId = System.getenv("ACCESS_KEY_ID");
        if (accessKeyId == null) {
            accessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
        }
        String secretAccessKey = System.getenv("SECRET_ACCESS_KEY");
        if (secretAccessKey == null) {
            secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        }
        String sessionToken = System.getenv("SESSION_TOKEN");
        if (sessionToken == null) {
            sessionToken = System.getenv("AWS_SESSION_TOKEN");
        }

        AwsCredentialsProvider credentialsProvider;
        if (accessKeyId != null && secretAccessKey != null) {
            credentialsProvider = StaticCredentialsProvider.create(
                    AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken));
        } else {
            credentialsProvider = DefaultCredentialsProvider.create();
        }

        S3Client s3Client;
        {
            var b = S3Client.builder().credentialsProvider(credentialsProvider);
            if (regionStr != null && !regionStr.trim().isEmpty()) {
                b.region(Region.of(regionStr));
            }
            if (endpointStr != null && !endpointStr.trim().isEmpty()) {
                b.endpointOverride(URI.create(endpointStr));
            }
            s3Client = b.build();
        }

        System.out.println("Bucket: " + bucketName + (regionStr != null ? "  Region: " + regionStr : ""));
        try (S3Client s3 = s3Client) {
            GetBucketEncryptionResponse response = s3.getBucketEncryption(
                    GetBucketEncryptionRequest.builder().bucket(bucketName).build());
            if (response.serverSideEncryptionConfiguration() == null
                    || response.serverSideEncryptionConfiguration().rules() == null
                    || response.serverSideEncryptionConfiguration().rules().isEmpty()) {
                System.out.println("Bucket default encryption: (no rules)");
                return;
            }
            ServerSideEncryptionRule rule = response.serverSideEncryptionConfiguration().rules().get(0);
            ServerSideEncryptionByDefault byDefault = rule.applyServerSideEncryptionByDefault();
            if (byDefault == null) {
                System.out.println("Bucket default encryption: (rule has no applyServerSideEncryptionByDefault)");
                return;
            }
            String sseAlgo = byDefault.sseAlgorithmAsString();
            String kmsKeyId = byDefault.kmsMasterKeyID();
            System.out.println("Bucket default encryption: SSE=" + sseAlgo + (kmsKeyId != null ? "  KMS key ID=" + kmsKeyId : ""));
            if ("aws:kms".equals(sseAlgo) && kmsKeyId != null && !kmsKeyId.isEmpty()) {
                if (kmsKeyId.startsWith("arn:") || !"aws/s3".equals(kmsKeyId)) {
                    System.out.println("  -> Customer-managed KMS key. Upload without kmsKeyId requires kms:GenerateDataKey (and kms:Decrypt for multipart) on this key.");
                } else {
                    System.out.println("  -> AWS managed key (aws/s3). Typically no extra KMS permissions needed for upload.");
                }
            } else if ("AES256".equals(sseAlgo)) {
                System.out.println("  -> SSE-S3. No KMS permissions needed for upload.");
            }
        } catch (S3Exception e) {
            if ("ServerSideEncryptionConfigurationNotFoundError".equals(e.awsErrorDetails().errorCode())) {
                System.out.println("Bucket default encryption: (none) — bucket has no default encryption configured.");
                System.out.println("  -> Upload without kmsKeyId will not apply SSE; no KMS permissions needed.");
            } else {
                System.out.println("Failed to get bucket encryption: " + e.awsErrorDetails().errorCode() + " — " + e.getMessage());
            }
        } catch (Exception e) {
            System.out.println("Failed to get bucket encryption: " + e.getMessage());
        }
    }

    /**
     * Prints which key S3 actually used to encrypt an object (evidence for AWS managed vs CMK).
     * HeadObject returns serverSideEncryption and ssekmsKeyId — the key ID is proof of which key was used.
     */
    public static void printObjectEncryptionEvidence(String bucketName, String key) {
        String provider = getProvider();
        if (!"aws".equals(provider)) {
            return;
        }
        String regionStr = System.getProperty("bucket.region", System.getenv("BUCKET_REGION"));
        String endpointStr = System.getProperty("bucket.endpoint", System.getenv("BUCKET_ENDPOINT"));
        String accessKeyId = System.getenv("ACCESS_KEY_ID");
        if (accessKeyId == null) {
            accessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
        }
        String secretAccessKey = System.getenv("SECRET_ACCESS_KEY");
        if (secretAccessKey == null) {
            secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        }
        String sessionToken = System.getenv("SESSION_TOKEN");
        if (sessionToken == null) {
            sessionToken = System.getenv("AWS_SESSION_TOKEN");
        }
        AwsCredentialsProvider credentialsProvider;
        if (accessKeyId != null && secretAccessKey != null) {
            credentialsProvider = StaticCredentialsProvider.create(
                    AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken));
        } else {
            credentialsProvider = DefaultCredentialsProvider.create();
        }
        S3Client s3Client;
        {
            var b = S3Client.builder().credentialsProvider(credentialsProvider);
            if (regionStr != null && !regionStr.trim().isEmpty()) {
                b.region(Region.of(regionStr));
            }
            if (endpointStr != null && !endpointStr.trim().isEmpty()) {
                b.endpointOverride(URI.create(endpointStr));
            }
            s3Client = b.build();
        }
        try (S3Client s3 = s3Client) {
            HeadObjectResponse head = s3.headObject(HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build());
            String sse = head.serverSideEncryptionAsString();
            String kmsKeyId = head.ssekmsKeyId();
            System.out.println("Evidence (HeadObject on uploaded object): SSE=" + sse + (kmsKeyId != null ? "  KMS key ID=" + kmsKeyId : ""));
            if (kmsKeyId != null && (kmsKeyId.contains("aws/s3") || !kmsKeyId.startsWith("arn:"))) {
                System.out.println("  -> This is the AWS managed key (aws/s3). Proof that we did NOT use the bucket default CMK.");
            } else if (kmsKeyId != null && kmsKeyId.startsWith("arn:")) {
                System.out.println("  -> This is a customer-managed key (CMK). Same as bucket default when no SSE headers were sent.");
            }
        } catch (Exception e) {
            System.out.println("Could not get object encryption evidence: " + e.getMessage());
        }
    }

    /**
     * Reproduces the KMS bucket-default encryption issue discussed in the thread:
     * - Upload WITHOUT kmsKeyId (no SSE headers) so S3 uses bucket default encryption.
     * - If the bucket has default SSE-KMS with a customer key, the uploader must have
     *   kms:GenerateDataKey (and kms:Decrypt for multipart) on that key.
     * - Without that permission, S3 returns 403.
     * Run with: -Dexec.args="kms-repro" to run only this test.
     */
    public static void reproduceKmsBucketDefaultIssue() {
        System.out.println("=== KMS BUCKET DEFAULT ENCRYPTION REPRODUCTION ===");
        System.out.println("This test uploads WITHOUT setting kmsKeyId (no SSE headers in request).");
        System.out.println("S3 will use the bucket's default encryption (if configured).");
        System.out.println("If bucket default is SSE-KMS with a customer key, the uploader role must have");
        System.out.println("kms:GenerateDataKey (and kms:Decrypt for multipart) on that key.");
        System.out.println("Without it, you get 403: User is not authorized to perform kms:GenerateDataKey...");
        System.out.println();

        // Show why upload might succeed or fail for this bucket
        investigateBucketEncryption();
        System.out.println();

        BucketClient client = getBucketClient(getProvider());
        String key = "kms-repro-" + System.currentTimeMillis();
        byte[] content = ("KMS bucket default repro " + key).getBytes();

        // Explicitly do NOT set kmsKeyId - MultiCloudJ sends no SSE headers; S3 uses bucket default
        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(key)
                .withMetadata(Map.of("test", "kms-bucket-default-repro"))
                .build();

        try {
            UploadResponse response = client.upload(uploadRequest, new ByteArrayInputStream(content));
            System.out.println("SUCCESS: Upload completed. Key=" + response.getKey() + " ETag=" + response.getETag());
            System.out.println("(Bucket default encryption was applied; your role has the required KMS permissions.)");
        } catch (Exception e) {
            // Surface HTTP 403 clearly (S3 returns 403 for kms:GenerateDataKey denied)
            Throwable t = e;
            while (t != null) {
                if (t instanceof S3Exception) {
                    S3Exception s3 = (S3Exception) t;
                    System.out.println();
                    System.out.println("*** HTTP " + s3.statusCode() + " Forbidden ***");
                    System.out.println("(S3 returned this status; the exception message below has the full details.)");
                    System.out.println();
                    break;
                }
                t = t.getCause();
            }
            System.out.println("FAILED: " + e.getMessage());
            if (e.getCause() != null) {
                System.out.println("Cause: " + e.getCause().getMessage());
            }
            if (e.getMessage() != null && e.getMessage().contains("kms:GenerateDataKey")) {
                System.out.println();
                System.out.println(">>> This is the issue: uploader needs kms:GenerateDataKey on the bucket default key.");
                System.out.println(">>> Grant the uploader role kms:GenerateDataKey and kms:Decrypt on that key (IAM or key policy).");
            }
            e.printStackTrace();
        }
        System.out.println("=== END KMS REPRODUCTION ===");
    }

    /**
     * Reproduces the client's workaround: upload with serverSideEncryption(AWS_KMS) but NO key ID.
     * S3 then uses the AWS managed key (aws/s3), not the bucket's default CMK, so the uploader
     * typically does NOT need kms:GenerateDataKey. Run with: -Dexec.args="kms-repro-aws-managed"
     */
    public static void reproduceAwsManagedKmsNoKeyRequired() {
        System.out.println("=== AWS MANAGED KMS (no key ID) REPRODUCTION ===");
        System.out.println("Upload WITH serverSideEncryption(aws:kms) but NO x-amz-server-side-encryption-aws-kms-key-id.");
        System.out.println();
        System.out.println("--- WHY no kms:GenerateDataKey is needed ---");
        System.out.println("1. When we send NO SSE headers:");
        System.out.println("   S3 applies the BUCKET DEFAULT encryption. If that is SSE-KMS with a customer-managed key (CMK),");
        System.out.println("   S3 uses that CMK to generate the data key. Your IAM principal must have kms:GenerateDataKey on that CMK.");
        System.out.println("2. When we send x-amz-server-side-encryption: aws:kms and NO key ID (this path):");
        System.out.println("   S3 uses the AWS MANAGED key (alias aws/s3), NOT the bucket default. That key is owned by AWS;");
        System.out.println("   S3 calls KMS on your behalf using aws/s3, so your principal does not need kms:GenerateDataKey on any CMK.");
        System.out.println("3. When we send aws:kms WITH a key ID: S3 uses that specific CMK → kms:GenerateDataKey required on that key.");
        System.out.println("---");
        System.out.println();

        investigateBucketEncryption();
        System.out.println();

        BucketClient client = getBucketClient(getProvider());
        String key = "kms-aws-managed-" + System.currentTimeMillis();
        byte[] content = ("AWS managed KMS repro " + key).getBytes();

        UploadRequest uploadRequest = UploadRequest.builder()
                .withKey(key)
                .withMetadata(Map.of("test", "kms-aws-managed-repro"))
                .build();

        try {
            UploadResponse response = client.upload(uploadRequest, new ByteArrayInputStream(content));
            System.out.println("SUCCESS: Upload completed. Key=" + response.getKey() + " ETag=" + response.getETag());
            System.out.println("(Object encrypted with AWS managed key aws/s3; no kms:GenerateDataKey on a CMK was required.)");
            String bucketName = System.getProperty("bucket.name", System.getenv("BUCKET_NAME") != null ? System.getenv("BUCKET_NAME") : "palsfdc");
            printObjectEncryptionEvidence(bucketName, response.getKey());
        } catch (Exception e) {
            Throwable t = e;
            while (t != null) {
                if (t instanceof S3Exception) {
                    S3Exception s3 = (S3Exception) t;
                    System.out.println();
                    System.out.println("*** HTTP " + s3.statusCode() + " ***");
                    System.out.println();
                    break;
                }
                t = t.getCause();
            }
            System.out.println("FAILED: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("=== END AWS MANAGED KMS REPRODUCTION ===");
    }

    /**
     * Main method to test directory operations
     */
    public static void main(String[] args) {
        // Run only KMS bucket-default reproduction if requested
        if (args.length > 0 && "kms-repro".equals(args[0])) {
            reproduceKmsBucketDefaultIssue();
            return;
        }
        if (args.length > 0 && "kms-repro-aws-managed".equals(args[0])) {
            reproduceAwsManagedKmsNoKeyRequired();
            return;
        }

        // Wrangler credential issue reproduction and pre-warm tests
        String wranglerMode = System.getProperty("wrangler.test.mode");
        if (wranglerMode != null && !wranglerMode.isEmpty()) {
            System.out.println("=== WRANGLER TEST MODE: " + wranglerMode + " ===");
            System.out.println("Provider: " + getProvider());
            String assumeRoleArn = System.getProperty("assume.role.arn", System.getenv("ASSUME_ROLE_ARN"));
            if (("reproduce".equalsIgnoreCase(wranglerMode) || "prewarm".equalsIgnoreCase(wranglerMode)
                    || "credential-issue".equalsIgnoreCase(wranglerMode)
                    || "fix-verify".equalsIgnoreCase(wranglerMode)
                    || "fix-verify-interrupt".equalsIgnoreCase(wranglerMode))
                    && (assumeRoleArn == null || assumeRoleArn.trim().isEmpty())) {
                System.err.println("For reproduce/prewarm/credential-issue/fix-verify set -Dassume.role.arn=<ARN> or ASSUME_ROLE_ARN (role your AWS_* creds can assume)");
                throw new IllegalArgumentException("assume.role.arn required for wrangler.test.mode=" + wranglerMode);
            }
            try {
                switch (wranglerMode.toLowerCase()) {
                    case "reproduce":
                        reproduceAssumeRoleInterrupt();
                        break;
                    case "credential-issue":
                        testReproduceCredentialIssue();
                        break;
                    case "fix-verify":
                        testAssumeRoleFirstUploadSucceeds();
                        break;
                    case "fix-verify-interrupt":
                        testAssumeRoleSingleThreadInterruptNoCacheMessage();
                        break;
                    case "prewarm":
                        prewarmThenUpload();
                        break;
                    case "upload":
                        upload();
                        break;
                    default:
                        System.err.println("Unknown wrangler.test.mode: " + wranglerMode
                                + " (use credential-issue, reproduce, fix-verify, fix-verify-interrupt, prewarm, or upload)");
                }
            } catch (Exception e) {
                System.err.println("Wrangler test failed: " + e.getMessage());
                e.printStackTrace();
                throw e;
            }
            return;
        }

        System.out.println("=== STARTING DIRECTORY OPERATIONS TEST ===");
        System.out.println("Provider: " + getProvider());
        
        try {
            // Run directory operations first (so they run even if bucket has KMS restrictions)
            System.out.println("=== Creating Test Directory ===");
            createTestDirectory();
            
            System.out.println("=== Testing Directory Upload (with tags) ===");
            uploadDirectory();

            getLogger().info("=== Testing Directory Download ===");
            downloadDirectory();
                       
            // TEMPORARY: Do not delete directoryTagTest/ so you can see it in the S3 console.
            // To re-enable delete, uncomment the next 2 lines and run with -Dskip.directory.delete=false to actually delete.
            System.out.println("=== Skipping Directory Delete; directoryTagTest/ left in bucket (check S3 console) ===");
            // getLogger().info("=== Testing Directory Delete ===");
            // deleteDirectory();
            
            System.out.println("Directory operations test completed!");

            // KMS bucket-default reproduction (may fail if no kms:GenerateDataKey on bucket key)
            try {
                System.out.println("=== KMS bucket default reproduction (upload without kmsKeyId) ===");
                reproduceKmsBucketDefaultIssue();
                System.out.println();
            } catch (Exception e) {
                System.out.println("KMS reproduction skipped or failed: " + e.getMessage());
            }

            // Single upload (may fail with same KMS restrictions)
            try {
                upload();
            } catch (Exception e) {
                System.out.println("Single upload skipped or failed: " + e.getMessage());
            }

            // Test multipart upload
            System.out.println("=== Testing Multipart Upload ===");
            fullMultipartUploadExample();
            System.out.println("Multipart upload test completed!");
            
        } catch (Exception e) {
            System.out.println("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}