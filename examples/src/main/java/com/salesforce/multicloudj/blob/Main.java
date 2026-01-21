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

public class Main {
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
        if (!java.nio.file.Files.exists(testDir)) {
            System.out.println("ERROR: Test directory does not exist at " + testDir);
            return;
        }
        System.out.println("Test directory exists: " + testDir);

        // Create a DirectoryUploadRequest
        DirectoryUploadRequest request = DirectoryUploadRequest.builder()
                .localSourceDirectory("/tmp/test-directory")  // Change this to your test directory
                .prefix("uploads/")
                .includeSubFolders(true)
                .build();

        System.out.println("DirectoryUploadRequest created: " + request);

        try {
            System.out.println("Calling asyncClient.uploadDirectory()...");
            
            // Use async method and wait for completion
            CompletableFuture<DirectoryUploadResponse> future = asyncClient.uploadDirectory(request);
            System.out.println("Got CompletableFuture, waiting for completion...");
            DirectoryUploadResponse response = future.get(); // Block until completion

            // Log the response
            System.out.println("Directory upload completed");
            System.out.println("Failed transfers: " + response.getFailedTransfers().size());
            
            if (!response.getFailedTransfers().isEmpty()) {
                System.out.println("Some files failed to upload:");
                response.getFailedTransfers().forEach(failure -> {
                    System.out.println("Failed: " + failure.getSource() + " - " + failure.getException().getMessage());
                });
            } else {
                System.out.println("All files uploaded successfully!");
            }
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
                .prefixToDownload("uploads/")
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
            CompletableFuture<Void> future = asyncClient.deleteDirectory("uploads/");
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

    private static BucketClient getBucketClient(String provider) {
        // Get configuration from environment variables or system properties
        String bucketName = System.getProperty("bucket.name", "palsfdc");
        String region = System.getProperty("bucket.region", System.getenv("BUCKET_REGION"));
        String endpoint = System.getProperty("bucket.endpoint", System.getenv("BUCKET_ENDPOINT"));
        String proxyEndpoint = System.getProperty("bucket.proxy.endpoint", System.getenv("BUCKET_PROXY_ENDPOINT"));
        
        if (bucketName == null || bucketName.trim().isEmpty()) {
            throw new IllegalArgumentException("Bucket name must be provided via 'bucket.name' system property or 'BUCKET_NAME' environment variable");
        }
        
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
        
        // Add credentials if available (for providers that need them)
        String accessKeyId = System.getenv("ACCESS_KEY_ID");
        String secretAccessKey = System.getenv("SECRET_ACCESS_KEY");
        String sessionToken = System.getenv("SESSION_TOKEN");
        
        if (accessKeyId != null && secretAccessKey != null) {
            StsCredentials credentials = new StsCredentials(accessKeyId, secretAccessKey, sessionToken);
            CredentialsOverrider credsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                    .withSessionCredentials(credentials).build();
            builder.withCredentialsOverrider(credsOverrider);
        }
        
        return builder.build();
    }


    private static AsyncBucketClient getAsyncBucketClient(String provider) {
        // Get configuration from environment variables or system properties
        String bucketName = System.getProperty("bucket.name", System.getenv("BUCKET_NAME"));
        String region = System.getProperty("bucket.region", System.getenv("BUCKET_REGION"));
        
        if (bucketName == null || bucketName.trim().isEmpty()) {
            throw new IllegalArgumentException("Bucket name must be provided via 'bucket.name' system property or 'BUCKET_NAME' environment variable");
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
        // Change this to test different providers
        return "gcp";  // or "aws" or "ali"
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
     * Main method to test directory operations
     */
    public static void main(String[] args) {
        System.out.println("=== STARTING DIRECTORY OPERATIONS TEST ===");
        System.out.println("Provider: " + getProvider());
        
        try {
            upload();
            // Create test directory first
            System.out.println("=== Creating Test Directory ===");
            //createTestDirectory();
            
            // Test directory upload only
            System.out.println("=== Testing Directory Upload ===");
            //uploadDirectory();

            // Test directory download
            getLogger().info("=== Testing Directory Download ===");
            //downloadDirectory();
                       
            // Test directory delete
            getLogger().info("=== Testing Directory Delete ===");
            //deleteDirectory();
            
            System.out.println("Directory operations test completed!");

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