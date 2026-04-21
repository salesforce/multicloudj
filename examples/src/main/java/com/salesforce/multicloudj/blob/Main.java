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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  /** Uploads content to a specified provider using the given bucket client. */
  public static void upload() {
    // Get the provider for the storage service
    String provider = getProvider();

    // Create a BucketClient instance based on the provider
    BucketClient client = getBucketClient(provider);

    // Get the input stream for the file to be uploaded
    InputStream content = getInputStream();

    // Build the UploadRequest with the necessary details
    UploadRequest uploadRequest =
        new UploadRequest.Builder()
            .withKey("bucket-path/chameleon.jpg")
            .withMetadata(Map.of("key1", "val1"))
            .withTags(Map.of("tagKey1", "tagVal1"))
            .build();

    // Upload the file to the storage service and store the response
    UploadResponse response = client.upload(uploadRequest, content);

    // Log the upload response
    getLogger().info("received upload response {}", response);
  }

  /** Downloads a blob from a specified bucket using the provided key. */
  public static void downloadBlob() throws FileNotFoundException {
    // Create a DownloadRequest object with the specified object key
    DownloadRequest downloadRequest =
        new DownloadRequest.Builder()
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

  /** Deletes a single Blob from substrate-specific Blob storage by key Object name of the Blob */
  public static void deleteBlob() {
    // Get the BucketClient instance using the provided getBucketClient() method
    BucketClient client = getBucketClient(getProvider());

    // Delete the blob with the key "key-to-be-deleted"
    client.delete("key-to-be-deleted", "version-1");
  }

  /**
   * Deletes a collection of Blobs from a substrate-specific Blob storage specified by a list of
   * keys/versions
   */
  public static void deleteManyBlobs() {
    BucketClient bucketClient = getBucketClient(getProvider());
    // Assume we have a list of keys/versionIds to delete
    // Note: You only need to specify a versionId if you're wanting to delete a specific version
    Collection<BlobIdentifier> blobsToDelete =
        Arrays.asList(
            new BlobIdentifier("key1", null),
            new BlobIdentifier("key2", "version2"),
            new BlobIdentifier("key3", "version3"));
    // Delete the blobs from the bucket
    bucketClient.delete(blobsToDelete);
    getLogger().info("Blobs deleted successfully.");
  }

  /** Copies a Blob from one bucket to other bucket */
  public static void copyFromBucketToBucket() {
    BucketClient bucketClient = getBucketClient(getProvider());

    CopyRequest copyRequest =
        CopyRequest.builder()
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
    getLogger()
        .info("copied blob has a lastModified timestamp of {}", copyResponse.getLastModified());
  }

  public static void generatePresignedUrl() {
    // Get the BucketClient instance using the getBucketClient() method
    BucketClient client = getBucketClient(getProvider());

    // Create a PresignedUrlRequest builder
    PresignedUrlRequest requestBuilder =
        PresignedUrlRequest.builder()

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

  /** Retrieves the metadata for the specified object for which to retrieve the metadata t */
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

  /** Retrieves the list of Blob in the bucket and returns an iterator of @{BlobInfo} */
  public static void getBlobListInBucket() {
    // Get the BucketClient instance using the getBucketClient method
    BucketClient bucketClient = getBucketClient(getProvider());

    // Create a ListBlobsRequest to list blobs with a prefix
    ListBlobsRequest request = new ListBlobsRequest.Builder().withPrefix("folder/").build();

    // Iterate through the blob list using the bucket client and request
    Iterator<BlobInfo> blobIterator = bucketClient.list(request);

    // Process each blob in the list
    while (blobIterator.hasNext()) {
      BlobInfo blobInfo = blobIterator.next();
      getLogger().info("Blob key: {}", blobInfo.getKey());
      getLogger().info("Blob size: {}", blobInfo.getObjectSize());
    }
  }

  /** Initiates a multipartUpload for a Blob */
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

  /** Demonstrates a full multipart upload lifecycle: initiate, upload parts, complete. */
  public static void fullMultipartUploadExample() {
    BucketClient bucketClient = getBucketClient(getProvider());
    String key = "multipart-example-" + System.currentTimeMillis();

    getLogger().info("Starting full multipart upload example for key: {}", key);

    // 1. Initiate
    MultipartUploadRequest request =
        new MultipartUploadRequest.Builder()
            .withKey(key)
            .withMetadata(Map.of("type", "multipart-example"))
            .build();

    MultipartUpload upload = bucketClient.initiateMultipartUpload(request);
    getLogger().info("Initiated multipart upload: id={}", upload.getId());

    try {
      List<UploadPartResponse> partResponses = new ArrayList<>();

      // 2. Upload Parts (Example: 2 parts, 5MB each to meet minimum part size requirements for some
      // providers)
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
      MultipartUploadResponse response =
          bucketClient.completeMultipartUpload(upload, partResponses);
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

  /** This method retrieves all tags associated with a specific blob. */
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

  /** This method sets tags on a blob. This replaces all existing tags. */
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

  // Shared constants used by the directory-operation example flow so that the upload,
  // download, delete and verification steps all agree on the same paths/keys.
  private static final String LOCAL_SOURCE_DIRECTORY = "/tmp/test-directory";
  private static final String LOCAL_DOWNLOAD_DIRECTORY = "/tmp/downloaded-directory";
  private static final String REMOTE_PREFIX = "uploads/";
  private static final Map<String, String> EXPECTED_FILE_CONTENT =
      Map.of(
          "file1.txt", "Hello from file1",
          "file2.txt", "Hello from file2",
          "subdir/file3.txt", "Hello from subdir/file3");
  private static final Map<String, String> DIRECTORY_TAGS =
      Map.of("env", "example", "owner", "multicloudj");

  /**
   * Uploads the local test directory ({@value #LOCAL_SOURCE_DIRECTORY}) to the bucket
   * under prefix {@value #REMOTE_PREFIX}. This exercises the provider's directory-upload
   * code path (on GCP, backed by {@code TransferManager.uploadFiles}) and includes the
   * same set of tags on every file.
   */
  public static void uploadDirectory() {
    System.out.println("Creating AsyncBucketClient for provider: " + getProvider());
    AsyncBucketClient asyncClient = getAsyncBucketClient(getProvider());

    java.nio.file.Path testDir = java.nio.file.Paths.get(LOCAL_SOURCE_DIRECTORY);
    if (!java.nio.file.Files.exists(testDir)) {
      throw new IllegalStateException("Missing local source directory: " + testDir);
    }

    DirectoryUploadRequest request =
        DirectoryUploadRequest.builder()
            .localSourceDirectory(LOCAL_SOURCE_DIRECTORY)
            .prefix(REMOTE_PREFIX)
            .includeSubFolders(true)
            .followSymbolicLinks(false)
            .tags(DIRECTORY_TAGS)
            .build();

    try {
      System.out.println("Calling asyncClient.uploadDirectory() with prefix=" + REMOTE_PREFIX);
      CompletableFuture<DirectoryUploadResponse> future = asyncClient.uploadDirectory(request);
      DirectoryUploadResponse response = future.get();

      System.out.println(
          "Directory upload completed; failedTransfers=" + response.getFailedTransfers().size());

      if (!response.getFailedTransfers().isEmpty()) {
        response
            .getFailedTransfers()
            .forEach(
                failure ->
                    System.out.println(
                        "Upload failed for "
                            + failure.getSource()
                            + ": "
                            + failure.getException().getMessage()));
        throw new IllegalStateException(
            "Directory upload produced "
                + response.getFailedTransfers().size()
                + " failures");
      }

      verifyUploadedBlobsAndTags();
    } catch (Exception e) {
      throw new RuntimeException("Directory upload failed: " + e.getMessage(), e);
    }
  }

  /**
   * Verifies that every file we expected to upload now exists in the bucket, and that the
   * tags specified in the upload request were applied to every uploaded blob. Uses the
   * synchronous {@link BucketClient} because it exposes {@code doesObjectExist} and
   * {@code getTags} directly.
   */
  private static void verifyUploadedBlobsAndTags() {
    BucketClient client = getBucketClient(getProvider());
    for (String relative : EXPECTED_FILE_CONTENT.keySet()) {
      String key = REMOTE_PREFIX + relative;
      if (!client.doesObjectExist(key, null)) {
        throw new IllegalStateException("Uploaded blob missing: " + key);
      }
      Map<String, String> actual = client.getTags(key);
      if (actual == null || !actual.equals(DIRECTORY_TAGS)) {
        throw new IllegalStateException(
            "Tags on " + key + " do not match. expected=" + DIRECTORY_TAGS + " actual=" + actual);
      }
      System.out.println("Verified uploaded blob " + key + " with tags " + actual);
    }
  }

  /**
   * Uploads a directory to blob storage with symbolic links enabled. When followSymbolicLinks is
   * true, any symlinks in the directory will be followed and the target files/directories will be
   * uploaded.
   */
  public static void uploadDirectoryWithSymbolicLinks() {
    AsyncBucketClient asyncClient = getAsyncBucketClient(getProvider());

    DirectoryUploadRequest request =
        DirectoryUploadRequest.builder()
            .localSourceDirectory("/tmp/test-directory")
            .prefix("uploads-with-symlinks/")
            .includeSubFolders(true)
            .followSymbolicLinks(true)
            .build();

    try {
      CompletableFuture<DirectoryUploadResponse> future = asyncClient.uploadDirectory(request);
      DirectoryUploadResponse response = future.get();

      getLogger().info("Directory upload with symbolic links completed");
      getLogger().info("Failed transfers: {}", response.getFailedTransfers().size());

      if (!response.getFailedTransfers().isEmpty()) {
        response
            .getFailedTransfers()
            .forEach(
                failure -> {
                  getLogger()
                      .error(
                          "Failed: {} - {}",
                          failure.getSource(),
                          failure.getException().getMessage());
                });
      } else {
        getLogger().info("All files (including symlinked) uploaded successfully!");
      }
    } catch (Exception e) {
      getLogger()
          .error("Directory upload with symbolic links failed: {}", e.getMessage(), e);
    }
  }

  /**
   * Downloads the prefix {@value #REMOTE_PREFIX} from the bucket into a clean local
   * directory and verifies each file landed with the expected content. This exercises
   * the provider's directory-download code path (on GCP, backed by
   * {@code TransferManager.downloadBlobs} with {@code stripPrefix}).
   */
  public static void downloadDirectory() {
    AsyncBucketClient asyncClient = getAsyncBucketClient(getProvider());

    try {
      // Start from a clean destination so stale files can't mask a broken download.
      java.nio.file.Path destRoot = java.nio.file.Paths.get(LOCAL_DOWNLOAD_DIRECTORY);
      safeDeleteLocalDirectory(destRoot);
      java.nio.file.Files.createDirectories(destRoot);

      DirectoryDownloadRequest request =
          DirectoryDownloadRequest.builder()
              .prefixToDownload(REMOTE_PREFIX)
              .localDestinationDirectory(LOCAL_DOWNLOAD_DIRECTORY)
              .build();

      CompletableFuture<DirectoryDownloadResponse> future = asyncClient.downloadDirectory(request);
      DirectoryDownloadResponse response = future.get();

      System.out.println(
          "Directory download completed; failedTransfers="
              + response.getFailedTransfers().size());

      if (!response.getFailedTransfers().isEmpty()) {
        response
            .getFailedTransfers()
            .forEach(
                failure ->
                    System.out.println(
                        "Download failed for "
                            + failure.getDestination()
                            + ": "
                            + failure.getException().getMessage()));
        throw new IllegalStateException(
            "Directory download produced "
                + response.getFailedTransfers().size()
                + " failures");
      }

      verifyDownloadedContent(destRoot);
    } catch (Exception e) {
      throw new RuntimeException("Directory download failed: " + e.getMessage(), e);
    }
  }

  /**
   * Reads every file we expected to download and asserts its content matches the value
   * that was originally written locally before the upload. Catches TransferManager
   * stripPrefix bugs and silent-empty-file bugs.
   */
  private static void verifyDownloadedContent(java.nio.file.Path destRoot) throws IOException {
    for (Map.Entry<String, String> entry : EXPECTED_FILE_CONTENT.entrySet()) {
      java.nio.file.Path localFile = destRoot.resolve(entry.getKey());
      if (!java.nio.file.Files.exists(localFile)) {
        throw new IllegalStateException(
            "Downloaded file missing: " + localFile + " (for remote key " + entry.getKey() + ")");
      }
      String actual =
          new String(
              java.nio.file.Files.readAllBytes(localFile), java.nio.charset.StandardCharsets.UTF_8);
      if (!actual.equals(entry.getValue())) {
        throw new IllegalStateException(
            "Downloaded content mismatch for "
                + localFile
                + ". expected=\""
                + entry.getValue()
                + "\" actual=\""
                + actual
                + "\"");
      }
      System.out.println("Verified downloaded file " + localFile + " (" + actual.length() + " bytes)");
    }
  }

  /**
   * Deletes everything under {@value #REMOTE_PREFIX} from the bucket and verifies that
   * none of the expected keys exist afterwards.
   */
  public static void deleteDirectory() {
    AsyncBucketClient asyncClient = getAsyncBucketClient(getProvider());

    try {
      CompletableFuture<Void> future = asyncClient.deleteDirectory(REMOTE_PREFIX);
      future.get();
      System.out.println("Directory deleted; verifying every uploaded key is gone");

      BucketClient client = getBucketClient(getProvider());
      for (String relative : EXPECTED_FILE_CONTENT.keySet()) {
        String key = REMOTE_PREFIX + relative;
        if (client.doesObjectExist(key, null)) {
          throw new IllegalStateException("Blob still exists after deleteDirectory: " + key);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Directory delete failed: " + e.getMessage(), e);
    }
  }

  /** Recursively deletes a local directory tree. Ignores errors. */
  private static void safeDeleteLocalDirectory(java.nio.file.Path root) {
    if (root == null || !java.nio.file.Files.exists(root)) {
      return;
    }
    try (java.util.stream.Stream<java.nio.file.Path> paths = java.nio.file.Files.walk(root)) {
      paths
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(
              p -> {
                try {
                  java.nio.file.Files.deleteIfExists(p);
                } catch (IOException ignored) {
                  // best-effort cleanup
                }
              });
    } catch (IOException ignored) {
      // best-effort cleanup
    }
  }

  /** This method uploads a part of the multipartUpload operation */
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

  /** This method Completes a multipartUpload */
  public static void completeMultiPartUpload() {
    BucketClient bucketClient = getBucketClient(getProvider());

    // List of UploadPartResponse objects (simulated here, in real use you'd get these from actual
    // uploads)
    List<UploadPartResponse> uploadPartResponses = new ArrayList<>();

    // Create a MultipartUpload object
    MultipartUploadRequest multipartUploadRequest = createMultipartUploadRequest();
    MultipartUpload multipartUpload = bucketClient.initiateMultipartUpload(multipartUploadRequest);
    // Upload some parts here
    MultipartUploadResponse response =
        bucketClient.completeMultipartUpload(multipartUpload, uploadPartResponses);
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

  private static MultipartUpload uploadFileInParts(
      BucketClient bucketClient, MultipartUploadRequest request) {
    // Upload the file in multiple parts using the BucketClient
    return bucketClient.initiateMultipartUpload(request);
  }

  private static BucketClient getBucketClient(String provider) {
    // Get configuration from environment variables or system properties
    String bucketName = System.getProperty("bucket.name", "palsfdc");
    String region = System.getProperty("bucket.region", System.getenv("BUCKET_REGION"));
    String endpoint = System.getProperty("bucket.endpoint", System.getenv("BUCKET_ENDPOINT"));
    String proxyEndpoint =
        System.getProperty("bucket.proxy.endpoint", System.getenv("BUCKET_PROXY_ENDPOINT"));

    if (bucketName == null || bucketName.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Bucket name must be provided via 'bucket.name' system property or 'BUCKET_NAME'"
              + " environment variable");
    }

    BucketClient.BlobBuilder builder = BucketClient.builder(provider).withBucket(bucketName);

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
      CredentialsOverrider credsOverrider =
          new CredentialsOverrider.Builder(CredentialsType.SESSION)
              .withSessionCredentials(credentials)
              .build();
      builder.withCredentialsOverrider(credsOverrider);
    }

    return builder.build();
  }

  private static AsyncBucketClient getAsyncBucketClient(String provider) {
    // Get configuration from environment variables or system properties
    String bucketName = System.getProperty("bucket.name", System.getenv("BUCKET_NAME"));
    String region = System.getProperty("bucket.region", System.getenv("BUCKET_REGION"));

    if (bucketName == null || bucketName.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Bucket name must be provided via 'bucket.name' system property or 'BUCKET_NAME'"
              + " environment variable");
    }

    ExecutorService executorService = Executors.newFixedThreadPool(4);
    AsyncBucketClient.Builder builder =
        AsyncBucketClient.builder(provider)
            .withBucket(bucketName)
            .withExecutorService(executorService);

    if (region != null && !region.trim().isEmpty()) {
      builder.withRegion(region);
    }

    return builder.build();
  }

  private static String getProvider() {
    // Change this to test different providers
    return "gcp"; // or "aws" or "ali"
  }

  private static InputStream getInputStream() {
    return new ByteArrayInputStream("Upload Content".getBytes());
  }

  private static Logger getLogger() {
    return LoggerFactory.getLogger("Main");
  }

  /**
   * Creates a fresh local test directory populated with the fixed file layout described
   * by {@link #EXPECTED_FILE_CONTENT}. Any existing directory at the same path is wiped
   * first so repeated runs don't accumulate stale state.
   */
  public static void createTestDirectory() {
    try {
      java.nio.file.Path testDir = java.nio.file.Paths.get(LOCAL_SOURCE_DIRECTORY);
      safeDeleteLocalDirectory(testDir);
      java.nio.file.Files.createDirectories(testDir);

      for (Map.Entry<String, String> entry : EXPECTED_FILE_CONTENT.entrySet()) {
        java.nio.file.Path file = testDir.resolve(entry.getKey());
        java.nio.file.Files.createDirectories(file.getParent());
        java.nio.file.Files.write(
            file, entry.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8));
      }

      System.out.println("Test directory created at: " + testDir);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create test directory: " + e.getMessage(), e);
    }
  }

  /**
   * Walks through an upload/verify + download/verify + delete/verify cycle against the
   * configured provider. Each step throws on failure so that the overall script exits
   * non-zero if any assertion is violated, giving a clear pass/fail signal for the
   * directory-operation code paths (e.g. the GCP {@code TransferManager} integration).
   *
   * <p>Uses {@link System#out} directly (in addition to the SLF4J logger) so that progress
   * is visible when the example is launched via {@code mvn exec:java} where no SLF4J
   * provider is configured in the examples classpath.
   */
  public static void main(String[] args) {
    System.out.println("=== STARTING DIRECTORY OPERATIONS TEST ===");
    System.out.println("Provider: " + getProvider());

    boolean success = false;
    try {
      System.out.println("=== Creating Test Directory ===");
      createTestDirectory();

      System.out.println("=== Testing Directory Upload (with tags) ===");
      uploadDirectory();
      System.out.println("uploadDirectory: PASS");

      System.out.println("=== Testing Directory Download (with content verification) ===");
      downloadDirectory();
      System.out.println("downloadDirectory: PASS");

      System.out.println("=== Testing Directory Delete (with existence verification) ===");
      deleteDirectory();
      System.out.println("deleteDirectory: PASS");

      success = true;
      System.out.println("=== ALL DIRECTORY OPERATIONS TESTS PASSED ===");
    } catch (Exception e) {
      System.out.println("Directory operations test FAILED: " + e.getMessage());
      e.printStackTrace(System.out);
    } finally {
      // Always clean up local temp directories so repeated runs start clean.
      safeDeleteLocalDirectory(java.nio.file.Paths.get(LOCAL_SOURCE_DIRECTORY));
      safeDeleteLocalDirectory(java.nio.file.Paths.get(LOCAL_DOWNLOAD_DIRECTORY));
    }

    // getAsyncBucketClient() creates a non-daemon FixedThreadPool executor that we
    // don't have a handle on; explicitly exit so {@code mvn exec:java} doesn't hang
    // waiting for those threads to terminate.
    System.exit(success ? 0 : 1);
  }
}
