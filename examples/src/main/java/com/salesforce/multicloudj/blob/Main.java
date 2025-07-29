package com.salesforce.multicloudj.blob;

import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
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
        StsCredentials credentials = new StsCredentials(
                System.getenv("ACCESS_KEY_ID"),
                System.getenv("SECRET_ACCESS_KEY"),
                System.getenv("SESSION_TOKEN"));
        CredentialsOverrider credsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION).withSessionCredentials(credentials).build();
        return BucketClient.builder(provider)
                .withBucket("bucket-1")
                .withRegion("us-west-2")
                .withEndpoint(URI.create("https://s3.us-west-2.amazonaws.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:443"))
                .withCredentialsOverrider(credsOverrider).build();
    }

    private static String getProvider() {
        // or "ali"
        return "aws";
    }

    private static InputStream getInputStream() {
        return new ByteArrayInputStream("Upload Content".getBytes());
    }

    private static Logger getLogger() {
        return LoggerFactory.getLogger("Main");
    }
}