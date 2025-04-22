# blob_description

Describes client code to interact with the Blob storage service.

## blob_client_init

```
StsCredentials credentials = new StsCredentials(
    System.getenv("ACCESS_KEY_ID"),
    System.getenv("SECRET_ACCESS_KEY"),
    System.getenv("SESSION_TOKEN")
);

CredentialsOverrider credsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
    .withSessionCredentials(credentials)
    .build();

return BucketClient.builder(provider)
    .withBucket("chameleon-jcloud")
    .withRegion("us-west-2")
    .withCredentialsOverrider(credsOverrider)
    .build();
```

```
// constructor injection of Bucket Client
private final Bucketclient bucketClient;

public Main(BucketClient bucketClient) {
        this.bucketClient = bucketClient;
}

```

## blob_upload

```
/**
* Uploads the Blob content to substrate-specific Blob storage
* @param uploadRequest Wrapper, containing upload data
* @return UploadResponse Response object containing the status and ID of the uploaded Blob
*/
public UploadResponse upload(UploadRequest uploadRequest) throws SubstrateSdkException
```

### blob_upload_example

```
// Build the UploadRequest with the necessary details
UploadRequest uploadRequest = new UploadRequest.Builder()
            .withContent(content)
            .build();

// Upload the file to the storage service and store the response using bucket client instance
UploadResponse response = client.upload(uploadRequest);
```

## blob_download

```
/**
* Downloads content based on the specified download request.
* @param downloadRequest The request object containing details about the download.
* @param content The output stream where the downloaded content will be written.
*/
public void download(DownloadRequest downloadRequest, OutputStream content) 
```

## blob_download_example

``` 
// Create a DownloadRequest object with the specified object key
DownloadRequest downloadRequest = new DownloadRequest.Builder()
      .withKey("examples/image1-chameleon.jpg")
      .build();

// Create an OutputStream to save the downloaded content to a file
OutputStream content = new FileOutputStream("/exampleFile.txt");

// Download the blob using the client, download request, and output stream using bucket client instance
client.download(downloadRequest, content);
```

## blob_delete

```
/**
* Deletes a single Blob from substrate-specific Blob storage by key Object name of the Blob.
* @param key Object name of the Blob
*/
public void delete(String key)
```

## blob_delete_example

``` 
// delete the key using bucket client instance
client.delete("key-to-be-deleted");
```

## blob_delete

```
/**
* Deletes a collection of Blobs from a substrate-specific Blob storage.
* @param keys A collection of blob keys to delete
*/
public void delete(Collection<String> keys)
```

## blob_delete_example

``` 
// Assume we have a list of keys to delete
List<String> keysToDelete = Arrays.asList("key1", "key2", "key3");
// Delete the keys from the bucket using bucket client instance
client.delete(keysToDelete);
```

## blob_copy

```
/**
* Copies the Blob from one bucket to other bucket
* @param destBucket The name of the destination bucket
* @param srcKey The name of the source Blob
* @param destKey The name of the destination Blob
* @return ETag of the copied Blob
*/
public String copy(String destBucket, String srcKey, String destKey) 
```

## blob_copy_example

``` 
//destination bucket key to be copied to
String destBucket = "destination-bucket";
//object key of the src blob
String srcKey = "src-key";
//object key of the destination blob
String destKey = "dest-key";

//receive ETag of the copied Blob or null if an exception occurs using bucket client instance
var eTag = bucketClient.copy(destBucket, srcKey, destKey);
```

## blob_getMetadata

```
/**
* Retrieves the metadata of the Blob
* @param key Name of the Blob, whose metadata is to be retrieved
* @return BlobMetadata of the Blob
*/
public BlobMetadata getMetadata(String key)
```

## blob_getMetadata_example

``` 
// reference to the blob key
var blobKey = "blob-key";

// Use the BucketClient instance to get the metadata for the specified blob key using bucket client instance
BlobMetadata metadata = client.getMetadata(blobKey);
```

## blob_list

```
/**
* Retrieves the list of Blob in the bucket
* @return Iterator object of the BlobInfo
*/
public Iterator<BlobInfo> list(ListBlobsRequest request) 
```

## blob_list_example

``` 
// Create a ListBlobsRequest to list blobs with a prefix
ListBlobsRequest request = new ListBlobsRequest.Builder()
        .withPrefix("folder/")
        .build();

// Iterate through the blob list using the bucket client and request
Iterator<BlobInfo> blobIterator = bucketClient.list(request);
```

## blob_initiateMultipartUpload

```
/**
* Initiates a multipartUpload for a Blob
*
* @param request Contains information about the blob to upload
*/
public MultipartUpload initiateMultipartUpload(MultipartUploadRequest request)
```

## blob_initiateMultipartUpload_example

``` 
//Create a request with the required parameters
MultipartUploadRequest request = new MultipartUploadRequest.Builder()
        .withKey("object-key")
        .withMetadata(Map.of("metadata-key", "metadata-value"))
        .build();

// Upload the file in multiple parts using the BucketClient instance
client.initiateMultipartUpload(request);
```

## blob_uploadMultipartPart

```
/**
* Uploads a part of the multipartUpload
* @param mpu The multipartUpload to use
* @param mpp The multipartPart data
*/
public UploadPartResponse uploadMultipartPart(MultipartUpload mpu, MultipartPart mpp)
```

## blob_uploadMultipartPart_example

``` 
// Create a MultipartUpload object
MultipartUpload multipartUpload = new MultipartUpload("multipart-upload", "key", "id");

// Create a MultipartPart object
MultipartPart multipartPart = new MultipartPart(1, "data-part-1".getBytes());

// Call the uploadMultipartPart method using bucketClient
UploadPartResponse response = bucketClient.uploadMultipartPart(multipartUpload, multipartPart);
```

## blob_completeMultipartPartUpload

```
/**
* Completes a multipartUpload
* @param mpu The multipartUpload to use
* @param parts A list of the parts contained in the multipartUpload
*/
public MultipartUploadResponse completeMultipartUpload(MultipartUpload mpu, List<UploadPartResponse> parts)
```

## blob_completeMultipartPartUpload_example

``` 
List<UploadPartResponse> uploadPartResponses = new ArrayList<>();

// Create a MultipartUpload object
MultipartUpload multipartUpload = new MultipartUpload("multipart-upload", "key", "id");

MultipartUploadResponse response = bucketClient.completeMultipartUpload(multipartUpload, uploadPartResponses);
```

## blob_listMultipartUpload

```
/**
* Returns a list of all uploaded parts for the given MultipartUpload
* @param mpu The multipartUpload to query against
*/
public List<UploadPartResponse> listMultipartUpload(MultipartUpload mpu)
```

## blob_listMultipartUpload_example

``` 
// Create a MultipartUpload object
MultipartUpload multipartUpload = new MultipartUpload("multipart-upload", "key", "id");

// Call the listMultipartUpload method using bucketClient
bucketClient.listMultipartUpload(multipartUpload);
```

## blob_abortMultipartUpload

```
/**
* Aborts a multipartUpload
* @param mpu The multipartUpload to abort
*/
public void abortMultipartUpload(MultipartUpload mpu)
```

## blob_abortMultipartUpload_example

``` 
// Create a MultipartUpload object
MultipartUpload multipartUpload = new MultipartUpload("multipart-upload", "key", "id");

// Call the abortMultipartUpload method using bucketClient
bucketClient.abortMultipartUpload(multipartUpload);
```

## blob_getTags

```
/**
* Returns a map of all the tags associated with the blob
* @param key Name of the blob whose tags are to be retrieved
* @return The blob's tags
*/
public Map<String, String> getTags(String key)
```

## blob_getTags_example

``` 
// Retrieve the tags for the blob with the key "blob-key"
Map<String, String> tags = client.getTags("blob-key");
```

## blob_setTags

```
/**
* Sets tags on a blob
* @param key Name of the blob to set tags on
* @param tags The tags to set
*/
public void setTags(String key, Map<String, String> tags)
```

## blob_setTags_example

``` 
// Create a map to store tags
Map<String, String> tags = new HashMap<>();

// Add tag1 and its value to the map
tags.put("tag1", "value1");

// Add tag2 and its value to the map
tags.put("tag2", "value2");

// Set the tags for the specified blob using the client instance
client.setTags("blob-key", tags);
```

## blob_generatePresignedUrl

```
/**
* Generates a presigned URL for uploading/downloading blobs
* @param request The presigned request
* @return Returns the presigned URL
*/
public URL generatePresignedUrl(PresignedUrlRequest request) 
```

## blob_generatePresignedUrl_example

``` 
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

// Generate the presigned URL using the bucketClient instance
URL presignedUrl = client.generatePresignedUrl(requestBuilder);
```
