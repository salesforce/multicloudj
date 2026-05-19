# Ali OSS SDK v1 to v2 API Migration

Complete list of v1 SDK API calls that need to be migrated to v2 across all Ali blob classes.

## AliBlobStore

| # | Operation | v1 API (`ossClient.*`) | v2 Equivalent | Status |
|---|-----------|------------------------|---------------|--------|
| 1 | getMetadata | `getObjectMetadata(GenericRequest)` | `headObject(HeadObjectRequest, OperationOptions)` | Done |
| 2 | upload (stream) | `putObject(PutObjectRequest)` | `putObject(PutObjectRequest, OperationOptions)` | Pending |
| 3 | upload (file) | `putObject(PutObjectRequest)` | `putObject(PutObjectRequest, OperationOptions)` | Pending |
| 4 | download | `getObject(GetObjectRequest)` | `getObject(GetObjectRequest, OperationOptions)` | Pending |
| 5 | delete (single) | `deleteObject(bucket, key)` | `deleteObject(DeleteObjectRequest, OperationOptions)` | Pending |
| 6 | delete (versioned) | `deleteVersion(bucket, key, versionId)` | `deleteObject(DeleteObjectRequest, OperationOptions)` with versionId | Pending |
| 7 | delete (batch) | `deleteObjects(DeleteObjectsRequest)` | `deleteMultipleObjects(DeleteMultipleObjectsRequest, OperationOptions)` | Pending |
| 8 | delete (batch versioned) | `deleteVersions(DeleteVersionsRequest)` | `deleteMultipleObjects(DeleteMultipleObjectsRequest, OperationOptions)` | Pending |
| 9 | copy | `copyObject(CopyObjectRequest)` | `copyObject(CopyObjectRequest, OperationOptions)` | Pending |
| 10 | doesObjectExist | `doesObjectExist(GenericRequest)` | `doesObjectExist(GetObjectMetaRequest)` | Done |
| 11 | doesBucketExist | `doesBucketExist(bucket)` | `doesBucketExist(bucket)` | Done |
| 12 | initiate multipart | `initiateMultipartUpload(InitiateMultipartUploadRequest)` | `initiateMultipartUpload(InitiateMultipartUploadRequest, OperationOptions)` | Pending |
| 13 | upload part | `uploadPart(UploadPartRequest)` | `uploadPart(UploadPartRequest, OperationOptions)` | Pending |
| 14 | complete multipart | `completeMultipartUpload(CompleteMultipartUploadRequest)` | `completeMultipartUpload(CompleteMultipartUploadRequest, OperationOptions)` | Pending |
| 15 | list parts | `listParts(ListPartsRequest)` | `listParts(ListPartsRequest, OperationOptions)` | Pending |
| 16 | abort multipart | `abortMultipartUpload(AbortMultipartUploadRequest)` | `abortMultipartUpload(AbortMultipartUploadRequest, OperationOptions)` | Pending |
| 17 | getTags | `getObjectTagging(bucket, key)` | `getObjectTagging(GetObjectTaggingRequest, OperationOptions)` | Pending |
| 18 | setTags | `setObjectTagging(bucket, key, TagSet)` | `putObjectTagging(PutObjectTaggingRequest, OperationOptions)` | Pending |
| 19 | presigned URL (upload) | `generatePresignedUrl(GeneratePresignedUrlRequest)` | `presign(PutObjectRequest)` | Pending |
| 20 | presigned URL (download) | `generatePresignedUrl(GeneratePresignedUrlRequest)` | `presign(GetObjectRequest)` | Pending |
| 21 | close | `ossClient.shutdown()` | `ossV2Client.close()` (AutoCloseable) | Pending |

## BlobInfoIterator

| # | Operation | v1 API (`ossClient.*`) | v2 Equivalent | Status |
|---|-----------|------------------------|---------------|--------|
| 22 | list objects | `listObjects(ListObjectsRequest)` | `listObjectsV2(ListObjectsV2Request, OperationOptions)` | Pending |

## AliBlobClient

| # | Operation | v1 API (`ossClient.*`) | v2 Equivalent | Status |
|---|-----------|------------------------|---------------|--------|
| 23 | listBuckets | `listBuckets()` | `listBuckets(ListBucketsRequest, OperationOptions)` | Pending |
| 24 | createBucket | `createBucket(bucketName)` | `putBucket(PutBucketRequest, OperationOptions)` | Pending |
| 25 | close | `ossClient.shutdown()` | `ossV2Client.close()` (AutoCloseable) | Pending |

## Notes

- All v2 API calls accept an `OperationOptions` parameter (pass `OperationOptions.defaults()` unless customization is needed).
- v2 exceptions: `OperationException` wraps `ServiceException`. Error code extraction: `((ServiceException) operationException.getCause()).errorCode()`.
- v2 uses `com.aliyun.sdk.service.oss2.*` package (not `com.aliyun.sdk.service.oss.*`).
- WireMock conformance test recordings must be re-captured after each API is migrated (v2 sends different HTTP signatures/headers).
