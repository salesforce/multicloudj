# Ali OSS SDK v1 to v2 API Migration

Complete list of v1 SDK API calls that need to be migrated to v2 across all Ali blob classes.

## AliBlobStore

| # | Operation | v1 API (`ossClient.*`) | v2 Equivalent | Conformance Tests | Status |
|---|-----------|------------------------|---------------|-------------------|--------|
| 1 | getMetadata | `getObjectMetadata(GenericRequest)` | `headObject(HeadObjectRequest, OperationOptions)` | `testGetMetadata`, `testGetMetadataBlobNotExists`, `testGetVersionedMetadata` | Done |
| 2 | upload (stream) | `putObject(PutObjectRequest)` | `putObject(PutObjectRequest, OperationOptions)` | `testUpload_nullKey`, `testUpload_emptyKey`, `testUpload_emptyContent`, `testUpload_happyPath`, `testUploadWithContentType`, `testDownloadWithKmsKey` | Done |
| 3 | upload (file) | `putObject(PutObjectRequest)` | `putObject(PutObjectRequest, OperationOptions)` | (same as above — shared upload path) | Done |
| 4 | download | `getObject(GetObjectRequest)` | `getObject(GetObjectRequest, OperationOptions)` | `testDownload_happy`, `testDownload_createParentPath`, `testVersionedDownload_happy`, `testVersionedDownload_noVersionId`, `testVersionedDownload_badVersionId`, `testDownloadWithKmsKey`, `testDownload_checkArchived` | Done |
| 5 | delete (single) | `deleteObject(bucket, key)` | `deleteObject(DeleteObjectRequest, OperationOptions)` | `testDelete`, `testVersionedDelete`, `testVersionedDelete_fileDoesNotExist` | Done |
| 6 | delete (versioned) | `deleteVersion(bucket, key, versionId)` | `deleteObject(DeleteObjectRequest, OperationOptions)` with versionId | `testVersionedDelete` | Done |
| 7 | delete (batch) | `deleteObjects(DeleteObjectsRequest)` | `deleteMultipleObjects(DeleteMultipleObjectsRequest, OperationOptions)` | `testBulkDelete` | Done |
| 8 | delete (batch versioned) | `deleteVersions(DeleteVersionsRequest)` | `deleteMultipleObjects(DeleteMultipleObjectsRequest, OperationOptions)` | `testBulkVersionedDelete_happyPath`, `testBulkVersionedDelete_happyPathWithNonExisting`, `testBulkVersionedDelete_duplicateDeletion` | Done |
| 9 | copy | `copyObject(CopyObjectRequest)` | `copyObject(CopyObjectRequest, OperationOptions)` | `testCopy`, `testVersionedCopy`, `testCopyFrom`, `testVersionedCopyFrom` | Done |
| 10 | doesObjectExist | `doesObjectExist(GenericRequest)` | `doesObjectExist(GetObjectMetaRequest)` | `testDoesObjectExist`, `testDoesObjectExist_versioned` | Done |
| 11 | doesBucketExist | `doesBucketExist(bucket)` | `doesBucketExist(bucket)` | `testDoesBucketExist`, `testDoesBucketExist_NonExistentBucket` | Done |
| 12 | initiate multipart | `initiateMultipartUpload(InitiateMultipartUploadRequest)` | `initiateMultipartUpload(InitiateMultipartUploadRequest, OperationOptions)` | `testMultipartUpload_singlePart`, `testMultipartUpload_multipleParts`, `testMultipartUpload_withKms`, `testMultipartUpload_withTags`, `testMultipartUpload_withContentType` | Done |
| 13 | upload part | `uploadPart(UploadPartRequest)` | `uploadPart(UploadPartRequest, OperationOptions)` | (same as #12 — part of multipart flow) | Done |
| 14 | complete multipart | `completeMultipartUpload(CompleteMultipartUploadRequest)` | `completeMultipartUpload(CompleteMultipartUploadRequest, OperationOptions)` | (same as #12 — part of multipart flow) | Done |
| 15 | list parts | `listParts(ListPartsRequest)` | `listParts(ListPartsRequest, OperationOptions)` | `testMultipartUpload_duplicateParts` | Done |
| 16 | abort multipart | `abortMultipartUpload(AbortMultipartUploadRequest)` | `abortMultipartUpload(AbortMultipartUploadRequest, OperationOptions)` | `testMultipartUpload_completeAnAbortedUpload` | Done |
| 17 | getTags | `getObjectTagging(bucket, key)` | `getObjectTagging(GetObjectTaggingRequest, OperationOptions)` | `testTagging` | Done |
| 18 | setTags | `setObjectTagging(bucket, key, TagSet)` | `putObjectTagging(PutObjectTaggingRequest, OperationOptions)` | `testTagging` | Done |
| 19 | presigned URL (upload) | `generatePresignedUrl(GeneratePresignedUrlRequest)` | `presign(PutObjectRequest, PresignOptions)` | `testGeneratePresignedUploadUrl_happyPath*`, `testPresignedUrlWithKmsKey_*` | Done |
| 20 | presigned URL (download) | `generatePresignedUrl(GeneratePresignedUrlRequest)` | `presign(GetObjectRequest, PresignOptions)` | `testGeneratePresignedDownloadUrl_happyPath`, `testGeneratePresignedDownloadUrl_nonExistingFile` | Done |
| 21 | close | `ossClient.shutdown()` | `ossV2Client.close()` (AutoCloseable) | N/A (lifecycle) | Done |

## BlobInfoIterator

| # | Operation | v1 API (`ossClient.*`) | v2 Equivalent | Conformance Tests | Status |
|---|-----------|------------------------|---------------|-------------------|--------|
| 22 | list objects | `listObjects(ListObjectsRequest)` | `listObjectsV2(ListObjectsV2Request, OperationOptions)` | `testList`, `testListPage`, `testListPage_WithDelimiter_*`, `testListPage_withTimeStamp` | Done |

## AliBlobClient

| # | Operation | v1 API (`ossClient.*`) | v2 Equivalent | Conformance Tests | Status |
|---|-----------|------------------------|---------------|-------------------|--------|
| 23 | listBuckets | `listBuckets()` | `listBuckets(ListBucketsRequest, OperationOptions)` | N/A (no conformance test) | Pending |
| 24 | createBucket | `createBucket(bucketName)` | `putBucket(PutBucketRequest, OperationOptions)` | `testCreateBucket` (AbstractBlobClientIT) | Done |
| 25 | close | `ossClient.shutdown()` | `ossV2Client.close()` (AutoCloseable) | N/A (lifecycle) | Pending |

## Notes

- All v2 API calls accept an `OperationOptions` parameter (pass `OperationOptions.defaults()` unless customization is needed).
- v2 exceptions: `OperationException` wraps `ServiceException`. Error code extraction: `((ServiceException) operationException.getCause()).errorCode()`.
- v2 uses `com.aliyun.sdk.service.oss2.*` package (not `com.aliyun.sdk.service.oss.*`).
- WireMock conformance test recordings must be re-captured after each API is migrated (v2 sends different HTTP signatures/headers).
