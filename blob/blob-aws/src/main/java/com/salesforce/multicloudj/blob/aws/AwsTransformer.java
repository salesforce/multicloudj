package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.FailedBlobDownload;
import com.salesforce.multicloudj.blob.driver.FailedBlobUpload;
import com.salesforce.multicloudj.blob.driver.ListBlobsBatch;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import software.amazon.awssdk.services.s3.model.StorageClass;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.common.util.HexUtil;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.transfer.s3.config.DownloadFilter;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryDownload;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryUpload;
import software.amazon.awssdk.transfer.s3.model.DownloadDirectoryRequest;
import software.amazon.awssdk.transfer.s3.model.UploadDirectoryRequest;
import software.amazon.awssdk.core.ResponseInputStream;

import java.io.InputStream;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class AwsTransformer {

    private final String bucket;
    public AwsTransformer(String bucket) {
        this.bucket = bucket;
    }

    public String getBucket() {
        return bucket;
    }

    public ListBlobsBatch toBatch(ListObjectsV2Response response) {
        List<BlobInfo> blobs = response
                .contents()
                .stream()
                .map(this::toInfo)
                .collect(Collectors.toList());

        List<String> prefixes = response
                .commonPrefixes()
                .stream()
                .map(CommonPrefix::prefix)
                .collect(Collectors.toList());

        return new ListBlobsBatch(blobs, prefixes);
    }

    public BlobInfo toInfo(S3Object s3) {
        return new BlobInfo.Builder()
                .withKey(s3.key())
                .withObjectSize(s3.size())
                .build();
    }

    public ListObjectsV2Request toRequest(ListBlobsRequest request) {
        return ListObjectsV2Request
                .builder()
                .bucket(getBucket())
                .delimiter(request.getDelimiter())
                .prefix(request.getPrefix())
                .build();
    }

    public ListObjectsV2Request toRequest(ListBlobsPageRequest request) {
        ListObjectsV2Request.Builder builder = ListObjectsV2Request
                .builder()
                .bucket(getBucket())
                .delimiter(request.getDelimiter())
                .prefix(request.getPrefix());

        if (request.getMaxResults() != null) {
            builder.maxKeys(request.getMaxResults());
        }

        if (request.getPaginationToken() != null) {
            builder.continuationToken(request.getPaginationToken());
        }

        return builder.build();
    }

    public AsyncRequestBody toAsyncRequestBody(UploadRequest uploadRequest, InputStream inputStream) {
        return AsyncRequestBody.fromInputStream(
                inputStream,
                uploadRequest.getContentLength(),
                Executors.newSingleThreadExecutor());
    }

    public PutObjectRequest toRequest(UploadRequest request) {
        List<Tag> tags = request.getTags().entrySet().stream()
                .map(entry -> Tag.builder().key(entry.getKey()).value(entry.getValue()).build())
                .collect(Collectors.toList());
        PutObjectRequest.Builder builder = PutObjectRequest
                .builder()
                .bucket(getBucket())
                .key(request.getKey())
                .metadata(request.getMetadata())
                .tagging(Tagging.builder().tagSet(tags).build());

        if (request.getKmsKeyId() != null && !request.getKmsKeyId().isEmpty()) {
            builder.serverSideEncryption("aws:kms")
                   .ssekmsKeyId(request.getKmsKeyId());
        }
        
        // Set storage class if provided
        if (request.getStorageClass() != null && !request.getStorageClass().isEmpty()) {
            try {
                StorageClass awsStorageClass = StorageClass.fromValue(request.getStorageClass());
                builder.storageClass(awsStorageClass);
            } catch (IllegalArgumentException e) {
                throw new InvalidArgumentException("Invalid storage class: " + request.getStorageClass(), e);
            }
        }
        
        return builder.build();
    }


    public GetObjectRequest toRequest(DownloadRequest request) {
        var builder = GetObjectRequest
                .builder()
                .bucket(getBucket())
                .key(request.getKey())
                .versionId(request.getVersionId());

        if(request.getStart() != null || request.getEnd() != null) {
            builder.range(createRangeString(request.getStart(), request.getEnd()));
        }
        return builder.build();
    }

    /**
     * Reading the first 500 bytes            - createRangeString(0, 500)    -   "bytes=0-500"
     * Reading a middle 500 bytes             - createRangeString(123, 623)  -   "bytes=123-623"
     * Reading the last 500 bytes             - createRangeString(null, 500) -   "bytes=-500"
     * Reading everything but first 500 bytes - createRangeString(500, null) -   "bytes=500-"
     */
    protected String createRangeString(Long start, Long end) {
        return "bytes=" + (start==null ? "" : start) + "-" + (end==null ? "" : end);
    }

    public DownloadResponse toDownloadResponse(DownloadRequest downloadRequest, GetObjectResponse response) {
        return DownloadResponse.builder()
                .key(downloadRequest.getKey())
                .metadata(BlobMetadata.builder()
                        .key(downloadRequest.getKey())
                        .versionId(response.versionId())
                        .eTag(response.eTag())
                        .lastModified(response.lastModified())
                        .metadata(response.metadata())
                        .objectSize(response.contentLength())
                        .build())
                .build();
    }

    public DownloadResponse toDownloadResponse(DownloadRequest downloadRequest, GetObjectResponse response, ResponseInputStream<GetObjectResponse> responseInputStream) {
        return DownloadResponse.builder()
                .key(downloadRequest.getKey())
                .metadata(BlobMetadata.builder()
                        .key(downloadRequest.getKey())
                        .versionId(response.versionId())
                        .eTag(response.eTag())
                        .lastModified(response.lastModified())
                        .metadata(response.metadata())
                        .objectSize(response.contentLength())
                        .build())
                .inputStream(responseInputStream)
                .build();
    }

    public DeleteObjectRequest toDeleteRequest(String key, String versionId) {
        return DeleteObjectRequest
                .builder()
                .bucket(getBucket())
                .key(key)
                .versionId(versionId)
                .build();
    }

    public DeleteObjectsRequest toDeleteRequests(Collection<BlobIdentifier> objects) {
        var objectIds = objects
                .stream()
                .map(object -> ObjectIdentifier.builder().key(object.getKey()).versionId(object.getVersionId()).build())
                .collect(Collectors.toList());

        return DeleteObjectsRequest
                .builder()
                .bucket(getBucket())
                .delete(Delete.builder().objects(objectIds).build())
                .build();
    }

    public CopyObjectRequest toRequest(CopyRequest request) {
        return CopyObjectRequest
                .builder()
                .sourceBucket(getBucket())
                .sourceKey(request.getSrcKey())
                .sourceVersionId(request.getSrcVersionId())
                .destinationBucket(request.getDestBucket())
                .destinationKey(request.getDestKey())
                .build();
    }

    public HeadObjectRequest toHeadRequest(String key, String versionId) {
        return HeadObjectRequest
                .builder()
                .bucket(getBucket())
                .key(key)
                .versionId(versionId)
                .build();
    }

    public BlobMetadata toMetadata(HeadObjectResponse response, String key) {
        Long objectSize = response.contentLength();
        Map<String, String> metadata = response.metadata();
        String eTag = response.eTag();
        return BlobMetadata
                .builder()
                .key(key)
                .versionId(response.versionId())
                .eTag(eTag)
                .objectSize(objectSize)
                .metadata(metadata)
                .lastModified(response.lastModified())
                .md5(eTagToMD5(eTag))
                .build();
    }

    byte[] eTagToMD5(String eTag) {
        if (eTag == null) {
            return new byte[0];
        }

        if (eTag.length() < 2 || eTag.charAt(0) != '"' || eTag.charAt(eTag.length() - 1) != '"') {
            return new byte[0];
        }

        String unquoted = eTag.substring(1, eTag.length() - 1);
        return HexUtil.convertToBytes(unquoted);
    }

    public CreateMultipartUploadRequest toCreateMultipartUploadRequest(MultipartUploadRequest request) {
        return CreateMultipartUploadRequest.builder()
                .bucket(getBucket())
                .key(request.getKey())
                .metadata(request.getMetadata())
                .build();
    }

    public UploadPartRequest toUploadPartRequest(MultipartUpload mpu, MultipartPart mpp) {
        return UploadPartRequest.builder()
                .bucket(getBucket())
                .key(mpu.getKey())
                .uploadId(mpu.getId())
                .partNumber(mpp.getPartNumber())
                .contentLength(mpp.getContentLength())
                .build();
    }

    public CompleteMultipartUploadRequest toCompleteMultipartUploadRequest(MultipartUpload mpu, List<UploadPartResponse> parts) {

        List<CompletedPart> completedParts = parts.stream()
                .sorted(Comparator.comparingInt(UploadPartResponse::getPartNumber))
                .map(part -> CompletedPart.builder()
                        .partNumber(part.getPartNumber())
                        .eTag(part.getEtag())
                        .build())
                .collect(Collectors.toList());

        return CompleteMultipartUploadRequest.builder()
                .bucket(getBucket())
                .key(mpu.getKey())
                .uploadId(mpu.getId())
                .multipartUpload(mp -> mp.parts(completedParts))
                .build();
    }

    public ListPartsRequest toListPartsRequest(MultipartUpload mpu) {
        return ListPartsRequest.builder()
                .bucket(getBucket())
                .key(mpu.getKey())
                .uploadId(mpu.getId())
                .build();
    }

    public AbortMultipartUploadRequest toAbortMultipartUploadRequest(MultipartUpload mpu) {
        return AbortMultipartUploadRequest.builder()
                .bucket(getBucket())
                .key(mpu.getKey())
                .uploadId(mpu.getId())
                .build();
    }

    public GetObjectTaggingRequest toGetObjectTaggingRequest(String key) {
        return GetObjectTaggingRequest.builder()
                .bucket(getBucket())
                .key(key)
                .build();
    }

    public PutObjectTaggingRequest toPutObjectTaggingRequest(String key, Map<String, String> tags) {
        List<Tag> listOfTags = tags.entrySet().stream()
                .map(entry -> Tag.builder().key(entry.getKey()).value(entry.getValue()).build())
                .collect(Collectors.toList());

        return PutObjectTaggingRequest.builder()
                .bucket(getBucket())
                .key(key)
                .tagging(Tagging.builder().tagSet(listOfTags).build())
                .build();
    }

    public PutObjectPresignRequest toPutObjectPresignRequest(PresignedUrlRequest request) {
        UploadRequest.Builder builder = UploadRequest.builder().withKey(request.getKey());
        if(request.getMetadata() != null) {
            builder.withMetadata(request.getMetadata());
        }
        if(request.getTags() != null) {
            builder.withTags(request.getTags());
        }
        if(request.getKmsKeyId() != null) {
            builder.withKmsKeyId(request.getKmsKeyId());
        }
        UploadRequest uploadRequest = builder.build();

        return PutObjectPresignRequest.builder()
                .signatureDuration(request.getDuration())
                .putObjectRequest(toRequest(uploadRequest))
                .build();
    }

    public GetObjectPresignRequest toGetObjectPresignRequest(PresignedUrlRequest request) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(getBucket())
                .key(request.getKey())
                .build();
        return GetObjectPresignRequest.builder()
                .signatureDuration(request.getDuration())
                .getObjectRequest(getObjectRequest)
                .build();
    }

    public DownloadDirectoryRequest toDownloadDirectoryRequest(DirectoryDownloadRequest request) {
        var downloadDirectoryRequestBuilder = DownloadDirectoryRequest.builder()
                .bucket(getBucket())
                .destination(Paths.get(request.getLocalDestinationDirectory()));

        // Download every blob that starts with this prefix
        if(request.getPrefixToDownload() != null && !request.getPrefixToDownload().isEmpty()) {
            downloadDirectoryRequestBuilder.listObjectsV2RequestTransformer(builder -> builder.prefix(request.getPrefixToDownload()));
        }

        // If we have prefixes to exclude from the download, then add in a filter here
        if(request.getPrefixesToExclude() != null && !request.getPrefixesToExclude().isEmpty()) {
            downloadDirectoryRequestBuilder.filter(getPrefixExclusionsFilter(request.getPrefixesToExclude()));
        }
        return downloadDirectoryRequestBuilder.build();
    }

    // Return false if we want to exclude this blob from the download
    protected DownloadFilter getPrefixExclusionsFilter(List<String> prefixesToExclude) {
        return s3Object -> {
            for(String prefixToExclude : prefixesToExclude) {
                if(s3Object.key().startsWith(prefixToExclude)) {
                    return false;
                }
            }
            return true;
        };
    }

    public DirectoryDownloadResponse toDirectoryDownloadResponse(CompletedDirectoryDownload completedDirectoryDownload) {
        return DirectoryDownloadResponse.builder()
                .failedTransfers(completedDirectoryDownload.failedTransfers()
                        .stream()
                        .map(item -> FailedBlobDownload.builder()
                                .destination(item.request().destination())
                                .exception(item.exception())
                                .build())
                        .collect(Collectors.toList()))
                .build();
    }

    public UploadDirectoryRequest toUploadDirectoryRequest(DirectoryUploadRequest request) {
        return UploadDirectoryRequest.builder()
                .bucket(getBucket())
                .source(Paths.get(request.getLocalSourceDirectory()))
                .maxDepth(request.isIncludeSubFolders() ? Integer.MAX_VALUE : 1)
                .s3Prefix(request.getPrefix())
                .build();
    }

    public DirectoryUploadResponse toDirectoryUploadResponse(CompletedDirectoryUpload completedDirectoryUpload) {
        return DirectoryUploadResponse.builder()
                .failedTransfers(completedDirectoryUpload.failedTransfers()
                        .stream()
                        .map(item -> FailedBlobUpload.builder()
                                .source(item.request().source())
                                .exception(item.exception())
                                .build())
                        .collect(Collectors.toList()))
                .build();
    }

    public List<List<BlobInfo>> partitionList(List<BlobInfo> blobInfos, int partitionSize) {
        List<List<BlobInfo>> partitionedList = new ArrayList<>();
        int listSize = blobInfos.size();

        for (int i=0; i<listSize; i+=partitionSize) {
            int endIndex = Math.min(i + partitionSize, listSize);
            partitionedList.add(new ArrayList<>(blobInfos.subList(i, endIndex)));
        }
        return partitionedList;
    }

    public List<BlobIdentifier> toBlobIdentifiers(List<BlobInfo> blobList) {
        return blobList.stream()
                .map(blob -> new BlobIdentifier(blob.getKey(), null))
                .collect(Collectors.toList());
    }

    /**
     * Converts MultiCloudJ RetryConfig to AWS SDK RetryStrategy
     *
     * @param retryConfig The retry configuration to convert
     * @return AWS SDK RetryStrategy
     * @throws InvalidArgumentException if retryConfig is null or has invalid values
     */
    public RetryStrategy toAwsRetryStrategy(RetryConfig retryConfig) {
        if (retryConfig == null) {
            throw new InvalidArgumentException("RetryConfig cannot be null");
        }
        if (retryConfig.getMaxAttempts() != null && retryConfig.getMaxAttempts() <= 0) {
            throw new InvalidArgumentException("RetryConfig.maxAttempts must be greater than 0, got: " + retryConfig.getMaxAttempts());
        }

        StandardRetryStrategy.Builder strategyBuilder = StandardRetryStrategy.builder();

        // Only set maxAttempts if provided, otherwise use AWS SDK default
        if (retryConfig.getMaxAttempts() != null) {
        strategyBuilder.maxAttempts(retryConfig.getMaxAttempts());
        }

        // Configure backoff strategy based on mode
        if (retryConfig.getMode() == RetryConfig.Mode.EXPONENTIAL) {
            if (retryConfig.getInitialDelayMillis() <= 0) {
                throw new InvalidArgumentException("RetryConfig.initialDelayMillis must be greater than 0 for EXPONENTIAL mode, got: " + retryConfig.getInitialDelayMillis());
            }
            if (retryConfig.getMaxDelayMillis() <= 0) {
                throw new InvalidArgumentException("RetryConfig.maxDelayMillis must be greater than 0 for EXPONENTIAL mode, got: " + retryConfig.getMaxDelayMillis());
            }
            strategyBuilder.backoffStrategy(
                    software.amazon.awssdk.retries.api.BackoffStrategy.exponentialDelay(
                            Duration.ofMillis(retryConfig.getInitialDelayMillis()),
                            Duration.ofMillis(retryConfig.getMaxDelayMillis())
                    )
            );
        } else {
            // FIXED mode
            if (retryConfig.getFixedDelayMillis() <= 0) {
                throw new InvalidArgumentException("RetryConfig.fixedDelayMillis must be greater than 0 for FIXED mode, got: " + retryConfig.getFixedDelayMillis());
            }
            strategyBuilder.backoffStrategy(
                    software.amazon.awssdk.retries.api.BackoffStrategy.fixedDelay(
                            Duration.ofMillis(retryConfig.getFixedDelayMillis())
                    )
            );
        }

        return strategyBuilder.build();
    }
}
