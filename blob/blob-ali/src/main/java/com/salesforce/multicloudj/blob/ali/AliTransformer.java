package com.salesforce.multicloudj.blob.ali;

import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.internal.OSSHeaders;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteVersionsRequest;
import com.aliyun.oss.model.GeneratePresignedUrlRequest;
import com.aliyun.oss.model.GenericRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ListPartsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PartListing;
import com.aliyun.oss.model.PartSummary;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.PutObjectResult;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public class AliTransformer {

    private final String bucket;

    public AliTransformer(String bucket) {
        this.bucket = bucket;
    }

    public PutObjectRequest toPutObjectRequest(UploadRequest uploadRequest, InputStream inputStream) {
        return new PutObjectRequest(bucket, uploadRequest.getKey(), inputStream, generateObjectMetadata(uploadRequest));
    }

    public PutObjectRequest toPutObjectRequest(UploadRequest uploadRequest, File file) {
        return new PutObjectRequest(bucket, uploadRequest.getKey(), file, generateObjectMetadata(uploadRequest));
    }

    protected ObjectMetadata generateObjectMetadata(UploadRequest uploadRequest) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setUserMetadata(uploadRequest.getMetadata());
        metadata.setObjectTagging(uploadRequest.getTags());
        return metadata;
    }

    public UploadResponse toUploadResponse(UploadRequest uploadRequest, PutObjectResult result) {
        return UploadResponse.builder()
                .key(uploadRequest.getKey())
                .versionId(result.getVersionId())
                .eTag(result.getETag())
                .build();
    }

    public GetObjectRequest toGetObjectRequest(DownloadRequest downloadRequest) {
        GetObjectRequest request = new GetObjectRequest(bucket, downloadRequest.getKey(), downloadRequest.getVersionId());
        if(downloadRequest.getStart() != null || downloadRequest.getEnd() != null) {
            Pair<Long, Long> range = computeRange(downloadRequest.getStart(), downloadRequest.getEnd());
            request.withRange(range.getLeft(), range.getRight());
        }
        return request;
    }

    /**
     * Reading the first 500 bytes            - computeRange(0, 500)    -   (0, 500)
     * Reading a middle 500 bytes             - computeRange(123, 623)  -   (123, 623)
     * Reading the last 500 bytes             - computeRange(null, 500) -   (-1, 500)
     * Reading everything but first 500 bytes - computeRange(500, null) -   (500, -1)
     */
    protected Pair<Long, Long> computeRange(Long start, Long end) {
        return new ImmutablePair<>(start==null ? -1 : start, end==null ? -1 : end);
    }

    public DownloadResponse toDownloadResponse(OSSObject ossObject) {
        return DownloadResponse.builder()
                .key(ossObject.getKey())
                .metadata(BlobMetadata.builder()
                        .key(ossObject.getKey())
                        .versionId(ossObject.getObjectMetadata().getVersionId())
                        .eTag(ossObject.getObjectMetadata().getETag())
                        .lastModified(ossObject.getObjectMetadata().getLastModified().toInstant())
                        .metadata(ossObject.getObjectMetadata().getUserMetadata())
                        .objectSize(ossObject.getObjectMetadata().getContentLength())
                        .build())
                .build();
    }

    public DeleteObjectsRequest toDeleteObjectsRequest(Collection<BlobIdentifier> objects) {
        return new DeleteObjectsRequest(bucket)
                .withKeys(
                        objects.stream()
                                .map(BlobIdentifier::getKey)
                                .collect(Collectors.toList()));
    }

    public DeleteVersionsRequest toDeleteVersionsRequest(Collection<BlobIdentifier> objects) {
        List<DeleteVersionsRequest.KeyVersion> objectsToDelete = new ArrayList<>();
        for(BlobIdentifier object : objects) {
            objectsToDelete.add(new DeleteVersionsRequest.KeyVersion(object.getKey(), object.getVersionId()));
        }
        return new DeleteVersionsRequest(bucket).withKeys(objectsToDelete);
    }

    public CopyObjectRequest toCopyObjectRequest(CopyRequest request) {
        return new CopyObjectRequest(
                bucket,
                request.getSrcKey(),
                request.getSrcVersionId(),
                request.getDestBucket(),
                request.getDestKey());
    }

    public CopyResponse toCopyResponse(String destKey, CopyObjectResult result) {
        return CopyResponse.builder()
                .key(destKey)
                .versionId(result.getVersionId())
                .eTag(result.getETag())
                .lastModified(result.getLastModified().toInstant())
                .build();
    }

    public GenericRequest toMetadataRequest(String key, String versionId) {
        return new GenericRequest()
                .withBucketName(bucket)
                .withKey(key)
                .withVersionId(versionId);
    }

    public BlobMetadata toBlobMetadata(String key, ObjectMetadata metadata) {
        long objectSize = metadata.getContentLength();
        Map<String, String> rawMetadata = metadata.getUserMetadata();
        return BlobMetadata.builder()
                .key(key)
                .versionId(metadata.getVersionId())
                .eTag(metadata.getETag())
                .objectSize(objectSize)
                .metadata(rawMetadata)
                .lastModified(metadata.getLastModified().toInstant())
                .build();
    }

    public InitiateMultipartUploadRequest toInitiateMultipartUploadRequest(MultipartUploadRequest request) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setUserMetadata(request.getMetadata());
        return new InitiateMultipartUploadRequest(getBucket(), request.getKey(), metadata);
    }

    public MultipartUpload toMultipartUpload(InitiateMultipartUploadResult initiateMultipartUploadResult) {
        return new MultipartUpload(
                initiateMultipartUploadResult.getBucketName(),
                initiateMultipartUploadResult.getKey(),
                initiateMultipartUploadResult.getUploadId());
    }

    public UploadPartRequest toUploadPartRequest(MultipartUpload mpu, MultipartPart mpp){
        return new UploadPartRequest(
                getBucket(),
                mpu.getKey(),
                mpu.getId(),
                mpp.getPartNumber(),
                mpp.getInputStream(),
                mpp.getContentLength());
    }

    public UploadPartResponse toUploadPartResponse(MultipartPart mpp, UploadPartResult uploadPartResult) {
        return new UploadPartResponse(mpp.getPartNumber(), uploadPartResult.getPartETag().getETag(), mpp.getContentLength());
    }

    public CompleteMultipartUploadRequest toCompleteMultipartUploadRequest(MultipartUpload mpu, List<UploadPartResponse> parts) {
        List<PartETag> completedParts = parts.stream()
                .sorted(Comparator.comparingInt(UploadPartResponse::getPartNumber))
                .map(part -> new PartETag(part.getPartNumber(), part.getEtag()))
                .collect(Collectors.toList());
        return new CompleteMultipartUploadRequest(getBucket(), mpu.getKey(), mpu.getId(), completedParts);
    }

    public ListPartsRequest toListPartsRequest(MultipartUpload mpu) {
        return new ListPartsRequest(bucket, mpu.getKey(), mpu.getId());
    }

    public List<UploadPartResponse> toListUploadPartResponse(PartListing partListing) {
        return partListing.getParts().stream()
                .sorted(Comparator.comparingInt(PartSummary::getPartNumber))
                .map((part) -> new com.salesforce.multicloudj.blob.driver.UploadPartResponse(part.getPartNumber(), part.getETag(), part.getSize()))
                .collect(Collectors.toList());
    }

    public AbortMultipartUploadRequest toAbortMultipartUploadRequest(MultipartUpload mpu) {
        return new AbortMultipartUploadRequest(bucket, mpu.getKey(), mpu.getId());
    }

    public GeneratePresignedUrlRequest toPresignedUrlUploadRequest(PresignedUrlRequest request) {
        Date expirationDate = Date.from(Instant.now().plus(request.getDuration()));
        GeneratePresignedUrlRequest presignedUrlRequest = new GeneratePresignedUrlRequest(getBucket(), request.getKey());
        presignedUrlRequest.setExpiration(expirationDate);
        presignedUrlRequest.setMethod(HttpMethod.PUT);
        presignedUrlRequest.setUserMetadata(request.getMetadata());

        // Note: Tagging is not supported by default for OSS presigned uploads so we have to manually append it
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setObjectTagging(request.getTags());
        Object encodedTagging = metadata.getRawMetadata().get(OSSHeaders.OSS_TAGGING);
        if(encodedTagging instanceof String) {
            presignedUrlRequest.addHeader(OSSHeaders.OSS_TAGGING, (String)encodedTagging);
        }
        return presignedUrlRequest;
    }

    public GeneratePresignedUrlRequest toPresignedUrlDownloadRequest(PresignedUrlRequest request) {
        Date expirationDate = Date.from(Instant.now().plus(request.getDuration()));
        GeneratePresignedUrlRequest presignedUrlRequest = new GeneratePresignedUrlRequest(getBucket(), request.getKey());
        presignedUrlRequest.setExpiration(expirationDate);
        presignedUrlRequest.setMethod(HttpMethod.GET);
        return presignedUrlRequest;
    }

    public com.aliyun.oss.model.ListObjectsRequest toListObjectsRequest(ListBlobsPageRequest request) {
        com.aliyun.oss.model.ListObjectsRequest listRequest = new com.aliyun.oss.model.ListObjectsRequest(bucket);
        
        if (request.getPrefix() != null) {
            listRequest.setPrefix(request.getPrefix());
        }
        
        if (request.getDelimiter() != null) {
            listRequest.setDelimiter(request.getDelimiter());
        }
        
        if (request.getPaginationToken() != null) {
            listRequest.setMarker(request.getPaginationToken());
        }
        
        if (request.getMaxResults() != null) {
            listRequest.setMaxKeys(request.getMaxResults());
        }
        
        return listRequest;
    }
}
