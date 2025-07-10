package com.salesforce.multicloudj.blob.gcp;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import lombok.Getter;

import java.util.Collections;
import java.util.HashMap;

@Getter
public class GcpTransformer {

    private final String bucket;

    public GcpTransformer(String bucket) {
        this.bucket = bucket;
    }

    public BlobInfo toBlobInfo(UploadRequest uploadRequest) {
        BlobInfo.Builder blobInfoBuilder = BlobInfo.newBuilder(getBucket(), uploadRequest.getKey());
        blobInfoBuilder.setMetadata(new HashMap<>(uploadRequest.getMetadata()));
        if(uploadRequest.getTags() != null && !uploadRequest.getTags().isEmpty()) {
            throw new UnSupportedOperationException("Tags are not supported by GCP");
        }
        return blobInfoBuilder.build();
    }

    public UploadResponse toUploadResponse(Blob blob) {
        return UploadResponse.builder()
                .key(blob.getName())
                .versionId(blob.getGeneration() != null ? blob.getGeneration().toString() : null)
                .eTag(blob.getEtag())
                .build();
    }

    public BlobId toBlobId(DownloadRequest downloadRequest) {
        return toBlobId(downloadRequest.getKey(), downloadRequest.getVersionId());
    }

    /**
     * Note: If the versionId is null, then the BlobId refers to the latest version of the blob
     */
    public BlobId toBlobId(String key, String versionId) {
        return BlobId.of(getBucket(), key, toGenerationId(versionId));
    }

    /**
     * This is a null-safe method of generating the generationId
     */
    public Long toGenerationId(String versionId) {
        if(versionId == null) {
            return null;
        }
        return Long.parseLong(versionId);
    }

    public DownloadResponse toDownloadResponse(Blob blob) {
        return DownloadResponse.builder()
                .key(blob.getName())
                .metadata(toBlobMetadata(blob))
                .build();
    }

    public BlobMetadata toBlobMetadata(Blob blob) {
        return BlobMetadata.builder()
                .key(blob.getName())
                .versionId(blob.getGeneration() != null ? blob.getGeneration().toString() : null)
                .eTag(blob.getEtag())
                .objectSize(blob.getSize())
                .metadata(blob.getMetadata()==null ? Collections.emptyMap() : blob.getMetadata())
                .lastModified(blob.getUpdateTimeOffsetDateTime() != null ? blob.getUpdateTimeOffsetDateTime().toInstant() : null)
                .build();
    }

    public Storage.CopyRequest toCopyRequest(CopyRequest request) {
        BlobId source = toBlobId(request.getSrcKey(), request.getSrcVersionId());
        BlobId target = toBlobId(request.getDestKey(), null);
        return Storage.CopyRequest.newBuilder()
                .setSource(source)
                .setTarget(target)
                .build();
    }

    public CopyResponse toCopyResponse(Blob blob) {
        return CopyResponse.builder()
                .key(blob.getName())
                .versionId(blob.getGeneration() != null ? blob.getGeneration().toString() : null)
                .eTag(blob.getEtag())
                .lastModified(blob.getUpdateTimeOffsetDateTime().toInstant())
                .build();
    }

    public BlobInfo toBlobInfo(PresignedUrlRequest presignedUrlRequest) {
        BlobInfo.Builder blobInfoBuilder = BlobInfo.newBuilder(getBucket(), presignedUrlRequest.getKey());
        blobInfoBuilder.setMetadata(new HashMap<>(presignedUrlRequest.getMetadata()));
        if(presignedUrlRequest.getTags() != null && !presignedUrlRequest.getTags().isEmpty()) {
            throw new UnSupportedOperationException("Tags are not supported by GCP");
        }
        return blobInfoBuilder.build();
    }

    public Storage.BlobListOption[] toBlobListOptions(ListBlobsPageRequest request) {
        java.util.List<Storage.BlobListOption> options = new java.util.ArrayList<>();
        
        if (request.getPrefix() != null) {
            options.add(Storage.BlobListOption.prefix(request.getPrefix()));
        }
        
        if (request.getDelimiter() != null) {
            options.add(Storage.BlobListOption.delimiter(request.getDelimiter()));
        }
        
        if (request.getPaginationToken() != null) {
            options.add(Storage.BlobListOption.pageToken(request.getPaginationToken()));
        }
        
        if (request.getMaxResults() != null) {
            options.add(Storage.BlobListOption.pageSize(request.getMaxResults().longValue()));
        }
        
        return options.toArray(new Storage.BlobListOption[0]);
    }
}