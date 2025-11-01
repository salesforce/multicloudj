package com.salesforce.multicloudj.blob.gcp;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.common.collect.ImmutableMap;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.util.HexUtil;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
public class GcpTransformer {

    private final String bucket;

    public GcpTransformer(String bucket) {
        this.bucket = bucket;
    }

    public BlobInfo toBlobInfo(UploadRequest uploadRequest) {
        if(uploadRequest.getTags() != null && !uploadRequest.getTags().isEmpty()) {
            throw new UnSupportedOperationException("Tags are not supported by GCP");
        }
        return toBlobInfo(uploadRequest.getKey(), uploadRequest.getMetadata(), uploadRequest.getStorageClass());
    }

    public UploadResponse toUploadResponse(Blob blob) {
        return UploadResponse.builder()
                .key(blob.getName())
                .versionId(blob.getGeneration() != null ? blob.getGeneration().toString() : null)
                .eTag(blob.getEtag())
                .build();
    }

    public BlobId toBlobId(DownloadRequest downloadRequest) {
        return toBlobId(bucket, downloadRequest.getKey(), downloadRequest.getVersionId());
    }

    /**
     * Note: If the versionId is null, then the BlobId refers to the latest version of the blob
     */
    public BlobId toBlobId(String bucket, String key, String versionId) {
        if (versionId == null) {
            return BlobId.of(bucket, key);
        } else {
            return BlobId.of(bucket, key, Long.parseLong(versionId));
        }
    }

    /**
     * Convenience method that uses the bucket from the transformer context
     */
    public BlobId toBlobId(String key, String versionId) {
        return toBlobId(getBucket(), key, versionId);
    }

    /**
     * No range specified                     - computeRange(null, null) -   (null, null)
     * Reading the first 500 bytes            - computeRange(0, 500)     -   (0, 501)
     * Reading a middle 500 bytes             - computeRange(123, 623)   -   (123, 624)
     * Reading the last 500 bytes             - computeRange(null, 500)  -   (size-500, size+1)
     * Reading everything but first 500 bytes - computeRange(500, null)  -   (500, null)
     */
    protected Pair<Long, Long> computeRange(Long start, Long end, long fileSize) {
        // Need to validate range here because read from blob store API fails silently without any exception
        if (start != null && start > fileSize) {
            throw new IllegalArgumentException("Start of range cannot be greater than file size: " + start);
        }
        Long startValue = start;
        Long endValue = null;
        if(end != null){
            endValue = end + 1;

            if(start == null) {
                startValue = fileSize - end;
                endValue = fileSize + 1;
            }
        }
        return new ImmutablePair<>(startValue, endValue);
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

    public DownloadResponse toDownloadResponse(Blob blob, InputStream inputStream) {
        return DownloadResponse.builder()
                .key(blob.getName())
                .metadata(toBlobMetadata(blob))
                .inputStream(inputStream)
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
                .md5(HexUtil.convertToBytes(blob.getMd5()))
                .build();
    }

    public Storage.CopyRequest toCopyRequest(CopyRequest request) {
        BlobId source = toBlobId(bucket, request.getSrcKey(), request.getSrcVersionId());
        BlobId target = toBlobId(request.getDestBucket(), request.getDestKey(), null);
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
        if(presignedUrlRequest.getTags() != null && !presignedUrlRequest.getTags().isEmpty()) {
            throw new UnSupportedOperationException("Tags are not supported by GCP");
        }
        return toBlobInfo(presignedUrlRequest.getKey(), presignedUrlRequest.getMetadata());
    }

    public Storage.BlobListOption[] toBlobListOptions(ListBlobsPageRequest request) {
        List<Storage.BlobListOption> options = new ArrayList<>();
        
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

    protected BlobInfo toBlobInfo(String key, Map<String, String> metadata) {
        return toBlobInfo(key, metadata, null);
    }

    protected BlobInfo toBlobInfo(String key, Map<String, String> metadata, String storageClass) {
        metadata = metadata != null ? ImmutableMap.copyOf(metadata) : Collections.emptyMap();
        BlobInfo.Builder builder = BlobInfo.newBuilder(bucket, key).setMetadata(metadata);

        // Set storage class if provided
        if (storageClass != null && !storageClass.isEmpty()) {
            try {
                StorageClass gcpStorageClass = StorageClass.valueOf(storageClass.toUpperCase());
                builder.setStorageClass(gcpStorageClass);
            } catch (IllegalArgumentException e) {
                throw new InvalidArgumentException("Invalid storage class: " + storageClass, e);
            }
        }

        return builder.build();
    }

    public Storage.BlobTargetOption[] getKmsTargetOptions(UploadRequest uploadRequest) {
        if (uploadRequest.getKmsKeyId() != null && !uploadRequest.getKmsKeyId().isEmpty()) {
            return new Storage.BlobTargetOption[] {
                Storage.BlobTargetOption.kmsKeyName(uploadRequest.getKmsKeyId())
            };
        }
        return new Storage.BlobTargetOption[0];
    }

    public Storage.BlobWriteOption[] getKmsWriteOptions(UploadRequest uploadRequest) {
        if (uploadRequest.getKmsKeyId() != null && !uploadRequest.getKmsKeyId().isEmpty()) {
            return new Storage.BlobWriteOption[] {
                Storage.BlobWriteOption.kmsKeyName(uploadRequest.getKmsKeyId())
            };
        }
        return new Storage.BlobWriteOption[0];
    }

    public String toPartName(MultipartUpload mpu, int partNumber) {
        return String.format("%s/%s/part-%d", mpu.getKey(), mpu.getId(), partNumber);
    }

    public UploadRequest toUploadRequest(MultipartUpload mpu, MultipartPart mpp) {
        String partKey = toPartName(mpu, mpp.getPartNumber());
        UploadRequest.Builder builder = UploadRequest.builder()
                .withKey(partKey)
                .withContentLength(mpp.getContentLength());

        if (mpu.getKmsKeyId() != null && !mpu.getKmsKeyId().isEmpty()) {
            builder.withKmsKeyId(mpu.getKmsKeyId());
        }

        return builder.build();
    }

    public BlobInfo toBlobInfo(MultipartUpload mpu) {
        return toBlobInfo(mpu.getKey(), mpu.getMetadata());
    }

    public List<BlobId> toBlobIdList(Page<Blob> blobs) {
        return blobs.streamAll()
                .map(Blob::getBlobId)
                .collect(Collectors.toList());
    }

    public List<UploadPartResponse> toUploadPartResponseList(Page<Blob> blobs) {
        return blobs.streamAll()
                .map(blob -> {
                    String partNumber = blob.getName().substring(blob.getName().lastIndexOf("-") + 1);
                    return new UploadPartResponse(Integer.parseInt(partNumber), blob.getEtag(), blob.getSize());
                })
                .collect(Collectors.toList());
    }


    /**
     * Converts a DirectoryUploadRequest to a list of file paths to upload.
     * This method handles directory traversal and filtering based on the request parameters.
     *
     * @param request the directory upload request
     * @return list of file paths to upload
     */
    public List<Path> toFilePaths(DirectoryUploadRequest request) {
        Path sourceDir = Paths.get(request.getLocalSourceDirectory());
        List<Path> filePaths = new ArrayList<>();

        try (Stream<Path> paths = Files.walk(sourceDir)) {
            filePaths = paths
                    .filter(Files::isRegularFile)
                    .filter(path -> {
                        // If includeSubFolders is false, only include files in the root directory
                        if (!request.isIncludeSubFolders()) {
                            Path relativePath = sourceDir.relativize(path);
                            return relativePath.getParent() == null;
                        }
                        return true;
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Failed to traverse directory: " + sourceDir, e);
        }

        return filePaths;
    }

    /**
     * Converts a file path to a blob key by applying the prefix and maintaining directory structure.
     *
     * @param sourceDir the source directory path
     * @param filePath the file path to convert
     * @param prefix the S3 prefix to apply
     * @return the blob key
     */
    public String toBlobKey(Path sourceDir, Path filePath, String prefix) {
        Path relativePath = sourceDir.relativize(filePath);
        String key = relativePath.toString().replace("\\", "/"); // Normalize path separators

        if (prefix != null && !prefix.isEmpty()) {
            // Ensure prefix ends with "/" if it doesn't already
            String normalizedPrefix = prefix.endsWith("/") ? prefix : prefix + "/";
            key = normalizedPrefix + key;
        }

        return key;
    }

    /**
     * Partitions a list of BlobInfo objects into smaller chunks for batch operations.
     *
     * @param blobInfos the list of BlobInfo objects to partition
     * @param partitionSize the maximum size of each partition
     * @return a list of partitioned BlobInfo lists
     */
    public List<List<com.salesforce.multicloudj.blob.driver.BlobInfo>> partitionList(List<com.salesforce.multicloudj.blob.driver.BlobInfo> blobInfos, int partitionSize) {
        List<List<com.salesforce.multicloudj.blob.driver.BlobInfo>> partitionedList = new ArrayList<>();
        int listSize = blobInfos.size();

        for (int i = 0; i < listSize; i += partitionSize) {
            int endIndex = Math.min(i + partitionSize, listSize);
            partitionedList.add(new ArrayList<>(blobInfos.subList(i, endIndex)));
        }
        return partitionedList;
    }

    /**
     * Converts a list of BlobInfo objects to BlobIdentifier objects for deletion.
     *
     * @param blobList the list of BlobInfo objects
     * @return a list of BlobIdentifier objects
     */
    public List<BlobIdentifier> toBlobIdentifiers(List<com.salesforce.multicloudj.blob.driver.BlobInfo> blobList) {
        return blobList.stream()
                .map(blob -> new BlobIdentifier(blob.getKey(), null))
                .collect(Collectors.toList());
    }
}
