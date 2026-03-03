package com.salesforce.multicloudj.blob.gcp;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobInfo.Retention;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.common.collect.ImmutableMap;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.CopyFromRequest;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.blob.driver.ObjectLockConfiguration;
import com.salesforce.multicloudj.blob.driver.ObjectLockInfo;
import com.salesforce.multicloudj.blob.driver.RetentionMode;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.common.util.HexUtil;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
public class GcpTransformer {

    private final String bucket;
    private static final String TAG_PREFIX = "gcp-tag-";

    public GcpTransformer(String bucket) {
        this.bucket = bucket;
    }

    public BlobInfo toBlobInfo(UploadRequest uploadRequest) {
        Map<String, String> metadata = new HashMap<>();
        if(uploadRequest.getMetadata() != null) {
            metadata.putAll(uploadRequest.getMetadata());
        }

        // Add tags to metadata with TAG_PREFIX
        if(uploadRequest.getTags() != null && !uploadRequest.getTags().isEmpty()) {
            uploadRequest.getTags().forEach((tagName, tagValue) -> metadata.put(TAG_PREFIX + tagName, tagValue));
        }

        // Delegate to the protected toBlobInfo method which handles storage class, checksum, and object lock
        return toBlobInfo(uploadRequest.getKey(), metadata, uploadRequest.getStorageClass(), null, uploadRequest.getObjectLock());
    }

    public UploadResponse toUploadResponse(Blob blob) {
        return UploadResponse.builder()
                .key(blob.getName())
                .versionId(blob.getGeneration() != null ? blob.getGeneration().toString() : null)
                .checksumValue(blob.getCrc32c())
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
        return toBlobId(this.bucket, key, versionId);
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
        // Extract object lock info if retention or holds are present
        ObjectLockInfo objectLockInfo = null;
        
        // Check for object retention
        Retention retention = blob.getRetention();
        boolean hasRetention = retention != null;
        
        // Check for object holds
        Boolean tempHold = blob.getTemporaryHold();
        Boolean eventHold = blob.getEventBasedHold();
        boolean hasHold = (tempHold != null && tempHold) || (eventHold != null && eventHold);

        if (hasRetention || hasHold) {
            RetentionMode mode = null;
            Instant retainUntilDate = null;
            
            if (hasRetention) {
                // Map provider retention mode to SDK retention mode
                mode = retention.getMode() == Retention.Mode.LOCKED
                        ? RetentionMode.COMPLIANCE
                        : RetentionMode.GOVERNANCE;
                retainUntilDate = retention.getRetainUntilTime() != null
                        ? retention.getRetainUntilTime().toInstant()
                        : null;
            }
            
            objectLockInfo = ObjectLockInfo.builder()
                    .mode(mode)
                    .retainUntilDate(retainUntilDate)
                    .legalHold(hasHold)
                    .useEventBasedHold(eventHold != null && eventHold)
                    .build();
        }

        return BlobMetadata.builder()
                .key(blob.getName())
                .versionId(blob.getGeneration() != null ? blob.getGeneration().toString() : null)
                .eTag(blob.getEtag())
                .objectSize(blob.getSize())
                .metadata(blob.getMetadata()==null ? Collections.emptyMap() : blob.getMetadata())
                .lastModified(blob.getUpdateTimeOffsetDateTime() != null ? blob.getUpdateTimeOffsetDateTime().toInstant() : null)
                .md5(HexUtil.convertToBytes(blob.getMd5()))
                .objectLockInfo(objectLockInfo)
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

    public Storage.CopyRequest toCopyRequest(CopyFromRequest request) {
        BlobId source = toBlobId(request.getSrcBucket(), request.getSrcKey(), request.getSrcVersionId());
        BlobId target = toBlobId(bucket, request.getDestKey(), null);
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
        Map<String, String> metadata = new HashMap<>();
        if(presignedUrlRequest.getMetadata() != null) {
            metadata.putAll(presignedUrlRequest.getMetadata());
        }

        // Add tags to metadata with TAG_PREFIX
        if(presignedUrlRequest.getTags() != null && !presignedUrlRequest.getTags().isEmpty()) {
            presignedUrlRequest.getTags().forEach((tagName, tagValue) -> metadata.put(TAG_PREFIX + tagName, tagValue));
        }

        return toBlobInfo(presignedUrlRequest.getKey(), metadata);
    }

    public BlobInfo toBlobInfo(MultipartUploadRequest request) {
        Map<String, String> metadata = new HashMap<>();
        if(request.getMetadata() != null) {
            metadata.putAll(request.getMetadata());
        }

        // Add tags to metadata with TAG_PREFIX
        if(request.getTags() != null && !request.getTags().isEmpty()) {
            request.getTags().forEach((tagName, tagValue) -> metadata.put(TAG_PREFIX + tagName, tagValue));
        }

        return toBlobInfo(request.getKey(), metadata);
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
        return toBlobInfo(key, metadata, null, null, null);
    }

    protected BlobInfo toBlobInfo(String key, Map<String, String> metadata, String storageClass) {
        return toBlobInfo(key, metadata, storageClass, null, null);
    }

    protected BlobInfo toBlobInfo(String key, Map<String, String> metadata, String storageClass,
                                  String checksumValue, ObjectLockConfiguration objectLock) {
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

        // Set CRC32C checksum if provided (GCP's native checksum algorithm)
        if (checksumValue != null && !checksumValue.isEmpty()) {
            builder.setCrc32c(checksumValue);
        }

        // Set object retention and holds if object lock is configured
        if (objectLock != null) {
            // Set object retention (retention mode and retain-until date)
            if (objectLock.getRetainUntilDate() != null) {
                Retention.Mode retentionMode = objectLock.getMode() == RetentionMode.COMPLIANCE
                        ? Retention.Mode.LOCKED
                        : Retention.Mode.UNLOCKED;
                
                builder.setRetention(
                    Retention.newBuilder()
                        .setMode(retentionMode)
                        .setRetainUntilTime(OffsetDateTime.ofInstant(
                            objectLock.getRetainUntilDate(), 
                            ZoneOffset.UTC))
                        .build()
                );
            }
            
            // Set object holds (legal hold)
            boolean useEventBased = objectLock.getUseEventBasedHold() != null 
                    ? objectLock.getUseEventBasedHold() 
                    : false;

            if (useEventBased) {
                builder.setEventBasedHold(objectLock.isLegalHold());
            } else {
                builder.setTemporaryHold(objectLock.isLegalHold());
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

    public BlobInfo toBlobInfo(MultipartUpload mpu) {
        Map<String, String> metadata = new HashMap<>();
        if(mpu.getMetadata() != null) {
            metadata.putAll(mpu.getMetadata());
        }

        // Add tags to metadata with TAG_PREFIX
        if(mpu.getTags() != null && !mpu.getTags().isEmpty()) {
            mpu.getTags().forEach((tagName, tagValue) -> metadata.put(TAG_PREFIX + tagName, tagValue));
        }

        return toBlobInfo(mpu.getKey(), metadata);
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
     * @param prefix the blob prefix to apply
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
     * Converts MultiCloudJ RetryConfig to GCP RetrySettings
     *
     * @param retryConfig The retry configuration to convert
     * @return GCP RetrySettings
     * @throws InvalidArgumentException if retryConfig is null or has invalid values
     */
    public RetrySettings toGcpRetrySettings(RetryConfig retryConfig) {
        if (retryConfig == null) {
            throw new InvalidArgumentException("RetryConfig cannot be null");
        }
        if (retryConfig.getMaxAttempts() != null && retryConfig.getMaxAttempts() <= 0) {
            throw new InvalidArgumentException("RetryConfig.maxAttempts must be greater than 0, got: " + retryConfig.getMaxAttempts());
        }

        RetrySettings.Builder settingsBuilder = RetrySettings.newBuilder();

        // Only set maxAttempts if provided, otherwise use GCP SDK default
        if (retryConfig.getMaxAttempts() != null) {
            settingsBuilder.setMaxAttempts(retryConfig.getMaxAttempts());
        }

        // If mode is not set, use GCP SDK's default backoff strategy

        // Configure backoff strategy based on mode
        if (retryConfig.getMode() == RetryConfig.Mode.EXPONENTIAL) {
            if (retryConfig.getInitialDelayMillis() <= 0) {
                throw new InvalidArgumentException("RetryConfig.initialDelayMillis must be greater than 0 for EXPONENTIAL mode, got: " + retryConfig.getInitialDelayMillis());
            }
            if (retryConfig.getMaxDelayMillis() <= 0) {
                throw new InvalidArgumentException("RetryConfig.maxDelayMillis must be greater than 0 for EXPONENTIAL mode, got: " + retryConfig.getMaxDelayMillis());
            }
            settingsBuilder.setInitialRetryDelayDuration(Duration.ofMillis(retryConfig.getInitialDelayMillis()))
                    .setRetryDelayMultiplier(retryConfig.getMultiplier())
                    .setMaxRetryDelayDuration(Duration.ofMillis(retryConfig.getMaxDelayMillis()));
        } else if (retryConfig.getMode() == RetryConfig.Mode.FIXED) {
            if (retryConfig.getFixedDelayMillis() <= 0) {
                throw new InvalidArgumentException("RetryConfig.fixedDelayMillis must be greater than 0 for FIXED mode, got: " + retryConfig.getFixedDelayMillis());
            }
            // FIXED mode is simulated by setting multiplier to 1.0 and both delays to the same value
            settingsBuilder.setInitialRetryDelayDuration(Duration.ofMillis(retryConfig.getFixedDelayMillis()))
                    .setRetryDelayMultiplier(1.0)
                    .setMaxRetryDelayDuration(Duration.ofMillis(retryConfig.getFixedDelayMillis()));
        }

        // Set total timeout if provided
        if (retryConfig.getTotalTimeout() != null) {
            if (retryConfig.getTotalTimeout() <= 0) {
                throw new InvalidArgumentException("RetryConfig.totalTimeout must be greater than 0, got: " + retryConfig.getTotalTimeout());
            }
            settingsBuilder.setTotalTimeoutDuration(Duration.ofMillis(retryConfig.getTotalTimeout()));
        }

        // Set attempt timeout if provided
        if (retryConfig.getAttemptTimeout() != null) {
            if (retryConfig.getAttemptTimeout() <= 0) {
                throw new InvalidArgumentException("RetryConfig.attemptTimeout must be greater than 0, got: " + retryConfig.getAttemptTimeout());
            }
            Duration attemptTimeout = Duration.ofMillis(retryConfig.getAttemptTimeout());
            settingsBuilder.setInitialRpcTimeoutDuration(attemptTimeout)
                    .setRpcTimeoutMultiplier(1.0)
                    .setMaxRpcTimeoutDuration(attemptTimeout);
        }

        return settingsBuilder.build();
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
