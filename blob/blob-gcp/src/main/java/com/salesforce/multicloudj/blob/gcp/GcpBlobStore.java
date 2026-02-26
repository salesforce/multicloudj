package com.salesforce.multicloudj.blob.gcp;

import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.api.gax.paging.Page;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.Credentials;
import com.google.auto.service.AutoService;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo.Retention;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.HttpStorageOptions;
import com.google.cloud.storage.MultipartUploadClient;
import com.google.cloud.storage.MultipartUploadSettings;
import com.google.cloud.storage.RequestBody;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.multipartupload.model.AbortMultipartUploadRequest;
import com.google.cloud.storage.multipartupload.model.CompleteMultipartUploadRequest;
import com.google.cloud.storage.multipartupload.model.CompleteMultipartUploadResponse;
import com.google.cloud.storage.multipartupload.model.CompletedMultipartUpload;
import com.google.cloud.storage.multipartupload.model.CompletedPart;
import com.google.cloud.storage.multipartupload.model.CreateMultipartUploadRequest;
import com.google.cloud.storage.multipartupload.model.CreateMultipartUploadResponse;
import com.google.cloud.storage.multipartupload.model.ListPartsRequest;
import com.google.cloud.storage.multipartupload.model.ListPartsResponse;
import com.google.cloud.storage.multipartupload.model.UploadPartRequest;
import com.google.cloud.storage.multipartupload.model.UploadPartResponse;
import com.google.common.io.ByteStreams;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.BlobStoreBuilder;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyFromRequest;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadResponse;
import com.salesforce.multicloudj.blob.driver.ObjectLockInfo;
import com.salesforce.multicloudj.blob.driver.RetentionMode;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.FailedBlobDownload;
import com.salesforce.multicloudj.blob.driver.FailedBlobUpload;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.MultipartUploadResponse;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.gcp.CommonErrorCodeMapping;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.gcp.GcpCredentialsProvider;
import com.salesforce.multicloudj.common.provider.Provider;
import lombok.Getter;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * GCP implementation of BlobStore
 */
@AutoService(AbstractBlobStore.class)
public class GcpBlobStore extends AbstractBlobStore {

    private final Storage storage;
    private final MultipartUploadClient multipartUploadClient;
    private final GcpTransformer transformer;
    private static final String TAG_PREFIX = "gcp-tag-";

    public GcpBlobStore() {
        this(new Builder(), null, null);
    }

    public GcpBlobStore(Builder builder, Storage storage, MultipartUploadClient mpuClient) {
        super(builder);
        this.storage = storage;
        this.multipartUploadClient = mpuClient;
        this.transformer = builder.transformerSupplier.get(bucket);
    }

    @Override
    public Provider.Builder builder() {
        return new Builder();
    }

    @Override
    protected UploadResponse doUpload(UploadRequest uploadRequest, InputStream inputStream) {
        try (WriteChannel writer = storage.writer(transformer.toBlobInfo(uploadRequest), transformer.getKmsWriteOptions(uploadRequest));
             var channel = Channels.newOutputStream(writer)) {
            ByteStreams.copy(inputStream, channel);
        } catch (IOException e) {
            throw new SubstrateSdkException("Request failed while uploading from input stream", e);
        }
        Blob blob = storage.get(getBucket(), uploadRequest.getKey());
        if(blob == null) {
            throw new SubstrateSdkException("Could not locate newly uploaded blob");
        }
        return transformer.toUploadResponse(blob);
    }

    @Override
    protected UploadResponse doUpload(UploadRequest uploadRequest, byte[] content) {
        Blob blob = storage.create(transformer.toBlobInfo(uploadRequest), content, transformer.getKmsTargetOptions(uploadRequest));
        return transformer.toUploadResponse(blob);
    }

    @Override
    protected UploadResponse doUpload(UploadRequest uploadRequest, File file) {
        return doUpload(uploadRequest, file.toPath());
    }

    @Override
    protected UploadResponse doUpload(UploadRequest uploadRequest, Path path) {
        try {
            Blob blob = storage.createFrom(transformer.toBlobInfo(uploadRequest), path, transformer.getKmsWriteOptions(uploadRequest));
            return transformer.toUploadResponse(blob);
        } catch (IOException e) {
            throw new SubstrateSdkException("Request failed while uploading from path", e);
        }
    }

    @Override
    protected DownloadResponse doDownload(DownloadRequest downloadRequest, OutputStream outputStream) {
        BlobId blobId = transformer.toBlobId(downloadRequest);
        try (ReadChannel reader = storage.reader(blobId);
             var channel = Channels.newInputStream(reader)) {

            Blob blob = storage.get(blobId);
            if(blob == null) {
                throw new SubstrateSdkException("Blob not found");
            }
            var range = transformer.computeRange(downloadRequest.getStart(), downloadRequest.getEnd(), blob.getSize());
            if(range.getLeft() != null) {
                reader.seek(range.getLeft());
            }
            if(range.getRight() != null) {
                reader.limit(range.getRight());
            }

            ByteStreams.copy(channel, outputStream);
            return transformer.toDownloadResponse(blob);
        } catch (IOException e) {
            throw new SubstrateSdkException("Request failed during download", e);
        }
    }

    @Override
    protected DownloadResponse doDownload(DownloadRequest downloadRequest, ByteArray byteArray) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DownloadResponse downloadResponse = doDownload(downloadRequest, outputStream);
        byteArray.setBytes(outputStream.toByteArray());
        return downloadResponse;
    }

    @Override
    protected DownloadResponse doDownload(DownloadRequest downloadRequest, File file) {
        return doDownload(downloadRequest, file.toPath());
    }

    /**
     * Performs Blob download and returns an InputStream
     *
     * @param downloadRequest Wrapper object containing download data
     * @return Returns a DownloadResponse object that contains metadata about the blob and an InputStream for reading the content
     */
    @Override
    protected DownloadResponse doDownload(DownloadRequest downloadRequest) {
        BlobId blobId = transformer.toBlobId(downloadRequest);
        Blob blob = storage.get(blobId);
        if(blob == null) {
            throw new SubstrateSdkException("Blob not found");
        }
        try {
            ReadChannel reader = blob.reader();
            var range = transformer.computeRange(downloadRequest.getStart(), downloadRequest.getEnd(), blob.getSize());
            if(range.getLeft() != null) {
                reader.seek(range.getLeft());
            }
            if(range.getRight() != null) {
                reader.limit(range.getRight());
            }
            InputStream inputStream = Channels.newInputStream(reader);
            return transformer.toDownloadResponse(blob, inputStream);
        } catch (IOException e) {
            throw new SubstrateSdkException("Failed to create input stream for download", e);
        }
    }

    /**
     * Performs Blob download
     *
     * @param downloadRequest Wrapper object containing download data
     * @param path The Path that blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     */
    @Override
    protected DownloadResponse doDownload(DownloadRequest downloadRequest, Path path) {
        try (OutputStream outputStream = Files.newOutputStream(path)) {
            return doDownload(downloadRequest, outputStream);
        } catch (IOException e) {
            throw new SubstrateSdkException("Request failed while saving content to path", e);
        }
    }

    @Override
    protected void doDelete(String key, String versionId) {
        validateBucketExists();
        storage.delete(transformer.toBlobId(bucket, key, versionId));
    }

    @Override
    protected void doDelete(Collection<BlobIdentifier> objects) {
        validateBucketExists();
        List<BlobId> blobIds = objects.stream()
                .map(obj -> transformer.toBlobId(bucket, obj.getKey(), obj.getVersionId()))
                .collect(Collectors.toList());
        storage.delete(blobIds);
    }

    @Override
    protected CopyResponse doCopy(CopyRequest request) {
        Storage.CopyRequest copyReq = transformer.toCopyRequest(request);
        Blob blob = storage.copy(copyReq).getResult();
        return transformer.toCopyResponse(blob);
    }

    @Override
    protected CopyResponse doCopyFrom(CopyFromRequest request) {
        Storage.CopyRequest copyReq = transformer.toCopyRequest(request);
        Blob blob = storage.copy(copyReq).getResult();
        return transformer.toCopyResponse(blob);
    }

    @Override
    protected BlobMetadata doGetMetadata(String key, String versionId) {
        BlobId blobId = transformer.toBlobId(bucket, key, versionId);
        Blob blob = storage.get(blobId);
        return transformer.toBlobMetadata(blob);
    }

    @Override
    protected Iterator<com.salesforce.multicloudj.blob.driver.BlobInfo> doList(ListBlobsRequest request) {
        List<Storage.BlobListOption> listOptions = new ArrayList<>();
        listOptions.add(Storage.BlobListOption.includeFolders(false));
        if(request.getPrefix() != null) {
            listOptions.add(Storage.BlobListOption.prefix(request.getPrefix()));
        }
        if(request.getDelimiter() != null) {
            listOptions.add(Storage.BlobListOption.delimiter(request.getDelimiter()));
        }
        Storage.BlobListOption[] listOptionsArray = listOptions.toArray(new Storage.BlobListOption[0]);
        Iterable<Blob> blobs = storage.list(getBucket(), listOptionsArray).iterateAll();

        return new Iterator<>() {
            private final Iterator<Blob> blobIterator = blobs.iterator();

            @Override
            public boolean hasNext() {
                return blobIterator.hasNext();
            }

            @Override
            public com.salesforce.multicloudj.blob.driver.BlobInfo next() {
                Blob blob = blobIterator.next();
                return com.salesforce.multicloudj.blob.driver.BlobInfo.builder()
                        .withKey(blob.getName())
                        .withObjectSize(blob.getSize())
                        .withLastModified(blob.getUpdateTimeOffsetDateTime() != null ? blob.getUpdateTimeOffsetDateTime().toInstant() : null)
                        .build();
            }
        };
    }

    /**
     * Lists a single page of objects in the bucket with pagination support
     *
     * @param request The list request containing filters and optional pagination token
     * @return ListBlobsPageResult containing the blobs, truncation status, and next page token
     */
    @Override
    protected ListBlobsPageResponse doListPage(ListBlobsPageRequest request) {
        // Use the Page API to get proper pagination support
        Page<Blob> page = storage.list(getBucket(), transformer.toBlobListOptions(request));

        List<com.salesforce.multicloudj.blob.driver.BlobInfo> blobs = new ArrayList<>();
        for (Blob blob : page.getValues()) {
            blobs.add(com.salesforce.multicloudj.blob.driver.BlobInfo.builder()
                    .withKey(blob.getName())
                    .withObjectSize(blob.getSize())
                    .withLastModified(blob.getUpdateTimeOffsetDateTime() != null ? blob.getUpdateTimeOffsetDateTime().toInstant() : null)
                    .build());
        }

        return new ListBlobsPageResponse(
                blobs,
                page.hasNextPage(),
                page.getNextPageToken()
        );
    }

    @Override
    protected MultipartUpload doInitiateMultipartUpload(MultipartUploadRequest request) {
        validateBucketExists();

        CreateMultipartUploadRequest.Builder createRequestBuilder = CreateMultipartUploadRequest.builder()
            .bucket(getBucket())
            .key(request.getKey());
        if (request.getKmsKeyId() != null && !request.getKmsKeyId().isEmpty()) {
            createRequestBuilder.kmsKeyName(request.getKmsKeyId());
        }

        if (request.getMetadata() != null) {
            createRequestBuilder.metadata(request.getMetadata());
        }

        CreateMultipartUploadResponse gcpMultipartUpload =
            multipartUploadClient.createMultipartUpload(createRequestBuilder.build());

        return MultipartUpload.builder()
                .bucket(getBucket())
                .key(request.getKey())
                .id(gcpMultipartUpload.uploadId())
                .metadata(request.getMetadata())
                .tags(request.getTags())
                .kmsKeyId(request.getKmsKeyId())
                .build();
    }

    @Override
    protected com.salesforce.multicloudj.blob.driver.UploadPartResponse doUploadMultipartPart(MultipartUpload mpu, MultipartPart mpp) {
        try {
            // Read the InputStream into a byte array, then wrap in ByteBuffer
            byte[] data = ByteStreams.toByteArray(mpp.getInputStream());
            ByteBuffer buffer = ByteBuffer.wrap(data);

            UploadPartRequest uploadPartRequest = UploadPartRequest.builder().bucket(getBucket())
                    .key(mpu.getKey()).partNumber(mpp.getPartNumber())
                .uploadId(mpu.getId())
                .build();

            UploadPartResponse gcpResponse =
                multipartUploadClient.uploadPart(uploadPartRequest, RequestBody.of(buffer));

            return new com.salesforce.multicloudj.blob.driver.UploadPartResponse(mpp.getPartNumber(), gcpResponse.eTag(), -1);
        } catch (IOException e) {
            throw new SubstrateSdkException("Failed to upload multipart part", e);
        }
    }

    @Override
    protected MultipartUploadResponse doCompleteMultipartUpload(MultipartUpload mpu, List<com.salesforce.multicloudj.blob.driver.UploadPartResponse> parts) {
        List<CompletedPart> completedParts = parts.stream()
                // Google cloud rejects the multipart upload if the parts are not in order,
                // we need to bring it to parity with other cloud providers.
                .sorted(Comparator.comparingInt(com.salesforce.multicloudj.blob.driver.UploadPartResponse::getPartNumber))
                .map(part -> CompletedPart.builder()
                    .partNumber(part.getPartNumber())
                    .eTag(part.getEtag())
                    .build())
                .collect(Collectors.toList());

        CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
            .parts(completedParts)
            .build();

        CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
            .bucket(getBucket())
            .key(mpu.getKey())
            .uploadId(mpu.getId())
            .multipartUpload(completedMultipartUpload)
            .build();

        CompleteMultipartUploadResponse response = multipartUploadClient.completeMultipartUpload(completeRequest);

        return new MultipartUploadResponse(response.etag());
    }

    @Override
    protected List<com.salesforce.multicloudj.blob.driver.UploadPartResponse> doListMultipartUpload(MultipartUpload mpu) {
        ListPartsRequest listPartsRequest = ListPartsRequest.builder()
            .bucket(getBucket())
            .key(mpu.getKey())
            .uploadId(mpu.getId())
            .build();
        ListPartsResponse response = multipartUploadClient.listParts(listPartsRequest);

        return response.getParts().stream()
                .map(part -> new com.salesforce.multicloudj.blob.driver.UploadPartResponse(
                    part.partNumber(),
                    part.eTag(),
                    part.size()
                ))
                .collect(Collectors.toList());
    }

    @Override
    protected void doAbortMultipartUpload(MultipartUpload mpu) {
        AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
            .bucket(getBucket())
            .key(mpu.getKey())
            .uploadId(mpu.getId())
            .build();
        multipartUploadClient.abortMultipartUpload(abortRequest);
    }

    /**
     * Validates that the bucket exists, throwing ResourceNotFoundException if not found.
     * Uses Objects.List with pageSize(1) instead of Buckets.Get so that only
     * {@code storage.objects.list} is required on the bucket, not {@code storage.buckets.get}.
     *
     * @throws ResourceNotFoundException if the bucket does not exist
     */
    private void validateBucketExists() {
        try {
            storage.list(getBucket(), Storage.BlobListOption.pageSize(1));
        } catch (StorageException e) {
            if (e.getCode() == 404) {
                throw new ResourceNotFoundException("Bucket not found: " + bucket, e);
            }
            throw new UnknownException("Failed to check bucket existence", e);
        }
    }

    @Override
    protected Map<String, String> doGetTags(String key) {
        Blob blob = storage.get(transformer.toBlobId(key, null));
        if(blob == null) {
            throw new SubstrateSdkException("Blob not found");
        }
        if(blob.getMetadata() == null) {
            return Collections.emptyMap();
        }
        return blob.getMetadata().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(TAG_PREFIX))
                .filter(entry -> entry.getValue()!=null)
                .map(entry -> Map.entry(entry.getKey().substring(TAG_PREFIX.length()), entry.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
    
    @Override
    protected void doSetTags(String key, Map<String, String> tags) {
        Blob blob = storage.get(transformer.toBlobId(key, null));
        if (blob == null) {
            throw new SubstrateSdkException("Blob not found");
        }

        // Copy all existing metadata
        Map<String, String> metadata = blob.getMetadata() != null
                ? new HashMap<>(blob.getMetadata())
                : new HashMap<>();

        // Delete all existing tags by setting them to null
        // In GCP Storage, setting a metadata key to null means "delete this key"
        // The storage.update method only add new tags, it does not remove existing tags.
        for (String k : new ArrayList<>(metadata.keySet())) {
            if (k.startsWith(TAG_PREFIX)) {
                metadata.put(k, null);
            }
        }

        // Add new tags (these will overwrite the nulls for those keys)
        if (tags != null) {
            tags.forEach((tagName, tagValue) ->
            metadata.put(TAG_PREFIX + tagName, tagValue));
        }

        Blob updatedBlob = blob.toBuilder().setMetadata(metadata).build();
        storage.update(updatedBlob);
    }

    @Override
    protected URL doGeneratePresignedUrl(PresignedUrlRequest request) {
        var blobInfo = transformer.toBlobInfo(request);
        HttpMethod httpMethod = null;
        switch(request.getType()) {
            case UPLOAD:
                httpMethod = HttpMethod.PUT;
                break;
            case DOWNLOAD:
                httpMethod = HttpMethod.GET;
                break;
        }
        List<Storage.SignUrlOption> options = new ArrayList<>();
        options.add(Storage.SignUrlOption.httpMethod(httpMethod));
        options.add(Storage.SignUrlOption.withV4Signature());
        if (request.getMetadata() != null) {
            options.add(Storage.SignUrlOption.withExtHeaders(request.getMetadata()));
        }

        return storage.signUrl(blobInfo,
                request.getDuration().toMillis(),
                TimeUnit.MILLISECONDS,
                options.toArray(new Storage.SignUrlOption[0]));
    }

    @Override
    protected boolean doDoesObjectExist(String key, String versionId) {
        return storage.get(transformer.toBlobId(key, versionId)) != null;
    }

    /**
     * Determines if the bucket exists
     * @return Returns true if the bucket exists. Returns false if it doesn't exist.
     */
    @Override
    protected boolean doDoesBucketExist() {
        try {
            Bucket bucketObj = storage.get(bucket);
            return bucketObj != null;
        } catch (StorageException e) {
            if (e.getCode() == 404) {
                return false;
            }
            throw new SubstrateSdkException("Failed to check bucket existence", e);
        }
    }

    /**
     * Maximum number of objects that can be deleted in a single batch operation.
     * GCP supports up to 1000 objects per batch delete.
     */
    private static final int MAX_OBJECTS_PER_BATCH_DELETE = 1000;

    @Override
    protected DirectoryUploadResponse doUploadDirectory(DirectoryUploadRequest directoryUploadRequest) {
        try {
            Path sourceDir = Paths.get(directoryUploadRequest.getLocalSourceDirectory());
            List<Path> filePaths = transformer.toFilePaths(directoryUploadRequest);
            List<FailedBlobUpload> failedUploads = new ArrayList<>();
            // Create directory marker object if prefix is specified
            if (directoryUploadRequest.getPrefix() != null && !directoryUploadRequest.getPrefix().isEmpty()) {
                try {
                    String dirMarkerKey = directoryUploadRequest.getPrefix();
                    if (!dirMarkerKey.endsWith("/")) {
                        dirMarkerKey += "/";
                    }
                    com.google.cloud.storage.BlobInfo dirMarkerInfo = com.google.cloud.storage.BlobInfo.newBuilder(getBucket(), dirMarkerKey).build();
                    storage.create(dirMarkerInfo, new byte[0]); // Create empty object as directory marker
                } catch (Exception e) {
                    // Don't fail the entire upload if directory marker creation fails
                }
            }

            for (Path filePath : filePaths) {
                try {
                    // Generate blob key
                    String blobKey = transformer.toBlobKey(sourceDir, filePath, directoryUploadRequest.getPrefix());

                    // Build metadata map with tags if provided
                    Map<String, String> metadata = new HashMap<>();
                    if (directoryUploadRequest.getTags() != null && !directoryUploadRequest.getTags().isEmpty()) {
                        directoryUploadRequest.getTags().forEach((tagName, tagValue) -> 
                                metadata.put(TAG_PREFIX + tagName, tagValue));
                    }

                    // Upload file to GCS with tags applied
                    com.google.cloud.storage.BlobInfo blobInfo = com.google.cloud.storage.BlobInfo.newBuilder(getBucket(), blobKey)
                            .setMetadata(metadata.isEmpty() ? null : metadata)
                            .build();
                    storage.createFrom(blobInfo, filePath);
                } catch (Exception e) {
                    failedUploads.add(FailedBlobUpload.builder()
                            .source(filePath)
                            .exception(e)
                            .build());
                }
            }

            return DirectoryUploadResponse.builder()
                    .failedTransfers(failedUploads)
                    .build();

        } catch (Exception e) {
            throw new SubstrateSdkException("Failed to upload directory", e);
        }
    }

    @Override
    protected DirectoryDownloadResponse doDownloadDirectory(DirectoryDownloadRequest req) {
        try {
            Path targetDir = Paths.get(req.getLocalDestinationDirectory());
            Files.createDirectories(targetDir);

            final String rawPrefix = req.getPrefixToDownload();
            final String prefix = (rawPrefix != null && !rawPrefix.isEmpty() && !rawPrefix.endsWith("/"))
                    ? rawPrefix + "/"
                    : rawPrefix;

            //This can optimize performance by fetching minimal metadata instead of full blob details.
            Storage.BlobListOption[] options = (prefix != null)
                    ? new Storage.BlobListOption[] {
                            Storage.BlobListOption.prefix(prefix),
                            Storage.BlobListOption.fields(Storage.BlobField.NAME, Storage.BlobField.SIZE)
                      }
                    : new Storage.BlobListOption[] {
                            Storage.BlobListOption.fields(Storage.BlobField.NAME, Storage.BlobField.SIZE)
                      };

            List<FailedBlobDownload> failed = new ArrayList<>();

            for (Blob blob : storage.list(getBucket(), options).iterateAll()) {
                final String name = blob.getName();

                String relativePath = (prefix != null) ? name.substring(prefix.length()) : name;
                if (relativePath.isEmpty()) {
                    continue;
                }

                Path localFilePath = targetDir.resolve(relativePath).normalize();

                try {
                    Path parent = localFilePath.getParent();

                    Files.createDirectories(parent);
                    blob.downloadTo(localFilePath);
                } catch (Exception e) {
                    failed.add(FailedBlobDownload.builder()
                            .destination(localFilePath)
                            .exception(e)
                            .build());
                }
            }

            return DirectoryDownloadResponse.builder()
                    .failedTransfers(failed)
                    .build();

        } catch (Exception e) {
            throw new SubstrateSdkException("Failed to download directory", e);
        }
    }

    @Override
    protected void doDeleteDirectory(String prefix) {
        try {
            // List all blobs with the given prefix and delete them in batches
            Storage.BlobListOption[] options = prefix != null ?
                    new Storage.BlobListOption[]{
                            Storage.BlobListOption.prefix(prefix)
                    } : new Storage.BlobListOption[0];

            List<Blob> blobs = new ArrayList<>();
            for (Blob blob : storage.list(getBucket(), options).getValues()) {
                blobs.add(blob);
            }

            // Convert GCP Blob objects to DriverBlobInfo objects for partitioning
            var blobInfos = new ArrayList<com.salesforce.multicloudj.blob.driver.BlobInfo>();
            for (Blob blob : blobs) {
                blobInfos.add(com.salesforce.multicloudj.blob.driver.BlobInfo.builder()
                        .withKey(blob.getName())
                        .withObjectSize(blob.getSize())
                        .build());
            }

            // Partition the blobs into smaller chunks for batch deletion
            var partitionedBlobLists = transformer.partitionList(blobInfos, MAX_OBJECTS_PER_BATCH_DELETE);

            // Delete each partition
            for (var blobList : partitionedBlobLists) {
                List<BlobId> blobIds = blobList.stream()
                        .map(blobInfo -> BlobId.of(getBucket(), blobInfo.getKey()))
                        .collect(Collectors.toList());

                storage.delete(blobIds);
            }

        } catch (Exception e) {
            throw new SubstrateSdkException("Failed to delete directory", e);
        }
    }

    /**
     * Gets object lock configuration for a blob.
     */
    @Override
    public ObjectLockInfo getObjectLock(String key, String versionId) {
        Blob blob = storage.get(transformer.toBlobId(bucket, key, versionId));
        if (blob == null) {
            throw new ResourceNotFoundException("Object not found: " + key);
        }

        // Check for object retention
        Retention retention = blob.getRetention();
        boolean hasRetention = retention != null;
        
        // Check for object holds
        Boolean tempHold = blob.getTemporaryHold();
        Boolean eventHold = blob.getEventBasedHold();
        boolean hasHold = (tempHold != null && tempHold) || (eventHold != null && eventHold);

        if (!hasRetention && !hasHold) {
            return null;
        }

        RetentionMode mode = null;
        java.time.Instant retainUntilDate = null;
        
        if (hasRetention) {
            // Map provider retention mode to SDK retention mode
            mode = retention.getMode() == Retention.Mode.LOCKED
                    ? RetentionMode.COMPLIANCE
                    : RetentionMode.GOVERNANCE;
            retainUntilDate = retention.getRetainUntilTime() != null
                    ? retention.getRetainUntilTime().toInstant()
                    : null;
        }

        return ObjectLockInfo.builder()
                .mode(mode)
                .retainUntilDate(retainUntilDate)
                .legalHold(hasHold)
                .useEventBasedHold(eventHold != null && eventHold)
                .build();
    }

    /**
     * Updates object retention date.
     * 
     * <p>For provider:
     * <ul>
     *   <li>GOVERNANCE mode (UNLOCKED): Can be updated with bypass header if user has permission</li>
     *   <li>COMPLIANCE mode (LOCKED): Cannot be shortened or removed, only increased</li>
     * </ul>
     */
    @Override
    public void updateObjectRetention(String key, String versionId, java.time.Instant retainUntilDate) {
        Blob blob = storage.get(transformer.toBlobId(bucket, key, versionId));
        if (blob == null) {
            throw new ResourceNotFoundException("Object not found: " + key);
        }

        Retention currentRetention = blob.getRetention();
        if (currentRetention == null) {
            throw new FailedPreconditionException(
                    "Object does not have retention configured. Cannot update retention.");
        }

        Retention.Mode currentMode = currentRetention.getMode();
        
        // Check if trying to shorten retention (not allowed for LOCKED/COMPLIANCE mode)
        if (currentMode == Retention.Mode.LOCKED) {
            java.time.Instant currentRetainUntil = currentRetention.getRetainUntilTime() != null
                    ? currentRetention.getRetainUntilTime().toInstant()
                    : null;
            if (currentRetainUntil != null && retainUntilDate.isBefore(currentRetainUntil)) {
                throw new FailedPreconditionException(
                        "Cannot reduce retention for objects in COMPLIANCE (LOCKED) mode. " +
                        "Only GOVERNANCE (UNLOCKED) mode objects can have their retention reduced, " +
                        "and COMPLIANCE mode retention can only be increased.");
            }
        }

        // Build updated retention with same mode but new retain-until time
        Retention updatedRetention = currentRetention.toBuilder()
                .setRetainUntilTime(java.time.OffsetDateTime.ofInstant(retainUntilDate, java.time.ZoneOffset.UTC))
                .build();

        com.google.cloud.storage.BlobInfo updatedBlobInfo = blob.toBuilder().setRetention(updatedRetention).build();
        
        // For GOVERNANCE (UNLOCKED) mode, use bypass header if shortening retention
        if (currentMode == Retention.Mode.UNLOCKED) {
            java.time.Instant currentRetainUntil = currentRetention.getRetainUntilTime() != null
                    ? currentRetention.getRetainUntilTime().toInstant()
                    : null;
            if (currentRetainUntil != null && retainUntilDate.isBefore(currentRetainUntil)) {
                // Shortening retention requires bypass header
                storage.update(updatedBlobInfo, Storage.BlobTargetOption.overrideUnlockedRetention(true));
            } else {
                // Increasing retention doesn't need bypass header
                storage.update(updatedBlobInfo);
            }
        } else {
            // COMPLIANCE (LOCKED) mode - only allow increasing
            storage.update(updatedBlobInfo);
        }
    }

    /**
     * Updates legal hold status on an object.
     */
    @Override
    public void updateLegalHold(String key, String versionId, boolean legalHold) {
        Blob blob = storage.get(transformer.toBlobId(bucket, key, versionId));
        if (blob == null) {
            throw new ResourceNotFoundException("Object not found: " + key);
        }

        // Determine which hold type to use based on existing configuration
        // If object has eventBasedHold, use that; otherwise use temporaryHold
        Boolean existingEventHold = blob.getEventBasedHold();
        boolean useEventBased = existingEventHold != null && existingEventHold;

        com.google.cloud.storage.BlobInfo.Builder builder = blob.toBuilder();
        if (useEventBased) {
            builder.setEventBasedHold(legalHold);
        } else {
            builder.setTemporaryHold(legalHold);
        }

        storage.update(builder.build());
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        if (t instanceof SubstrateSdkException) {
            return (Class<? extends SubstrateSdkException>) t.getClass();
        } else if (t instanceof ApiException) {
            ApiException exception = (ApiException) t;
            StatusCode statusCode = exception.getStatusCode();
            return CommonErrorCodeMapping.getException(statusCode.getCode());
        } else if (t instanceof StorageException) {
            return CommonErrorCodeMapping.getException(((StorageException) t).getCode());
        } else if(t instanceof IllegalArgumentException) {
            return InvalidArgumentException.class;
        }
        return UnknownException.class;
    }

    /**
     * Closes the underlying GCP Storage client and releases any resources.
     */
    @Override
    public void close() {
        try {
            if (storage != null) {
                storage.close();
            }
        } catch (Exception e) {
            throw new SubstrateSdkException("Failed to close storage client", e);
        }
    }

    @Getter
    public static class Builder extends AbstractBlobStore.Builder<GcpBlobStore, Builder> {

        private Storage storage;
        private MultipartUploadClient mpuClient;
        private GcpTransformerSupplier transformerSupplier = new GcpTransformerSupplier();

        public Builder() {
            providerId(GcpConstants.PROVIDER_ID);
        }

        @Override
        public Builder self() {
            return this;
        }

        public Builder withStorage(Storage storage) {
            this.storage = storage;
            return this;
        }

        public Builder withMultipartUploadClient(MultipartUploadClient mpuClient) {
            this.mpuClient = mpuClient;
            return this;
        }

        public Builder withTransformerSupplier(GcpTransformerSupplier transformerSupplier) {
            this.transformerSupplier = transformerSupplier;
            return this;
        }

        /**
         * Copies all configuration from another BlobStoreBuilder using reflection.
         * This automatically handles all fields without needing manual updates when new configs are added.
         * @param source The source builder to copy from
         * @return An instance of self
         */
        public Builder copyFrom(BlobStoreBuilder<?> source) {
            try {
                // Find all "with*" methods in this builder
                Method[] methods = BlobStoreBuilder.class.getDeclaredMethods();

                for (Method method : methods) {
                    String methodName = method.getName();

                    // Look for "with*" setter methods
                    if (methodName.startsWith("with") && method.getParameterCount() == 1) {
                        // Extract property name (e.g., "withBucket" -> "Bucket")
                        String propertyName = methodName.substring(4);

                        // Try to find corresponding getter (e.g., "getBucket")
                        String getterName = "get" + propertyName;

                        try {
                            Method getter = BlobStoreBuilder.class.getMethod(getterName);
                            Object value = getter.invoke(source);

                            // Only copy non-null values
                            if (value != null) {
                                method.invoke(this, value);
                            }
                        } catch (NoSuchMethodException e) {
                            // Getter doesn't exist, skip this property
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to copy builder configuration", e);
            }
            return this;
        }

        /**
         * Normalizes endpoint to ensure it ends with "/"
         */
        private static String normalizeEndpoint(URI endpoint) {
            if (endpoint == null) {
                return null;
            }
            String endpointStr = endpoint.toString();
            if (!endpointStr.endsWith("/")) {
                endpointStr = endpointStr + "/";
            }
            return endpointStr;
        }

        /**
         * Creates HttpTransportOptions with ApacheHttpTransport
         */
        private static HttpTransportOptions buildTransportOptions(Builder builder) {
            CloseableHttpClient httpClient = buildHttpClient(builder);
            ApacheHttpTransport transport = new ApacheHttpTransport(httpClient);
            return HttpTransportOptions.newBuilder()
                    .setHttpTransportFactory(() -> transport)
                    .build();
        }

        /**
         * Helper function for generating the Storage client
         */
        private static Storage buildStorage(Builder builder) {
            HttpTransportOptions transportOptions = buildTransportOptions(builder);

            StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder();
            storageOptionsBuilder.setTransportOptions(transportOptions);

            String endpoint = normalizeEndpoint(builder.getEndpoint());
            if (endpoint != null) {
                storageOptionsBuilder.setHost(endpoint);
            }

            if (builder.getCredentialsOverrider() != null) {
                Credentials credentials = GcpCredentialsProvider.getCredentials(builder.getCredentialsOverrider());
                storageOptionsBuilder.setCredentials(credentials);
            }

            if (builder.getRetryConfig() != null) {
                GcpTransformer transformer = builder.transformerSupplier.get(builder.getBucket());
                storageOptionsBuilder.setRetrySettings(transformer.toGcpRetrySettings(builder.getRetryConfig()));
            }

            return storageOptionsBuilder.build().getService();
        }

        /**
         * Helper function for generating the MultipartUpload client
         */
        private static MultipartUploadClient buildMultipartUploadClient(Builder builder) {
            HttpTransportOptions transportOptions = buildTransportOptions(builder);

            HttpStorageOptions.Builder storageOptionsBuilder  = HttpStorageOptions.http().setTransportOptions(transportOptions);

            String endpoint = normalizeEndpoint(builder.getEndpoint());
            if (endpoint != null) {
                storageOptionsBuilder.setHost(endpoint);
            }

            if (builder.getCredentialsOverrider() != null) {
                Credentials credentials = GcpCredentialsProvider.getCredentials(builder.getCredentialsOverrider());
                storageOptionsBuilder.setCredentials(credentials);
            }

            if (builder.getRetryConfig() != null) {
                GcpTransformer transformer = builder.transformerSupplier.get(builder.getBucket());
                storageOptionsBuilder.setRetrySettings(transformer.toGcpRetrySettings(builder.getRetryConfig()));
            }

            return MultipartUploadClient.create(MultipartUploadSettings.of(storageOptionsBuilder.build()));
        }

        private static CloseableHttpClient buildHttpClient(Builder builder) {
            HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
            httpClientBuilder.setDefaultRequestConfig(buildRequestConfig(builder));
            httpClientBuilder.setConnectionManager(buildConnectionManager(builder));
            if(builder.getIdleConnectionTimeout() != null) {
                httpClientBuilder.evictIdleConnections(builder.getIdleConnectionTimeout().toMillis(), TimeUnit.MILLISECONDS);
            }
            return httpClientBuilder.build();
        }

        private static HttpClientConnectionManager buildConnectionManager(Builder builder) {
            PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
            if(builder.getMaxConnections() != null) {
                connectionManager.setMaxTotal(builder.getMaxConnections());
                connectionManager.setDefaultMaxPerRoute(builder.getMaxConnections());
            }
            return connectionManager;
        }

        private static RequestConfig buildRequestConfig(Builder builder) {
            RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
            if(builder.getSocketTimeout() != null) {
                requestConfigBuilder.setSocketTimeout((int)builder.getSocketTimeout().toMillis());
            }
            if(builder.getProxyEndpoint() != null) {
                HttpHost proxyHost = new HttpHost(builder.getProxyEndpoint().getHost(),
                        builder.getProxyEndpoint().getPort(),
                        builder.getProxyEndpoint().getScheme());
                requestConfigBuilder.setProxy(proxyHost);
            }
            return requestConfigBuilder.build();
        }

        @Override
        public GcpBlobStore build() {
            Storage storage = this.storage;
            MultipartUploadClient mpuClient = this.mpuClient;
            if(storage == null) {
                storage = buildStorage(this);
            }
            if(mpuClient == null) {
                mpuClient = buildMultipartUploadClient(this);
            }
            return new GcpBlobStore(this, storage, mpuClient);
        }
    }
}