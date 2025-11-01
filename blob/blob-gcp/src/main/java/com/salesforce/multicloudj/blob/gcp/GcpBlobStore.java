package com.salesforce.multicloudj.blob.gcp;

import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.api.gax.paging.Page;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.Credentials;
import com.google.auth.ServiceAccountSigner;
import com.google.auto.service.AutoService;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.io.ByteStreams;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.FailedBlobDownload;
import com.salesforce.multicloudj.blob.driver.FailedBlobUpload;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.MultipartUploadResponse;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.gcp.CommonErrorCodeMapping;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.gcp.GcpCredentialsProvider;
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
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * GCP implementation of BlobStore
 */
@SuppressWarnings("rawtypes")
@AutoService(AbstractBlobStore.class)
public class GcpBlobStore extends AbstractBlobStore<GcpBlobStore> {

    private final Storage storage;
    private final GcpTransformer transformer;
    private static final String TAG_PREFIX = "gcp-tag-";

    public GcpBlobStore() {
        this(new Builder(), null);
    }

    public GcpBlobStore(Builder builder, Storage storage) {
        super(builder);
        this.storage = storage;
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
    protected BlobMetadata doGetMetadata(String key, String versionId) {
        BlobId blobId = transformer.toBlobId(bucket, key, versionId);
        Blob blob = storage.get(blobId);
        return transformer.toBlobMetadata(blob);
    }

    @Override
    protected Iterator<BlobInfo> doList(ListBlobsRequest request) {
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
            public BlobInfo next() {
                Blob blob = blobIterator.next();
                return BlobInfo.builder()
                        .withKey(blob.getName())
                        .withObjectSize(blob.getSize())
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

        List<BlobInfo> blobs = new ArrayList<>();
        for (Blob blob : page.getValues()) {
            blobs.add(BlobInfo.builder()
                    .withKey(blob.getName())
                    .withObjectSize(blob.getSize())
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

        String uploadId = UUID.randomUUID().toString();

        return MultipartUpload.builder()
                .bucket(getBucket())
                .key(request.getKey())
                .id(uploadId)
                .metadata(request.getMetadata())
                .kmsKeyId(request.getKmsKeyId())
                .build();
    }

    @Override
    protected UploadPartResponse doUploadMultipartPart(MultipartUpload mpu, MultipartPart mpp) {
        UploadRequest uploadRequest = transformer.toUploadRequest(mpu, mpp);
        UploadResponse uploadResponse = doUpload(uploadRequest, mpp.getInputStream());
        return new UploadPartResponse(mpp.getPartNumber(), uploadResponse.getETag(), mpp.getContentLength());
    }

    @Override
    protected MultipartUploadResponse doCompleteMultipartUpload(MultipartUpload mpu, List<UploadPartResponse> parts) {
        List<BlobId> sourceBlobs = parts.stream()
                .map(part -> BlobId.of(getBucket(), transformer.toPartName(mpu, part.getPartNumber())))
                .collect(Collectors.toList());
        List<String> blobPartKeys = sourceBlobs.stream()
                .map(part -> part.getName())
                .collect(Collectors.toList());

        Blob composedBlob = storage.compose(Storage.ComposeRequest.newBuilder()
                .setTarget(transformer.toBlobInfo(mpu))
                .addSource(blobPartKeys)
                .build());

        // Clean up the temporary part objects
        storage.delete(sourceBlobs);

        return new MultipartUploadResponse(composedBlob.getEtag());
    }

    @Override
    protected List<UploadPartResponse> doListMultipartUpload(MultipartUpload mpu) {
        return transformer.toUploadPartResponseList(listMultipartParts(mpu));
    }

    @Override
    protected void doAbortMultipartUpload(MultipartUpload mpu) {
        Page<Blob> blobs = listMultipartParts(mpu);
        List<BlobId> blobIds = transformer.toBlobIdList(blobs);
        storage.delete(blobIds);
    }

    /**
     * Validates that the bucket exists, throwing ResourceNotFoundException if not found.
     *
     * @throws ResourceNotFoundException if the bucket does not exist
     */
    private void validateBucketExists() {
        try {
            Bucket bucketObj = storage.get(bucket);
            if (bucketObj == null) {
                throw new ResourceNotFoundException("Bucket not found: " + bucket);
            }
        } catch (StorageException e) {
            throw new ResourceNotFoundException("Bucket not found: " + bucket, e);
        }
    }

    private Page<Blob> listMultipartParts(MultipartUpload mpu) {
        return storage.list(getBucket(), Storage.BlobListOption.prefix(mpu.getKey() + "/" + mpu.getId() + "/part-"));
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
        if(blob == null) {
            throw new SubstrateSdkException("Blob not found");
        }

        Map<String, String> metadata = new HashMap<>();
        if(blob.getMetadata() != null) {
            metadata.putAll(blob.getMetadata());
        }
        // Remove all existing tags
        metadata.entrySet().removeIf(entry -> entry.getKey().startsWith(TAG_PREFIX));

        // Add in all the new tags
        if(tags != null) {
            tags.forEach((tagName, tagValue) -> metadata.put(TAG_PREFIX + tagName, tagValue));
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
        return storage.signUrl(blobInfo,
                request.getDuration().toMillis(),
                TimeUnit.MILLISECONDS,
                Storage.SignUrlOption.httpMethod(httpMethod),
                Storage.SignUrlOption.withV4Signature());
    }

    @Override
    protected boolean doDoesObjectExist(String key, String versionId) {
        return storage.get(transformer.toBlobId(key, versionId)) != null;
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

                    // Upload file to GCS - use same approach as single file upload
                    com.google.cloud.storage.BlobInfo blobInfo = com.google.cloud.storage.BlobInfo.newBuilder(getBucket(), blobKey).build();
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
            var blobInfos = new ArrayList<BlobInfo>();
            for (Blob blob : blobs) {
                blobInfos.add(BlobInfo.builder()
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

    @Getter
    public static class Builder extends AbstractBlobStore.Builder<GcpBlobStore> {

        private Storage storage;
        private GcpTransformerSupplier transformerSupplier = new GcpTransformerSupplier();

        public Builder() {
            providerId(GcpConstants.PROVIDER_ID);
        }

        public Builder withStorage(Storage storage) {
            this.storage = storage;
            return this;
        }

        public Builder withTransformerSupplier(GcpTransformerSupplier transformerSupplier) {
            this.transformerSupplier = transformerSupplier;
            return this;
        }

        /**
         * Helper function for generating the Storage client
         */
        private static Storage buildStorage(Builder builder) {
            CloseableHttpClient httpClient = buildHttpClient(builder);

            ApacheHttpTransport transport = new ApacheHttpTransport(httpClient);
            HttpTransportOptions transportOptions = HttpTransportOptions.newBuilder()
                    .setHttpTransportFactory(() -> transport)
                    .build();

            StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder();
            storageOptionsBuilder.setTransportOptions(transportOptions);

            if (builder.getEndpoint() != null) {
                storageOptionsBuilder.setHost(builder.getEndpoint().toString());
            }

            if (builder.getCredentialsOverrider() != null) {
                Credentials credentials = GcpCredentialsProvider.getCredentials(builder.getCredentialsOverrider());
                storageOptionsBuilder.setCredentials(credentials);
            }

            return storageOptionsBuilder.build().getService();
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
            if(storage == null) {
                storage = buildStorage(this);
            }
            return new GcpBlobStore(this, storage);
        }
    }
}