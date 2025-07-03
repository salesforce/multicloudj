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
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.io.ByteStreams;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
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
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.gcp.CommonErrorCodeMapping;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
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
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    public GcpBlobStore() {
        this(new Builder(), null);
    }

    public GcpBlobStore(Builder builder, Storage storage) {
        super(builder);
        this.storage = storage;
        this.transformer = builder.getTransformerSupplier().get(bucket);
    }

    @Override
    public Provider.Builder builder() {
        return new Builder();
    }

    @Override
    protected UploadResponse doUpload(UploadRequest uploadRequest, InputStream inputStream) {
        try (WriteChannel writer = storage.writer(transformer.toBlobInfo(uploadRequest));
             var channel = Channels.newOutputStream(writer)) {
            ByteStreams.copy(inputStream, channel);
            Blob blob = storage.get(getBucket(), uploadRequest.getKey());
            return transformer.toUploadResponse(blob);
        } catch (IOException e) {
            throw new SubstrateSdkException("Request failed while uploading from input stream", e);
        }
    }

    @Override
    protected UploadResponse doUpload(UploadRequest uploadRequest, byte[] content) {
        Blob blob = storage.create(transformer.toBlobInfo(uploadRequest), content);
        return transformer.toUploadResponse(blob);
    }

    @Override
    protected UploadResponse doUpload(UploadRequest uploadRequest, File file) {
        return doUpload(uploadRequest, file.toPath());
    }

    @Override
    protected UploadResponse doUpload(UploadRequest uploadRequest, Path path) {
        try {
            Blob blob = storage.createFrom(transformer.toBlobInfo(uploadRequest), path);
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

            if(downloadRequest.getStart() != null) {
                reader.seek(downloadRequest.getStart());
            }
            if(downloadRequest.getEnd() != null) {
                reader.limit(downloadRequest.getEnd());
            }

            ByteStreams.copy(channel, outputStream);
            Blob blob = storage.get(blobId);
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
        storage.delete(transformer.toBlobId(key, versionId));
    }

    @Override
    protected void doDelete(Collection<BlobIdentifier> objects) {
        List<BlobId> blobIds = objects.stream()
                .map(obj -> transformer.toBlobId(obj.getKey(), obj.getVersionId()))
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
        BlobId blobId = transformer.toBlobId(key, versionId);
        Blob blob = storage.get(blobId);
        return transformer.toBlobMetadata(blob);
    }

    @Override
    protected Iterator<BlobInfo> doList(ListBlobsRequest request) {
        List<Storage.BlobListOption> listOptions = new ArrayList<>();
        if(request.getPrefix() != null) {
            listOptions.add(Storage.BlobListOption.prefix(request.getPrefix()));
        }
        if(request.getDelimiter() != null) {
            listOptions.add(Storage.BlobListOption.delimiter(request.getDelimiter()));
        }
        Storage.BlobListOption[] listOptionsArray = listOptions.toArray(new Storage.BlobListOption[0]);
        Iterable<com.google.cloud.storage.Blob> blobs = storage.list(getBucket(), listOptionsArray).iterateAll();

        return new Iterator<>() {
            private final Iterator<com.google.cloud.storage.Blob> it = blobs.iterator();

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public BlobInfo next() {
                com.google.cloud.storage.Blob blob = it.next();
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
        Page<com.google.cloud.storage.Blob> page = storage.list(getBucket(), transformer.toBlobListOptions(request));
        
        List<BlobInfo> blobs = page.streamAll()
                .map(blob -> BlobInfo.builder()
                        .withKey(blob.getName())
                        .withObjectSize(blob.getSize())
                        .build())
                .collect(Collectors.toList());

        return new ListBlobsPageResponse(
                blobs,
                page.hasNextPage(),
                page.getNextPageToken()
        );
    }

    @Override
    protected MultipartUpload doInitiateMultipartUpload(MultipartUploadRequest request) {
        throw new UnSupportedOperationException("MultipartUploads are not supported by GCP");
    }

    @Override
    protected UploadPartResponse doUploadMultipartPart(MultipartUpload mpu, MultipartPart mpp) {
        throw new UnSupportedOperationException("MultipartUploads are not supported by GCP");
    }

    @Override
    protected MultipartUploadResponse doCompleteMultipartUpload(MultipartUpload mpu, List<UploadPartResponse> parts) {
        throw new UnSupportedOperationException("MultipartUploads are not supported by GCP");
    }

    @Override
    protected List<UploadPartResponse> doListMultipartUpload(MultipartUpload mpu) {
        throw new UnSupportedOperationException("MultipartUploads are not supported by GCP");
    }

    @Override
    protected void doAbortMultipartUpload(MultipartUpload mpu) {
        throw new UnSupportedOperationException("MultipartUploads are not supported by GCP");
    }

    @Override
    protected Map<String, String> doGetTags(String key) {
        throw new UnSupportedOperationException("Tags are not supported by GCP");
    }

    @Override
    protected void doSetTags(String key, Map<String, String> tags) {
        throw new UnSupportedOperationException("Tags are not supported by GCP");
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
            Storage storage = getStorage();
            if(storage == null) {
                storage = buildStorage(this);
            }
            return new GcpBlobStore(this, storage);
        }
    }
}