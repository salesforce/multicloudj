package com.salesforce.multicloudj.blob.gcp;

import com.google.auto.service.AutoService;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
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
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.provider.Provider;
import lombok.Getter;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * GCP implementation of BlobStore
 */
@SuppressWarnings("rawtypes")
@AutoService(AbstractBlobStore.class)
public class GcpBlobStore extends AbstractBlobStore<GcpBlobStore> {

    private final Bucket client;

    public GcpBlobStore() {
        this(new Builder(), null);
    }

    public GcpBlobStore(Builder builder, Bucket client) {
        super(builder);
        this.client = client;
    }

    @Override
    public Provider.Builder builder() {
        return new Builder();
    }

    @Override
    protected UploadResponse doUpload(UploadRequest uploadRequest, InputStream inputStream) {
        return null;
    }

    @Override
    protected UploadResponse doUpload(UploadRequest uploadRequest, byte[] content) {
        return null;
    }

    @Override
    protected UploadResponse doUpload(UploadRequest uploadRequest, File file) {
        return null;
    }

    @Override
    protected UploadResponse doUpload(UploadRequest uploadRequest, Path path) {
        return null;
    }

    @Override
    protected DownloadResponse doDownload(DownloadRequest downloadRequest, OutputStream outputStream) {
        return null;
    }

    @Override
    protected DownloadResponse doDownload(DownloadRequest downloadRequest, ByteArray byteArray) {
        return null;
    }

    @Override
    protected DownloadResponse doDownload(DownloadRequest downloadRequest, File file) {
        return null;
    }

    @Override
    protected DownloadResponse doDownload(DownloadRequest downloadRequest, Path path) {
        return null;
    }

    @Override
    protected void doDelete(String key, String versionId) {

    }

    @Override
    protected void doDelete(Collection<BlobIdentifier> objects) {

    }

    @Override
    protected CopyResponse doCopy(CopyRequest request) {
        return null;
    }

    @Override
    protected BlobMetadata doGetMetadata(String key, String versionId) {
        return null;
    }

    @Override
    protected Iterator<BlobInfo> doList(ListBlobsRequest request) {
        return null;
    }

    @Override
    protected MultipartUpload doInitiateMultipartUpload(MultipartUploadRequest request) {
        return null;
    }

    @Override
    protected UploadPartResponse doUploadMultipartPart(MultipartUpload mpu, MultipartPart mpp) {
        return null;
    }

    @Override
    protected MultipartUploadResponse doCompleteMultipartUpload(MultipartUpload mpu, List<UploadPartResponse> parts) {
        return null;
    }

    @Override
    protected List<UploadPartResponse> doListMultipartUpload(MultipartUpload mpu) {
        return List.of();
    }

    @Override
    protected void doAbortMultipartUpload(MultipartUpload mpu) {

    }

    @Override
    protected Map<String, String> doGetTags(String key) {
        return Map.of();
    }

    @Override
    protected void doSetTags(String key, Map<String, String> tags) {

    }

    @Override
    protected URL doGeneratePresignedUrl(PresignedUrlRequest request) {
        return null;
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        return null;
    }

    @Getter
    public static class Builder extends AbstractBlobStore.Builder<GcpBlobStore> {

        private Bucket client;

        public Builder() {
            providerId(GcpConstants.PROVIDER_ID);
        }

        private static Bucket buildClient(Builder builder) {

            Storage storage = StorageOptions.getDefaultInstance().getService();

            /**
             * TODO: Put the following in here:
             *    - Specify the proxy hostname/port
             *    - Use the CredentialsOverrider
             *    - Allow endpoint override
             *    - maxConnections / socketTimeout / idleConnectionTimeout
             */

            return storage.get(builder.getBucket());
        }

        public GcpBlobStore.Builder withClient(Bucket client) {
            this.client = client;
            return this;
        }

        @Override
        public GcpBlobStore build() {
            if(client == null) {
                client = buildClient(this);
            }
            return new GcpBlobStore(this, client);
        }
    }
}