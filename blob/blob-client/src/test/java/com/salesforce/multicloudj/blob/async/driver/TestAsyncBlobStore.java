package com.salesforce.multicloudj.blob.async.driver;

import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.BlobStoreValidator;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsBatch;
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
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class TestAsyncBlobStore extends AbstractAsyncBlobStore {

    public static final String PROVIDER_ID = "async-test";

    public TestAsyncBlobStore(
            String providerId,
            String bucket,
            String region,
            CredentialsOverrider credentialsOverrider,
            BlobStoreValidator validator
    ) {
        super(providerId, bucket, region, credentialsOverrider, validator);
    }

    @Override
    protected CompletableFuture<UploadResponse> doUpload(UploadRequest uploadRequest, InputStream inputStream) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<UploadResponse> doUpload(UploadRequest uploadRequest, byte[] content) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<UploadResponse> doUpload(UploadRequest uploadRequest, File file) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<UploadResponse> doUpload(UploadRequest uploadRequest, Path path) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<DownloadResponse> doDownload(DownloadRequest request, OutputStream outputStream) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<DownloadResponse> doDownload(DownloadRequest request, ByteArray byteArray) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<DownloadResponse> doDownload(DownloadRequest request, File file) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<DownloadResponse> doDownload(DownloadRequest request, Path path) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<Void> doDelete(String key, String versionId) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<Void> doDelete(Collection<BlobIdentifier> objects) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<CopyResponse> doCopy(CopyRequest request) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<BlobMetadata> doGetMetadata(String key, String versionId) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<Void> doList(ListBlobsRequest request, Consumer<ListBlobsBatch> consumer) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<ListBlobsPageResponse> doListPage(ListBlobsPageRequest request) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<MultipartUpload> doInitiateMultipartUpload(MultipartUploadRequest request) {
        return null;
    }

    @Override
    protected CompletableFuture<UploadPartResponse> doUploadMultipartPart(MultipartUpload mpu, MultipartPart mpp) {
        return null;
    }

    @Override
    protected CompletableFuture<MultipartUploadResponse> doCompleteMultipartUpload(MultipartUpload mpu, List<UploadPartResponse> parts) {
        return null;
    }

    @Override
    protected CompletableFuture<List<UploadPartResponse>> doListMultipartUpload(MultipartUpload mpu) {
        return null;
    }

    @Override
    protected CompletableFuture<Void> doAbortMultipartUpload(MultipartUpload mpu) {
        return null;
    }

    @Override
    protected CompletableFuture<Map<String, String>> doGetTags(String key) {
        return null;
    }

    @Override
    protected CompletableFuture<Void> doSetTags(String key, Map<String, String> tags) {
        return null;
    }

    @Override
    protected CompletableFuture<URL> doGeneratePresignedUrl(PresignedUrlRequest request) {
        return null;
    }

    @Override
    protected CompletableFuture<Boolean> doDoesObjectExist(String key, String versionId) {
        return null;
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        return SubstrateSdkException.class;
    }

    @Override
    protected CompletableFuture<DirectoryDownloadResponse> doDownloadDirectory(DirectoryDownloadRequest directoryDownloadRequest) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<DirectoryUploadResponse> doUploadDirectory(DirectoryUploadRequest directoryUploadRequest) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<Void> doDeleteDirectory(String prefix) {
        return CompletableFuture.completedFuture(null);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AsyncBlobStoreProvider.Builder {

        public Builder() {
            providerId(PROVIDER_ID);
        }

        @Override
        public Builder withBucket(String bucket) {
            super.withBucket(bucket);
            return this;
        }

        @Override
        public Builder withRegion(String region) {
            super.withRegion(region);
            return this;
        }

        @Override
        public Builder withEndpoint(URI endpoint) {
            super.withEndpoint(endpoint);
            return this;
        }

        @Override
        public Builder withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
            super.withCredentialsOverrider(credentialsOverrider);
            return this;
        }

        @Override
        public Builder withValidator(BlobStoreValidator validator) {
            super.withValidator(validator);
            return this;
        }

        @Override
        public Builder withProperties(Properties properties) {
            super.withProperties(properties);
            return this;
        }

        @Override
        public TestAsyncBlobStore build() {
            return new TestAsyncBlobStore(
                    getProviderId(),
                    getBucket(),
                    getRegion(),
                    getCredentialsOverrider(),
                    getValidator()
            );
        }
    }
}
