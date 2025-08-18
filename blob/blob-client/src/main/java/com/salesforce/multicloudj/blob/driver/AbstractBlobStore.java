package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
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
 * Base class for substrate-specific implementations.AbstractBlobStore
 * This class serves the purpose of providing common (i.e. substrate-agnostic) functionality
 */
public abstract class AbstractBlobStore<T extends AbstractBlobStore<T>> implements BlobStore {

    @Getter
    private final String providerId;
    @Getter
    protected final String bucket;
    @Getter
    protected final String region;
    protected final CredentialsOverrider credentialsOverrider;
    protected final BlobStoreValidator validator;

    protected AbstractBlobStore(Builder<T> builder) {
        this(
                builder.getProviderId(),
                builder.getBucket(),
                builder.getRegion(),
                builder.getCredentialsOverrider(),
                builder.getValidator()
        );
    }

    public AbstractBlobStore(
            String providerId,
            String bucket,
            String region,
            CredentialsOverrider credentials,
            BlobStoreValidator validator
    ){
        this.providerId = providerId;
        this.bucket = bucket;
        this.region = region;
        this.credentialsOverrider = credentials;
        this.validator = validator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UploadResponse upload(UploadRequest uploadRequest, InputStream inputStream) {
        validator.validate(uploadRequest);
        return doUpload(uploadRequest, inputStream);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UploadResponse upload(UploadRequest uploadRequest, byte[] content) {
        validator.validate(uploadRequest);
        return doUpload(uploadRequest, content);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UploadResponse upload(UploadRequest uploadRequest, File file) {
        validator.validate(uploadRequest);
        return doUpload(uploadRequest, file);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UploadResponse upload(UploadRequest uploadRequest, Path path) {
        validator.validate(uploadRequest);
        return doUpload(uploadRequest, path);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DownloadResponse download(DownloadRequest downloadRequest, OutputStream outputStream) {
        validator.validate(downloadRequest);
        return doDownload(downloadRequest, outputStream);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DownloadResponse download(DownloadRequest downloadRequest, ByteArray byteArray) {
        validator.validate(downloadRequest);
        return doDownload(downloadRequest, byteArray);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DownloadResponse download(DownloadRequest downloadRequest, File file) {
        validator.validate(downloadRequest);
        return doDownload(downloadRequest, file);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DownloadResponse download(DownloadRequest downloadRequest, Path path) {
        validator.validate(downloadRequest);
        return doDownload(downloadRequest, path);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DownloadResponse download(DownloadRequest downloadRequest) {
        validator.validate(downloadRequest);
        return doDownload(downloadRequest);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String key, String versionId) {
        validator.validateDelete(key);
        doDelete(key, versionId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(Collection<BlobIdentifier> objects) {
        validator.validateBlobIdentifiers(objects);
        doDelete(objects);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CopyResponse copy(CopyRequest request) {
        validator.validate(request);
        return doCopy(request);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlobMetadata getMetadata(String key, String versionId) {
        validator.validateKey(key);
        return doGetMetadata(key, versionId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<BlobInfo> list(ListBlobsRequest request) {
        return doList(request);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListBlobsPageResponse listPage(ListBlobsPageRequest request) {
        return doListPage(request);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MultipartUpload initiateMultipartUpload(MultipartUploadRequest request) {
        return doInitiateMultipartUpload(request);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UploadPartResponse uploadMultipartPart(MultipartUpload mpu, MultipartPart mpp) {
        validator.validate(mpu, getBucket());
        return doUploadMultipartPart(mpu, mpp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MultipartUploadResponse completeMultipartUpload(MultipartUpload mpu, List<UploadPartResponse> parts) {
        validator.validate(mpu, getBucket());
        return doCompleteMultipartUpload(mpu, parts);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<UploadPartResponse> listMultipartUpload(MultipartUpload mpu) {
        validator.validate(mpu, getBucket());
        return doListMultipartUpload(mpu);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void abortMultipartUpload(MultipartUpload mpu) {
        validator.validate(mpu, getBucket());
        doAbortMultipartUpload(mpu);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getTags(String key) {
        validator.validateKey(key);
        return doGetTags(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setTags(String key, Map<String, String> tags) {
        validator.validateKey(key);
        validator.validateTags(tags);
        doSetTags(key, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public URL generatePresignedUrl(PresignedUrlRequest request) {
        validator.validate(request);
        return doGeneratePresignedUrl(request);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean doesObjectExist(String key, String versionId) {
        validator.validateKey(key);
        return doDoesObjectExist(key, versionId);
    }

    protected abstract UploadResponse doUpload(UploadRequest uploadRequest, InputStream inputStream);

    protected abstract UploadResponse doUpload(UploadRequest uploadRequest, byte[] content);

    protected abstract UploadResponse doUpload(UploadRequest uploadRequest, File file);

    protected abstract UploadResponse doUpload(UploadRequest uploadRequest, Path path);

    protected abstract DownloadResponse doDownload(DownloadRequest downloadRequest, OutputStream outputStream);

    protected abstract DownloadResponse doDownload(DownloadRequest downloadRequest, ByteArray byteArray);

    protected abstract DownloadResponse doDownload(DownloadRequest downloadRequest, File file);

    protected abstract DownloadResponse doDownload(DownloadRequest downloadRequest, Path path);

    protected abstract DownloadResponse doDownload(DownloadRequest downloadRequest);

    protected abstract void doDelete(String key, String versionId);

    protected abstract void doDelete(Collection<BlobIdentifier> objects);

    protected abstract CopyResponse doCopy(CopyRequest request);

    protected abstract BlobMetadata doGetMetadata(String key, String versionId);

    protected abstract Iterator<BlobInfo> doList(ListBlobsRequest request);

    protected abstract ListBlobsPageResponse doListPage(ListBlobsPageRequest request);

    protected abstract MultipartUpload doInitiateMultipartUpload(MultipartUploadRequest request);

    protected abstract UploadPartResponse doUploadMultipartPart(MultipartUpload mpu, MultipartPart mpp);

    protected abstract MultipartUploadResponse doCompleteMultipartUpload(MultipartUpload mpu, List<UploadPartResponse> parts);

    protected abstract List<UploadPartResponse> doListMultipartUpload(MultipartUpload mpu);

    protected abstract void doAbortMultipartUpload(MultipartUpload mpu);

    protected abstract Map<String, String> doGetTags(String key);

    protected abstract void doSetTags(String key, Map<String, String> tags);

    protected abstract URL doGeneratePresignedUrl(PresignedUrlRequest request);

    protected abstract boolean doDoesObjectExist(String key, String versionId);

    public abstract static class Builder<T extends AbstractBlobStore<T>>
            extends BlobStoreBuilder<T>
            implements Provider.Builder {

        @Override
        public Builder<T> providerId(String providerId) {
            super.providerId(providerId);
            return this;
        }
    }
}
