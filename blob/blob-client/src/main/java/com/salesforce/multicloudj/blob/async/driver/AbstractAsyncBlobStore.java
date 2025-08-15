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
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import lombok.Getter;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Baseline blob store for async api calls.
 */
public abstract class AbstractAsyncBlobStore implements AsyncBlobStore {

    @Getter
    private final String providerId;
    @Getter
    protected final String bucket;
    @Getter
    protected final String region;
    protected final CredentialsOverrider credentialsOverrider;
    protected final BlobStoreValidator validator;

    protected AbstractAsyncBlobStore(
            String providerId,
            String bucket,
            String region,
            CredentialsOverrider credentialsOverrider,
            BlobStoreValidator validator
    ) {
        this.providerId = providerId;
        this.bucket = bucket;
        this.region = region;
        this.credentialsOverrider = credentialsOverrider;
        this.validator = validator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, InputStream inputStream) {
        validator.validate(uploadRequest);
        return doUpload(uploadRequest, inputStream);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, byte[] content) {
        validator.validate(uploadRequest);
        return doUpload(uploadRequest, content);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, File file) {
        validator.validate(uploadRequest);
        return doUpload(uploadRequest, file);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, Path path) {
        validator.validate(uploadRequest);
        return doUpload(uploadRequest, path);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, OutputStream outputStream) {
        validator.validate(downloadRequest);
        return doDownload(downloadRequest, outputStream);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, ByteArray byteArray) {
        validator.validate(downloadRequest);
        return doDownload(downloadRequest, byteArray);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, File file) {
        validator.validate(downloadRequest);
        return doDownload(downloadRequest, file);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, Path path) {
        validator.validate(downloadRequest);
        return doDownload(downloadRequest, path);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest) {
        validator.validate(downloadRequest);
        return doDownload(downloadRequest);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Void> delete(String key, String versionId) {
        validator.validateDelete(key);
        return doDelete(key, versionId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Void> delete(Collection<BlobIdentifier> objects) {
        validator.validateBlobIdentifiers(objects);
        return doDelete(objects);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<CopyResponse> copy(CopyRequest request) {
        validator.validate(request);
        return doCopy(request);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<BlobMetadata> getMetadata(String key, String versionId) {
        validator.validateKey(key);
        return doGetMetadata(key, versionId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Void> list(ListBlobsRequest request, Consumer<ListBlobsBatch> consumer) {
        return doList(request, consumer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<ListBlobsPageResponse> listPage(ListBlobsPageRequest request) {
        return doListPage(request);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<MultipartUpload> initiateMultipartUpload(MultipartUploadRequest request) {
        return doInitiateMultipartUpload(request);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<UploadPartResponse> uploadMultipartPart(MultipartUpload mpu, MultipartPart mpp) {
        validator.validate(mpu, getBucket());
        return doUploadMultipartPart(mpu, mpp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<MultipartUploadResponse> completeMultipartUpload(MultipartUpload mpu, List<UploadPartResponse> parts) {
        validator.validate(mpu, getBucket());
        return doCompleteMultipartUpload(mpu, parts);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<List<UploadPartResponse>> listMultipartUpload(MultipartUpload mpu) {
        validator.validate(mpu, getBucket());
        return doListMultipartUpload(mpu);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Void> abortMultipartUpload(MultipartUpload mpu) {
        validator.validate(mpu, getBucket());
        return doAbortMultipartUpload(mpu);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Map<String, String>> getTags(String key) {
        validator.validateKey(key);
        return doGetTags(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Void> setTags(String key, Map<String, String> tags) {
        validator.validateKey(key);
        validator.validateTags(tags);
        return doSetTags(key, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<URL> generatePresignedUrl(PresignedUrlRequest request) {
        validator.validate(request);
        return doGeneratePresignedUrl(request);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Boolean> doesObjectExist(String key, String versionId) {
        validator.validateKey(key);
        return doDoesObjectExist(key, versionId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<DirectoryDownloadResponse> downloadDirectory(DirectoryDownloadRequest directoryDownloadRequest){
        return doDownloadDirectory(directoryDownloadRequest);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<DirectoryUploadResponse> uploadDirectory(DirectoryUploadRequest directoryUploadRequest) {
        return doUploadDirectory(directoryUploadRequest);
    }

    protected abstract CompletableFuture<UploadResponse> doUpload(UploadRequest uploadRequest, InputStream inputStream);

    protected abstract CompletableFuture<UploadResponse> doUpload(UploadRequest uploadRequest, byte[] content);

    protected abstract CompletableFuture<UploadResponse> doUpload(UploadRequest uploadRequest, File file);

    protected abstract CompletableFuture<UploadResponse> doUpload(UploadRequest uploadRequest, Path path);

    protected abstract CompletableFuture<DownloadResponse> doDownload(DownloadRequest request, OutputStream outputStream);

    protected abstract CompletableFuture<DownloadResponse> doDownload(DownloadRequest request, ByteArray byteArray);

    protected abstract CompletableFuture<DownloadResponse> doDownload(DownloadRequest request, File file);

    protected abstract CompletableFuture<DownloadResponse> doDownload(DownloadRequest request, Path path);

    protected abstract CompletableFuture<DownloadResponse> doDownload(DownloadRequest request);

    protected abstract CompletableFuture<Void> doDelete(String key, String versionId);

    protected abstract CompletableFuture<Void> doDelete(Collection<BlobIdentifier> objects);

    protected abstract CompletableFuture<CopyResponse> doCopy(CopyRequest request);

    protected abstract CompletableFuture<BlobMetadata> doGetMetadata(String key, String versionId);

    protected abstract CompletableFuture<Void> doList(ListBlobsRequest request, Consumer<ListBlobsBatch> consumer);

    protected abstract CompletableFuture<ListBlobsPageResponse> doListPage(ListBlobsPageRequest request);

    protected abstract CompletableFuture<MultipartUpload> doInitiateMultipartUpload(MultipartUploadRequest request);

    protected abstract CompletableFuture<UploadPartResponse> doUploadMultipartPart(MultipartUpload mpu, MultipartPart mpp);

    protected abstract CompletableFuture<MultipartUploadResponse> doCompleteMultipartUpload(MultipartUpload mpu, List<UploadPartResponse> parts);

    protected abstract CompletableFuture<List<UploadPartResponse>> doListMultipartUpload(MultipartUpload mpu);

    protected abstract CompletableFuture<Void> doAbortMultipartUpload(MultipartUpload mpu);

    protected abstract CompletableFuture<Map<String, String>> doGetTags(String key);

    protected abstract CompletableFuture<Void> doSetTags(String key, Map<String, String> tags);

    protected abstract CompletableFuture<URL> doGeneratePresignedUrl(PresignedUrlRequest request);

    protected abstract CompletableFuture<Boolean> doDoesObjectExist(String key, String versionId);

    protected abstract CompletableFuture<DirectoryDownloadResponse> doDownloadDirectory(DirectoryDownloadRequest directoryDownloadRequest);

    protected abstract CompletableFuture<DirectoryUploadResponse> doUploadDirectory(DirectoryUploadRequest directoryUploadRequest);
}
