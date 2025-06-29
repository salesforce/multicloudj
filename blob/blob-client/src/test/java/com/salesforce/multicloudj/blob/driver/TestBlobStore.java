package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.provider.Provider;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TestBlobStore extends AbstractBlobStore<TestBlobStore> {
    public TestBlobStore(Builder builder) {
        super(builder);
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
    protected DownloadResponse doDownload(DownloadRequest downloadRequest, File file) {
        return null;
    }

    @Override
    protected DownloadResponse doDownload(DownloadRequest downloadRequest, Path path) {
        return null;
    }

    @Override
    protected DownloadResponse doDownload(DownloadRequest downloadRequest, ByteArray byteArray) {
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
    protected MultipartUpload doInitiateMultipartUpload(final MultipartUploadRequest request) {
        return null;
    }

    @Override
    protected com.salesforce.multicloudj.blob.driver.UploadPartResponse doUploadMultipartPart(final MultipartUpload mpu, final MultipartPart mpp) {
        return null;
    }

    @Override
    protected MultipartUploadResponse doCompleteMultipartUpload(final MultipartUpload mpu, final List<UploadPartResponse> parts){
        return null;
    }

    @Override
    protected List<com.salesforce.multicloudj.blob.driver.UploadPartResponse> doListMultipartUpload(final MultipartUpload mpu){
        return null;
    }

    @Override
    protected void doAbortMultipartUpload(final MultipartUpload mpu){

    }

    @Override
    protected Map<String, String> doGetTags(String key) {
        return null;
    }

    @Override
    protected void doSetTags(String key, Map<String, String> tags) {

    }

    @Override
    protected URL doGeneratePresignedUrl(PresignedUrlRequest request) {
        return null;
    }

    @Override
    protected boolean doDoesObjectExist(String key, String versionId) {
        return false;
    }

    @Override
    public Provider.Builder builder() {
        return new Builder();
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        return null;
    }

    public static class Builder extends AbstractBlobStore.Builder<TestBlobStore> {

        protected Builder() {
            providerId("test");
        }

        @Override
        public TestBlobStore build() {
            return new TestBlobStore(this);
        }
    }
}
