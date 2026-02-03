package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.registry.model.Layer;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * Implementation of Layer for a layer on a remote registry.
 * "Remote" means the layer blob is fetched via OciRegistryClient.downloadBlob when
 * getUncompressed() is called, then decompressed (gzip). Used by RemoteImage.
 */
final class RemoteLayer implements Layer {

    private final OciRegistryClient client;
    private final String repository;
    private final String digest;

    RemoteLayer(OciRegistryClient client, String repository, String digest) {
        this.client = client;
        this.repository = repository;
        this.digest = digest;
    }

    @Override
    public String getDigest() throws IOException {
        return digest;
    }

    @Override
    public InputStream getUncompressed() throws IOException {
        InputStream compressed = client.downloadBlob(repository, digest);
        return new GzipCompressorInputStream(compressed);
    }

    @Override
    public long getSize() throws IOException {
        return -1;
    }
}
