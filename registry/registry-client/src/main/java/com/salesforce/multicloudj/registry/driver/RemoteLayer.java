package com.salesforce.multicloudj.registry.driver;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * Implementation of Layer interface for layers from a registry.
 * Similar to go-containerregistry's remote.Layer.
 */
class RemoteLayer implements Layer {
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
        // Download the compressed layer blob (triggers HTTP GET /v2/{repo}/blobs/{digest})
        InputStream compressed = client.downloadBlob(repository, digest);
        // Decompress gzip
        return new GzipCompressorInputStream(compressed);
    }

    @Override
    public long getSize() throws IOException {
        // TODO: Fetch actual size via HEAD request
        return -1;
    }
}
