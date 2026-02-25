package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.registry.model.Image;
import com.salesforce.multicloudj.registry.model.Layer;
import com.salesforce.multicloudj.registry.model.Manifest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Image implementation for a remote registry; layer blobs are fetched via OciRegistryClient on demand.
 * Digest is the config digest (OCI image ID).
 */
final class RemoteImage implements Image {

    private final OciRegistryClient client;
    private final String repository;
    private final String imageRef;
    private final Manifest manifest;

    RemoteImage(OciRegistryClient client, String repository, String imageRef, Manifest manifest) {
        this.client = client;
        this.repository = repository;
        this.imageRef = imageRef;
        this.manifest = manifest;
    }

    @Override
    public List<Layer> getLayers() throws IOException {
        List<String> layerDigests = manifest.getLayerDigests();
        if (layerDigests == null) {
            throw new IOException("Image manifest is missing layer digests");
        }
        List<Layer> layers = new ArrayList<>();
        for (String layerDigest : layerDigests) {
            layers.add(new RemoteLayer(client, repository, layerDigest));
        }
        return layers;
    }

    @Override
    public String getDigest() throws IOException {
        String configDigest = manifest.getConfigDigest();
        if (configDigest == null || configDigest.isEmpty()) {
            throw new IOException("Image manifest is missing config digest");
        }
        return configDigest;
    }

    @Override
    public String getImageRef() throws IOException {
        return imageRef;
    }
}
