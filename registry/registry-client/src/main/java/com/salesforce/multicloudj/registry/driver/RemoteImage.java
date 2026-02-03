package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.registry.model.Image;
import com.salesforce.multicloudj.registry.model.Layer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of Image for images on a remote registry.
 * "Remote" means layer blobs are fetched via OciRegistryClient on demand.
 * getDigest() returns config digest (OCI image ID).
 */
final class RemoteImage implements Image {

    private final OciRegistryClient client;
    private final String repository;
    private final String imageRef;
    private final Manifest manifest;
    private final String digest;

    RemoteImage(OciRegistryClient client, String repository, String imageRef, Manifest manifest) {
        this.client = client;
        this.repository = repository;
        this.imageRef = imageRef;
        this.manifest = manifest;
        this.digest = manifest.getConfigDigest();
    }

    @Override
    public List<Layer> getLayers() throws IOException {
        List<Layer> layers = new ArrayList<>();
        for (String layerDigest : manifest.getLayerDigests()) {
            layers.add(new RemoteLayer(client, repository, layerDigest));
        }
        return layers;
    }

    @Override
    public String getDigest() throws IOException {
        return digest;
    }

    @Override
    public String getImageRef() throws IOException {
        return imageRef;
    }
}
