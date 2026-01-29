package com.salesforce.multicloudj.registry.driver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of Image interface for images pulled from a registry.
 * Similar to go-containerregistry's remote.Image.
 */
class RemoteImage implements Image {
    private final OciRegistryClient client;
    private final String repository;
    private final String imageRef;
    private final OciRegistryClient.Manifest manifest;
    private final String digest;

    RemoteImage(OciRegistryClient client, String repository, String imageRef, OciRegistryClient.Manifest manifest) {
        this.client = client;
        this.repository = repository;
        this.imageRef = imageRef;
        this.manifest = manifest;
        // Calculate digest from imageRef or manifest
        if (imageRef != null && imageRef.contains("@sha256:")) {
            this.digest = imageRef.substring(imageRef.indexOf("@sha256:") + 1);
        } else {
            this.digest = "sha256:unknown"; // TODO: Calculate from manifest
        }
    }

    @Override
    public List<Layer> getLayers() throws IOException {
        List<Layer> layers = new ArrayList<>();
        for (String layerDigest : manifest.layerDigests) {
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

    void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
