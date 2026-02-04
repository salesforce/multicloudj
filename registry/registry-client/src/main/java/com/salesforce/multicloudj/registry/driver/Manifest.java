package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.registry.model.Platform;
import java.util.List;

/**
 * Parsed OCI manifest returned by OciRegistryClient.fetchManifest().
 * <p>
 * Registry can return two shapes: (1) <b>image manifest</b> — one image with config + layer digests;
 * (2) <b>index</b> — multi-platform list of manifests (each entry has platform + digest). Pull flow:
 * if index, pick one entry by Platform then fetch that manifest; then use configDigest/layerDigests
 * to download blobs and build RemoteImage.
 * <p>
 * For image manifest: configDigest is the OCI image ID (used by Image.getDigest()). 
 */
public final class Manifest {

    private final String configDigest;
    private final List<String> layerDigests;
    private final List<IndexEntry> indexManifests;
    private final boolean index;
    private final String digest;

    private Manifest(String configDigest, List<String> layerDigests, List<IndexEntry> indexManifests, boolean index, String digest) {
        this.configDigest = configDigest;
        this.layerDigests = layerDigests;
        this.indexManifests = indexManifests;
        this.index = index;
        this.digest = digest;
    }

    public static Manifest image(String configDigest, List<String> layerDigests, String digest) {
        return new Manifest(configDigest, layerDigests, null, false, digest);
    }

    public static Manifest index(List<IndexEntry> indexManifests, String digest) {
        return new Manifest(null, null, indexManifests, true, digest);
    }

    public String getDigest() {
        return digest;
    }

    public boolean isIndex() {
        return index;
    }

    public String getConfigDigest() {
        return configDigest;
    }

    public List<String> getLayerDigests() {
        return layerDigests;
    }

    public List<IndexEntry> getIndexManifests() {
        return indexManifests;
    }

    /**
     * Entry in an OCI image index (platform + digest).
     */
    public static final class IndexEntry {
        private final String digest;
        private final Platform platform;

        public IndexEntry(String digest, Platform platform) {
            this.digest = digest;
            this.platform = platform;
        }

        public String getDigest() {
            return digest;
        }

        public String getOs() {
            return platform != null ? platform.getOperatingSystem() : null;
        }

        public String getArchitecture() {
            return platform != null ? platform.getArchitecture() : null;
        }
    }
}
