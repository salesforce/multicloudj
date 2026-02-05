package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.registry.model.Platform;
import java.util.List;

/**
 * Parsed OCI manifest from the registry.
 * Two shapes: (1) image manifest — config and layer digests; (2) index — multi-platform list.
 * For image manifest, config digest is the OCI image ID (exposed as the image digest).
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
