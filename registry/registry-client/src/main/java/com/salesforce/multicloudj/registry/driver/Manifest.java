package com.salesforce.multicloudj.registry.driver;

import java.util.List;

/**
 * Parsed OCI manifest: either an image manifest (config + layers) or an index (multi-platform).
 * Optional {@code digest} is the manifest blob digest from the registry (e.g. Docker-Content-Digest),
 * used as the image digest when building RemoteImage.
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

    public static Manifest image(String configDigest, List<String> layerDigests) {
        return new Manifest(configDigest, layerDigests, null, false, null);
    }

    public static Manifest image(String configDigest, List<String> layerDigests, String digest) {
        return new Manifest(configDigest, layerDigests, null, false, digest);
    }

    public static Manifest index(List<IndexEntry> indexManifests) {
        return new Manifest(null, null, indexManifests, true, null);
    }

    public static Manifest index(List<IndexEntry> indexManifests, String digest) {
        return new Manifest(null, null, indexManifests, true, digest);
    }

    /**
     * Digest of this manifest blob from registry (e.g. Docker-Content-Digest), or null.
     */
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
        private final String platformOs;
        private final String platformArch;

        public IndexEntry(String digest, String platformOs, String platformArch) {
            this.digest = digest;
            this.platformOs = platformOs;
            this.platformArch = platformArch;
        }

        public String getDigest() {
            return digest;
        }

        public String getPlatformOs() {
            return platformOs;
        }

        public String getPlatformArch() {
            return platformArch;
        }
    }
}
