package com.salesforce.multicloudj.registry.model;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Parsed OCI manifest from the registry.
 * Supports two shapes: (1) image manifest — config and layer digests; (2) index — multi-platform list.
 * <p>{@code digest} identifies this manifest; {@code configDigest} points to image config blob;
 * {@code layerDigests} lists the filesystem layers in order.
 *
 * @see <a href="https://github.com/opencontainers/image-spec/blob/v1.0.1/manifest.md">OCI Image Manifest Specification</a>
 */
public final class Manifest {

    private final String configDigest;
    private final List<String> layerDigests;
    private final List<LayerInfo> layerInfos;
    private final List<IndexEntry> indexManifests;
    private final boolean index;
    private final String digest;
    private final Map<String, String> annotations;
    private final String subject;

    private Manifest(String configDigest, List<String> layerDigests, List<LayerInfo> layerInfos, 
                     List<IndexEntry> indexManifests, boolean index, String digest, 
                     Map<String, String> annotations, String subject) {
        this.configDigest = configDigest;
        this.layerDigests = layerDigests;
        this.layerInfos = layerInfos;
        this.indexManifests = indexManifests;
        this.index = index;
        this.digest = digest;
        this.annotations = annotations;
        this.subject = subject;
    }

    public static Manifest image(String configDigest, List<String> layerDigests, String digest) {
        return new Manifest(configDigest, layerDigests, null, null, false, digest, null, null);
    }

    public static Manifest image(String configDigest, List<LayerInfo> layerInfos, String digest, 
                                Map<String, String> annotations, String subject) {
        List<String> digests = layerInfos != null ? 
            layerInfos.stream().map(LayerInfo::getDigest).collect(Collectors.toList()) : null;
        return new Manifest(configDigest, digests, layerInfos, null, false, digest, annotations, subject);
    }

    public static Manifest index(List<IndexEntry> indexManifests, String digest) {
        return new Manifest(null, null, null, indexManifests, true, digest, null, null);
    }

    public static Manifest index(List<IndexEntry> indexManifests, String digest, 
                                 Map<String, String> annotations, String subject) {
        return new Manifest(null, null, null, indexManifests, true, digest, annotations, subject);
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

    public List<LayerInfo> getLayerInfos() {
        return layerInfos;
    }

    public List<IndexEntry> getIndexManifests() {
        return indexManifests;
    }

    public Map<String, String> getAnnotations() {
        return annotations;
    }

    public String getSubject() {
        return subject;
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

        public Platform getPlatform() {
            return platform;
        }
    }

    /**
     * Layer information including digest and media type.
     */
    public static final class LayerInfo {
        private final String digest;
        private final String mediaType;
        private final Long size;

        public LayerInfo(String digest, String mediaType, Long size) {
            this.digest = digest;
            this.mediaType = mediaType;
            this.size = size;
        }

        public String getDigest() {
            return digest;
        }

        public String getMediaType() {
            return mediaType;
        }

        public Long getSize() {
            return size;
        }
    }
}
