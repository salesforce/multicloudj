package com.salesforce.multicloudj.registry.driver;

/**
 * OCI Image Manifest JSON field names as defined in the OCI Image Specification.
 * 
 * @see <a href="https://github.com/opencontainers/image-spec/blob/v1.0.1/manifest.md">OCI Image Manifest Specification</a>
 * @see <a href="https://github.com/opencontainers/image-spec/blob/v1.0.1/image-index.md">OCI Image Index Specification</a>
 */
final class OciManifestFields {

    private OciManifestFields() {
        // Utility class - prevent instantiation
    }

    // OCI media types
    static final String MEDIA_TYPE_OCI_INDEX = "application/vnd.oci.image.index.v1+json";
    static final String MEDIA_TYPE_OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json";
    static final String MEDIA_TYPE_DOCKER_MANIFEST_LIST = "application/vnd.docker.distribution.manifest.list.v2+json";
    static final String MEDIA_TYPE_DOCKER_MANIFEST = "application/vnd.docker.distribution.manifest.v2+json";

    // Common descriptor fields
    static final String MEDIA_TYPE = "mediaType";
    static final String DIGEST = "digest";
    static final String SIZE = "size";

    // Image manifest fields
    static final String CONFIG = "config";
    static final String LAYERS = "layers";

    // Image index fields
    static final String MANIFESTS = "manifests";

    // Optional fields (manifest and index)
    static final String ANNOTATIONS = "annotations";
    static final String SUBJECT = "subject";

    // Platform fields (in image index manifest entries)
    static final String PLATFORM = "platform";
    static final String OS = "os";
    static final String ARCHITECTURE = "architecture";
    static final String OS_VERSION = "os.version";
    static final String OS_FEATURES = "os.features";
    static final String VARIANT = "variant";
}
