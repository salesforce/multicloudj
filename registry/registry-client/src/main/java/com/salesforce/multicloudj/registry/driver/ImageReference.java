package com.salesforce.multicloudj.registry.driver;

/**
 * Parsed image reference for OCI Registry API v2.
 * <p>
 * User passes one string (e.g. "my-image:latest" or "my-image@sha256:abc..."); we split it
 * because the API uses separate path segments: GET /v2/{repository}/manifests/{reference}
 * and GET /v2/{repository}/blobs/{digest}. So pull() will call
 * fetchManifest(ref.getRepository(), ref.getReference()) and
 * downloadBlob(ref.getRepository(), layerDigest).
 */
public final class ImageReference {

    private final String repository;
    private final String reference;
    private final String fullRef;

    private ImageReference(String repository, String reference, String fullRef) {
        this.repository = repository;
        this.reference = reference;
        this.fullRef = fullRef;
    }

    public static ImageReference parse(String imageRef) {
        if (imageRef == null || imageRef.trim().isEmpty()) {
            throw new IllegalArgumentException("Image reference cannot be null or empty");
        }
        String ref = imageRef.trim();
        if (ref.contains("@sha256:")) {
            String[] parts = ref.split("@sha256:");
            return new ImageReference(parts[0], "sha256:" + parts[1], ref);
        }
        if (ref.contains(":")) {
            String[] parts = ref.split(":", 2);
            return new ImageReference(parts[0], parts[1], ref);
        }
        return new ImageReference(ref, "latest", ref + ":latest");
    }

    /** For /v2/{repository}/... (manifests and blobs). */
    public String getRepository() {
        return repository;
    }

    /** Tag or digest for /v2/{repository}/manifests/{reference}. */
    public String getReference() {
        return reference;
    }

    @Override
    public String toString() {
        return fullRef;
    }
}
