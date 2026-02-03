package com.salesforce.multicloudj.registry.driver;

/**
 * Parsed image reference (repository + tag or digest).
 * Examples: "my-image:latest", "my-image@sha256:abc123..."
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

    public String getRepository() {
        return repository;
    }

    public String getReference() {
        return reference;
    }

    public boolean isDigest() {
        return reference.startsWith("sha256:");
    }

    @Override
    public String toString() {
        return fullRef;
    }
}
