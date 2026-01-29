package com.salesforce.multicloudj.registry.driver;

/**
 * Utility class for parsing image references.
 * Examples: "my-image:latest", "my-image@sha256:abc123..."
 */
public class ImageReference {
    private final String repository;
    private final String reference; // tag or digest
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
        
        // Check if it's a digest reference (contains @sha256:)
        if (ref.contains("@sha256:")) {
            String[] parts = ref.split("@sha256:");
            String repo = parts[0];
            String digest = "sha256:" + parts[1];
            return new ImageReference(repo, digest, ref);
        }
        
        // Check if it's a tag reference (contains :)
        if (ref.contains(":")) {
            String[] parts = ref.split(":", 2);
            String repo = parts[0];
            String tag = parts[1];
            return new ImageReference(repo, tag, ref);
        }
        
        // Default to "latest" tag
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
