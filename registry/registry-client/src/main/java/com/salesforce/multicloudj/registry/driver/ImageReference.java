package com.salesforce.multicloudj.registry.driver;

import lombok.Getter;

/**
 * Represents a Docker image reference, similar to go-containerregistry's name.Reference.
 * Parses image references in formats:
 * - "name:tag" (e.g., "my-image:latest")
 * - "name@digest" (e.g., "my-image@sha256:abc123...")
 * - "registry/name:tag" (e.g., "gcr.io/my-project/my-image:v1")
 */
@Getter
public class ImageReference {
    private final String registry;
    private final String repository;
    private final String reference; // tag or digest
    private final boolean isDigest;

    private ImageReference(String registry, String repository, String reference, boolean isDigest) {
        this.registry = registry;
        this.repository = repository;
        this.reference = reference;
        this.isDigest = isDigest;
    }

    /**
     * Parses an image reference string.
     * Examples:
     * - "my-image:latest" -> registry="", repository="my-image", reference="latest"
     * - "my-image@sha256:abc123" -> registry="", repository="my-image", reference="sha256:abc123", isDigest=true
     * - "gcr.io/project/image:v1" -> registry="gcr.io", repository="project/image", reference="v1"
     */
    public static ImageReference parse(String imageRef) {
        if (imageRef == null || imageRef.trim().isEmpty()) {
            throw new IllegalArgumentException("Image reference cannot be null or empty");
        }

        String ref = imageRef.trim();
        boolean isDigest = ref.contains("@");
        
        // Split by @ (digest) or : (tag)
        String[] parts;
        String reference;
        if (isDigest) {
            parts = ref.split("@", 2);
            reference = parts[1];
        } else {
            parts = ref.split(":", 2);
            reference = parts.length > 1 ? parts[1] : "latest";
        }
        
        String namePart = parts[0];
        
        // Check if registry is specified (contains /)
        String registry = "";
        String repository = namePart;
        
        if (namePart.contains("/")) {
            String[] nameParts = namePart.split("/", 2);
            // Registry is only valid if it looks like a host (contains . or :, or is localhost)
            boolean looksLikeRegistry = nameParts[0].contains(".") 
                || nameParts[0].contains(":") 
                || nameParts[0].equals("localhost");
            
            if (looksLikeRegistry) {
                registry = nameParts[0];
                repository = nameParts[1];
            }
            // Otherwise, treat the whole thing as repository (e.g., "my-repo/image" -> repository="my-repo/image")
        }
        
        return new ImageReference(registry, repository, reference, isDigest);
    }

    /**
     * Returns the full reference string for API calls.
     */
    public String getReferenceString() {
        if (isDigest) {
            return repository + "@" + reference;
        } else {
            return repository + ":" + reference;
        }
    }

    @Override
    public String toString() {
        if (registry.isEmpty()) {
            return getReferenceString();
        }
        return registry + "/" + getReferenceString();
    }
}
