package com.salesforce.multicloudj.registry.driver;

import org.apache.commons.lang3.StringUtils;
import java.util.regex.Pattern;

/**
 * Parsed image reference for OCI Registry API v2.
 * <p>
 * User passes one string (e.g. "my-image:latest" or "my-image@sha256:abc..."); we split it
 * because the API uses separate path segments: GET /v2/{repository}/manifests/{reference}
 * and GET /v2/{repository}/blobs/{digest}. 
 * <p>
 * <ul>
 *   <li>Split on "@" delimiter </li>
 *   <li>Validate digest algorithm prefix (must be "sha256:")</li>
 *   <li>Validate digest hex format: 64 lowercase hex characters [a-f0-9]{64}</li>
 * </ul>
 */
public final class ImageReference {

    private static final String DIGEST_DELIMITER = "@";
    private static final String SHA256_PREFIX = "sha256:";
    private static final int SHA256_HEX_LENGTH = 64;
    private static final Pattern SHA256_HEX_PATTERN = Pattern.compile("^[a-f0-9]{" + SHA256_HEX_LENGTH + "}$");

    private final String repository;
    private final String reference;
    private final String fullReference;

    private ImageReference(String repository, String reference, String fullReference) {
        this.repository = repository;
        this.reference = reference;
        this.fullReference = fullReference;
    }

    /**
     * Parses an image reference string. If it contains "@", treated as digest; otherwise as tag.
     * @param imageRef image reference string (e.g. "my-image:latest" or "my-image@sha256:abc...")
     * @return parsed ImageReference
     * @throws IllegalArgumentException if the reference format is invalid
     */
    public static ImageReference parse(String imageRef) {
        if (StringUtils.isBlank(imageRef)) {
            throw new IllegalArgumentException("Image reference cannot be null or empty");
        }
        String ref = imageRef.trim();
        if (ref.contains(DIGEST_DELIMITER)) {
            return parseDigest(ref);
        }
        return parseTag(ref);
    }

    /** Parses a digest reference: split on "@", validate sha256 prefix and hex format. */
    private static ImageReference parseDigest(String ref) {
        String[] parts = ref.split(DIGEST_DELIMITER, 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                String.format("a digest must contain exactly one '@' separator (e.g. registry/repository@digest) saw: %s", ref));
        }
        
        String base = parts[0].trim();
        String dig = parts[1].trim();
        
        if (StringUtils.isBlank(base)) {
            throw new IllegalArgumentException("Repository cannot be empty: " + ref);
        }
        if (StringUtils.isBlank(dig)) {
            throw new IllegalArgumentException("Digest cannot be empty: " + ref);
        }
        if (!dig.startsWith(SHA256_PREFIX)) {
            throw new IllegalArgumentException(
                String.format("unsupported digest algorithm: %s (expected sha256)", dig));
        }
        String hex = dig.substring(SHA256_PREFIX.length());
        validateSha256Hex(hex, ref);
        
        return new ImageReference(base, dig, ref);
    }

    /**
     * Validates SHA256 hex format: must be exactly 64 lowercase hex characters.
     */
    private static void validateSha256Hex(String hex, String originalRef) {
        if (hex.length() != SHA256_HEX_LENGTH) {
            throw new IllegalArgumentException(
                String.format("invalid checksum digest length: expected %d characters, got %d: %s",
                    SHA256_HEX_LENGTH, hex.length(), originalRef));
        }
        
        if (!SHA256_HEX_PATTERN.matcher(hex).matches()) {
            throw new IllegalArgumentException(
                String.format("invalid checksum digest format: must be 64 lowercase hex characters [a-f0-9]: %s", originalRef));
        }
    }

    private static ImageReference parseTag(String ref) {
        if (ref.contains(":")) {
            String[] parts = ref.split(":", 2);
            if (parts.length == 2 && !StringUtils.isBlank(parts[1])) {
                return new ImageReference(parts[0], parts[1], ref);
            }
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
        return fullReference;
    }
}
