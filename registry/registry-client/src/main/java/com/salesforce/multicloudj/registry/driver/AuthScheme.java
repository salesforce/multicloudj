package com.salesforce.multicloudj.registry.driver;

/**
 * Authentication schemes supported by OCI registries.
 */
public enum AuthScheme {
    ANONYMOUS,
    BASIC,
    BEARER;

    /**
     * Parses a string value to an AuthScheme.
     * Returns ANONYMOUS for null or blank values.
     *
     * @param value the scheme string (case-insensitive)
     * @return the corresponding AuthScheme
     * @throws UnsupportedOperationException if the scheme is not supported
     */
    public static AuthScheme fromString(String value) {
        if (value == null || value.isBlank()) {
            return ANONYMOUS;
        }
        try {
            return valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new UnsupportedOperationException("Unsupported authentication scheme: " + value);
        }
    }
}
