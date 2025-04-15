package com.salesforce.multicloudj.blob.driver;

import lombok.Getter;

/**
 * Contains the fields necessary to uniquely identify a blob
 */
@Getter
public class BlobIdentifier {

    private final String key;
    private final String versionId;

    /**
     * Constructs a BlobIdentifier
     * @param key - Required parameter. Must be non-empty and non-null.
     * @param versionId - Optional parameter, only used when versioning is enabled in a bucket. Leave null otherwise.
     */
    public BlobIdentifier(final String key, final String versionId) {
        this.key = key;
        this.versionId = versionId;
    }
}
