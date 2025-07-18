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
     * @param versionId - (Optional) Specifies the versionId of the blob to download.
     *       <p>For buckets without versioning enabled:</p>
     *       <ul>
     *           <li>This field has no purpose for non-versioned buckets. Leave it null</li>
     *           <li>Note: Some substrates do return a value for this field, and it can be used in requests,
     *               but it doesn't do anything</li>
     *       </ul>
     *
     *       <p>For buckets with versioning enabled:</p>
     *       <ul>
     *           <li>This field is optional</li>
     *           <li>If you set the value to null then it will target the latest version of the blob</li>
     *           <li>If you set the value to a specific versionId, then it will target that version of the blob</li>
     *           <li>If you use an invalid versionId it will not be able to find your blob</li>
     *       </ul>
     */
    public BlobIdentifier(final String key, final String versionId) {
        this.key = key;
        this.versionId = versionId;
    }
}
