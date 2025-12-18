package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Getter;

import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Wrapper object for directory upload data
 */
@Builder
@Getter
public class DirectoryUploadRequest {
    private final String localSourceDirectory;
    private final String prefix;
    private final boolean includeSubFolders;
    /**
     * (Optional parameter) The map of tagName to tagValue to be associated with all blobs in the directory
     */
    private final Map<String, String> tags;

    public Map<String, String> getTags() {
        return tags == null ? Map.of() : unmodifiableMap(tags);
    }
}
