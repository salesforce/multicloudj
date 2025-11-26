package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Getter;

import java.util.Map;

/**
 * This object stores the identifying information for a multipart upload request.
 * It's used by the client whenever a multipart operation is being performed.
 */
@Builder
@Getter
public class MultipartUpload {
    private final String bucket;
    private final String key;
    private final String id;
    private final Map<String, String> metadata;
    private final Map<String, String> tags;
    private final String kmsKeyId;
}
