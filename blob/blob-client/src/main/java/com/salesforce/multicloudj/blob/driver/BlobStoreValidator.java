package com.salesforce.multicloudj.blob.driver;

import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/**
 * Helper for validating input for BlobStore operations.
 */
public class BlobStoreValidator {

    static final String INVALID_BUCKET_NAME_MSG = "Bucket name cannot be null or empty";
    static final String INVALID_OBJECT_NAME_MSG = "Object name cannot be null or empty";
    static final String INVALID_OBJECT_NAME_COLLECTION_MSG = "Collection of object names cannot be null or empty";
    static final String INVALID_BLOB_IDENTIFIERS_COLLECTION_MSG = "Collection of blob identifiers cannot be null or empty";
    static final String INVALID_TAGS_COLLECTION_MSG = "Map of tags cannot be null or empty";
    static final String MISMATCHED_BUCKET_NAME_MSG = "Bucket names must be equal";
    static final String DURATION_INVALID_MSG = "Duration must be a non-null non-zero positive value. Duration was '%s'";
    static final String PRESIGNED_URL_OPERATION_INVALID_MSG = "The presigned url operation type must be non-null";
    static final String INVALID_ENDPOINT_NULL_MSG = "Endpoint cannot be null";
    static final String INVALID_ENDPOINT_PROTOCOL_MSG = "Endpoint must have a protocol of http or https. value=%s";
    static final String INVALID_ENDPOINT_HOSTNAME_MSG = "Endpoint must have a non-empty hostname value. value=%s";
    static final String INVALID_ENDPOINT_PORT_MSG = "Endpoint must have a non-zero positive value for the port number. value=%s";
    static final String INVALID_ENDPOINT_NO_PATH_ALLOWED_MSG = "Endpoint must not have a path specified. value=%s";
    static final String INVALID_ENDPOINT_NO_QUERY_PARAMS_ALLOWED_MSG = "Endpoint must not have query params specified. value=%s";
    static final String INVALID_MAX_CONNECTIONS_MSG = "Maximum connections must be a positive value. value=%s";
    static final String INVALID_SOCKET_TIMEOUT_MSG = "Socket timeout must be a non-negative value if specified. Duration was '%s'";
    static final String INVALID_RANGED_READ_NEGATIVE_BOUNDARIES_MSG = "Ranged read boundaries cannot be negative. start=%s end=%s";
    static final String INVALID_RANGED_READ_BOUNDARIES_MSG = "Ranged read start cannot be larger than end. start=%s end=%s";

    /**
     * Inspects the input string and throws an IllegalArgumentException if the input is `null`, empty, or blank.
     * @param string the string to inspect
     * @param msg the error message to use if the input is empty.
     */
    public void requireNotBlank(String string, String msg) {
        if (StringUtils.isBlank(string)) {
            throw new IllegalArgumentException(msg);
        }
    }

    /**
     * Inspects the input collection and throws an IllegalArgumentException if the collection is `null`,
     * empty, or contains any values that are null, empty, or blank.
     * @param collection the collection to inspect
     * @param msg the error message to use if the input is invalid.
     */
    public void requireNotEmpty(Collection<?> collection, String msg) {
        if (collection == null || collection.isEmpty()) {
            throw new IllegalArgumentException(msg);
        }
    }

    /**
     * Inspects the input map and throws an IllegalArgumentException if the map is empty or null
     * @param map the map to inspect
     * @param msg the error message to use if the input is invalid.
     */
    public void requireNotEmpty(Map<String, String> map, String msg) {
        if (map == null || map.isEmpty()) {
            throw new IllegalArgumentException(msg);
        }
    }

    /**
     * Inspects the strings and throws an IllegalArgumentException if they don't match.
     * This is a case-insensitive comparison
     * @param first - the first string
     * @param second - the second strings
     * @param msg the error message to use if the input is invalid.
     */
    public void requireEqualsIgnoreCase(String first, String second, String msg){
        if(!StringUtils.equalsIgnoreCase(first, second)){
            throw new IllegalArgumentException(msg);
        }
    }

    /**
     * Validates that the supplied key is not empty. This is identical to calling
     * {@code requireNotBlank(key, INVALID_OBJECT_NAME_MSG)}
     * @param key the key to inspect
     */
    public void validateKey(String key) {
        requireNotBlank(key, INVALID_OBJECT_NAME_MSG);
    }

    /**
     * Validates that the supplied bucket is not empty. This is identical to calling
     * {@code requireNotBlank(bucket, INVALID_BUCKET_NAME_MSG)}
     * @param bucket the bucket to inspect
     */
    public void validateBucket(String bucket) {
        requireNotBlank(bucket, INVALID_BUCKET_NAME_MSG);
    }

    /**
     * Validates that the supplied keys are not empty. This is identical to calling
     * {@code requireNotEmpty(keys, INVALID_OBJECT_NAME_COLLECTION_MSG)}
     * @param keys the keys to inspect
     */
    public void validateKeys(Collection<String> keys) {
        requireNotEmpty(keys, INVALID_OBJECT_NAME_COLLECTION_MSG);
        keys.forEach(this::validateKey);
    }

    /**
     * Validates that the supplied list of objects are not empty or null.
     * Also validates that the keys are not empty or null.
     * @param objects the BlobIdentifiers to inspect
     */
    public void validateBlobIdentifiers(Collection<BlobIdentifier> objects) {
        requireNotEmpty(objects, INVALID_BLOB_IDENTIFIERS_COLLECTION_MSG);
        objects.forEach(object -> validateKey(object.getKey()));
    }

    /**
     * Validates that the supplied duration is a non-null, non-zero, positive value
     * @param duration The duration
     */
    public void validateDuration(Duration duration) {
        if(duration == null || duration.isNegative() || duration.isZero()) {
            throw new IllegalArgumentException(String.format(DURATION_INVALID_MSG, duration));
        }
    }

    /**
     * Validates that the presigned operation is not null
     * @param presignedOperation The PresignedOperation type
     */
    public void validatePresignedOperationType(PresignedOperation presignedOperation) {
        if(presignedOperation == null) {
            throw new IllegalArgumentException(PRESIGNED_URL_OPERATION_INVALID_MSG);
        }
    }

    /**
     * Validates the input request. This is identical to calling
     * {@code validateKey(request.getKey())}
     * @param request the request to inspect.
     */
    public void validate(UploadRequest request) {
        validateKey(request.getKey());
    }

    /**
     * Validates the input request. This is identical to calling
     * {@code validateKey(request.getKey())}
     * @param request the request to inspect.
     */
    public void validate(DownloadRequest request) {
        validateKey(request.getKey());
        validateRange(request.getStart(), request.getEnd());
    }

    /**
     * Validates the input request to ensure it has valid {srcKey, destKey, destBucket}.
     * This is identical to calling
     * <pre>{@code
     *     validateKey(request.getSrcKey());
     *     validateKey(request.getDestKey());
     *     requireNotBlank(request.getDestBucket(), INVALID_BUCKET_NAME_MSG);
     * }</pre>
     * @param request the request to inspect.
     */
    public void validate(CopyRequest request) {
        validateKey(request.getSrcKey());
        validateKey(request.getDestKey());
        validateBucket(request.getDestBucket());
    }

    /**
     * Validates the input key. This is identical to calling {@code validateKey(key)}
     * @param key the key to inspect.
     */
    public void validateDelete(String key) {
        validateKey(key);
    }

    /**
     * Validates the MultipartUpload is for the corresponding bucket
     * @param mpu the multipartUpload
     * @param bucketName the name of the bucket the multipartUpload should be for
     */
    public void validate(MultipartUpload mpu, String bucketName){
        requireEqualsIgnoreCase(mpu.getBucket(), bucketName, MISMATCHED_BUCKET_NAME_MSG);
    }

    /**
     * Validates that the supplied tags are not empty or null
     * @param tags the tags to inspect
     */
    public void validateTags(Map<String, String> tags) {
        requireNotEmpty(tags, INVALID_TAGS_COLLECTION_MSG);
    }

    /**
     * Validates that a PresignedUrlRequest is valid
     * @param request The presign request
     */
    public void validate(PresignedUrlRequest request) {
        validatePresignedOperationType(request.getType());
        validateKey(request.getKey());
        validateDuration(request.getDuration());
    }

    /**
     * Validates that a URI specifies the schema/protocol is http/https, and that the hostname is non-empty and non-null,
     * and optionally validates that the port exists and is a positive values.
     * @param endpoint The endpoint (e.g. https://proxy.example.com:8080)
     * @param requirePort Determines if we require a port number as part of the endpoint definition
     */
    public void validateEndpoint(URI endpoint, boolean requirePort) {
        if(endpoint == null) {
            throw new IllegalArgumentException(INVALID_ENDPOINT_NULL_MSG);
        }
        String protocol = endpoint.getScheme();
        if(protocol == null) {
            throw new IllegalArgumentException(String.format(INVALID_ENDPOINT_PROTOCOL_MSG, "null"));
        }
        boolean isProtocolValid = StringUtils.equalsIgnoreCase(protocol, "http") || StringUtils.equalsIgnoreCase(protocol, "https");
        if (!isProtocolValid) {
            throw new IllegalArgumentException(String.format(INVALID_ENDPOINT_PROTOCOL_MSG, protocol));
        }
        if(endpoint.getHost() == null || endpoint.getHost().isBlank()) {
            throw new IllegalArgumentException(String.format(INVALID_ENDPOINT_HOSTNAME_MSG, endpoint.getHost()));
        }
        if(requirePort && endpoint.getPort() < 0) {
            throw new IllegalArgumentException(String.format(INVALID_ENDPOINT_PORT_MSG, endpoint.getPort()));
        }
        if(endpoint.getPath() != null && !endpoint.getPath().isBlank()) {
            throw new IllegalArgumentException(String.format(INVALID_ENDPOINT_NO_PATH_ALLOWED_MSG, endpoint.getPath()));
        }
        if(endpoint.getQuery() != null) {
            throw new IllegalArgumentException(String.format(INVALID_ENDPOINT_NO_QUERY_PARAMS_ALLOWED_MSG, endpoint.getQuery()));
        }
    }

    public void validateMaxConnections(Integer maxConnections) {
        if(maxConnections == null || maxConnections <= 0) {
            throw new IllegalArgumentException(String.format(INVALID_MAX_CONNECTIONS_MSG, maxConnections));
        }
    }

    public void validateSocketTimeout(Duration socketTimeout) {
        if(socketTimeout == null || socketTimeout.isNegative()) {
            throw new IllegalArgumentException(String.format(INVALID_SOCKET_TIMEOUT_MSG, socketTimeout));
        }
    }

    public void validateRange(Long start, Long end) {
        if ((start != null && start < 0) || (end != null && end < 0)) {
            throw new IllegalArgumentException(String.format(INVALID_RANGED_READ_NEGATIVE_BOUNDARIES_MSG, start, end));
        }
        if (start != null && end != null && end < start) {
            throw new IllegalArgumentException(String.format(INVALID_RANGED_READ_BOUNDARIES_MSG, start, end));
        }
    }
}
