package com.salesforce.multicloudj.common.aws;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceConflictException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides the mapping of various error codes to SDK's exceptions
 */
public class CommonErrorCodeMapping {

    private CommonErrorCodeMapping() {
    }

    private static final Map<String, Class<? extends SubstrateSdkException>> ERROR_MAPPING;

    static {
        // The common error codes as source of truth is here:
        // https://docs.aws.amazon.com/STS/latest/APIReference/CommonErrors.html
        // https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html
        // https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
        Map<String, Class<? extends SubstrateSdkException>> map = new HashMap<>();
        
        // Common errors from STS/DynamoDB
        map.put("IncompleteSignature", InvalidArgumentException.class);
        map.put("InternalFailure", UnknownException.class);
        map.put("InvalidAction", InvalidArgumentException.class);
        map.put("InvalidClientTokenId", InvalidArgumentException.class);
        map.put("NotAuthorized", UnAuthorizedException.class);
        map.put("OptInRequired", UnAuthorizedException.class);
        map.put("RequestExpired", ResourceExhaustedException.class);
        map.put("ServiceUnavailable", UnknownException.class);
        map.put("ThrottlingException", ResourceExhaustedException.class);
        map.put("ValidationError", InvalidArgumentException.class);
        
        // All 403 Forbidden errors from S3
        map.put("AccessDenied", UnAuthorizedException.class);
        map.put("AccountNotAuthorized", UnAuthorizedException.class);
        map.put("AccountProblem", UnAuthorizedException.class);
        map.put("AllAccessDisabled", UnAuthorizedException.class);
        map.put("InvalidAccessKeyId", InvalidArgumentException.class);
        map.put("InvalidPayer", UnAuthorizedException.class);
        map.put("InvalidSecurity", UnAuthorizedException.class);
        map.put("NotSignedUp", UnAuthorizedException.class);
        map.put("RequestTimeTooSkewed", InvalidArgumentException.class);
        map.put("SignatureDoesNotMatch", InvalidArgumentException.class);
        map.put("TokenRefreshRequired", UnAuthorizedException.class);
        
        // Common 400 Bad Request errors
        map.put("BadDigest", InvalidArgumentException.class);
        map.put("InvalidRequest", InvalidArgumentException.class);
        map.put("InvalidArgument", InvalidArgumentException.class);
        map.put("MalformedPolicy", InvalidArgumentException.class);
        map.put("MalformedXML", InvalidArgumentException.class);
        map.put("MetadataTooLarge", InvalidArgumentException.class);
        map.put("MissingContentLength", InvalidArgumentException.class);
        map.put("MissingSecurityHeader", InvalidArgumentException.class);
        map.put("RequestTimeout", ResourceExhaustedException.class);
        
        // Common 404 Not Found errors
        map.put("NoSuchKey", ResourceNotFoundException.class);
        map.put("NoSuchBucket", ResourceNotFoundException.class);
        map.put("NoSuchVersion", ResourceNotFoundException.class);
        map.put("NoSuchUpload", ResourceNotFoundException.class);
        
        // Common 409 Conflict errors
        map.put("BucketAlreadyExists", ResourceAlreadyExistsException.class);
        map.put("BucketAlreadyOwnedByYou", ResourceAlreadyExistsException.class);
        map.put("OperationAborted", ResourceConflictException.class);
        
        // Common 503 Service Unavailable errors
        map.put("SlowDown", ResourceExhaustedException.class);
        
        ERROR_MAPPING = Collections.unmodifiableMap(map);
    }

    public static Map<String, Class<? extends SubstrateSdkException>> get() {
        return ERROR_MAPPING;
    }
}
