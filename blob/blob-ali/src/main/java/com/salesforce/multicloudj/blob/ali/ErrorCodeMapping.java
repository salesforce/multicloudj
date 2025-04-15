package com.salesforce.multicloudj.blob.ali;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;

import java.util.HashMap;
import java.util.Map;

/**
 * Provides the mapping of various error codes to SDK's exceptions
 *
 * For reference, see: https://www.alibabacloud.com/help/en/oss/developer-reference/error-handling-1
 */
public class ErrorCodeMapping {

    private ErrorCodeMapping() {}

    private static final Map<String, Class<? extends SubstrateSdkException>> ERROR_MAPPING = new HashMap<>();

    static {
        ERROR_MAPPING.put("AccessDenied", UnAuthorizedException.class);
        ERROR_MAPPING.put("InvalidSecurityToken", InvalidArgumentException.class);
        ERROR_MAPPING.put("InvalidToken", UnAuthorizedException.class);
        ERROR_MAPPING.put("InvalidAccessKeyId", InvalidArgumentException.class);
        ERROR_MAPPING.put("ServerError", UnknownException.class);
        ERROR_MAPPING.put("NoSuchBucket", InvalidArgumentException.class);
        ERROR_MAPPING.put("NoSuchKey", InvalidArgumentException.class);
        ERROR_MAPPING.put("InvalidRequest", InvalidArgumentException.class);
        ERROR_MAPPING.put("SignatureDoesNotMatch", UnAuthorizedException.class);
        ERROR_MAPPING.put("BucketAlreadyExists", InvalidArgumentException.class);
        ERROR_MAPPING.put("InvalidBucketName", InvalidArgumentException.class);
        ERROR_MAPPING.put("MissingArgument", InvalidArgumentException.class);
    }

    static Class<? extends SubstrateSdkException> getException(String errorCode) {
        return ERROR_MAPPING.getOrDefault(errorCode, UnknownException.class);
    }
}
