package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.common.aws.CommonErrorCodeMapping;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
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
public class ErrorCodeMapping {

    private ErrorCodeMapping() {}

    private static final Map<String, Class<? extends SubstrateSdkException>> ERROR_MAPPING;

    static {
        Map<String, Class<? extends SubstrateSdkException>> map = new HashMap<>(CommonErrorCodeMapping.get());
        map.put("InvalidAccessKeyId", InvalidArgumentException.class);
        map.put("NoSuchKey", ResourceNotFoundException.class);
        map.put("NoSuchBucket", InvalidArgumentException.class);
        map.put("InvalidObjectState", UnAuthorizedException.class);
        ERROR_MAPPING = Collections.unmodifiableMap(map);
    }

    static Class<? extends SubstrateSdkException> getException(String errorCode) {
        return ERROR_MAPPING.getOrDefault(errorCode, UnknownException.class);
    }
}
