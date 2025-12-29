package com.salesforce.multicloudj.iam.aws;

import com.salesforce.multicloudj.common.aws.CommonErrorCodeMapping;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceConflictException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Maps AWS IAM error codes to SubstrateSdkException types.
 */
public final class ErrorCodeMapping {

    private ErrorCodeMapping() {}

    private static final Map<String, Class<? extends SubstrateSdkException>> ERROR_MAPPING;

    static {
        Map<String, Class<? extends SubstrateSdkException>> map = new HashMap<>(CommonErrorCodeMapping.get());

        // AWS IAM specific error codes
        // https://docs.aws.amazon.com/IAM/latest/APIReference/CommonErrors.html
        // https://docs.aws.amazon.com/IAM/latest/APIReference/API_Operations.html

        // InvalidArgument errors
        map.put("InvalidInput", InvalidArgumentException.class);
        map.put("MalformedPolicyDocument", InvalidArgumentException.class);

        // ResourceNotFound errors
        map.put("NoSuchEntity", ResourceNotFoundException.class);
        map.put("NoSuchEntityException", ResourceNotFoundException.class);

        // ResourceAlreadyExists errors
        map.put("EntityAlreadyExists", ResourceAlreadyExistsException.class);
        map.put("EntityAlreadyExistsException", ResourceAlreadyExistsException.class);

        // ResourceConflict errors
        map.put("DeleteConflict", ResourceConflictException.class);
        map.put("DeleteConflictException", ResourceConflictException.class);

        // ResourceExhausted errors
        map.put("LimitExceeded", ResourceExhaustedException.class);
        map.put("LimitExceededException", ResourceExhaustedException.class);

        // Unknown errors
        map.put("ServiceFailure", UnknownException.class);
        map.put("ServiceFailureException", UnknownException.class);

        ERROR_MAPPING = Collections.unmodifiableMap(map);
    }

    static Class<? extends SubstrateSdkException> getException(String errorCode) {
        return ERROR_MAPPING.getOrDefault(errorCode, UnknownException.class);
    }
}
