package com.salesforce.multicloudj.docstore.gcp;

import com.google.api.gax.rpc.StatusCode;
import com.salesforce.multicloudj.common.exceptions.DeadlineExceededException;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps Google Cloud status codes to appropriate SubstrateSdkException types.
 * This provides consistent error handling across all GCP implementations.
 */
public class ErrorCodeMapping {

    private static final Map<StatusCode.Code, Class<? extends SubstrateSdkException>> EXCEPTION_MAP = new HashMap<>();

    static {
        // Resource-related exceptions
        EXCEPTION_MAP.put(StatusCode.Code.NOT_FOUND, ResourceNotFoundException.class);
        EXCEPTION_MAP.put(StatusCode.Code.ALREADY_EXISTS, ResourceAlreadyExistsException.class);

        // Input validation exceptions
        EXCEPTION_MAP.put(StatusCode.Code.INVALID_ARGUMENT, InvalidArgumentException.class);
        EXCEPTION_MAP.put(StatusCode.Code.OUT_OF_RANGE, InvalidArgumentException.class);
        EXCEPTION_MAP.put(StatusCode.Code.FAILED_PRECONDITION, FailedPreconditionException.class);

        // Authorization exceptions - currently mapped to Unknown
        // TODO: Create specific exception types for these
        EXCEPTION_MAP.put(StatusCode.Code.PERMISSION_DENIED, UnAuthorizedException.class);
        EXCEPTION_MAP.put(StatusCode.Code.UNAUTHENTICATED, UnAuthorizedException.class);

        // Service availability exceptions
        EXCEPTION_MAP.put(StatusCode.Code.UNAVAILABLE, UnknownException.class);
        EXCEPTION_MAP.put(StatusCode.Code.RESOURCE_EXHAUSTED, ResourceExhaustedException.class);
        EXCEPTION_MAP.put(StatusCode.Code.DEADLINE_EXCEEDED, DeadlineExceededException.class);

        // Operation/transaction exceptions
        EXCEPTION_MAP.put(StatusCode.Code.ABORTED, DeadlineExceededException.class);
        EXCEPTION_MAP.put(StatusCode.Code.CANCELLED, UnknownException.class);

        // Server-side errors
        EXCEPTION_MAP.put(StatusCode.Code.INTERNAL, UnknownException.class);
        EXCEPTION_MAP.put(StatusCode.Code.UNIMPLEMENTED, UnSupportedOperationException.class);
        EXCEPTION_MAP.put(StatusCode.Code.UNKNOWN, UnknownException.class);
    }

    /**
     * Returns the appropriate SubstrateSdkException class for a given GCP/gRPC status code.
     *
     * @param code the status code from a GCP ApiException
     * @return the corresponding SubstrateSdkException class
     */
    public static Class<? extends SubstrateSdkException> getException(StatusCode.Code code) {
        if (code == null) {
            return UnknownException.class;
        }

        return EXCEPTION_MAP.getOrDefault(code, UnknownException.class);
    }
} 