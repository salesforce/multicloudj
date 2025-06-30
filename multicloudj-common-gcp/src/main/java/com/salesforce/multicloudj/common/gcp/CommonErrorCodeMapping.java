package com.salesforce.multicloudj.common.gcp;

import com.google.api.gax.rpc.StatusCode;
import com.salesforce.multicloudj.common.exceptions.DeadlineExceededException;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceConflictException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;

import java.util.HashMap;
import java.util.Map;

/**
 * Provides the mapping of various error codes to SDK's exceptions
 */
public class CommonErrorCodeMapping {

    private CommonErrorCodeMapping() {
    }

    private static final Map<StatusCode.Code, Class<? extends SubstrateSdkException>> ERROR_MAPPING = new HashMap<>();
    private static final Map<Integer, Class<? extends SubstrateSdkException>> STORAGE_EXCEPTION_MAP = new HashMap<>();

    // https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
    static {
        ERROR_MAPPING.put(StatusCode.Code.CANCELLED, UnknownException.class);
        ERROR_MAPPING.put(StatusCode.Code.UNKNOWN, UnknownException.class);
        ERROR_MAPPING.put(StatusCode.Code.INVALID_ARGUMENT, InvalidArgumentException.class);
        ERROR_MAPPING.put(StatusCode.Code.DEADLINE_EXCEEDED, DeadlineExceededException.class);
        ERROR_MAPPING.put(StatusCode.Code.NOT_FOUND, ResourceNotFoundException.class);
        ERROR_MAPPING.put(StatusCode.Code.ALREADY_EXISTS, ResourceAlreadyExistsException.class);
        ERROR_MAPPING.put(StatusCode.Code.PERMISSION_DENIED, UnAuthorizedException.class);
        ERROR_MAPPING.put(StatusCode.Code.RESOURCE_EXHAUSTED, ResourceExhaustedException.class);
        ERROR_MAPPING.put(StatusCode.Code.FAILED_PRECONDITION, FailedPreconditionException.class);
        ERROR_MAPPING.put(StatusCode.Code.ABORTED, DeadlineExceededException.class);
        ERROR_MAPPING.put(StatusCode.Code.OUT_OF_RANGE, InvalidArgumentException.class);
        ERROR_MAPPING.put(StatusCode.Code.UNIMPLEMENTED, UnSupportedOperationException.class);
        ERROR_MAPPING.put(StatusCode.Code.INTERNAL, UnknownException.class);
        ERROR_MAPPING.put(StatusCode.Code.UNAVAILABLE, UnknownException.class);
        ERROR_MAPPING.put(StatusCode.Code.DATA_LOSS, UnknownException.class);
        ERROR_MAPPING.put(StatusCode.Code.UNAUTHENTICATED, UnAuthorizedException.class);
    }

    // https://cloud.google.com/storage/docs/json_api/v1/status-codes
    static {
        STORAGE_EXCEPTION_MAP.put(0, UnknownException.class);
        STORAGE_EXCEPTION_MAP.put(400, InvalidArgumentException.class);
        STORAGE_EXCEPTION_MAP.put(401, UnAuthorizedException.class);
        STORAGE_EXCEPTION_MAP.put(403, UnAuthorizedException.class);
        STORAGE_EXCEPTION_MAP.put(404, ResourceNotFoundException.class);
        STORAGE_EXCEPTION_MAP.put(405, InvalidArgumentException.class);
        STORAGE_EXCEPTION_MAP.put(409, ResourceConflictException.class);
        STORAGE_EXCEPTION_MAP.put(412, FailedPreconditionException.class);
        STORAGE_EXCEPTION_MAP.put(413, InvalidArgumentException.class);
        STORAGE_EXCEPTION_MAP.put(429, ResourceExhaustedException.class);
        STORAGE_EXCEPTION_MAP.put(500, UnknownException.class);
        STORAGE_EXCEPTION_MAP.put(501, UnSupportedOperationException.class);
        STORAGE_EXCEPTION_MAP.put(503, UnknownException.class);
        STORAGE_EXCEPTION_MAP.put(504, DeadlineExceededException.class);
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

        return ERROR_MAPPING.getOrDefault(code, UnknownException.class);
    }

    /**
     * Returns the appropriate SubstrateSdkException class for a given StorageException
     * @param code The StorageException code
     * @return the corresponding SubstrateSdkException class
     */
    public static Class<? extends SubstrateSdkException> getException(int code) {
        return STORAGE_EXCEPTION_MAP.getOrDefault(code, UnknownException.class);
    }
}
