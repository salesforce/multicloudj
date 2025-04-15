package com.salesforce.multicloudj.docstore.ali;

import com.salesforce.multicloudj.common.exceptions.DeadlineExceededException;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;

import java.util.HashMap;
import java.util.Map;

/**
 * Provides the mapping of various error codes to SDK's exceptions
 *
 * For reference, see: https://www.alibabacloud.com/help/en/tablestore/developer-reference/error-codes
 */
public class ErrorCodeMapping {
    static final String OTS_CONDITIONAL_CHECK_FAILED = "OTSConditionCheckFail";
    private ErrorCodeMapping() {}

    private static final Map<String, Class<? extends SubstrateSdkException>> ERROR_MAPPING = new HashMap<>();

    static {
        ERROR_MAPPING.put("OTSAuthFailed", UnAuthorizedException.class);
        ERROR_MAPPING.put(OTS_CONDITIONAL_CHECK_FAILED, FailedPreconditionException.class);
        ERROR_MAPPING.put("OTSNoPermissionAccess", UnAuthorizedException.class);
        ERROR_MAPPING.put("OTSRequestBodyTooLarge", InvalidArgumentException.class);
        ERROR_MAPPING.put("OTSRequestTimeout", ResourceExhaustedException.class);
        ERROR_MAPPING.put("OTSMethodNotAllowed", InvalidArgumentException.class);
        ERROR_MAPPING.put("OTSInternalServerError", UnknownException.class);
        ERROR_MAPPING.put("OTSQuotaExhausted", ResourceExhaustedException.class);
        ERROR_MAPPING.put("OTSInvalidPK", InvalidArgumentException.class);
        ERROR_MAPPING.put("OTSOutOfRowSizeLimit", ResourceExhaustedException.class);
        ERROR_MAPPING.put("OTSOutOfColumnCountLimit", ResourceExhaustedException.class);
        ERROR_MAPPING.put("OTSServerBusy", UnknownException.class);
        ERROR_MAPPING.put("OTSTimeout", DeadlineExceededException.class);
        ERROR_MAPPING.put("OTSServerUnavailable", UnknownException.class);
        ERROR_MAPPING.put("OTSPartitionUnavailable", UnknownException.class);
        ERROR_MAPPING.put("OTSRowOperationConflict", FailedPreconditionException.class);
        ERROR_MAPPING.put("OTSObjectAlreadyExist", ResourceAlreadyExistsException.class);
        ERROR_MAPPING.put("OTSObjectNotExist", ResourceNotFoundException.class);
        ERROR_MAPPING.put("OTSTableNotReady", ResourceNotFoundException.class);

    }

    static Class<? extends SubstrateSdkException> getException(String errorCode) {
        return ERROR_MAPPING.getOrDefault(errorCode, UnknownException.class);
    }
}
