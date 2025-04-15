package com.salesforce.multicloudj.common.aws;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;

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
        ERROR_MAPPING = Map.ofEntries(
                Map.entry("AccessDeniedException", UnAuthorizedException.class),
                Map.entry("IncompleteSignature", InvalidArgumentException.class),
                Map.entry("InternalFailure", UnknownException.class),
                Map.entry("InvalidAction", InvalidArgumentException.class),
                Map.entry("InvalidClientTokenId", InvalidArgumentException.class),
                Map.entry("NotAuthorized", UnAuthorizedException.class),
                Map.entry("OptInRequired", UnAuthorizedException.class),
                Map.entry("RequestExpired", ResourceExhaustedException.class),
                Map.entry("ServiceUnavailable", UnknownException.class),
                Map.entry("ThrottlingException", ResourceExhaustedException.class),
                Map.entry("ValidationError", InvalidArgumentException.class)
        );
    }

    public static Map<String, Class<? extends SubstrateSdkException>> get() {
        return ERROR_MAPPING;
    }
}
