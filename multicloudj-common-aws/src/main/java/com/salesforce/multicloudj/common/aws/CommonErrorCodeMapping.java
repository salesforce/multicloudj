package com.salesforce.multicloudj.common.aws;

import java.util.Map;
import java.util.function.Function;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.TransactionFailedException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;

/**
 * Provides the mapping of various error codes to SDK's exceptions.
 */
public class CommonErrorCodeMapping {

    private CommonErrorCodeMapping() {
    }

    private static final Map<String, Class<? extends SubstrateSdkException>> ERROR_MAPPING;

    static {
        // The common error codes as source of truth is here:
        // https://docs.aws.amazon.com/STS/latest/APIReference/CommonErrors.html
        // https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html
        // These are errors that are common across multiple AWS services (STS, DynamoDB, etc.)
        ERROR_MAPPING = Map.ofEntries(
                Map.entry("IncompleteSignature", UnAuthorizedException.class),
                Map.entry("InternalFailure", TransactionFailedException.class),
                Map.entry("InvalidAction", InvalidArgumentException.class),
                Map.entry("InvalidClientTokenId", UnAuthorizedException.class),
                Map.entry("NotAuthorized", UnAuthorizedException.class),
                Map.entry("OptInRequired", UnAuthorizedException.class),
                Map.entry("RequestExpired", ResourceExhaustedException.class),
                Map.entry("ServiceUnavailable", ResourceExhaustedException.class),
                Map.entry("ThrottlingException", ResourceExhaustedException.class),
                Map.entry("ValidationError", InvalidArgumentException.class),
                Map.entry("AccessDenied", UnAuthorizedException.class),
                Map.entry("AccountNotAuthorized", UnAuthorizedException.class),
                Map.entry("AccountProblem", UnAuthorizedException.class),
                Map.entry("AllAccessDisabled", UnAuthorizedException.class),
                Map.entry("InvalidAccessKeyId", UnAuthorizedException.class),
                Map.entry("InvalidPayer", UnAuthorizedException.class),
                Map.entry("InvalidSecurity", UnAuthorizedException.class),
                Map.entry("NotSignedUp", UnAuthorizedException.class),
                Map.entry("RequestTimeTooSkewed", InvalidArgumentException.class),
                Map.entry("SignatureDoesNotMatch", InvalidArgumentException.class),
                Map.entry("TokenRefreshRequired", UnAuthorizedException.class)
        );
    }

    public static Map<String, Class<? extends SubstrateSdkException>> get() {
        return ERROR_MAPPING;
    }

    public static Class<? extends SubstrateSdkException> mapException(
            Throwable t,
            Function<String, Class<? extends SubstrateSdkException>> errorCodeMapper) {

        if (t instanceof SubstrateSdkException && !t.getClass().equals(SubstrateSdkException.class)) {
            return (Class<? extends SubstrateSdkException>) t.getClass();
        }
        if (t instanceof AwsServiceException) {
            AwsServiceException serviceException = (AwsServiceException) t;
            if (serviceException.awsErrorDetails() != null) {
                String errorCode = serviceException.awsErrorDetails().errorCode();
                return errorCodeMapper.apply(errorCode);
            }
            return UnknownException.class;
        }
        if (t instanceof SdkClientException || t instanceof IllegalArgumentException) {
            return InvalidArgumentException.class;
        }
        return UnknownException.class;
    }
}
