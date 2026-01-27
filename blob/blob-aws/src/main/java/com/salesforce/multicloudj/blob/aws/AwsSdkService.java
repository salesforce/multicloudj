package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.NetworkConnectivityException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.service.SdkService;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;

public interface AwsSdkService extends SdkService {

    @Override
    default Class<? extends SubstrateSdkException> getException(Throwable t) {
        if (t instanceof AwsServiceException) {
            AwsServiceException awsServiceException = (AwsServiceException) t;
            String requestId = awsServiceException.requestId();
            if (awsServiceException.statusCode() == 403 && (requestId == null || requestId.isEmpty())) {
                return NetworkConnectivityException.class;
            }

            String errorCode = awsServiceException.awsErrorDetails().errorCode();
            return ErrorCodeMapping.getException(errorCode);
        } else if (t instanceof SdkClientException || t instanceof IllegalArgumentException) {
            return InvalidArgumentException.class;
        }
        return UnknownException.class;
    }
}
