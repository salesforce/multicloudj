package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.common.aws.AwsRetryClassifier;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.service.SdkService;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;

public interface AwsSdkService extends SdkService {

  @Override
  default SubstrateSdkException mapException(Throwable t) {
    Class<? extends SubstrateSdkException> exceptionClass;
    if (t instanceof AwsServiceException) {
      AwsServiceException awsServiceException = (AwsServiceException) t;
      String requestId = awsServiceException.requestId();
      if ((requestId == null || requestId.isEmpty()) && awsServiceException.statusCode() == 403) {
        exceptionClass = UnAuthorizedException.class;
      } else {
        String errorCode = awsServiceException.awsErrorDetails().errorCode();
        exceptionClass = ErrorCodeMapping.getException(errorCode);
      }
    } else if (t instanceof SdkClientException || t instanceof IllegalArgumentException) {
      exceptionClass = InvalidArgumentException.class;
    } else {
      exceptionClass = UnknownException.class;
    }
    return ExceptionHandler.build(exceptionClass, t, AwsRetryClassifier.classify(t));
  }
}
