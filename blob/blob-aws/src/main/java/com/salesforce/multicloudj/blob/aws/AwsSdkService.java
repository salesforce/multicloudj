package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.common.aws.AwsRetryClassifier;
import com.salesforce.multicloudj.common.exceptions.DeadlineExceededException;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.service.SdkService;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;

public interface AwsSdkService extends SdkService {

  @Override
  default SubstrateSdkException mapException(Throwable t) {
    boolean acquisitionTimeout = false;
    if (!(t instanceof AwsServiceException)) {
      boolean sdkClientFailure = false;
      for (Throwable cause : ExceptionUtils.getThrowableList(t)) {
        sdkClientFailure |= cause instanceof SdkClientException;
        if (sdkClientFailure
            && (cause instanceof ConnectionPoolTimeoutException
                || cause instanceof TimeoutException)) {
          acquisitionTimeout = true;
          break;
        }
      }
    }
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
    } else if (t instanceof SdkClientException
        || t instanceof IllegalArgumentException
        || acquisitionTimeout) {
      exceptionClass =
          acquisitionTimeout ? DeadlineExceededException.class : InvalidArgumentException.class;
    } else {
      exceptionClass = UnknownException.class;
    }
    return ExceptionHandler.build(
        exceptionClass, t, acquisitionTimeout ? Boolean.TRUE : AwsRetryClassifier.classify(t));
  }
}
