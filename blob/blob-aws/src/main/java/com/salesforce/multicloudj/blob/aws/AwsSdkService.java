package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.common.aws.AwsRetryClassifier;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceConflictException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.service.SdkService;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;

public interface AwsSdkService extends SdkService {

  @Override
  default SubstrateSdkException mapException(Throwable t) {
    Throwable failure = unwrapAsyncFailure(t);
    Class<? extends SubstrateSdkException> exceptionClass;
    if (failure instanceof AwsServiceException) {
      AwsServiceException awsServiceException = (AwsServiceException) failure;
      String requestId = awsServiceException.requestId();
      if ((requestId == null || requestId.isEmpty()) && awsServiceException.statusCode() == 403) {
        exceptionClass = UnAuthorizedException.class;
      } else {
        String errorCode = getErrorCode(awsServiceException);
        exceptionClass = ErrorCodeMapping.getException(errorCode);
      }
    } else if (failure instanceof SdkClientException
        || failure instanceof IllegalArgumentException) {
      exceptionClass = InvalidArgumentException.class;
    } else {
      exceptionClass = UnknownException.class;
    }
    return ExceptionHandler.build(
        exceptionClass, failure, AwsRetryClassifier.classify(failure));
  }

  /**
   * Translates failures whose meaning depends on an opt-in conditional upload.
   *
   * <p>A failed create-if-absent condition is a definitive existing-resource result. A concurrent
   * conditional-request conflict is transient and remains distinct so callers can retry it.
   */
  default RuntimeException translateUploadFailure(UploadRequest request, Throwable t) {
    Throwable failure = unwrapAsyncFailure(t);
    if (request.isCreateIfAbsent() && failure instanceof AwsServiceException) {
      AwsServiceException serviceException = (AwsServiceException) failure;
      String errorCode = getErrorCode(serviceException);
      if (serviceException.statusCode() == 412 || "PreconditionFailed".equals(errorCode)) {
        return new ResourceAlreadyExistsException("Blob already exists", serviceException);
      }
      if (serviceException.statusCode() == 409
          && "ConditionalRequestConflict".equals(errorCode)) {
        return new ResourceConflictException(
            "Conditional blob upload conflicted", serviceException, true);
      }
    }
    if (failure instanceof RuntimeException) {
      return (RuntimeException) failure;
    }
    return mapException(failure);
  }

  private static String getErrorCode(AwsServiceException exception) {
    return exception.awsErrorDetails() == null
        ? null
        : exception.awsErrorDetails().errorCode();
  }

  private static Throwable unwrapAsyncFailure(Throwable t) {
    Throwable failure = t;
    while ((failure instanceof CompletionException || failure instanceof ExecutionException)
        && failure.getCause() != null) {
      failure = failure.getCause();
    }
    return failure;
  }
}
