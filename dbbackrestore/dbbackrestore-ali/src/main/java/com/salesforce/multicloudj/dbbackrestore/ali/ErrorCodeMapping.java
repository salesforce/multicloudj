package com.salesforce.multicloudj.dbbackrestore.ali;

import com.salesforce.multicloudj.common.exceptions.*;

/**
 * Maps Alibaba Cloud HBR exceptions to MultiCloudJ exception types.
 *
 * @since 0.2.25
 */
public class ErrorCodeMapping {

  /**
   * Maps an Alibaba Cloud exception to the appropriate MultiCloudJ exception type.
   *
   * @param throwable the exception to map
   * @return the MultiCloudJ exception class
   */
  public static Class<? extends SubstrateSdkException> getException(Throwable throwable) {
    if (throwable == null) {
      return UnknownException.class;
    }

    String message = throwable.getMessage();
    if (message == null) {
      return UnknownException.class;
    }

    // Common Alibaba Cloud error patterns
    if (message.contains("EntityNotExist") || message.contains("NotFound")) {
      return ResourceNotFoundException.class;
    }
    
    if (message.contains("InvalidAccessKeyId") || message.contains("SignatureDoesNotMatch")
        || message.contains("Forbidden") || message.contains("InvalidParameter.Unauthorized")) {
      return UnAuthorizedException.class;
    }
    
    if (message.contains("Throttling") || message.contains("QpsLimitExceeded")
        || message.contains("TooManyRequests")) {
      return ResourceExhaustedException.class;
    }
    
    if (message.contains("InternalError") || message.contains("ServiceUnavailable")) {
      return UnknownException.class;
    }
    
    if (message.contains("InvalidParameter") || message.contains("MissingParameter")
        || message.contains("IllegalArgument")) {
      return InvalidArgumentException.class;
    }

    // Default to UnknownException
    return UnknownException.class;
  }
}
