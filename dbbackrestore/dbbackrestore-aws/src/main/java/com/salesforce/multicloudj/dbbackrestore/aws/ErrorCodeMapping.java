package com.salesforce.multicloudj.dbbackrestore.aws;

import com.google.common.collect.ImmutableMap;
import com.salesforce.multicloudj.common.aws.CommonErrorCodeMapping;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

import java.util.HashMap;
import java.util.Map;

/**
 * Provides the mapping of various AWS Backup error codes to SDK's exceptions.
 *
 * @since 0.2.25
 */
public class ErrorCodeMapping {

  private ErrorCodeMapping() {
  }

  private static final Map<String, Class<? extends SubstrateSdkException>> ERROR_MAPPING;

  static {
    Map<String, Class<? extends SubstrateSdkException>> map = new HashMap<>(
        CommonErrorCodeMapping.get());
    // AWS Backup specific error codes
    map.put("ResourceNotFoundException", ResourceNotFoundException.class);
    ERROR_MAPPING = ImmutableMap.copyOf(map);
  }

  static Class<? extends SubstrateSdkException> getException(String errorCode) {
    return ERROR_MAPPING.getOrDefault(errorCode, UnknownException.class);
  }

  static Class<? extends SubstrateSdkException> getException(Throwable t) {
    if (t instanceof AwsServiceException) {
      AwsServiceException serviceException = (AwsServiceException) t;
      if (serviceException.awsErrorDetails() == null) {
        return UnknownException.class;
      }

      String errorCode = serviceException.awsErrorDetails().errorCode();
      return ERROR_MAPPING.getOrDefault(errorCode, UnknownException.class);
    }
    return UnknownException.class;
  }
}
