package com.salesforce.multicloudj.kms.gcp;

import com.google.api.gax.rpc.StatusCode;
import com.salesforce.multicloudj.common.exceptions.DeadlineExceededException;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import java.util.EnumMap;
import java.util.Map;

/** Classifies Cloud KMS status codes into cloud-agnostic exception types. */
final class ErrorCodeMapping {

  private static final Map<StatusCode.Code, Class<? extends SubstrateSdkException>> MAPPING =
      new EnumMap<>(StatusCode.Code.class);

  static {
    MAPPING.put(StatusCode.Code.NOT_FOUND, ResourceNotFoundException.class);
    MAPPING.put(StatusCode.Code.ALREADY_EXISTS, ResourceAlreadyExistsException.class);
    MAPPING.put(StatusCode.Code.FAILED_PRECONDITION, FailedPreconditionException.class);
    MAPPING.put(StatusCode.Code.INVALID_ARGUMENT, InvalidArgumentException.class);
    MAPPING.put(StatusCode.Code.OUT_OF_RANGE, InvalidArgumentException.class);
    MAPPING.put(StatusCode.Code.PERMISSION_DENIED, UnAuthorizedException.class);
    MAPPING.put(StatusCode.Code.UNAUTHENTICATED, UnAuthorizedException.class);
    MAPPING.put(StatusCode.Code.RESOURCE_EXHAUSTED, ResourceExhaustedException.class);
    MAPPING.put(StatusCode.Code.DEADLINE_EXCEEDED, DeadlineExceededException.class);
    MAPPING.put(StatusCode.Code.UNIMPLEMENTED, UnSupportedOperationException.class);
  }

  private ErrorCodeMapping() {}

  static Class<? extends SubstrateSdkException> getException(StatusCode.Code code) {
    if (code == null) {
      return UnknownException.class;
    }
    return MAPPING.getOrDefault(code, UnknownException.class);
  }
}
