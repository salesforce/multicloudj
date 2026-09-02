package com.salesforce.multicloudj.pubsub.ali;

import com.aliyun.mns.common.ClientException;
import com.aliyun.mns.common.ServiceException;
import com.aliyun.mns.common.ServiceHandlingRequiredException;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;

/**
 * Translates Alibaba SMQ (MNS) SDK throwables into multicloudj {@link SubstrateSdkException}s.
 *
 * <p>Shared by the SMQ topic and subscription implementations. MNS surfaces
 * failures as {@link ServiceException} (server-side, carries the wire {@code Code}), {@link
 * ClientException} (client-side / transport), and the checked {@link
 * ServiceHandlingRequiredException} (per-message handling failure on pop/delete, also carries a
 * {@code Code}).
 */
final class MnsExceptionMapper {

  private MnsExceptionMapper() {}

  static SubstrateSdkException map(Throwable t) {
    Class<? extends SubstrateSdkException> exceptionClass;
    if (t instanceof ServiceException) {
      exceptionClass = ErrorCodeMapping.getException(((ServiceException) t).getErrorCode());
    } else if (t instanceof ServiceHandlingRequiredException) {
      exceptionClass =
          ErrorCodeMapping.getException(((ServiceHandlingRequiredException) t).getErrorCode());
    } else if (t instanceof ClientException) {
      // Client-side / transport failures carry no service Code; default to unknown.
      exceptionClass = UnknownException.class;
    } else if (t instanceof IllegalArgumentException) {
      exceptionClass = InvalidArgumentException.class;
    } else {
      exceptionClass = UnknownException.class;
    }
    // MNS exceptions do not expose an HTTP status, so we let the exception type's default
    // retryability apply (null hint); ErrorCodeMapping already routes throttling codes to a
    // retryable ResourceExhaustedException.
    return ExceptionHandler.build(exceptionClass, t, null);
  }

  /**
   * Maps a bare SMQ error {@code Code} (as carried by a per-message failure result inside a batch
   * exception) to a typed {@link SubstrateSdkException}, wrapping {@code cause}. The exception
   * type's default retryability applies (null hint), matching {@link #map(Throwable)}.
   */
  static SubstrateSdkException mapErrorCode(String errorCode, Throwable cause) {
    return ExceptionHandler.build(ErrorCodeMapping.getException(errorCode), cause, null);
  }
}
