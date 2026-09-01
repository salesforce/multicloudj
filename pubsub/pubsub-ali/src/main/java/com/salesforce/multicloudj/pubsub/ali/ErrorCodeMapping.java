package com.salesforce.multicloudj.pubsub.ali;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import java.util.HashMap;
import java.util.Map;

/**
 * Maps Alibaba SMQ (MNS) service error codes to multicloudj exceptions. Consumed by the SMQ topic
 * and subscription implementations when translating a {@code ServiceException} into a {@link
 * SubstrateSdkException}.
 *
 * <p>For reference, see:
 * https://www.alibabacloud.com/help/en/mns/developer-reference/error-code-list
 */
public class ErrorCodeMapping {

  private ErrorCodeMapping() {}

  private static final Map<String, Class<? extends SubstrateSdkException>> ERROR_MAPPING =
      new HashMap<>();

  static {
    // Missing resources.
    ERROR_MAPPING.put("QueueNotExist", ResourceNotFoundException.class);
    ERROR_MAPPING.put("TopicNotExist", ResourceNotFoundException.class);
    ERROR_MAPPING.put("SubscriptionNotExist", ResourceNotFoundException.class);
    ERROR_MAPPING.put("MessageNotExist", ResourceNotFoundException.class);

    // Authentication / authorization.
    ERROR_MAPPING.put("AccessDenied", UnAuthorizedException.class);
    ERROR_MAPPING.put("SignatureDoesNotMatch", UnAuthorizedException.class);
    ERROR_MAPPING.put("InvalidAccessKeyId", InvalidArgumentException.class);
    ERROR_MAPPING.put("InvalidSecurityToken", InvalidArgumentException.class);

    // Request validation.
    ERROR_MAPPING.put("InvalidArgument", InvalidArgumentException.class);
    ERROR_MAPPING.put("MissingArgument", InvalidArgumentException.class);
    ERROR_MAPPING.put("InvalidQueueName", InvalidArgumentException.class);
    ERROR_MAPPING.put("InvalidTopicName", InvalidArgumentException.class);
    ERROR_MAPPING.put("QueueAlreadyExist", InvalidArgumentException.class);
    ERROR_MAPPING.put("TopicAlreadyExist", InvalidArgumentException.class);
    ERROR_MAPPING.put("ReceiptHandleError", InvalidArgumentException.class);

    // Server-side.
    ERROR_MAPPING.put("InternalError", UnknownException.class);
  }

  static Class<? extends SubstrateSdkException> getException(String errorCode) {
    return ERROR_MAPPING.getOrDefault(errorCode, UnknownException.class);
  }
}
