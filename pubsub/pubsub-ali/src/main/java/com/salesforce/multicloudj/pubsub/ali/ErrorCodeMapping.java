package com.salesforce.multicloudj.pubsub.ali;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import java.util.HashMap;
import java.util.Map;

/**
 * Maps Alibaba SMQ (MNS) service error codes (the {@code Code} value in the error response) to
 * multicloudj exceptions. Consumed by the SMQ topic and subscription implementations when
 * translating a {@code ServiceException} into a {@link SubstrateSdkException}.
 *
 * <p>The keys are the documented wire values from the SMQ error-response reference:
 * https://www.alibabacloud.com/help/en/mns/developer-reference/syntax-of-error-responses
 */
public class ErrorCodeMapping {

  private ErrorCodeMapping() {}

  private static final Map<String, Class<? extends SubstrateSdkException>> ERROR_MAPPING =
      new HashMap<>();

  static {
    // Authentication / authorization (HTTP 403).
    ERROR_MAPPING.put("AccessDenied", UnAuthorizedException.class);
    ERROR_MAPPING.put("InvalidAccessKeyId", UnAuthorizedException.class);
    ERROR_MAPPING.put("SignatureDoesNotMatch", UnAuthorizedException.class);

    // Request / header / parameter / naming validation (HTTP 400).
    ERROR_MAPPING.put("InvalidAuthorizationHeader", InvalidArgumentException.class);
    ERROR_MAPPING.put("MissingAuthorizationHeader", InvalidArgumentException.class);
    ERROR_MAPPING.put("InvalidDateHeader", InvalidArgumentException.class);
    ERROR_MAPPING.put("MissingDateHeader", InvalidArgumentException.class);
    ERROR_MAPPING.put("InvalidDegist", InvalidArgumentException.class);
    ERROR_MAPPING.put("InvalidRequestURL", InvalidArgumentException.class);
    ERROR_MAPPING.put("InvalidQueryString", InvalidArgumentException.class);
    ERROR_MAPPING.put("MalformedXML", InvalidArgumentException.class);
    ERROR_MAPPING.put("InvalidArgument", InvalidArgumentException.class);
    ERROR_MAPPING.put("InvalidQueueName", InvalidArgumentException.class);
    ERROR_MAPPING.put("QueueNameLengthError", InvalidArgumentException.class);
    ERROR_MAPPING.put("TopicNameInvalid", InvalidArgumentException.class);
    ERROR_MAPPING.put("TopicNameLengthError", InvalidArgumentException.class);
    ERROR_MAPPING.put("SubscriptionNameInvalid", InvalidArgumentException.class);
    ERROR_MAPPING.put("SubscriptionNameLengthError", InvalidArgumentException.class);
    ERROR_MAPPING.put("EndpointInvalid", InvalidArgumentException.class);
    ERROR_MAPPING.put("MissingReceiptHandle", InvalidArgumentException.class);
    ERROR_MAPPING.put("MissingVisibilityTimeout", InvalidArgumentException.class);
    ERROR_MAPPING.put("ReceiptHandleError", InvalidArgumentException.class);

    // Missing resources (HTTP 404).
    ERROR_MAPPING.put("QueueNotExist", ResourceNotFoundException.class);
    ERROR_MAPPING.put("TopicNotExist", ResourceNotFoundException.class);
    ERROR_MAPPING.put("SubscriptionNotExist", ResourceNotFoundException.class);
    ERROR_MAPPING.put("MessageNotExist", ResourceNotFoundException.class);

    // Resource-creation conflicts (HTTP 409).
    ERROR_MAPPING.put("QueueAlreadyExist", ResourceAlreadyExistsException.class);
    ERROR_MAPPING.put("TopicAlreadyExist", ResourceAlreadyExistsException.class);
    ERROR_MAPPING.put("SubscriptionAlreadyExist", ResourceAlreadyExistsException.class);

    // Throttling / rate limiting (HTTP 400 / 429).
    ERROR_MAPPING.put("QpsLimitExceeded", ResourceExhaustedException.class);
    ERROR_MAPPING.put("TooManyRequests", ResourceExhaustedException.class);

    // Server-side (HTTP 500).
    ERROR_MAPPING.put("InternalError", UnknownException.class);
    ERROR_MAPPING.put("InternalServerError", UnknownException.class);
  }

  static Class<? extends SubstrateSdkException> getException(String errorCode) {
    return ERROR_MAPPING.getOrDefault(errorCode, UnknownException.class);
  }
}
