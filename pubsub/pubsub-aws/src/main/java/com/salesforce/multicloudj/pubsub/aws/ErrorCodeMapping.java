package com.salesforce.multicloudj.pubsub.aws;

import com.salesforce.multicloudj.common.aws.CommonErrorCodeMapping;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Maps AWS SQS and SNS error codes to SubstrateSdkException types.
 */
public class ErrorCodeMapping {

  private ErrorCodeMapping() {}

  private static final Map<String, Class<? extends SubstrateSdkException>> ERROR_MAPPING;

  static {
    Map<String, Class<? extends SubstrateSdkException>> map =
        new HashMap<>(CommonErrorCodeMapping.get());

    // SQS specific error codes
    // https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/

    // InvalidArgument errors
    map.put("InvalidParameterValue", InvalidArgumentException.class);
    map.put("InvalidParameter", InvalidArgumentException.class);
    map.put("InvalidMessageBody", InvalidArgumentException.class);
    map.put("InvalidMessageAttributes", InvalidArgumentException.class);
    map.put("MessageTooLarge", InvalidArgumentException.class);
    map.put("BatchEntryIdsNotDistinct", InvalidArgumentException.class);
    map.put("InvalidIdFormat", InvalidArgumentException.class);
    map.put("ReceiptHandleIsInvalid", InvalidArgumentException.class);
    map.put("MessageNotInflight", InvalidArgumentException.class);
    map.put("InvalidAttributeName", InvalidArgumentException.class);
    map.put("InvalidBatchEntryId", InvalidArgumentException.class);
    map.put("InvalidMessageContents", InvalidArgumentException.class);
    map.put("TooManyEntriesInBatchRequest", InvalidArgumentException.class);
    map.put("UnsupportedOperation", InvalidArgumentException.class);
    map.put("EmptyBatchRequest", InvalidArgumentException.class);
    map.put("BatchRequestTooLong", InvalidArgumentException.class);

    // PermissionDenied errors
    map.put("AccessDenied", UnAuthorizedException.class);
    map.put("UnauthorizedOperation", UnAuthorizedException.class);
    map.put("InvalidSecurityException", UnAuthorizedException.class);

    // NotFound errors
    map.put("QueueDoesNotExist", ResourceNotFoundException.class);
    map.put("AWS.SimpleQueueService.NonExistentQueue", ResourceNotFoundException.class);
    map.put("NotFoundException", ResourceNotFoundException.class);

    // ResourceExhausted errors
    map.put("OverLimit", ResourceExhaustedException.class);
    map.put("ThrottlingException", ResourceExhaustedException.class);
    map.put("ThrottledException", ResourceExhaustedException.class);

    // FailedPrecondition errors
    map.put("PurgeQueueInProgress", InvalidArgumentException.class);
    map.put("QueueDeletedRecently", InvalidArgumentException.class);
    map.put("QueueNameExists", InvalidArgumentException.class);

    // Unknown errors
    map.put("InternalError", UnknownException.class);
    map.put("ServiceUnavailable", UnknownException.class);
    map.put("RequestCanceled", UnknownException.class);

    // SNS specific error codes
    // https://docs.aws.amazon.com/sns/latest/api/API_Publish.html
    
    // InvalidArgument errors
    map.put("InvalidParameter", InvalidArgumentException.class);
    map.put("InvalidParameterValue", InvalidArgumentException.class);
    map.put("InvalidMessage", InvalidArgumentException.class);
    map.put("InvalidMessageAttributes", InvalidArgumentException.class);
    map.put("MessageTooLong", InvalidArgumentException.class);
    map.put("InvalidTopicArn", InvalidArgumentException.class);
    map.put("InvalidTargetArn", InvalidArgumentException.class);
    map.put("InvalidPhoneNumber", InvalidArgumentException.class);
    map.put("InvalidSubject", InvalidArgumentException.class);
    
    // PermissionDenied errors
    map.put("AuthorizationError", UnAuthorizedException.class);
    map.put("KMSAccessDenied", UnAuthorizedException.class);
    map.put("KMSInvalidState", UnAuthorizedException.class);
    
    // NotFound errors
    map.put("NotFound", ResourceNotFoundException.class);
    map.put("TopicNotFound", ResourceNotFoundException.class);
    map.put("EndpointNotFound", ResourceNotFoundException.class);
    map.put("SubscriptionNotFound", ResourceNotFoundException.class);
    
    // ResourceExhausted errors
    map.put("Throttled", ResourceExhaustedException.class);
    map.put("TooManyEntriesInBatchRequest", ResourceExhaustedException.class);
    
    // PlatformApplication errors
    map.put("PlatformApplicationDisabled", InvalidArgumentException.class);
    map.put("InvalidSecurity", UnAuthorizedException.class);

    ERROR_MAPPING = Collections.unmodifiableMap(map);
  }

  static Class<? extends SubstrateSdkException> getException(String errorCode) {
    return ERROR_MAPPING.getOrDefault(errorCode, UnknownException.class);
  }
}