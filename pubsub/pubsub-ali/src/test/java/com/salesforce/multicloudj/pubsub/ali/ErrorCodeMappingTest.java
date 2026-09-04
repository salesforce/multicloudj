package com.salesforce.multicloudj.pubsub.ali;

import static com.salesforce.multicloudj.pubsub.ali.ErrorCodeMapping.getException;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import org.junit.jupiter.api.Test;

public class ErrorCodeMappingTest {

  @Test
  void mapsAuthFailures() {
    assertEquals(UnAuthorizedException.class, getException("AccessDenied"));
    assertEquals(UnAuthorizedException.class, getException("InvalidAccessKeyId"));
    assertEquals(UnAuthorizedException.class, getException("SignatureDoesNotMatch"));
  }

  @Test
  void mapsRequestAndHeaderValidation() {
    assertEquals(InvalidArgumentException.class, getException("InvalidAuthorizationHeader"));
    assertEquals(InvalidArgumentException.class, getException("MissingAuthorizationHeader"));
    assertEquals(InvalidArgumentException.class, getException("InvalidDateHeader"));
    assertEquals(InvalidArgumentException.class, getException("MissingDateHeader"));
    assertEquals(InvalidArgumentException.class, getException("TimeExpired"));
    assertEquals(InvalidArgumentException.class, getException("InvalidDegist"));
    assertEquals(InvalidArgumentException.class, getException("InvalidRequestURL"));
    assertEquals(InvalidArgumentException.class, getException("InvalidQueryString"));
    assertEquals(InvalidArgumentException.class, getException("MalformedXML"));
    assertEquals(InvalidArgumentException.class, getException("InvalidArgument"));
  }

  @Test
  void mapsNamingValidation() {
    assertEquals(InvalidArgumentException.class, getException("InvalidQueueName"));
    assertEquals(InvalidArgumentException.class, getException("QueueNameLengthError"));
    assertEquals(InvalidArgumentException.class, getException("TopicNameInvalid"));
    assertEquals(InvalidArgumentException.class, getException("TopicNameLengthError"));
    assertEquals(InvalidArgumentException.class, getException("SubscriptionNameInvalid"));
    assertEquals(InvalidArgumentException.class, getException("SubscriptionNameLengthError"));
    assertEquals(InvalidArgumentException.class, getException("EndpointInvalid"));
  }

  @Test
  void mapsMessageAndReceiptHandleValidation() {
    assertEquals(InvalidArgumentException.class, getException("MissingReceiptHandle"));
    assertEquals(InvalidArgumentException.class, getException("MissingVisibilityTimeout"));
    assertEquals(InvalidArgumentException.class, getException("ReceiptHandleError"));
  }

  @Test
  void mapsMissingResources() {
    assertEquals(ResourceNotFoundException.class, getException("QueueNotExist"));
    assertEquals(ResourceNotFoundException.class, getException("TopicNotExist"));
    assertEquals(ResourceNotFoundException.class, getException("SubscriptionNotExist"));
    assertEquals(ResourceNotFoundException.class, getException("MessageNotExist"));
  }

  @Test
  void mapsResourceCreationConflicts() {
    assertEquals(ResourceAlreadyExistsException.class, getException("QueueAlreadyExist"));
    assertEquals(ResourceAlreadyExistsException.class, getException("TopicAlreadyExist"));
    assertEquals(ResourceAlreadyExistsException.class, getException("SubscriptionAlreadyExist"));
  }

  @Test
  void mapsThrottling() {
    assertEquals(ResourceExhaustedException.class, getException("QpsLimitExceeded"));
    assertEquals(ResourceExhaustedException.class, getException("TooManyRequests"));
  }

  @Test
  void mapsServerErrors() {
    assertEquals(UnknownException.class, getException("InternalError"));
    assertEquals(UnknownException.class, getException("InternalServerError"));
  }

  @Test
  void mapsUnknownAndUnmappedCodes() {
    assertEquals(UnknownException.class, getException("SomethingUnmapped"));
    assertEquals(UnknownException.class, getException(null));
    // Guard against re-introducing guessed codes that are not real MNS wire values.
    assertEquals(UnknownException.class, getException("InvalidTopicName"));
    assertEquals(UnknownException.class, getException("InvalidSecurityToken"));
  }
}
