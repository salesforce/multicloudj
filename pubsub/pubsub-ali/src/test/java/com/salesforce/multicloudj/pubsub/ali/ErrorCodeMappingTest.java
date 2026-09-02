package com.salesforce.multicloudj.pubsub.ali;

import static com.salesforce.multicloudj.pubsub.ali.ErrorCodeMapping.getException;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import org.junit.jupiter.api.Test;

public class ErrorCodeMappingTest {

  @Test
  void mapsMissingResourceCodes() {
    assertEquals(ResourceNotFoundException.class, getException("QueueNotExist"));
    assertEquals(ResourceNotFoundException.class, getException("TopicNotExist"));
    assertEquals(ResourceNotFoundException.class, getException("SubscriptionNotExist"));
    assertEquals(ResourceNotFoundException.class, getException("MessageNotExist"));
  }

  @Test
  void mapsResourceCreationConflicts() {
    assertEquals(ResourceAlreadyExistsException.class, getException("QueueAlreadyExist"));
    assertEquals(ResourceAlreadyExistsException.class, getException("TopicAlreadyExist"));
  }

  @Test
  void mapsAuthFailures() {
    assertEquals(UnAuthorizedException.class, getException("AccessDenied"));
    assertEquals(UnAuthorizedException.class, getException("SignatureDoesNotMatch"));
    assertEquals(UnAuthorizedException.class, getException("InvalidAccessKeyId"));
    assertEquals(UnAuthorizedException.class, getException("InvalidSecurityToken"));
  }

  @Test
  void mapsValidationAndServerCodes() {
    assertEquals(InvalidArgumentException.class, getException("InvalidArgument"));
    assertEquals(InvalidArgumentException.class, getException("MissingArgument"));
    assertEquals(InvalidArgumentException.class, getException("ReceiptHandleError"));
    assertEquals(UnknownException.class, getException("InternalError"));
  }

  @Test
  void unknownAndNullCodesDefaultToUnknown() {
    assertEquals(UnknownException.class, getException("SomethingUnmapped"));
    assertEquals(UnknownException.class, getException(null));
  }
}
