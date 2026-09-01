package com.salesforce.multicloudj.pubsub.ali;

import static com.salesforce.multicloudj.pubsub.ali.ErrorCodeMapping.getException;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import org.junit.jupiter.api.Test;

public class ErrorCodeMappingTest {

  @Test
  void mapsKnownCodes() {
    assertEquals(ResourceNotFoundException.class, getException("QueueNotExist"));
    assertEquals(ResourceNotFoundException.class, getException("TopicNotExist"));
    assertEquals(ResourceNotFoundException.class, getException("SubscriptionNotExist"));
    assertEquals(ResourceNotFoundException.class, getException("MessageNotExist"));
    assertEquals(UnAuthorizedException.class, getException("AccessDenied"));
    assertEquals(UnAuthorizedException.class, getException("SignatureDoesNotMatch"));
    assertEquals(InvalidArgumentException.class, getException("InvalidAccessKeyId"));
    assertEquals(InvalidArgumentException.class, getException("InvalidArgument"));
    assertEquals(InvalidArgumentException.class, getException("ReceiptHandleError"));
    assertEquals(UnknownException.class, getException("InternalError"));
  }

  @Test
  void unknownAndNullCodesDefaultToUnknown() {
    assertEquals(UnknownException.class, getException("SomethingUnmapped"));
    assertEquals(UnknownException.class, getException(null));
  }
}
