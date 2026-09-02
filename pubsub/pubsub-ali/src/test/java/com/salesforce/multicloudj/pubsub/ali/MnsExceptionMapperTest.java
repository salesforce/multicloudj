package com.salesforce.multicloudj.pubsub.ali;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.aliyun.mns.common.ClientException;
import com.aliyun.mns.common.ServiceException;
import com.aliyun.mns.common.ServiceHandlingRequiredException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import org.junit.jupiter.api.Test;

public class MnsExceptionMapperTest {

  @Test
  void serviceExceptionMapsByErrorCode() {
    ServiceException auth = mock(ServiceException.class);
    when(auth.getErrorCode()).thenReturn("AccessDenied");
    assertInstanceOf(UnAuthorizedException.class, MnsExceptionMapper.map(auth));

    ServiceException notFound = mock(ServiceException.class);
    when(notFound.getErrorCode()).thenReturn("QueueNotExist");
    assertInstanceOf(ResourceNotFoundException.class, MnsExceptionMapper.map(notFound));
  }

  @Test
  void serviceHandlingRequiredExceptionMapsByErrorCode() {
    ServiceHandlingRequiredException e = mock(ServiceHandlingRequiredException.class);
    when(e.getErrorCode()).thenReturn("MessageNotExist");
    assertInstanceOf(ResourceNotFoundException.class, MnsExceptionMapper.map(e));
  }

  @Test
  void clientExceptionMapsToUnknown() {
    ClientException e = mock(ClientException.class);
    assertInstanceOf(UnknownException.class, MnsExceptionMapper.map(e));
  }

  @Test
  void illegalArgumentMapsToInvalidArgument() {
    assertInstanceOf(
        InvalidArgumentException.class, MnsExceptionMapper.map(new IllegalArgumentException("x")));
  }

  @Test
  void unknownThrowableMapsToUnknown() {
    assertInstanceOf(UnknownException.class, MnsExceptionMapper.map(new RuntimeException("x")));
  }

  @Test
  void substrateSdkExceptionIsPassedThrough() {
    SubstrateSdkException original = new InvalidArgumentException("boom");
    assertSame(original, MnsExceptionMapper.map(original));
  }
}
