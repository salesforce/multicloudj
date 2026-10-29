package com.salesforce.multicloudj.kms.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.aliyun.tea.TeaException;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ErrorCodeMappingTest {

  private static TeaException tea(String code) {
    TeaException exception = new TeaException();
    exception.setCode(code);
    exception.setStatusCode(400);
    return exception;
  }

  @ParameterizedTest
  @CsvSource({
    "Forbidden.KeyNotFound,ResourceNotFoundException",
    "Forbidden.ResourceNotFound,ResourceNotFoundException",
    "EntityNotExist.Key,ResourceNotFoundException",
    "EntityNotExist.Alias,ResourceNotFoundException",
    "Throttling,ResourceExhaustedException",
    "Throttling.User,ResourceExhaustedException",
    "EntityAlreadyExists.Alias,ResourceAlreadyExistsException",
    "Rejected.Disabled,FailedPreconditionException",
    "Rejected.PendingDeletion,FailedPreconditionException",
    "Rejected.UnexpectedState,FailedPreconditionException",
    "Forbidden.KeyNotEnabled,FailedPreconditionException",
    "InvalidParameter,InvalidArgumentException",
    "InvalidParameter.KeyId,InvalidArgumentException",
    "MissingParameter.Plaintext,InvalidArgumentException",
    "Rejected.ValidationFailed,InvalidArgumentException",
    "Rejected.InvalidCiphertext,InvalidArgumentException",
    "InvalidCiphertext,InvalidArgumentException",
    "Forbidden.NoPermission,UnAuthorizedException",
    "Forbidden.RAM,UnAuthorizedException",
    "NoPermission,UnAuthorizedException",
    "InternalFailure,UnknownException"
  })
  void classifiesCode(String code, String expectedType) {
    assertEquals(expectedType, ErrorCodeMapping.getException(tea(code)).getSimpleName());
  }

  @Test
  void unwrapsCheckedExceptionWrapper() {
    Throwable wrapped = new AliKms.KmsCallException(tea("Forbidden.KeyNotFound"));

    assertEquals(ResourceNotFoundException.class, ErrorCodeMapping.getException(wrapped));
  }

  @Test
  void nonTeaFailureIsUnknown() {
    assertEquals(
        UnknownException.class, ErrorCodeMapping.getException(new IllegalStateException("x")));
  }

  @Test
  void teaExceptionWithoutCodeIsUnknown() {
    assertEquals(UnknownException.class, ErrorCodeMapping.getException(new TeaException()));
  }

  @Test
  void findTeaExceptionReturnsNullWithoutTeaCause() {
    assertNull(ErrorCodeMapping.findTeaException(new RuntimeException(new Exception())));
  }

  @Test
  void mapExceptionPassesSubstrateExceptionsThrough() {
    AliKms kms = new AliKms();
    SubstrateSdkException original = new InvalidArgumentException("bad");

    assertSame(original, kms.mapException(original));
  }

  @Test
  void mapExceptionTranslatesWrappedTeaException() {
    AliKms kms = new AliKms();

    SubstrateSdkException mapped =
        kms.mapException(new AliKms.KmsCallException(tea("Throttling.Api")));

    assertEquals(ResourceExhaustedException.class, mapped.getClass());
    assertEquals(
        ResourceAlreadyExistsException.class,
        kms.mapException(tea("EntityAlreadyExists.Alias")).getClass());
    assertEquals(
        FailedPreconditionException.class, kms.mapException(tea("Rejected.Disabled")).getClass());
    assertEquals(
        UnAuthorizedException.class, kms.mapException(tea("Forbidden.NoPermission")).getClass());
  }
}
