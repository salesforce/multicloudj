package com.salesforce.multicloudj.kms.ali;

import com.aliyun.tea.TeaException;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import java.util.List;

/** Classifies Alibaba Cloud KMS error codes into MultiCloudJ exception types. */
final class ErrorCodeMapping {

  private enum Match {
    EQUALS,
    PREFIX,
    CONTAINS
  }

  private record Rule(Match match, String pattern, Class<? extends SubstrateSdkException> type) {
    boolean matches(String code) {
      return switch (match) {
        case EQUALS -> code.equals(pattern);
        case PREFIX -> code.startsWith(pattern);
        case CONTAINS -> code.contains(pattern);
      };
    }
  }

  /** Evaluated in order; the first matching rule wins. */
  private static final List<Rule> RULES =
      List.of(
          new Rule(Match.EQUALS, "Forbidden.KeyNotFound", ResourceNotFoundException.class),
          new Rule(Match.EQUALS, "Forbidden.ResourceNotFound", ResourceNotFoundException.class),
          new Rule(Match.PREFIX, "EntityNotExist", ResourceNotFoundException.class),
          new Rule(Match.PREFIX, "Throttling", ResourceExhaustedException.class),
          new Rule(Match.CONTAINS, "AlreadyExists", ResourceAlreadyExistsException.class),
          new Rule(Match.EQUALS, "Rejected.Disabled", FailedPreconditionException.class),
          new Rule(Match.EQUALS, "Rejected.PendingDeletion", FailedPreconditionException.class),
          new Rule(Match.EQUALS, "Rejected.UnexpectedState", FailedPreconditionException.class),
          new Rule(Match.PREFIX, "Forbidden.KeyNotEnabled", FailedPreconditionException.class),
          new Rule(Match.PREFIX, "InvalidParameter", InvalidArgumentException.class),
          new Rule(Match.PREFIX, "MissingParameter", InvalidArgumentException.class),
          new Rule(Match.EQUALS, "Rejected.ValidationFailed", InvalidArgumentException.class),
          new Rule(Match.CONTAINS, "InvalidCiphertext", InvalidArgumentException.class),
          new Rule(Match.CONTAINS, "EncryptionContext", InvalidArgumentException.class),
          new Rule(Match.EQUALS, "Forbidden.NoPermission", UnAuthorizedException.class),
          new Rule(Match.PREFIX, "Forbidden.RAM", UnAuthorizedException.class),
          new Rule(Match.EQUALS, "NoPermission", UnAuthorizedException.class),
          new Rule(Match.PREFIX, "InvalidAccessKeyId", UnAuthorizedException.class),
          new Rule(Match.EQUALS, "SignatureDoesNotMatch", UnAuthorizedException.class));

  private static final int MAX_CAUSE_DEPTH = 8;

  private ErrorCodeMapping() {}

  /**
   * Returns the MultiCloudJ exception type for a failure, looking through wrapper exceptions for
   * the underlying {@link TeaException}.
   */
  static Class<? extends SubstrateSdkException> getException(Throwable throwable) {
    TeaException tea = findTeaException(throwable);
    if (tea == null || tea.getCode() == null) {
      return UnknownException.class;
    }
    for (Rule rule : RULES) {
      if (rule.matches(tea.getCode())) {
        return rule.type();
      }
    }
    return UnknownException.class;
  }

  /** Finds the first {@link TeaException} in the cause chain, or null. */
  static TeaException findTeaException(Throwable throwable) {
    Throwable current = throwable;
    for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
      if (current instanceof TeaException) {
        return (TeaException) current;
      }
      current = current.getCause();
    }
    return null;
  }
}
