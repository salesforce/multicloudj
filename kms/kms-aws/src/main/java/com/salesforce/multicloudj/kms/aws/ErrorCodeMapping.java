package com.salesforce.multicloudj.kms.aws;

import com.salesforce.multicloudj.common.aws.CommonErrorCodeMapping;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import java.util.HashMap;
import java.util.Map;

/** Maps AWS KMS error codes to SubstrateSdkException types. */
final class ErrorCodeMapping {

  private ErrorCodeMapping() {}

  private static final Map<String, Class<? extends SubstrateSdkException>> ERROR_MAPPING;

  static {
    Map<String, Class<? extends SubstrateSdkException>> map =
        new HashMap<>(CommonErrorCodeMapping.get());

    // https://docs.aws.amazon.com/kms/latest/APIReference/CommonErrors.html
    // https://docs.aws.amazon.com/kms/latest/APIReference/API_Operations.html
    map.put("NotFoundException", ResourceNotFoundException.class);

    map.put("KMSInvalidStateException", FailedPreconditionException.class);
    map.put("DisabledException", FailedPreconditionException.class);
    map.put("ExpiredImportTokenException", FailedPreconditionException.class);

    map.put("InvalidCiphertextException", InvalidArgumentException.class);
    map.put("InvalidImportTokenException", InvalidArgumentException.class);
    map.put("IncorrectKeyMaterialException", InvalidArgumentException.class);
    map.put("MalformedPolicyDocumentException", InvalidArgumentException.class);
    map.put("TagException", InvalidArgumentException.class);
    map.put("InvalidKeyUsageException", InvalidArgumentException.class);
    map.put("InvalidArnException", InvalidArgumentException.class);
    map.put("ValidationException", InvalidArgumentException.class);

    map.put("AlreadyExistsException", ResourceAlreadyExistsException.class);

    map.put("LimitExceededException", ResourceExhaustedException.class);
    map.put("ThrottlingException", ResourceExhaustedException.class);

    map.put("AccessDeniedException", UnAuthorizedException.class);

    ERROR_MAPPING = Map.copyOf(map);
  }

  static Class<? extends SubstrateSdkException> getException(String errorCode) {
    return ERROR_MAPPING.getOrDefault(errorCode, UnknownException.class);
  }
}
