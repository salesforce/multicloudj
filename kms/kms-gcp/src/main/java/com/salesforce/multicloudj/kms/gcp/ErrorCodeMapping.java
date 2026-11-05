package com.salesforce.multicloudj.kms.gcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.HttpResponseException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.salesforce.multicloudj.common.exceptions.DeadlineExceededException;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import java.util.EnumMap;
import java.util.Map;

/** Classifies Cloud KMS status codes into cloud-agnostic exception types. */
final class ErrorCodeMapping {

  private static final Map<StatusCode.Code, Class<? extends SubstrateSdkException>> MAPPING =
      new EnumMap<>(StatusCode.Code.class);

  static {
    MAPPING.put(StatusCode.Code.NOT_FOUND, ResourceNotFoundException.class);
    MAPPING.put(StatusCode.Code.ALREADY_EXISTS, ResourceAlreadyExistsException.class);
    MAPPING.put(StatusCode.Code.FAILED_PRECONDITION, FailedPreconditionException.class);
    MAPPING.put(StatusCode.Code.INVALID_ARGUMENT, InvalidArgumentException.class);
    MAPPING.put(StatusCode.Code.OUT_OF_RANGE, InvalidArgumentException.class);
    MAPPING.put(StatusCode.Code.PERMISSION_DENIED, UnAuthorizedException.class);
    MAPPING.put(StatusCode.Code.UNAUTHENTICATED, UnAuthorizedException.class);
    MAPPING.put(StatusCode.Code.RESOURCE_EXHAUSTED, ResourceExhaustedException.class);
    MAPPING.put(StatusCode.Code.DEADLINE_EXCEEDED, DeadlineExceededException.class);
    MAPPING.put(StatusCode.Code.UNIMPLEMENTED, UnSupportedOperationException.class);
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ErrorCodeMapping() {}

  static Class<? extends SubstrateSdkException> getException(ApiException exception) {
    StatusCode.Code bodyCode = canonicalCodeFromBody(exception);
    if (bodyCode != null) {
      return getException(bodyCode);
    }
    return exception.getStatusCode() == null
        ? UnknownException.class
        : getException(exception.getStatusCode().getCode());
  }

  static Class<? extends SubstrateSdkException> getException(StatusCode.Code code) {
    if (code == null) {
      return UnknownException.class;
    }
    return MAPPING.getOrDefault(code, UnknownException.class);
  }

  /**
   * Over HTTP/JSON, gax derives the status code from the HTTP status alone, so distinct canonical
   * codes that share an HTTP status (for example FAILED_PRECONDITION and INVALID_ARGUMENT, both
   * 400) collapse into one. The response body carries the canonical code in {@code error.status}.
   */
  private static StatusCode.Code canonicalCodeFromBody(ApiException exception) {
    for (Throwable cause = exception.getCause(); cause != null; cause = cause.getCause()) {
      if (cause instanceof HttpResponseException) {
        String content = ((HttpResponseException) cause).getContent();
        if (content == null || content.isBlank()) {
          return null;
        }
        try {
          JsonNode status = MAPPER.readTree(content).path("error").path("status");
          return status.isTextual() ? StatusCode.Code.valueOf(status.asText()) : null;
        } catch (Exception e) {
          return null;
        }
      }
    }
    return null;
  }
}
