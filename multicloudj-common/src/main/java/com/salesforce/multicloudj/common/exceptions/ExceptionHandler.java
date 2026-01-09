package com.salesforce.multicloudj.common.exceptions;

import java.lang.reflect.Method;

/**
 * Utility class for handling and propagating exceptions in the Substrate SDK.
 */
public class ExceptionHandler {

    /**
     * Handles the given exception t which are the provider specific exceptions,
     * wraps and propagates it in a specific SubstrateSdkException. This serves as an abstraction layer
     * for the exception handling. So our end users can get to deal with substrate agnostic exceptions
     * and also can get the provider specific exceptions if they are interested in going for detail.
     * <p>
     * This method examines the provided exception class and throws a corresponding
     * SubstrateSdkException based on the type. If no specific match is found, it throws
     * an UnknownException.
     *
     * @param exceptionClass The class of the exception to be handled, this is SDK defined exception for abstraction.
     * @param t The original Throwable from provider that was caught.
     * @throws SubstrateSdkException A specific subclass of SubstrateSdkException based on the input,
     *         or UnknownException if no specific match is found.
     */
    public static void handleAndPropagate(Class<? extends SubstrateSdkException> exceptionClass, Throwable t)
            throws SubstrateSdkException {
        String detailedMessage = buildDetailedMessage(t);
        
        if (exceptionClass == null) {
            throw new UnknownException(detailedMessage != null ? detailedMessage : null, t);
        } else if (ResourceAlreadyExistsException.class.isAssignableFrom(exceptionClass)) {
            throw new ResourceAlreadyExistsException(detailedMessage != null ? detailedMessage : null, t);
        } else if (ResourceNotFoundException.class.isAssignableFrom(exceptionClass)) {
            throw new ResourceNotFoundException(detailedMessage != null ? detailedMessage : null, t);
        } else if (ResourceConflictException.class.isAssignableFrom(exceptionClass)) {
            throw new ResourceConflictException(detailedMessage != null ? detailedMessage : null, t);
        } else if (UnAuthorizedException.class.isAssignableFrom(exceptionClass)) {
            throw new UnAuthorizedException(detailedMessage != null ? detailedMessage : null, t);
        } else if (ResourceExhaustedException.class.isAssignableFrom(exceptionClass)) {
            throw new ResourceExhaustedException(detailedMessage != null ? detailedMessage : null, t);
        } else if (InvalidArgumentException.class.isAssignableFrom(exceptionClass)) {
            throw new InvalidArgumentException(detailedMessage != null ? detailedMessage : null, t);
        } else if (FailedPreconditionException.class.isAssignableFrom(exceptionClass)) {
            throw new FailedPreconditionException(detailedMessage != null ? detailedMessage : null, t);
        } else if (TransactionFailedException.class.isAssignableFrom(exceptionClass)) {
            throw new TransactionFailedException(detailedMessage != null ? detailedMessage : null, t);
        } else if (UnknownException.class.isAssignableFrom(exceptionClass)) {
            throw new UnknownException(detailedMessage != null ? detailedMessage : null, t);
        } else if (SubstrateSdkException.class.isAssignableFrom(exceptionClass)) {
            throw (SubstrateSdkException) t;
        } else {
            throw new UnknownException(detailedMessage != null ? detailedMessage : null, t);
        }
    }

    /**
     * Builds a detailed error message from the throwable, extracting additional information
     * from provider-specific exceptions (AWS, GCP, Ali, etc.).
     * <p>
     * This method uses reflection to extract details from cloud provider exceptions without
     * requiring dependencies on provider SDKs in the common module.
     *
     * @param t The throwable to extract details from
     * @return A detailed error message, or null if no additional details can be extracted
     */
    private static String buildDetailedMessage(Throwable t) {
        if (t == null) {
            return null;
        }

        // Try AWS Service Exception first
        String message = extractAwsExceptionDetails(t);
        if (message != null) {
            return message;
        }

        // Try GCP ApiException
        message = extractGcpExceptionDetails(t);
        if (message != null) {
            return message;
        }

        // Try Ali ClientException
        message = extractAliExceptionDetails(t);
        if (message != null) {
            return message;
        }

        return null;
    }

    /**
     * Extracts detailed information from an AWS Service Exception using reflection.
     *
     * @param exception The exception to extract details from
     * @return A detailed error message string, or null if not an AWS exception
     */
    private static String extractAwsExceptionDetails(Throwable exception) {
        try {
            Class<?> awsServiceExceptionClass = Class.forName("software.amazon.awssdk.awscore.exception.AwsServiceException");
            if (!awsServiceExceptionClass.isInstance(exception)) {
                return null;
            }

            // Get awsErrorDetails() method
            Method awsErrorDetailsMethod = awsServiceExceptionClass.getMethod("awsErrorDetails");
            Object awsErrorDetails = awsErrorDetailsMethod.invoke(exception);

            if (awsErrorDetails == null) {
                return null;
            }

            Class<?> awsErrorDetailsClass = awsErrorDetails.getClass();
            StringBuilder message = new StringBuilder();

            // Extract error code
            try {
                Method errorCodeMethod = awsErrorDetailsClass.getMethod("errorCode");
                String errorCode = (String) errorCodeMethod.invoke(awsErrorDetails);
                if (errorCode != null && !errorCode.isEmpty()) {
                    message.append("Error Code: ").append(errorCode);
                }
            } catch (ReflectiveOperationException e) {
                // Ignore if method doesn't exist or fails
            }

            // Extract error message
            try {
                Method errorMessageMethod = awsErrorDetailsClass.getMethod("errorMessage");
                String errorMessage = (String) errorMessageMethod.invoke(awsErrorDetails);
                if (errorMessage != null && !errorMessage.isEmpty()) {
                    if (message.length() > 0) {
                        message.append(", ");
                    }
                    message.append("Error Message: ").append(errorMessage);
                }
            } catch (ReflectiveOperationException e) {
                // Ignore if method doesn't exist or fails
            }

            // Extract service name
            try {
                Method serviceNameMethod = awsServiceExceptionClass.getMethod("serviceName");
                String serviceName = (String) serviceNameMethod.invoke(exception);
                if (serviceName != null && !serviceName.isEmpty()) {
                    if (message.length() > 0) {
                        message.append(", ");
                    }
                    message.append("Service: ").append(serviceName);
                }
            } catch (ReflectiveOperationException e) {
                // Ignore if method doesn't exist or fails
            }

            // Extract status code
            try {
                Method statusCodeMethod = awsServiceExceptionClass.getMethod("statusCode");
                Integer statusCode = (Integer) statusCodeMethod.invoke(exception);
                if (statusCode != null) {
                    if (message.length() > 0) {
                        message.append(", ");
                    }
                    message.append("Status Code: ").append(statusCode);
                }
            } catch (ReflectiveOperationException e) {
                // Ignore if method doesn't exist or fails
            }

            // Extract request ID
            try {
                Method requestIdMethod = awsServiceExceptionClass.getMethod("requestId");
                String requestId = (String) requestIdMethod.invoke(exception);
                if (requestId != null && !requestId.isEmpty()) {
                    if (message.length() > 0) {
                        message.append(", ");
                    }
                    message.append("Request ID: ").append(requestId);
                }
            } catch (ReflectiveOperationException e) {
                // Ignore if method doesn't exist or fails
            }

            return message.length() > 0 ? message.toString() : null;
        } catch (SecurityException | ReflectiveOperationException e) {
            // AWS SDK not available or reflection failed
            return null;
        }
    }

    /**
     * Extracts detailed information from a GCP ApiException using reflection.
     *
     * @param exception The exception to extract details from
     * @return A detailed error message string, or null if not a GCP exception
     */
    private static String extractGcpExceptionDetails(Throwable exception) {
        try {
            Class<?> apiExceptionClass = Class.forName("com.google.api.gax.rpc.ApiException");
            if (!apiExceptionClass.isInstance(exception)) {
                return null;
            }

            StringBuilder message = new StringBuilder();

            // Extract status code
            try {
                Method getStatusCodeMethod = apiExceptionClass.getMethod("getStatusCode");
                Object statusCode = getStatusCodeMethod.invoke(exception);
                if (statusCode != null) {
                    // Get the code from StatusCode
                    Method getCodeMethod = statusCode.getClass().getMethod("getCode");
                    Object code = getCodeMethod.invoke(statusCode);
                    if (code != null) {
                        message.append("Status Code: ").append(code);
                    }
                }
            } catch (ReflectiveOperationException e) {
                // Ignore if method doesn't exist or fails
            }

            // Extract error message
            String errorMessage = exception.getMessage();
            if (errorMessage != null && !errorMessage.isEmpty()) {
                if (message.length() > 0) {
                    message.append(", ");
                }
                message.append("Error Message: ").append(errorMessage);
            }

            // Extract isRetryable
            try {
                Method isRetryableMethod = apiExceptionClass.getMethod("isRetryable");
                Boolean isRetryable = (Boolean) isRetryableMethod.invoke(exception);
                if (isRetryable != null && isRetryable) {
                    if (message.length() > 0) {
                        message.append(", ");
                    }
                    message.append("Retryable: true");
                }
            } catch (ReflectiveOperationException e) {
                // Ignore if method doesn't exist or fails
            }

            return message.length() > 0 ? message.toString() : null;
        } catch (SecurityException | ReflectiveOperationException e) {
            // GCP SDK not available or reflection failed
            return null;
        }
    }

    /**
     * Extracts detailed information from an Ali Cloud ClientException using reflection.
     *
     * @param exception The exception to extract details from
     * @return A detailed error message string, or null if not an Ali exception
     */
    private static String extractAliExceptionDetails(Throwable exception) {
        try {
            Class<?> clientExceptionClass = Class.forName("com.aliyuncs.exceptions.ClientException");
            Throwable targetException = exception;
            
            // Check if the exception itself is a ClientException, or if it's wrapped
            if (!clientExceptionClass.isInstance(exception)) {
                if (exception.getCause() != null && clientExceptionClass.isInstance(exception.getCause())) {
                    targetException = exception.getCause();
                } else {
                    return null;
                }
            }

            StringBuilder message = new StringBuilder();

            // Extract error code
            try {
                Method getErrCodeMethod = clientExceptionClass.getMethod("getErrCode");
                String errorCode = (String) getErrCodeMethod.invoke(targetException);
                if (errorCode != null && !errorCode.isEmpty()) {
                    message.append("Error Code: ").append(errorCode);
                }
            } catch (ReflectiveOperationException e) {
                // Ignore if method doesn't exist or fails
            }

            // Extract error message
            String errorMessage = targetException.getMessage();
            if (errorMessage != null && !errorMessage.isEmpty()) {
                if (message.length() > 0) {
                    message.append(", ");
                }
                message.append("Error Message: ").append(errorMessage);
            }

            // Extract request ID
            try {
                Method getRequestIdMethod = clientExceptionClass.getMethod("getRequestId");
                String requestId = (String) getRequestIdMethod.invoke(targetException);
                if (requestId != null && !requestId.isEmpty()) {
                    if (message.length() > 0) {
                        message.append(", ");
                    }
                    message.append("Request ID: ").append(requestId);
                }
            } catch (ReflectiveOperationException e) {
                // Ignore if method doesn't exist or fails
            }

            return message.length() > 0 ? message.toString() : null;
        } catch (SecurityException | ReflectiveOperationException e) {
            // Ali SDK not available or reflection failed
            return null;
        }
    }

}