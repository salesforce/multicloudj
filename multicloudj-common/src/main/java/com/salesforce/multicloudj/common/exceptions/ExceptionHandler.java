package com.salesforce.multicloudj.common.exceptions;

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
        if (exceptionClass == null) {
            throw new UnknownException(t);
        } else if (ResourceAlreadyExistsException.class.isAssignableFrom(exceptionClass)) {
            throw new ResourceAlreadyExistsException(t);
        } else if (ResourceNotFoundException.class.isAssignableFrom(exceptionClass)) {
            throw new ResourceNotFoundException(t);
        } else if (ResourceConflictException.class.isAssignableFrom(exceptionClass)) {
            throw new ResourceConflictException(t);
        } else if (UnAuthorizedException.class.isAssignableFrom(exceptionClass)) {
            throw new UnAuthorizedException(t);
        } else if (ResourceExhaustedException.class.isAssignableFrom(exceptionClass)) {
            throw new ResourceExhaustedException(t);
        } else if (InvalidArgumentException.class.isAssignableFrom(exceptionClass)) {
            throw new InvalidArgumentException(t);
        } else if (FailedPreconditionException.class.isAssignableFrom(exceptionClass)) {
            throw new FailedPreconditionException(t);
        } else if (UnknownException.class.isAssignableFrom(exceptionClass)) {
            throw new UnknownException(t);
        } else if (SubstrateSdkException.class.isAssignableFrom(exceptionClass)) {
            throw (SubstrateSdkException) t;
        } else {
            throw new UnknownException(t);
        }
    }
}