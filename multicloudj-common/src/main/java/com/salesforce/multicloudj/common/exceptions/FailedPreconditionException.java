package com.salesforce.multicloudj.common.exceptions;

public class FailedPreconditionException extends SubstrateSdkException {

    public FailedPreconditionException() {
        super();
    }

    public FailedPreconditionException(String message, Throwable cause) {
        super(message, cause);
    }

    public FailedPreconditionException(String message) {
        super(message);
    }

    public FailedPreconditionException(Throwable cause) {
        super(cause);
    }
}
