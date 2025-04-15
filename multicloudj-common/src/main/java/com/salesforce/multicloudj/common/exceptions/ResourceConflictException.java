package com.salesforce.multicloudj.common.exceptions;

public class ResourceConflictException extends SubstrateSdkException {

    public ResourceConflictException() {
        super();
    }

    public ResourceConflictException(String message, Throwable cause) {
        super(message, cause);
    }

    public ResourceConflictException(String message) {
        super(message);
    }

    public ResourceConflictException(Throwable cause) {
        super(cause);
    }
}
