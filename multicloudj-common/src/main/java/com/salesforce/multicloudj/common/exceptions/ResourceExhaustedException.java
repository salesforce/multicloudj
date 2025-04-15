package com.salesforce.multicloudj.common.exceptions;

public class ResourceExhaustedException extends SubstrateSdkException {

    public ResourceExhaustedException() {
        super();
    }

    public ResourceExhaustedException(String message, Throwable cause) {
        super(message, cause);
    }

    public ResourceExhaustedException(String message) {
        super(message);
    }

    public ResourceExhaustedException(Throwable cause) {
        super(cause);
    }
}
