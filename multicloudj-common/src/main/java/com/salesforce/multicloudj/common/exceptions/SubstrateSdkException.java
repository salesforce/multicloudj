package com.salesforce.multicloudj.common.exceptions;

public class SubstrateSdkException extends RuntimeException {

    public SubstrateSdkException() {
        super();
    }

    public SubstrateSdkException(String message, Throwable cause) {
        super(message, cause);
    }

    public SubstrateSdkException(String message) {
        super(message);
    }

    public SubstrateSdkException(Throwable cause) {
        super(cause);
    }
}
