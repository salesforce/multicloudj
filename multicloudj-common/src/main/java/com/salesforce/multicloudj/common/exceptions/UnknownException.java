package com.salesforce.multicloudj.common.exceptions;

public class UnknownException extends SubstrateSdkException {

    public UnknownException() {
        super();
    }

    public UnknownException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnknownException(String message) {
        super(message);
    }

    public UnknownException(Throwable cause) {
        super(cause);
    }
}
