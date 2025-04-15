package com.salesforce.multicloudj.common.exceptions;

public class InvalidArgumentException extends SubstrateSdkException {

    public InvalidArgumentException() {
        super();
    }

    public InvalidArgumentException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidArgumentException(String message) {
        super(message);
    }

    public InvalidArgumentException(Throwable cause) {
        super(cause);
    }
}
