package com.salesforce.multicloudj.common.exceptions;

public class UnSupportedOperationException extends SubstrateSdkException {

    public UnSupportedOperationException() {
        super();
    }

    public UnSupportedOperationException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnSupportedOperationException(String message) {
        super(message);
    }

    public UnSupportedOperationException(Throwable cause) {
        super(cause);
    }
}
