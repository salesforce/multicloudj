package com.salesforce.multicloudj.common.exceptions;

public class UnAuthorizedException extends SubstrateSdkException {

    public UnAuthorizedException() {
        super();
    }

    public UnAuthorizedException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnAuthorizedException(String message) {
        super(message);
    }

    public UnAuthorizedException(Throwable cause) {
        super(cause);
    }
}
