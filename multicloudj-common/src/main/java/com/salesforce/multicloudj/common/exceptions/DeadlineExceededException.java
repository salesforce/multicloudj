package com.salesforce.multicloudj.common.exceptions;

public class DeadlineExceededException extends SubstrateSdkException {

    public DeadlineExceededException() {
        super();
    }

    public DeadlineExceededException(String message, Throwable cause) {
        super(message, cause);
    }

    public DeadlineExceededException(String message) {
        super(message);
    }

    public DeadlineExceededException(Throwable cause) {
        super(cause);
    }
}
