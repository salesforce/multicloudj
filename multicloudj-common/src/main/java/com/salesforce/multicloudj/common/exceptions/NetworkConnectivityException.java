package com.salesforce.multicloudj.common.exceptions;

public class NetworkConnectivityException extends SubstrateSdkException {

    public NetworkConnectivityException() {
        super();
    }

    public NetworkConnectivityException(String message, Throwable cause) {
        super(message, cause);
    }

    public NetworkConnectivityException(String message) {
        super(message);
    }

    public NetworkConnectivityException(Throwable cause) {
        super(cause);
    }
}
