package com.salesforce.multicloudj.common.service;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;

/**
 * Base interface for services backed by substrate-specific implementations.
 * This provides a minimal set of methods such as exception translation.
 */
public interface SdkService {

    /**
     * Retrieves the unique identifier for this service implementation.
     *
     * @return A String representing the provider's ID such as aws.
     */
    String getProviderId();

    /**
     * Maps a given Throwable from the provider implementation
     * to a specific SubstrateSdkException. This is used for exception handling
     * abstraction.
     *
     * @param t The Throwable to be mapped.
     * @return The Class of the corresponding SubstrateSdkException.
     */
    Class<? extends SubstrateSdkException> getException(Throwable t);

}
