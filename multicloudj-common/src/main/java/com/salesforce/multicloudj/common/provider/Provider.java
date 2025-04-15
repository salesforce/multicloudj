package com.salesforce.multicloudj.common.provider;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;

/**
 * Interface representing a provider in the SDK.
 * This provider interface should be implemented by every single
 * abstract class for the service which gets extended by
 * provider implementations. See this abstract class for Sts implementing this
 * provider as example
 * {@link com.salesforce.multicloudj.sts.driver.AbstractSts}.
 */
public interface Provider {

    /**
     * Retrieves the unique identifier for this provider.
     *
     * @return A String representing the provider's ID such as aws.
     */
    String getProviderId();

    /**
     * Creates and returns a new Builder instance for this provider.
     *
     * @return A Builder instance for constructing this provider.
     */
    Builder builder();

    /**
     * Maps a given Throwable from the provider implementation
     * to a specific SubstrateSdkException. This is used for exception handling
     * abstraction.
     *
     * @param t The Throwable to be mapped.
     * @return The Class of the corresponding SubstrateSdkException.
     */
    Class<? extends SubstrateSdkException> getException(Throwable t);

    /**
     * Interface for building Provider instances which is implemented by abstract class for service.
     */
    interface Builder {

        /**
         * Sets the provider ID for the Provider being built.
         *
         * @param providerId A String representing the provider's ID.
         * @return This Builder instance for method chaining.
         */
        Builder providerId(String providerId);
    }
}