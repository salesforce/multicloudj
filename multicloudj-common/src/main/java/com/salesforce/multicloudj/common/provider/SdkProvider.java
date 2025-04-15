package com.salesforce.multicloudj.common.provider;

import com.salesforce.multicloudj.common.service.SdkService;

import java.util.Properties;

public interface SdkProvider<T extends SdkService> {

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
    Builder<T> builder();

    /**
     * Interface for building Provider instances which is implemented by abstract class for service.
     */
    interface Builder<T extends SdkService> {

        /**
         * Sets the provider ID for the Provider being built.
         *
         * @param providerId A String representing the provider's ID.
         * @return This Builder instance for method chaining.
         */
        Builder<T> providerId(String providerId);

        /**
         * Returns the providerId set on this builder.
         * @return the id of the provider this builder is from.
         */
        String getProviderId();

        /**
         * Catch-all for substrate-specific configuration parameters not reflected in the Builder api.
         * @param properties the properties to pass to the substrate specific implementation
         * @return This Builder instance for method chaining.
         */
        Builder<T> withProperties(Properties properties);

        /**
         * Returns the custom properties set on this builder.
         * @return the properties previously set on this builder.
         */
        Properties getProperties();

        /**
         * Performs the logic to build the target service instance.
         * @return the service configured through this builder.
         */
        T build();
    }
}
