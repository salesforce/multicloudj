package com.salesforce.multicloudj.common.provider;

import com.salesforce.multicloudj.common.service.SdkService;

/**
 * Interface representing a provider in the SDK. This provider interface should be implemented by
 * every single abstract class for the service which gets extended by provider implementations. See
 * the Abstract class for each service as examples of implementing this provider interface.
 *
 * <p>Inherits {@link SdkService#mapException(Throwable)} from {@link SdkService}.
 */
public interface Provider extends SdkService {

  /**
   * Creates and returns a new Builder instance for this provider.
   *
   * @return A Builder instance for constructing this provider.
   */
  Builder builder();

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
