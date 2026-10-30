package com.salesforce.multicloudj.kms.client;

import com.google.common.collect.ImmutableSet;
import com.salesforce.multicloudj.kms.driver.AbstractKms;
import java.util.ServiceLoader;

/** This class will attempt to find a KMS provider in the classpath by providerId */
public class ProviderSupplier {

  private ProviderSupplier() {}

  /**
   * Finds the provider builder for the specified provider ID.
   *
   * @param providerId Id of the provider to be found
   * @return Builder object for the provider
   * @throws IllegalArgumentException if no provider is found for the given ID
   */
  static AbstractKms.Builder<?, ?> findProviderBuilder(String providerId) {
    ServiceLoader<AbstractKms> services = ServiceLoader.load(AbstractKms.class);
    Iterable<AbstractKms> all = ImmutableSet.<AbstractKms>builder().addAll(services).build();

    for (AbstractKms provider : all) {
      if (provider.getProviderId().equals(providerId)) {
        return createBuilderInstance(provider);
      }
    }
    throw new IllegalArgumentException("No KMS provider found for providerId: " + providerId);
  }

  private static AbstractKms.Builder<?, ?> createBuilderInstance(AbstractKms provider) {
    try {
      return (AbstractKms.Builder<?, ?>) provider.getClass().getMethod("builder").invoke(provider);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to create builder for provider: " + provider.getClass().getName(), e);
    }
  }
}
