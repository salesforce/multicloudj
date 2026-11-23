package com.salesforce.multicloudj.registry.client;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

@SuppressWarnings("rawtypes")
class ContainerRegistryClientTest {

  @Test
  void testWithRetryConfig_DelegatesToRegistryBuilder() {
    AbstractRegistry.Builder registryBuilder = mock(AbstractRegistry.Builder.class);
    RetryConfig retryConfig =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.FIXED)
            .maxAttempts(3)
            .fixedDelayMillis(100L)
            .build();

    try (MockedStatic<ProviderSupplier> supplier = mockStatic(ProviderSupplier.class)) {
      supplier.when(() -> ProviderSupplier.findProviderBuilder("test")).thenReturn(registryBuilder);

      ContainerRegistryClient.builder("test").withRetryConfig(retryConfig);

      verify(registryBuilder).withRetryConfig(retryConfig);
    }
  }
}
