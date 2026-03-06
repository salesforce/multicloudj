package com.salesforce.multicloudj.pubsub.client;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class ProviderSupplierTest {
  @Test
  void testProviderNotFound() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ProviderSupplier.findTopicProviderBuilder("test-provider"));
  }
}
