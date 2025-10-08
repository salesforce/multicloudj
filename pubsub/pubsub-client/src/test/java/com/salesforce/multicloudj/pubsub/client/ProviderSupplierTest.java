package com.salesforce.multicloudj.pubsub.client;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProviderSupplierTest {
    @Test
    void testProviderNotFound() {
        assertThrows(IllegalArgumentException.class, 
            () -> ProviderSupplier.findTopicProviderBuilder("test-provider"));
    }
} 