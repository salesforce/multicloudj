package com.salesforce.multicloudj.docstore.client;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProviderSupplierTest {
    @Test
    void testProviderNotFound() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> ProviderSupplier.findProviderBuilder("aws"));
    }
}
