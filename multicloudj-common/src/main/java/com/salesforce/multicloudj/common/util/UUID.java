package com.salesforce.multicloudj.common.util;

import lombok.Setter;

import java.util.function.Supplier;

public class UUID {
    @Setter
    private static Supplier<String> uuidSupplier = () -> java.util.UUID.randomUUID().toString();

    public static String uniqueString() {
        return uuidSupplier.get();
    }
}
