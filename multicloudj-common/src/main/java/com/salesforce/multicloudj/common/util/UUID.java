package com.salesforce.multicloudj.common.util;

import java.util.function.Supplier;
import lombok.Setter;

public class UUID {
  @Setter
  private static Supplier<String> uuidSupplier = () -> java.util.UUID.randomUUID().toString();

  public static String uniqueString() {
    return uuidSupplier.get();
  }
}
