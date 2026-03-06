package com.salesforce.multicloudj.registry.model;

import java.io.InputStream;

/** Interface for a single image layer; implementations are provided by the registry driver. */
public interface Layer {
  String getDigest();

  InputStream getUncompressed();

  long getSize();
}
