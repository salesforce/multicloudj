package com.salesforce.multicloudj.registry.model;

import java.util.List;

/** Interface for a container image; implementations are provided by the registry driver. */
public interface Image {
  List<Layer> getLayers();

  String getDigest();

  String getImageRef();
}
