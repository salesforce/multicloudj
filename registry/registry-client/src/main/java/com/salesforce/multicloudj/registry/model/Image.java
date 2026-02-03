package com.salesforce.multicloudj.registry.model;

import java.io.IOException;
import java.util.List;

/**
 * Interface for a container image.
 * Implementations are provided by the registry driver (e.g. RemoteImage).
 */
public interface Image {
    List<Layer> getLayers() throws IOException;

    String getDigest() throws IOException;

    String getImageRef() throws IOException;
}
