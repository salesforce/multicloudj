package com.salesforce.multicloudj.registry.driver;

import java.io.IOException;
import java.util.List;

/**
 * Interface for a container image.
 * Similar to go-containerregistry's v1.Image interface.
 */
public interface Image {
    List<Layer> getLayers() throws IOException;
    String getDigest() throws IOException;
    String getImageRef() throws IOException;
}
