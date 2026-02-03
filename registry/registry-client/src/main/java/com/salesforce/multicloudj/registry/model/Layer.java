package com.salesforce.multicloudj.registry.model;

import java.io.IOException;
import java.io.InputStream;

/**
 * Interface for a single image layer.
 * Implementations are provided by the registry driver (e.g. RemoteLayer).
 */
public interface Layer {
    String getDigest() throws IOException;

    InputStream getUncompressed() throws IOException;

    long getSize() throws IOException;
}
