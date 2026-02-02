package com.salesforce.multicloudj.registry.model;

import java.io.IOException;
import java.io.InputStream;

/**
 * Interface for accessing the properties of a particular image layer.
 * Implementations are provided by registry driver.
 */
public interface Layer {
    String getDigest() throws IOException;

    InputStream getUncompressed() throws IOException;

    long getSize() throws IOException;
}
