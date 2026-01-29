package com.salesforce.multicloudj.registry.driver;

import java.io.IOException;
import java.io.InputStream;

/**
 * Interface for accessing the properties of a particular layer.
 * Similar to go-containerregistry's v1.Layer interface.
 */
public interface Layer {
    /**
     * Returns the Hash of the compressed layer.
     */
    String getDigest() throws IOException;

    /**
     * Returns an InputStream for the uncompressed layer contents.
     * This triggers the actual download from the registry.
     */
    InputStream getUncompressed() throws IOException;

    /**
     * Returns the compressed size of the Layer.
     */
    long getSize() throws IOException;
}
