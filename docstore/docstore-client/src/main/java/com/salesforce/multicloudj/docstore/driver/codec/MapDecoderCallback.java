package com.salesforce.multicloudj.docstore.driver.codec;

/**
 * MapDecoderCallback providers the callback function which should be called by the
 * decodeMap implementation of a given provider. The function basically enables the
 * decoding of an item for a given key.
 */
public interface MapDecoderCallback {
    boolean decode(String fieldName, Decoder decoder);
}
