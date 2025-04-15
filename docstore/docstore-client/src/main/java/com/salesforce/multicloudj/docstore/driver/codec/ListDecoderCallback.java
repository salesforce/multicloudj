package com.salesforce.multicloudj.docstore.driver.codec;

/**
 * ListDecoderCallback providers the callback function which should be called by the
 * decodeList implementation of a given provider. The function basically enables the
 * decoding of an item at specific index in the list.
 */
public interface ListDecoderCallback {
    boolean decode(int index, Decoder decoder);
}

