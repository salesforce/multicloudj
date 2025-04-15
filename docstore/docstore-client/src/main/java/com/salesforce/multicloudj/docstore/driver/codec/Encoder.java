package com.salesforce.multicloudj.docstore.driver.codec;

/**
 * Encoder defines methods for encoding various data types into a target format.
 * It supports encoding null values, primitive types (booleans, strings, integers, floats),
 * byte arrays, lists, and maps. Implementations of this interface are responsible for
 * serializing the encoded data. The providers need to provide the encoding implementations for
 * encoding the native data types to the provider/substrate specific format.
 */
public interface Encoder {
    void encodeNil();

    void encodeBool(boolean value);

    void encodeString(String value);

    void encodeInt(int value);

    void encodeLong(long value);

    void encodeFloat(double value);

    void encodeBytes(byte[] bytes);

    Encoder encodeList(int n);

    Encoder encodeArray(int n);

    void listIndex(int i);

    Encoder encodeMap(int n);

    void mapKey(String key);
}