package com.salesforce.multicloudj.docstore.driver.codec;

/**
 * Decoder defines methods for decoding a given format of data to the java native types
 * such as User defined classes or the {@code Map<String, Object>}.
 * It supports decoding null values, primitive types (booleans, strings, integers, floats),
 * byte arrays, lists, and maps. Implementations of this interface are responsible for
 * deserializing the data for a specific provider. The providers need to provide the decoder implementations for
 * decoding to the provider/substrate specific format to the java native data types .
 */
public interface Decoder {
    String asString();

    Integer asInt();

    Long asLong();

    Float asFloat();

    Double asDouble();

    byte[] asBytes();

    Boolean asBool();

    Boolean asNull();

    void decodeList(ListDecoderCallback callback);

    int listLen();

    void decodeMap(MapDecoderCallback callback);

    Object asInterface();

    String toString();
}

