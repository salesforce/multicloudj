package com.salesforce.multicloudj.docstore.gcp;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.Value;
import com.salesforce.multicloudj.docstore.driver.codec.Decoder;
import com.salesforce.multicloudj.docstore.driver.codec.ListDecoderCallback;
import com.salesforce.multicloudj.docstore.driver.codec.MapDecoderCallback;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SpannerDecoder implements the Decoder interface for Google Cloud Spanner.
 * It converts Spanner Struct types into Java objects for retrieval from Spanner.
 * <p> 
 * This decoder handles:
 * - Primitive types (boolean, int, long, double, string)
 * - Byte arrays
 * - Lists and arrays
 * - Maps and structs
 * - Null values
 */
public class SpannerDecoder implements Decoder {
    private Struct struct;
    private Value currentValue;

    /**
     * Creates a new SpannerDecoder with the specified Struct.
     *
     * @param struct The Spanner Struct to decode
     */
    public SpannerDecoder(Struct struct) {
        this.struct = struct;
        this.currentValue = null;
    }

    public SpannerDecoder(Value value) {
        this.currentValue = value;
    }

    /**
     * Decodes a Spanner Value into a string.
     *
     * @return The decoded string value, or null if the value is null
     */
    @Override
    public String asString() {
        return currentValue.getString();
    }

    /**
     * Decodes a Spanner Value into an integer.
     *
     * @return The decoded integer value, or null if the value is null
     */
    @Override
    public Integer asInt() {
        return (int) currentValue.getInt64();
    }

    /**
     * Decodes a Spanner Value into a long.
     *
     * @return The decoded long value, or null if the value is null
     */
    @Override
    public Long asLong() {
        return currentValue.getInt64();
    }

    /**
     * Decodes a Spanner Value into a float.
     *
     * @return The decoded float value, or null if the value is null
     */
    @Override
    public Float asFloat() {
        return currentValue.getFloat32();
    }

    /**
     * Decodes a Spanner Value into a double.
     *
     * @return The decoded double value, or null if the value is null
     */
    @Override
    public Double asDouble() {
        return currentValue.getFloat64();
    }

    /**
     * Decodes a Spanner Value into a byte array.
     *
     * @return The decoded byte array, or null if the value is null
     */
    @Override
    public byte[] asBytes() {
        return currentValue.getBytes().toByteArray();
    }

    /**
     * Decodes a Spanner Value into a boolean.
     *
     * @return The decoded boolean value, or null if the value is null
     */
    @Override
    public Boolean asBool() {
        return currentValue.getBool();
    }

    /**
     * Checks if the Spanner Value is null.
     *
     * @return true if the value is null, false otherwise
     */
    @Override
    public Boolean asNull() {
        return (currentValue == null || currentValue.isNull()) && (struct == null);
    }

    /**
     * Decodes a list of Spanner Values using the provided callback.
     *
     * @param callback The callback to handle each list element
     */
    @Override
    public void decodeList(ListDecoderCallback callback) {
       // TODO: Implement decodeList
    }

    /**
     * Gets the length of the list in the Spanner Value.
     *
     * @return The number of elements in the list
     */
    @Override
    public int listLen() {
        // TODO: Implement listLen
        return 0;
    }

    /**
     * Decodes a map of Spanner Values using the provided callback.
     *
     * @param callback The callback to handle each map entry
     */
    @Override
    public void decodeMap(MapDecoderCallback callback) {
        if (struct == null) {
            return;
        }
        for (StructField field : struct.getType().getStructFields()) {
            SpannerDecoder decoder = new SpannerDecoder(struct);
            decoder.currentValue = struct.getValue(field.getName());
            callback.decode(field.getName(), decoder);
        }
    }

    /**
     * Converts the Spanner Value into a Java object.
     * This method handles all supported Spanner types and converts them
     * into their corresponding Java types.
     *
     * @return The converted Java object
     */
    @Override
    public Object asInterface() {
        return toValue(currentValue);
    }

    /**
     * Sets the current value to decode.
     *
     * @param value The value to decode
     */
    public void setValue(Value value) {
        this.currentValue = value;
    }

    /**
     * Converts a Spanner Value into a Java object.
     * This method handles all supported Spanner types and converts them
     * into their corresponding Java types.
     *
     * @param value The Spanner Value to convert
     * @return The converted Java object
     */
    private Object toValue(Value value) {
        if (value == null || value.isNull()) {
            return null;
        }

        switch (value.getType().getCode()) {
            case BOOL:
                return value.getBool();
            case INT64:
                return value.getInt64();
            case FLOAT64:
                return value.getFloat64();
            case FLOAT32:
                return value.getFloat32();
            case STRING:
                return value.getString();
            case BYTES:
                return value.getBytes().toByteArray();
            case ARRAY:
                List<Object> list = new ArrayList<>();
                //List<Value> array = value.getArrayValue();
                //for (Value item : array) {
                //    list.add(toValue(item));
                //}
                return list;
            case STRUCT:
                Map<String, Object> map = new HashMap<>();
                Struct s = value.getStruct();
                for (StructField field : s.getType().getStructFields()) {
                    String columnName = field.getName();
                    map.put(columnName, toValue(s.getValue(columnName)));
                }
                return map;
            case TIMESTAMP:
                return value.getTimestamp();
            default:
                throw new IllegalArgumentException("Unsupported type: " + value.getType());
        }
    }
}