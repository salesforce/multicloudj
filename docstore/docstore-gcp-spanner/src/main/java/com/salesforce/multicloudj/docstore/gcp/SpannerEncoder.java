package com.salesforce.multicloudj.docstore.gcp;

import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.Type;
import com.google.cloud.ByteArray;
import com.salesforce.multicloudj.docstore.driver.codec.Encoder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SpannerEncoder implements the Encoder interface for Google Cloud Spanner.
 * It converts Java objects into Spanner Value types for storage in Spanner.
 * 
 * This encoder handles:
 * - Primitive types (boolean, int, long, double, string)
 * - Byte arrays
 * - Lists and arrays
 * - Maps and structs
 * - Null values
 */
public class SpannerEncoder implements Encoder {
    /**
     * -- GETTER --
     *  Gets the map of encoded Spanner Values.
     *
     */
    @Getter
    private final Map<String, Value> map = new HashMap<>();

    private Value value;
    private List<Value> array;

    /**
     * Encodes a null value into a Spanner Value.
     * The type of null value is determined by the context.
     */
    @Override
    public void encodeNil() {
        value = Value.string(null);
    }

    /**
     * Encodes a boolean value into a Spanner Value.
     *
     * @param value The boolean value to encode
     */
    @Override
    public void encodeBool(boolean value) {
        this.value = Value.bool(value);
    }

    /**
     * Encodes an integer value into a Spanner Value.
     *
     * @param value The integer value to encode
     */
    @Override
    public void encodeInt(int value) {
        this.value = Value.int64(value);
    }

    /**
     * Encodes a long value into a Spanner Value.
     *
     * @param value The long value to encode
     */
    @Override
    public void encodeLong(long value) {
        this.value = Value.int64(value);
    }

    /**
     * Encodes a double value into a Spanner Value.
     *
     * @param value The double value to encode
     */
    @Override
    public void encodeFloat(double value) {
        this.value = Value.float64(value);
    }

    /**
     * Encodes a string value into a Spanner Value.
     *
     * @param value The string value to encode
     */
    @Override
    public void encodeString(String value) {
        this.value = Value.string(value);
    }

    /**
     * Encodes a byte array into a Spanner Value.
     *
     * @param bytes The byte array to encode
     */
    @Override
    public void encodeBytes(byte[] bytes) {
        this.value = Value.bytes(ByteArray.copyFrom(bytes));
    }

    /**
     * Creates a new encoder for encoding a list.
     *
     * @param n The size of the list
     * @return A new encoder for the list
     */
    @Override
    public Encoder encodeList(int n) {
        array = new ArrayList<>(n);
        return new ListEncoder(array, this);
    }

    /**
     * Adds a value to the list at the specified index.
     *
     * @param i The index where to add the value
     */
    @Override
    public void listIndex(int i) {
        if (array != null) {
            array.add(i, value);
        }
    }

    /**
     * Creates a new encoder for encoding a map.
     *
     * @param n The size of the map
     * @return A new encoder for the map
     */
    @Override
    public Encoder encodeMap(int n) {
        return new MapEncoder(map);
    }

    /**
     * Throws an exception as this method should not be called directly.
     * Use MapEncoder.mapKey() instead.
     *
     * @param key The key to add
     */
    @Override
    public void mapKey(String key) {
        throw new IllegalStateException("Invalid call.");
    }

    /**
     * Creates a new encoder for encoding an array.
     * This is equivalent to encodeList for Spanner.
     *
     * @param n The size of the array
     * @return A new encoder for the array
     */
    @Override
    public Encoder encodeArray(int n) {
        return encodeList(n);
    }

    /**
     * Gets the encoded Spanner Value.
     * If the value is a map or array, it will be reconstructed from the map or array field.
     *
     * @return The encoded Spanner Value
     */
    public Value getValue() {
        if (value != null && value.getType().getCode() == Type.Code.ARRAY) {
            // The change to array is stored in the array field. Set the value with array again.
            if (array.isEmpty()) {
                // For empty arrays, we need to determine the type from the context
                // Since we don't have a default type, we'll use string array as a safe default
                this.value = Value.stringArray(new ArrayList<>());
            } else {
                // Determine the type of the first element to create the appropriate array
                Value firstElement = array.get(0);
                switch (firstElement.getType().getCode()) {
                    case INT64:
                        List<Long> longList = new ArrayList<>();
                        for (Value v : array) {
                            longList.add(v.getInt64());
                        }
                        this.value = Value.int64Array(longList);
                        break;
                    case FLOAT64:
                        List<Double> doubleList = new ArrayList<>();
                        for (Value v : array) {
                            doubleList.add(v.getFloat64());
                        }
                        this.value = Value.float64Array(doubleList);
                        break;
                    case STRING:
                        List<String> stringList = new ArrayList<>();
                        for (Value v : array) {
                            stringList.add(v.getString());
                        }
                        this.value = Value.stringArray(stringList);
                        break;
                    case BOOL:
                        List<Boolean> boolList = new ArrayList<>();
                        for (Value v : array) {
                            boolList.add(v.getBool());
                        }
                        this.value = Value.boolArray(boolList);
                        break;
                    case BYTES:
                        List<ByteArray> bytesList = new ArrayList<>();
                        for (Value v : array) {
                            bytesList.add(v.getBytes());
                        }
                        this.value = Value.bytesArray(bytesList);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported array element type: " + firstElement.getType());
                }
            }
        }
        return this.value;
    }

    /**
     * MapEncoder is a specialized encoder for encoding map entries.
     * It extends SpannerEncoder to handle map key-value pairs.
     */
    public static class MapEncoder extends SpannerEncoder {
        private final Map<String, Value> m;

        /**
         * Creates a new MapEncoder with the specified map.
         *
         * @param map The map to store encoded values
         */
        public MapEncoder(Map<String, Value> map) {
            m = map;
        }

        /**
         * Adds a key-value pair to the map.
         *
         * @param k The key to add
         */
        @Override
        public void mapKey(String k) {
            m.put(k, getValue());
        }
    }

    /**
     * ListEncoder is a specialized encoder for encoding list elements.
     * It extends SpannerEncoder to handle list elements.
     */
    public static class ListEncoder extends SpannerEncoder {
        private final List<Value> l;
        private final SpannerEncoder parent;

        /**
         * Creates a new ListEncoder with the specified list and parent encoder.
         *
         * @param list The list to store encoded values
         * @param parent The parent encoder to update
         */
        public ListEncoder(List<Value> list, SpannerEncoder parent) {
            l = list;
            this.parent = parent;
        }

        /**
         * Adds a value to the list at the specified index.
         *
         * @param i The index where to add the value
         */
        @Override
        public void listIndex(int i) {
            l.add(i, getValue());
            // Update the parent encoder's value with the current array
            if (l.isEmpty()) {
                parent.value = Value.stringArray(new ArrayList<>());
            } else {
                Value firstElement = l.get(0);
                switch (firstElement.getType().getCode()) {
                    case INT64:
                        List<Long> longList = new ArrayList<>();
                        for (Value v : l) {
                            longList.add(v.getInt64());
                        }
                        parent.value = Value.int64Array(longList);
                        break;
                    case FLOAT64:
                        List<Double> doubleList = new ArrayList<>();
                        for (Value v : l) {
                            doubleList.add(v.getFloat64());
                        }
                        parent.value = Value.float64Array(doubleList);
                        break;
                    case STRING:
                        List<String> stringList = new ArrayList<>();
                        for (Value v : l) {
                            stringList.add(v.getString());
                        }
                        parent.value = Value.stringArray(stringList);
                        break;
                    case BOOL:
                        List<Boolean> boolList = new ArrayList<>();
                        for (Value v : l) {
                            boolList.add(v.getBool());
                        }
                        parent.value = Value.boolArray(boolList);
                        break;
                    case BYTES:
                        List<ByteArray> bytesList = new ArrayList<>();
                        for (Value v : l) {
                            bytesList.add(v.getBytes());
                        }
                        parent.value = Value.bytesArray(bytesList);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported array element type: " + firstElement.getType());
                }
            }
        }
    }
} 