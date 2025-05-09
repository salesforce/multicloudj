package com.salesforce.multicloudj.docstore.gcp;

import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.ByteString;
import com.salesforce.multicloudj.docstore.driver.codec.Encoder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public class FSEncoder implements Encoder {

    private List<Value> list = new ArrayList<>();

    private final Map<String, Value> map = new HashMap<>();

    private Value value;

    @Override
    public void encodeNil() {
        value = Value.newBuilder()
            .setNullValue(com.google.protobuf.NullValue.NULL_VALUE)
            .build();
    }

    @Override
    public void encodeBool(boolean value) {
        this.value = Value.newBuilder()
            .setBooleanValue(value)
            .build();
    }

    @Override
    public void encodeString(String value) {
        this.value = Value.newBuilder()
            .setStringValue(value)
            .build();
    }

    @Override
    public void encodeInt(int value) {
        this.value = Value.newBuilder()
            .setIntegerValue(value)
            .build();
    }

    @Override
    public void encodeLong(long value) {
        this.value = Value.newBuilder()
            .setIntegerValue(value)
            .build();
    }

    @Override
    public void encodeFloat(double value) {
        this.value = Value.newBuilder()
            .setDoubleValue(value)
            .build();
    }

    @Override
    public void encodeBytes(byte[] bytes) {
        this.value = Value.newBuilder()
            .setBytesValue(ByteString.copyFrom(bytes))
            .build();
    }

    @Override
    public Encoder encodeList(int n) {
        this.value = Value.newBuilder()
            .setArrayValue(ArrayValue.newBuilder()
                .addAllValues(list)
                .build())
            .build();
        return new ListEncoder(list);
    }

    @Override
    public void listIndex(int i) {
        list.set(i, value);
    }

    @Override
    public Encoder encodeMap(int n) {
        this.value = Value.newBuilder()
            .setMapValue(MapValue.newBuilder()
                .putAllFields(map)
                .build())
            .build();
        return new MapEncoder(map);
    }

    @Override
    public void mapKey(String key) {
        throw new IllegalStateException("Invalid call.");
    }

    @Override
    public Encoder encodeArray(int n) {
        return null;
    }

    public static class MapEncoder extends FSEncoder {
        private final Map<String, Value> m;

        public MapEncoder(Map<String, Value> map) {
            m = map;
        }

        @Override
        public void mapKey(String k) {
            m.put(k, getValue());
        }
    }

    public Value getValue() {
        if (value.getValueTypeCase() == Value.ValueTypeCase.MAP_VALUE) {
            // The change to map is stored in the map field. Set the value with map again.
            this.value = Value.newBuilder()
                    .setMapValue(MapValue.newBuilder()
                            .putAllFields(map)
                            .build())
                    .build();
        } else if (value.getValueTypeCase() == Value.ValueTypeCase.ARRAY_VALUE) {
            // The change to list is stored in the list field. Set the value with list again.
            this.value = Value.newBuilder()
                    .setArrayValue(ArrayValue.newBuilder()
                            .addAllValues(list)
                            .build())
                    .build();
        }
        return this.value;
    }

    public static class ListEncoder extends FSEncoder {
        private final List<Value> l;

        public ListEncoder(List<Value> list) {
            l = list;
        }

        @Override
        public void listIndex(int i) {
            l.add(i, getValue());
        }
    }
} 