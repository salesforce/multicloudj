package com.salesforce.multicloudj.docstore.gcp;

import com.google.firestore.v1.Value;
import com.salesforce.multicloudj.docstore.driver.codec.Decoder;
import com.salesforce.multicloudj.docstore.driver.codec.ListDecoderCallback;
import com.salesforce.multicloudj.docstore.driver.codec.MapDecoderCallback;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

public class FSDecoder implements Decoder {
    private final Value value;

    public FSDecoder(Value value) {
        this.value = value;
    }

    static final double EPSILON = 1e-9;

    @Override
    public String asString() {
        if (value == null || value.getValueTypeCase() == Value.ValueTypeCase.NULL_VALUE) {
            return null;
        }
        if (value.getValueTypeCase() == Value.ValueTypeCase.STRING_VALUE) {
            return value.getStringValue();
        }
        return value.toString();
    }

    @Override
    public Integer asInt() {
        if (value == null || value.getValueTypeCase() == Value.ValueTypeCase.NULL_VALUE) {
            return null;
        }
        if (value.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE) {
            return (int) value.getIntegerValue();
        }
        return Integer.parseInt(value.toString());
    }

    @Override
    public Long asLong() {
        if (value == null || value.getValueTypeCase() == Value.ValueTypeCase.NULL_VALUE) {
            return null;
        }
        if (value.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE) {
            return value.getIntegerValue();
        }
        return Long.parseLong(value.toString());
    }

    @Override
    public Float asFloat() {
        if (value == null || value.getValueTypeCase() == Value.ValueTypeCase.NULL_VALUE) {
            return null;
        }
        if (value.getValueTypeCase() == Value.ValueTypeCase.DOUBLE_VALUE) {
            return (float) value.getDoubleValue();
        }
        return Float.parseFloat(value.toString());
    }

    @Override
    public Double asDouble() {
        if (value == null || value.getValueTypeCase() == Value.ValueTypeCase.NULL_VALUE) {
            return null;
        }
        if (value.getValueTypeCase() == Value.ValueTypeCase.DOUBLE_VALUE) {
            return value.getDoubleValue();
        }
        return Double.parseDouble(value.toString());
    }

    @Override
    public byte[] asBytes() {
        if (value == null || value.getValueTypeCase() == Value.ValueTypeCase.NULL_VALUE) {
            return null;
        }
        if (value.getValueTypeCase() == Value.ValueTypeCase.BYTES_VALUE) {
            return value.getBytesValue().toByteArray();
        }
        throw new IllegalArgumentException("Value is not a byte array");
    }

    @Override
    public Boolean asBool() {
        if (value == null || value.getValueTypeCase() == Value.ValueTypeCase.NULL_VALUE) {
            return null;
        }
        if (value.getValueTypeCase() == Value.ValueTypeCase.BOOLEAN_VALUE) {
            return value.getBooleanValue();
        }
        return Boolean.parseBoolean(value.toString());
    }

    @Override
    public Boolean asNull() {
        return value == null || value.getValueTypeCase() == Value.ValueTypeCase.NULL_VALUE;
    }

    @Override
    public void decodeList(ListDecoderCallback callback) {
        if (value != null && value.getValueTypeCase() == Value.ValueTypeCase.ARRAY_VALUE) {
            List<Value> list = value.getArrayValue().getValuesList();
            for (int i = 0; i < list.size(); i++) {
                if (!callback.decode(i, new FSDecoder(list.get(i)))) {
                    break;
                }
            }
        }
    }

    @Override
    public int listLen() {
        if (value != null && value.getValueTypeCase() == Value.ValueTypeCase.ARRAY_VALUE) {
            return value.getArrayValue().getValuesCount();
        }
        return 0;
    }

    @Override
    public void decodeMap(MapDecoderCallback callback) {
        if (value != null && value.getValueTypeCase() == Value.ValueTypeCase.MAP_VALUE) {
            Map<String, Value> map = value.getMapValue().getFieldsMap();
            for (Map.Entry<String, Value> entry : map.entrySet()) {
                if (!callback.decode(entry.getKey(), new FSDecoder(entry.getValue()))) {
                    break;
                }
            }
        }
    }

    @Override
    public Object asInterface() {
        return toValue(value);
    }

    private Object toValue(Value value) {
        if (value == null || value.getValueTypeCase() == Value.ValueTypeCase.NULL_VALUE) {
            return null;
        }

        switch (value.getValueTypeCase()) {
            case BOOLEAN_VALUE:
                return value.getBooleanValue();
            case INTEGER_VALUE:
                long l = value.getIntegerValue();
                int i = (int)l;
                if (i == l) {
                    return i;
                }
                return l;
            case DOUBLE_VALUE:
                double d = value.getDoubleValue();
                float f = (float)d;
                if (Math.abs(f - d) < EPSILON) {
                    return f;
                }
                return d;
            case STRING_VALUE:
                return value.getStringValue();
            case BYTES_VALUE:
                return value.getBytesValue().toByteArray();
            case ARRAY_VALUE:
                List<Object> list = new ArrayList<>();
                for (Value item : value.getArrayValue().getValuesList()) {
                    list.add(toValue(item));
                }
                return list;
            case MAP_VALUE:
                Map<String, Object> map = new HashMap<>();
                for (Map.Entry<String, Value> entry : value.getMapValue().getFieldsMap().entrySet()) {
                    map.put(entry.getKey(), toValue(entry.getValue()));
                }
                return map;
            default:
                throw new IllegalArgumentException("Unsupported value type: " + value.getValueTypeCase());
        }
    }
} 