package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.salesforce.multicloudj.docstore.driver.codec.Encoder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AliEncoder implements Encoder {

    private final ColumnValue nullValue = ColumnValue.INTERNAL_NULL_VALUE;

    @Getter
    Map<String, ColumnValue> map;   // This map is passed down to mapEncoder to receive changes.
    @Getter
    List<ColumnValue> list;

    @Getter
    private ColumnValue columnValue;

    @Getter
    private Object objectValue;

    @Override
    public void encodeNil() {
        columnValue = nullValue;
    }

    @Override
    public void encodeBool(boolean value) {
        columnValue = ColumnValue.fromBoolean(value);
    }

    @Override
    public void encodeString(String value) {
        if (value.isEmpty()) {
            columnValue = nullValue;
        } else {
            columnValue = ColumnValue.fromString(value);
        }
    }

    @Override
    public void encodeInt(int value) {
        columnValue = ColumnValue.fromLong(value);
        objectValue = ColumnValue.fromLong(value);
    }

    @Override
    public void encodeLong(long value) {
        columnValue = ColumnValue.fromLong(value);
        objectValue = ColumnValue.fromLong(value);
    }

    @Override
    public void encodeFloat(double value) {
        columnValue = ColumnValue.fromDouble(value);
        objectValue = ColumnValue.fromDouble(value);
    }

    @Override
    public void encodeBytes(byte[] bytes) {
        columnValue = ColumnValue.fromBinary(bytes);
        objectValue = ColumnValue.fromBinary(bytes);
    }

    @Override
    public Encoder encodeList(int n) {
        this.list = new ArrayList<>(n);
        return new ListEncoder(list);
    }

    @Override
    public void listIndex(int i) {
        throw new UnsupportedOperationException("listIndex is only supported on ListEncoder");
    }

    @Override
    public Encoder encodeMap(int n) {
        this.map = new HashMap<>(n);
        return new MapEncoder(map);
    }

    @Override
    public void mapKey(String key) {
        throw new UnsupportedOperationException("mapKey is only supported on MapEncoder");
    }

    @Override
    public Encoder encodeArray(int n) {
        return null;
    }

    public static class MapEncoder extends AliEncoder {
        private final Map<String, ColumnValue> m;

        public MapEncoder(Map<String, ColumnValue> map) {
            m = map;
        }

        @Override
        public void mapKey(String k) {
            m.put(k, getColumnValue());
        }
    }

    public static class ListEncoder extends AliEncoder {
        private final List<ColumnValue> l;

        public ListEncoder(List<ColumnValue> list) {
            l = list;
        }

        @Override
        public void listIndex(int i) {
            l.add(i, getColumnValue());
        }
    }
}