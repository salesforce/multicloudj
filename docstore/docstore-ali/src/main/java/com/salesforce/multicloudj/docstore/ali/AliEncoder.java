package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.salesforce.multicloudj.docstore.driver.codec.Encoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.Getter;

public class AliEncoder implements Encoder {

  private final ColumnValue nullValue = ColumnValue.INTERNAL_NULL_VALUE;

  @Getter Map<String, ColumnValue> map; // This map is passed down to mapEncoder to receive changes.
  @Getter List<ColumnValue> list;

  @Getter private ColumnValue columnValue;

  @Getter private Object objectValue;

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
    // Use a TreeMap so encoded columns serialize in a deterministic (name-sorted) order,
    // independent of the source document's map iteration order (which is salted per JVM run for
    // Map.of / HashMap-backed docs). This keeps the serialized row bytes reproducible across runs,
    // which the record/replay conformance harness relies on. Tablestore treats a row PUT as an
    // unordered column set, so ordering has no functional effect on the write itself.
    this.map = new TreeMap<>();
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
