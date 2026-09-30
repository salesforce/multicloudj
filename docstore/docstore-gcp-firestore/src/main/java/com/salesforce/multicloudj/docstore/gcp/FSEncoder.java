package com.salesforce.multicloudj.docstore.gcp;

import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.ByteString;
import com.salesforce.multicloudj.docstore.driver.codec.Encoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;

@Getter
public class FSEncoder implements Encoder {

  private List<Value> list = new ArrayList<>();

  private Map<String, Value> map = new HashMap<>();

  protected Value value;

  @Override
  public void encodeNil() {
    value = Value.newBuilder().setNullValue(com.google.protobuf.NullValue.NULL_VALUE).build();
  }

  @Override
  public void encodeBool(boolean value) {
    this.value = Value.newBuilder().setBooleanValue(value).build();
  }

  @Override
  public void encodeString(String value) {
    this.value = Value.newBuilder().setStringValue(value).build();
  }

  @Override
  public void encodeInt(int value) {
    this.value = Value.newBuilder().setIntegerValue(value).build();
  }

  @Override
  public void encodeLong(long value) {
    this.value = Value.newBuilder().setIntegerValue(value).build();
  }

  @Override
  public void encodeFloat(double value) {
    this.value = Value.newBuilder().setDoubleValue(value).build();
  }

  @Override
  public void encodeBytes(byte[] bytes) {
    this.value = Value.newBuilder().setBytesValue(ByteString.copyFrom(bytes)).build();
  }

  @Override
  public Encoder encodeList(int n) {
    list = new ArrayList<>();
    value = arrayValue(list);
    return new ListEncoder(list);
  }

  @Override
  public void listIndex(int i) {
    list.set(i, value);
  }

  @Override
  public Encoder encodeMap(int n) {
    map = new HashMap<>();
    value = mapValue(map);
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

  public Value getValue() {
    if (value.getValueTypeCase() == Value.ValueTypeCase.MAP_VALUE) {
      value = mapValue(map);
    } else if (value.getValueTypeCase() == Value.ValueTypeCase.ARRAY_VALUE) {
      value = arrayValue(list);
    }
    return value;
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

  private static Value mapValue(Map<String, Value> fields) {
    return Value.newBuilder()
        .setMapValue(MapValue.newBuilder().putAllFields(fields).build())
        .build();
  }

  private static Value arrayValue(List<Value> values) {
    return Value.newBuilder()
        .setArrayValue(ArrayValue.newBuilder().addAllValues(values).build())
        .build();
  }
}
