package com.salesforce.multicloudj.docstore.aws;

import com.salesforce.multicloudj.docstore.driver.codec.Encoder;
import lombok.Getter;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AwsEncoder implements Encoder {

    private final AttributeValue nullValue = AttributeValue.builder().nul(true).build();

    @Getter
    AttributeValue[] list = null;

    @Getter
    Map<String, AttributeValue> map = new HashMap<>();   // This map is passed down to mapEncoder to receive changes.

    private AttributeValue attributeValue;

    public AttributeValue getAttributeValue() {
        // TODO: probably required to check other non-primitive types as well.
        if (attributeValue.type() == AttributeValue.Type.M) {
            // The change to map is stored in the map field. Set the attributeValue with m again.
            attributeValue = attributeValue.toBuilder().m(map).build();
        }
        return attributeValue;
    }

    @Override
    public void encodeNil() {
         attributeValue = nullValue;
    }

    @Override
    public void encodeBool(boolean value) {
        attributeValue = AttributeValue.builder().bool(value).build();
    }

    @Override
    public void encodeString(String value) {
        if (value.isEmpty()) {
            attributeValue = nullValue;
        } else {
            attributeValue = AttributeValue.builder().s(value).build();
        }
    }

    @Override
    public void encodeInt(int value) {
        attributeValue = AttributeValue.builder().n(Integer.toString(value)).build();
    }

    @Override
    public void encodeLong(long value) {
        attributeValue = AttributeValue.builder().n(Long.toString(value)).build();
    }

    @Override
    public void encodeFloat(double value) {
        attributeValue = AttributeValue.builder().n(Double.toString(value)).build();
    }

    @Override
    public void encodeBytes(byte[] bytes) {
        attributeValue = AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build();
    }

    @Override
    public Encoder encodeList(int n) {
        List<AttributeValue> list = new ArrayList<>(n);
        this.attributeValue = AttributeValue.builder().l(list).build();
        return new ListEncoder(list);
    }

    @Override
    public void listIndex(int i) {
        list[i] = attributeValue;
    }

    @Override
    public Encoder encodeMap(int n) {
        this.attributeValue = AttributeValue.builder().m(map).build();
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

    public static class MapEncoder extends AwsEncoder implements Encoder {
        private final Map<String, AttributeValue> m;

        public MapEncoder(Map<String, AttributeValue> map) {
            m = map;
        }

        public void mapKey(String k) {
            m.put(k, getAttributeValue());
        }
    }

    public static class ListEncoder extends AwsEncoder implements Encoder {
        private final List<AttributeValue> l;

        public ListEncoder(List<AttributeValue> list) {
            l = list;
        }

        @Override
        public void listIndex(int i) {
            l.add(i, getAttributeValue());
        }
    }
}
