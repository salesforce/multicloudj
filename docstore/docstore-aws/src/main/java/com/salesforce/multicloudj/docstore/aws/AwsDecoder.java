package com.salesforce.multicloudj.docstore.aws;

import com.salesforce.multicloudj.docstore.driver.codec.Decoder;
import com.salesforce.multicloudj.docstore.driver.codec.ListDecoderCallback;
import com.salesforce.multicloudj.docstore.driver.codec.MapDecoderCallback;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AwsDecoder implements Decoder {
    private final AttributeValue attributeValue;

    public AwsDecoder(AttributeValue attributeValue) {
        this.attributeValue = attributeValue;
    }

    static final double EPSILON = 1e-9;
    @Override
    public String asString() {
        if (attributeValue.nul() != null || attributeValue.s() == null) {
            return null;
        }
        return this.attributeValue.s();
    }

    @Override
    public Integer asInt() {
        if (attributeValue.nul() != null || attributeValue.n() == null) {
            return null;
        }
        return Integer.parseInt(this.attributeValue.n());
    }

    @Override
    public Long asLong() {
        if (attributeValue.nul() != null || attributeValue.n() == null) {
            return null;
        }
        return Long.parseLong(this.attributeValue.n());
    }

    @Override
    public Float asFloat() {
        if (attributeValue.nul() != null || attributeValue.n() == null) {
            return null;
        }
        return Float.parseFloat(this.attributeValue.n());
    }

    @Override
    public Double asDouble() {
        if (attributeValue.nul() != null || attributeValue.n() == null) {
            return null;
        }
        return Double.parseDouble(this.attributeValue.n());
    }

    @Override
    public byte[] asBytes() {
        if (attributeValue.nul() != null || attributeValue.b() == null) {
            return null;
        }
        return attributeValue.b().asByteArray();
    }

    @Override
    public Boolean asBool() {
        if (attributeValue.nul() != null || this.attributeValue.bool() == null) {
            return null;
        }
        return this.attributeValue.bool();
    }

    @Override
    public Boolean asNull() {
        return this.attributeValue.nul() != null;
    }

    @Override
    public void decodeList(ListDecoderCallback callback) {
        // TODO:
    }

    @Override
    public int listLen() {
        if (this.attributeValue.l() == null) {
            return 0;
        }
        return this.attributeValue.l().size();
    }

    @Override
    public void decodeMap(MapDecoderCallback callback) {
        for (Map.Entry<String, AttributeValue> entry : this.attributeValue.m().entrySet()) {
            if (!callback.decode(entry.getKey(), new AwsDecoder(entry.getValue()))) {
                break;
            }
        }
    }

    @Override
    public Object asInterface() {
        return toValue(this.attributeValue);
    }

    private Object toValue(AttributeValue attributeValue) {
        switch (attributeValue.type()) {
            case NUL:
                return null;
            case BOOL:
                return attributeValue.bool();
            case N:
                try {
                    double d = Double.parseDouble(attributeValue.n());
                    int i = (int)d;
                    if (i == d) {
                        return i;
                    }

                    long l = (long)d;
                    if (l == d) {
                        return l;
                    }

                    float f = (float)d;
                    if ((Math.abs(f - d) < EPSILON)) {
                        return f;
                    }

                    return d;
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(attributeValue.n() + " is not a number");
                }
            case B:
                return attributeValue.b();
            case S:
                return attributeValue.s();
            case L:
                // convert each element in L.
                List<Object> s = new ArrayList<>();
                for (int i = 0; i < attributeValue.l().size(); i++) {
                    Object x = toValue(attributeValue.l().get(i));
                    s.add(x);
                }
                return s;
            case M:
                // convert each element in M.
                Map<String, Object> m = new HashMap<>();
                for (Map.Entry<String, AttributeValue> entry : attributeValue.m().entrySet()) {
                    Object x = toValue(entry.getValue());
                    m.put(entry.getKey(), x);
                }
                return m;
            default:
                throw new IllegalArgumentException(attributeValue.type() + " is not supported.");
        }
    }
}
