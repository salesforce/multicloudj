package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.Row;
import com.salesforce.multicloudj.docstore.driver.codec.Decoder;
import com.salesforce.multicloudj.docstore.driver.codec.ListDecoderCallback;
import com.salesforce.multicloudj.docstore.driver.codec.MapDecoderCallback;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class AliDecoder implements Decoder {
    private Object value = null;

    public AliDecoder(Object value) {
        this.value = value;
    }

    @Override
    public String asString() {
        if (this.value == null || this.value == ColumnValue.INTERNAL_NULL_VALUE) {
            return null;
        }
        if (this.value instanceof PrimaryKeyValue) {
            return ((PrimaryKeyValue) this.value).asString();
        }
        if (this.value instanceof ColumnValue) {
            return ((ColumnValue) this.value).asString();
        }
        if (this.value instanceof String) {
            return (String) this.value;
        }
        return null;
    }

    @Override
    public Integer asInt() {
        if (this.value == null || this.value == ColumnValue.INTERNAL_NULL_VALUE) {
            return null;
        }
        if (this.value instanceof PrimaryKeyValue) {
            return Math.toIntExact(((PrimaryKeyValue) this.value).asLong());
        }
        if (this.value instanceof ColumnValue) {
            return Math.toIntExact(((ColumnValue) this.value).asLong());
        }
        if (this.value instanceof Integer) {
            return (int) this.value;
        }
        return null;
    }

    @Override
    public Long asLong() {
        if (this.value == null || this.value == ColumnValue.INTERNAL_NULL_VALUE) {
            return null;
        }
        if (this.value instanceof PrimaryKeyValue) {
            return ((PrimaryKeyValue) this.value).asLong();
        }
        if (this.value instanceof ColumnValue) {
            return ((ColumnValue) this.value).asLong();
        }
        if (this.value instanceof Long) {
            return (long) this.value;
        }
        return null;
    }

    @Override
    public Float asFloat() {
        if (this.value == null || this.value == ColumnValue.INTERNAL_NULL_VALUE) {
            return null;
        }
        if (this.value instanceof ColumnValue) {
            return (float) ((ColumnValue) this.value).asDouble();
        }
        if (this.value instanceof Double) {
            return ((Double) this.value).floatValue();
        }
        return null;
    }

    @Override
    public Double asDouble() {
        if (this.value == null || this.value == ColumnValue.INTERNAL_NULL_VALUE) {
            return null;
        }
        if (this.value instanceof ColumnValue) {
            return ((ColumnValue) this.value).asDouble();
        }
        if (this.value instanceof Double) {
            return (double) this.value;
        }
        return null;
    }

    @Override
    public byte[] asBytes() {
        if (this.value == null || this.value == ColumnValue.INTERNAL_NULL_VALUE) {
            return null;
        }
        if (this.value instanceof PrimaryKeyValue) {
            return ((PrimaryKeyValue) this.value).asBinary();
        }
        if (this.value instanceof ColumnValue) {
            return ((ColumnValue) this.value).asBinary();
        }
        if (this.value instanceof ByteBuffer) {
            return ((ByteBuffer) this.value).array();
        }
        return null;
    }

    @Override
    public Boolean asBool() {
        if (this.value == null || this.value == ColumnValue.INTERNAL_NULL_VALUE) {
            return null;
        }
        if (this.value instanceof ColumnValue) {
            return ((ColumnValue) this.value).asBoolean();
        }
        if (this.value instanceof Boolean) {
            return (Boolean) this.value;
        }
        return null;
    }

    @Override
    public Boolean asNull() {
        return this.value == null || this.value == ColumnValue.INTERNAL_NULL_VALUE;
    }

    @Override
    public void decodeList(ListDecoderCallback callback) {
        // TODO:
    }

    @Override
    public int listLen() {
        return 0;
    }

    @Override
    public void decodeMap(MapDecoderCallback callback) {
        if (this.value instanceof HashMap) {
            for (Map.Entry<?, ?> m : ((HashMap<?, ?>) this.value).entrySet()) {
                callback.decode((String) m.getKey(), new AliDecoder(m.getValue()));
            }
            return;
        }
        for (PrimaryKeyColumn column : ((Row)this.value).getPrimaryKey().getPrimaryKeyColumns()) {
            if (!callback.decode(column.getName(), new AliDecoder(column.getValue()))) {
                break;
            }
        }

        for (Column column : ((Row)this.value).getColumns()) {
            if (!callback.decode(column.getName(), new AliDecoder(column.getValue()))) {
                break;
            }
        }
    }

    @Override
    public Object asInterface() {
        if (this.value instanceof ColumnValue) {
            return toValue((ColumnValue) this.value);
        }
        if (this.value instanceof PrimaryKeyValue) {
            return toValue((PrimaryKeyValue) this.value);
        }

        if (this.value instanceof String ||
                this.value instanceof Boolean ||
                this.value instanceof Integer ||
                this.value instanceof Long ||
                this.value instanceof Float ||
                this.value instanceof Double) {
            return this.value;
        }

        return null;
    }

    private Object toValue(ColumnValue attributeValue) {
        switch (attributeValue.getType()) {
            case BOOLEAN:
                return attributeValue.asBoolean();
            case INTEGER:
                try {
                    return attributeValue.asLong();
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(attributeValue.asLong() + " is not a number");
                }
            case DOUBLE:
                return attributeValue.asDouble();
            case BINARY:
                return attributeValue.asBinary();
            case STRING:
                return attributeValue.asString();
            default:
                throw new IllegalArgumentException(attributeValue.getType() + " is not supported.");
        }
    }

    private Object toValue(PrimaryKeyValue pkValue) {
        switch (pkValue.getType()) {
            case INTEGER:
                try {
                    return pkValue.asLong();
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(pkValue.asLong() + " is not a number");
                }
            case STRING:
                return pkValue.asString();

            case BINARY:
                return pkValue.asBinary();
            default:
                throw new IllegalArgumentException(pkValue.getType() + " is not supported.");
        }
    }
}
