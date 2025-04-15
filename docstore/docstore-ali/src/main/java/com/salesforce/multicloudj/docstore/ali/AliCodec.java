package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.codec.Codec;

import java.util.Map;
import java.util.function.Predicate;

public class AliCodec {

    public static PrimaryKey encodeDocKeyFields(Document doc, String primaryKeyField, String sortKeyField) {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();

        // Helper predicate to check if the field exists and add it to the primary key builder
        Predicate<String> addFieldIfExists = fieldName -> {
            Object fieldValue = doc.getField(fieldName);
            if (fieldValue == null) {
                return false;
            }
            ColumnValue encodedValue = encodeValue(fieldValue);
            primaryKeyBuilder.addPrimaryKeyColumn(fieldName, PrimaryKeyValue.fromColumn(encodedValue));
            return true;
        };

        // Attempt to set the primary key field and return null if missing
        if (!addFieldIfExists.test(primaryKeyField)) {
            return null;
        }

        // Attempt to set the sort key field if provided and return null if missing
        if (sortKeyField != null && !addFieldIfExists.test(sortKeyField)) {
            return null;
        }

        return primaryKeyBuilder.build();
    }

    public static ColumnValue encodeValue(Object value) {
        AliEncoder encoder = new AliEncoder();
        Codec.encode(value, encoder);
        return encoder.getColumnValue();
    }

    public static Map<String, ColumnValue> encodeDoc(Document doc) {
        AliEncoder encoder = new AliEncoder();
        doc.encode(encoder);
        return encoder.getMap();
    }

    public static void decodeDoc(Object item, Document doc) {
        doc.decode(new AliDecoder(item));
    }
}