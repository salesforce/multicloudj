package com.salesforce.multicloudj.docstore.gcp;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.codec.Codec;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

/**
 * SpannerCodec provides utility methods for encoding and decoding data between Java native types
 * and Google Cloud Spanner's value types.
 * 
 * This class handles the conversion of:
 * - Java objects to Spanner Value types for storage
 * - Spanner Struct objects to Java objects for retrieval
 * - Document fields to Spanner Value types for updates
 */
public class SpannerCodec {

    /**
     * Encodes document key fields into a map of Spanner Values.
     * This method is used to create primary key and sort key fields for Spanner operations.
     *
     * @param doc The document containing the key fields
     * @param primaryKeyField The name of the primary key field
     * @param sortKeyField The name of the sort key field (optional)
     * @return A map of field names to Spanner Values, or null if required fields are missing
     */
    public static Map<String, Value> encodeDocKeyFields(Document doc, String primaryKeyField, String sortKeyField) {
        Map<String, Value> m = new HashMap<>();
        Predicate<String> addFieldIfExists = fieldName -> {
            Object field = doc.getField(fieldName);
            if (field == null) {
                return false;
            }
            Value value = (Value) encodeValue(field);
            m.put(fieldName, value);
            return true;
        };

        // Primary key field is required
        if (!addFieldIfExists.test(primaryKeyField)) {
            return null;
        }

        // Sort key field is optional
        if (sortKeyField != null && !addFieldIfExists.test(sortKeyField)) {
            return null;
        }

        return m;
    }

    /**
     * Encodes a Java object into a Spanner Value type.
     * This method uses the SpannerEncoder to convert various Java types into their
     * corresponding Spanner Value representations.
     *
     * @param value The Java object to encode
     * @return A Spanner Value representing the encoded object
     */
    public static Object encodeValue(Object value) {
        SpannerEncoder encoder = new SpannerEncoder();
        Codec.encode(value, encoder);
        return encoder.getValue();
    }

    /**
     * Encodes a Document object into a map of Spanner Value types.
     * This method converts all fields in the Document into their corresponding
     * Spanner Value representations.
     *
     * @param doc The Document to encode
     * @return A map of field names to Spanner Values
     */
    public static Map<String, Value> encodeDoc(Document doc) {
        SpannerEncoder encoder = new SpannerEncoder();
        doc.encode(encoder);
        return encoder.getMap();
    }

    /**
     * Decodes a Spanner Struct into a Document object.
     * This method converts the Spanner Struct's fields into a map and uses
     * SpannerDecoder to populate the target Document.
     *
     * @param struct The Spanner Struct to decode
     * @param doc The target Document to populate with decoded values
     */
    public static void decodeDoc(Struct struct, Document doc) {
        Map<String, Value> fields = new HashMap<>();
        for (int i = 0; i < struct.getColumnCount(); i++) {
            String columnName = struct.getColumnType(i).toString();
            Value value = struct.getValue(i);
            if (value != null) {
                fields.put(columnName, value);
            }
        }
        doc.decode(new SpannerDecoder(struct));
    }
} 