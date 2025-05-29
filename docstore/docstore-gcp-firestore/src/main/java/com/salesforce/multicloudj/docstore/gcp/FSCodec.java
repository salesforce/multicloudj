package com.salesforce.multicloudj.docstore.gcp;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.Value;
import com.salesforce.multicloudj.docstore.driver.codec.Codec;

import java.util.Map;

/**
 * FSCodec provides utility methods for encoding and decoding data between Java native types
 * and Google Cloud Firestore's protobuf-based value types.
 * 
 * This class handles the conversion of:
 * - Java objects to Firestore Value types for storage
 * - Firestore Document snapshots to Java objects for retrieval
 * - Document fields to Firestore Value types for updates
 */
public class FSCodec {
    /**
     * Encodes a Java object into a Firestore Value type.
     * This method uses the FSEncoder to convert various Java types into their
     * corresponding Firestore Value representations.
     *
     * @param value The Java object to encode
     * @return A Firestore Value representing the encoded object
     */
    public static Value encodeValue(Object value) {
        FSEncoder encoder = new FSEncoder();
        Codec.encode(value, encoder);
        return encoder.getValue();
    }

    /**
     * Encodes a Document object into a map of Firestore Value types.
     * This method converts all fields in the Document into their corresponding
     * Firestore Value representations.
     *
     * @param doc The Document to encode
     * @return A map of field names to Firestore Values
     */
    public static Map<String, Value> encodeDoc(com.salesforce.multicloudj.docstore.driver.Document doc) {
        FSEncoder encoder = new FSEncoder();
        doc.encode(encoder);
        return encoder.getMap();
    }

    /**
     * Decodes a Firestore Document snapshot into a Document object.
     * This method converts the Firestore Document's fields into a map value,
     * uses FSDecoder to populate the target Document, and sets the revision field
     * using the document's update time if specified.
     *
     * @param fsDocument The Firestore Document snapshot to decode
     * @param doc The target Document to populate with decoded values
     * @param revisionField The name of the revision field, or null if not used
     */
    public static void decodeDoc(Document fsDocument, com.salesforce.multicloudj.docstore.driver.Document doc, String revisionField) {
        Value mapValue = Value.newBuilder()
            .setMapValue(com.google.firestore.v1.MapValue.newBuilder()
                .putAllFields(fsDocument.getFieldsMap())
                .build())
            .build();
        doc.decode(new FSDecoder(mapValue));
        
        // Set the revision field in the document, if it exists, to the update time
        if (revisionField != null && !revisionField.isEmpty() && 
            doc.hasField(revisionField) && fsDocument.hasUpdateTime()) {
            // Convert update time to string representation (seconds since epoch)
            String updateTime = String.valueOf(fsDocument.getUpdateTime().getSeconds());
            doc.setField(revisionField, updateTime);
        }
    }
} 