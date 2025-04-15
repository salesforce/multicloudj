package com.salesforce.multicloudj.docstore.aws;

import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.codec.Codec;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class AwsCodec {

    public static AttributeValue encodeDocKeyFields(Document doc, String pkey, String skey) {
        Map<String, AttributeValue> m = new HashMap<>();
        Predicate<String> addFieldIfExists = fieldName -> {
            Object field = doc.getField(fieldName);
            if (field == null) {
                return false;
            }
            AttributeValue av = encodeValue(field);
            m.put(fieldName, av);
            return true;
        };

        if (!addFieldIfExists.test(pkey)) {
            return null;
        }

        if (skey != null && !addFieldIfExists.test(skey)) {
            return null;
        }


        return AttributeValue.builder().m(m).build();
    }

    public static AttributeValue encodeValue(Object value) {
        AwsEncoder encoder = new AwsEncoder();
        Codec.encode(value, encoder);
        return encoder.getAttributeValue();
    }

    public static AttributeValue encodeDoc(Document doc) {
        AwsEncoder encoder = new AwsEncoder();
        doc.encode(encoder);
        return encoder.getAttributeValue();
    }

    public static void decodeDoc(AttributeValue item, Document doc) {
        doc.decode(new AwsDecoder(item));
    }
}
