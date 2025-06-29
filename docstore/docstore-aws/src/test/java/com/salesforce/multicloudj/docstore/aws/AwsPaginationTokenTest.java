package com.salesforce.multicloudj.docstore.aws;

import com.salesforce.multicloudj.docstore.driver.Document;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AwsPaginationTokenTest {

    @Test
    void testDefaultConstructor() {
        AwsPaginationToken token = new AwsPaginationToken();
        assertTrue(token.isEmpty());
        assertNull(token.getExclusiveStartKey());
    }

    @Test
    void testIsEmpty() {
        AwsPaginationToken emptyToken = new AwsPaginationToken();
        assertTrue(emptyToken.isEmpty());

        Map<String, AttributeValue> nonEmptyMap = new HashMap<>();
        nonEmptyMap.put("id", AttributeValue.builder().s("test-id").build());
    }

    @Test
    void testSettersAndGetters() {
        AwsPaginationToken token = new AwsPaginationToken();

        Map<String, AttributeValue> startKey = new HashMap<>();
        startKey.put("id", AttributeValue.builder().s("test-id").build());

        token.setExclusiveStartKey(startKey);
        assertEquals(startKey, token.getExclusiveStartKey());

        token.setExclusiveStartKey(null);
        assertNull(token.getExclusiveStartKey());
        assertTrue(token.isEmpty());
    }
}
