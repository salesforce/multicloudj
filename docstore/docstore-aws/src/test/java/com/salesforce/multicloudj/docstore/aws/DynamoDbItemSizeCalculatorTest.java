package com.salesforce.multicloudj.docstore.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamoDbItemSizeCalculatorTest {

  @Test
  void testCalculateItemSize_emptyItem() {
    Map<String, AttributeValue> item = new HashMap<>();
    long size = DynamoDbItemSizeCalculator.calculateItemSize(item);
    assertEquals(0, size);
  }

  @Test
  void testCalculateItemSize_multipleAttributes() {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("12345").build());
    item.put("name", AttributeValue.builder().s("John Doe").build());
    item.put("age", AttributeValue.builder().n("30").build());

    // Expected: "id"(2) + "12345"(5) + "name"(4) + "John Doe"(8) + "age"(3) + number size
    long expectedSize =
        2 + 5 + 4 + 8 + 3 + calculateExpectedNumberSize("30"); // 30 has 2 digits -> 2 bytes

    long actualSize = DynamoDbItemSizeCalculator.calculateItemSize(item);
    assertEquals(expectedSize, actualSize);
  }

  @Test
  void testCalculateAttributeValueSize_list() {
    AttributeValue av =
        AttributeValue.builder()
            .l(
                AttributeValue.builder().s("a").build(),
                AttributeValue.builder().s("bb").build(),
                AttributeValue.builder().s("ccc").build())
            .build();

    Map<String, AttributeValue> item = new HashMap<>();
    item.put("k", av);
    long size = DynamoDbItemSizeCalculator.calculateItemSize(item) - 1;

    // Expected: list overhead (3 + 3 elements) + "a"(1) + "bb"(2) + "ccc"(3) = 12
    assertEquals(12, size);
  }

  @Test
  void testCalculateAttributeValueSize_map() {
    Map<String, AttributeValue> nestedMap = new HashMap<>();
    nestedMap.put("key1", AttributeValue.builder().s("value1").build());
    nestedMap.put("key2", AttributeValue.builder().s("value2").build());

    AttributeValue av = AttributeValue.builder().m(nestedMap).build();

    Map<String, AttributeValue> item = new HashMap<>();
    item.put("k", av);
    long size = DynamoDbItemSizeCalculator.calculateItemSize(item) - 1;

    // Expected: map overhead (3 + 2 entries) + "key1"(4) + "value1"(6) + "key2"(4) + "value2"(6)
    assertEquals(25, size);
  }

  @Test
  void testCalculateAttributeValueSize_numberSet() {
    AttributeValue av = AttributeValue.builder().ns("1", "22", "333").build();

    Map<String, AttributeValue> item = new HashMap<>();
    item.put("k", av);
    long size = DynamoDbItemSizeCalculator.calculateItemSize(item) - 1;

    // Expected: 2 bytes (1 digit) + 2 bytes (2 digits) + 3 bytes (3 digits) = 7
    assertEquals(7, size);
  }

  @Test
  void testCalculateItemSize_atLimit() {
    Map<String, AttributeValue> item = new HashMap<>();

    // Create a string that brings the total item size to exactly 400 KB
    // Account for attribute name "data" (4 bytes)
    int targetSize = DynamoDbItemSizeCalculator.ITEM_SIZE_LIMIT_BYTES;
    int attributeNameSize = "data".getBytes(StandardCharsets.UTF_8).length;
    int stringValueSize = targetSize - attributeNameSize;

    StringBuilder largeString = new StringBuilder(stringValueSize);
    for (int i = 0; i < stringValueSize; i++) {
      largeString.append("a");
    }

    item.put("data", AttributeValue.builder().s(largeString.toString()).build());

    long actualSize = DynamoDbItemSizeCalculator.calculateItemSize(item);
    assertEquals(DynamoDbItemSizeCalculator.ITEM_SIZE_LIMIT_BYTES, actualSize);
  }

  @Test
  void testCalculateItemSize_overLimit() {
    Map<String, AttributeValue> item = new HashMap<>();

    // Create a string that exceeds 400 KB
    int targetSize = DynamoDbItemSizeCalculator.ITEM_SIZE_LIMIT_BYTES + 1000;
    int attributeNameSize = "data".getBytes(StandardCharsets.UTF_8).length;
    int stringValueSize = targetSize - attributeNameSize;

    StringBuilder largeString = new StringBuilder(stringValueSize);
    for (int i = 0; i < stringValueSize; i++) {
      largeString.append("a");
    }

    item.put("data", AttributeValue.builder().s(largeString.toString()).build());

    long actualSize = DynamoDbItemSizeCalculator.calculateItemSize(item);
    assertTrue(actualSize > DynamoDbItemSizeCalculator.ITEM_SIZE_LIMIT_BYTES);
    assertEquals(targetSize, actualSize);
  }

  @Test
  void testNumberSize_formulaCoverage() {
    // 1 digit -> 2 bytes (minimum)
    assertEquals(2, calculateExpectedNumberSize("5"));
    // 3 digits -> 3 bytes (ceil(3/2) + 1)
    assertEquals(3, calculateExpectedNumberSize("123"));
    // Decimal and sign handling
    assertEquals(4, calculateExpectedNumberSize("-123.456"));
    // Scientific notation normalization
    assertEquals(7, calculateExpectedNumberSize("1E10"));
    // Long number still within supported bound
    assertEquals(20, calculateExpectedNumberSize("12345678901234567890123456789012345678"));
  }

  /**
   * Helper method to calculate expected number size following DynamoDB rules: approximately 1 byte
   * per 2 significant digits plus 1 byte.
   */
  private long calculateExpectedNumberSize(String numberString) {
    BigDecimal number = new BigDecimal(numberString);
    String plainString = number.stripTrailingZeros().toPlainString();
    String digitsOnly = plainString.replace("-", "").replace(".", "");
    int significantDigits = digitsOnly.length();
    return Math.max(2, Math.min(39, ((significantDigits + 1L) / 2) + 1));
  }
}
