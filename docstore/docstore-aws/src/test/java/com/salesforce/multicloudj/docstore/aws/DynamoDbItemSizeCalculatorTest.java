package com.salesforce.multicloudj.docstore.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamoDbItemSizeCalculatorTest {

  @Test
  void testCalculateItemSize_emptyItem() {
    Map<String, AttributeValue> item = new HashMap<>();
    long size = DynamoDbItemSizeCalculator.calculateItemSize(item);
    assertEquals(0, size);
  }

  @Test
  void testCalculateItemSize_singleStringAttribute() {
    Map<String, AttributeValue> item = new HashMap<>();
    String attributeName = "name";
    String attributeValue = "test";

    item.put(attributeName, AttributeValue.builder().s(attributeValue).build());

    long expectedSize =
        attributeName.getBytes(StandardCharsets.UTF_8).length
            + attributeValue.getBytes(StandardCharsets.UTF_8).length;

    long actualSize = DynamoDbItemSizeCalculator.calculateItemSize(item);
    assertEquals(expectedSize, actualSize);
  }

  @Test
  void testCalculateItemSize_multipleAttributes() {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("12345").build());
    item.put("name", AttributeValue.builder().s("John Doe").build());
    item.put("age", AttributeValue.builder().n("30").build());

    // Expected: "id"(2) + "12345"(5) + "name"(4) + "John Doe"(8) + "age"(3) + number size
    long expectedSize =
        2 + 5 + 4 + 8 + 3 + calculateExpectedNumberSize("30"); // 30 has 2 digits -> 1 byte

    long actualSize = DynamoDbItemSizeCalculator.calculateItemSize(item);
    assertEquals(expectedSize, actualSize);
  }

  @Test
  void testCalculateAttributeValueSize_string() {
    String value = "hello";
    AttributeValue av = AttributeValue.builder().s(value).build();

    long expectedSize = value.getBytes(StandardCharsets.UTF_8).length;

    // Use reflection to call private method or test via public method
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("test", av);
    long actualSize =
        DynamoDbItemSizeCalculator.calculateItemSize(item)
            - "test".getBytes(StandardCharsets.UTF_8).length;

    assertEquals(expectedSize, actualSize);
  }

  @Test
  void testCalculateAttributeValueSize_number() {
    // Small number: 1 digit -> 1 byte
    AttributeValue av1 = AttributeValue.builder().n("5").build();
    Map<String, AttributeValue> item1 = new HashMap<>();
    item1.put("k", av1);
    long size1 = DynamoDbItemSizeCalculator.calculateItemSize(item1) - 1; // subtract key size
    assertEquals(1, size1);

    // Larger number: 10 digits -> 5 bytes
    AttributeValue av2 = AttributeValue.builder().n("1234567890").build();
    Map<String, AttributeValue> item2 = new HashMap<>();
    item2.put("k", av2);
    long size2 = DynamoDbItemSizeCalculator.calculateItemSize(item2) - 1;
    assertEquals(5, size2);
  }

  @Test
  void testCalculateAttributeValueSize_binary() {
    byte[] data = new byte[] {1, 2, 3, 4, 5};
    AttributeValue av = AttributeValue.builder().b(SdkBytes.fromByteArray(data)).build();

    Map<String, AttributeValue> item = new HashMap<>();
    item.put("k", av);
    long size = DynamoDbItemSizeCalculator.calculateItemSize(item) - 1; // subtract key size

    assertEquals(data.length, size);
  }

  @Test
  void testCalculateAttributeValueSize_boolean() {
    AttributeValue av = AttributeValue.builder().bool(true).build();

    Map<String, AttributeValue> item = new HashMap<>();
    item.put("k", av);
    long size = DynamoDbItemSizeCalculator.calculateItemSize(item) - 1;

    assertEquals(1, size);
  }

  @Test
  void testCalculateAttributeValueSize_null() {
    AttributeValue av = AttributeValue.builder().nul(true).build();

    Map<String, AttributeValue> item = new HashMap<>();
    item.put("k", av);
    long size = DynamoDbItemSizeCalculator.calculateItemSize(item) - 1;

    assertEquals(1, size);
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

    // Expected: 1 + 2 + 3 = 6
    assertEquals(6, size);
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

    // Expected: "key1"(4) + "value1"(6) + "key2"(4) + "value2"(6) = 20
    assertEquals(20, size);
  }

  @Test
  void testCalculateAttributeValueSize_stringSet() {
    AttributeValue av = AttributeValue.builder().ss("apple", "banana", "cherry").build();

    Map<String, AttributeValue> item = new HashMap<>();
    item.put("k", av);
    long size = DynamoDbItemSizeCalculator.calculateItemSize(item) - 1;

    // Expected: 5 + 6 + 6 = 17
    assertEquals(17, size);
  }

  @Test
  void testCalculateAttributeValueSize_numberSet() {
    AttributeValue av = AttributeValue.builder().ns("1", "22", "333").build();

    Map<String, AttributeValue> item = new HashMap<>();
    item.put("k", av);
    long size = DynamoDbItemSizeCalculator.calculateItemSize(item) - 1;

    // Expected: 1 byte (1 digit) + 1 byte (2 digits) + 2 bytes (3 digits) = 4
    assertEquals(4, size);
  }

  @Test
  void testCalculateAttributeValueSize_binarySet() {
    byte[] data1 = new byte[] {1, 2};
    byte[] data2 = new byte[] {3, 4, 5};

    AttributeValue av =
        AttributeValue.builder()
            .bs(SdkBytes.fromByteArray(data1), SdkBytes.fromByteArray(data2))
            .build();

    Map<String, AttributeValue> item = new HashMap<>();
    item.put("k", av);
    long size = DynamoDbItemSizeCalculator.calculateItemSize(item) - 1;

    // Expected: 2 + 3 = 5
    assertEquals(5, size);
  }

  @Test
  void testCalculateItemSize_complexNestedStructure() {
    Map<String, AttributeValue> item = new HashMap<>();

    // Top-level attributes
    item.put("id", AttributeValue.builder().s("user123").build());
    item.put("active", AttributeValue.builder().bool(true).build());

    // Nested map
    Map<String, AttributeValue> address = new HashMap<>();
    address.put("street", AttributeValue.builder().s("123 Main St").build());
    address.put("city", AttributeValue.builder().s("Seattle").build());
    item.put("address", AttributeValue.builder().m(address).build());

    // List of strings
    item.put(
        "tags",
        AttributeValue.builder()
            .l(
                AttributeValue.builder().s("tag1").build(),
                AttributeValue.builder().s("tag2").build())
            .build());

    long actualSize = DynamoDbItemSizeCalculator.calculateItemSize(item);

    // Calculate expected size:
    // "id"(2) + "user123"(7) = 9
    // "active"(6) + 1 (boolean) = 7
    // "address"(7) + ["street"(6) + "123 Main St"(11) + "city"(4) + "Seattle"(7)] = 7 + 28 = 35
    // "tags"(4) + ["tag1"(4) + "tag2"(4)] = 4 + 8 = 12
    // Total: 9 + 7 + 35 + 12 = 63
    assertEquals(63, actualSize);
  }

  @Test
  void testCalculateItemSize_utf8Characters() {
    Map<String, AttributeValue> item = new HashMap<>();

    // UTF-8 multi-byte characters
    String emoji = "😀"; // 4 bytes in UTF-8
    String japanese = "こんにちは"; // 15 bytes in UTF-8

    item.put("emoji", AttributeValue.builder().s(emoji).build());
    item.put("text", AttributeValue.builder().s(japanese).build());

    long actualSize = DynamoDbItemSizeCalculator.calculateItemSize(item);

    // Expected: "emoji"(5) + 4 + "text"(4) + 15 = 28
    assertEquals(28, actualSize);
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
  void testNumberSize_smallNumbers() {
    // 1 digit -> 1 byte
    assertEquals(1, calculateExpectedNumberSize("5"));
    assertEquals(1, calculateExpectedNumberSize("9"));

    // 2 digits -> 1 byte
    assertEquals(1, calculateExpectedNumberSize("42"));
    assertEquals(1, calculateExpectedNumberSize("99"));

    // 3 digits -> 2 bytes
    assertEquals(2, calculateExpectedNumberSize("123"));
    assertEquals(2, calculateExpectedNumberSize("999"));
  }

  @Test
  void testNumberSize_largeNumbers() {
    // 10 digits -> 5 bytes
    assertEquals(5, calculateExpectedNumberSize("1234567890"));

    // 20 digits -> 10 bytes
    assertEquals(10, calculateExpectedNumberSize("12345678901234567890"));

    // 38 digits -> max 38 bytes (DynamoDB limit)
    String maxDigits = "12345678901234567890123456789012345678";
    assertEquals(19, calculateExpectedNumberSize(maxDigits));
  }

  @Test
  void testNumberSize_decimal() {
    // Decimal: count significant digits only
    assertEquals(2, calculateExpectedNumberSize("12.34")); // 4 significant digits -> 2 bytes
    assertEquals(3, calculateExpectedNumberSize("123.456")); // 6 significant digits -> 3 bytes
  }

  @Test
  void testNumberSize_negative() {
    // Negative sign doesn't count toward size
    assertEquals(1, calculateExpectedNumberSize("-5")); // 1 digit -> 1 byte
    assertEquals(2, calculateExpectedNumberSize("-123")); // 3 digits -> 2 bytes
  }

  @Test
  void testNumberSize_scientific() {
    // Scientific notation - expands to full number
    // 1E10 = 10000000000 (11 digits) -> 6 bytes
    assertEquals(6, calculateExpectedNumberSize("1E10"));
    // 1.23E10 = 12300000000 (11 digits) -> 6 bytes
    assertEquals(6, calculateExpectedNumberSize("1.23E10"));
  }

  @Test
  void testNumberSize_trailingZeros() {
    // stripTrailingZeros only removes trailing zeros after decimal point
    // 100 stays as 100 (3 digits) -> 2 bytes
    assertEquals(2, calculateExpectedNumberSize("100"));
    // 1.2300 becomes 1.23 (3 significant digits) -> 2 bytes
    assertEquals(2, calculateExpectedNumberSize("1.2300"));
  }

  /**
   * Helper method to calculate expected number size following DynamoDB rules: approximately 1 byte
   * per 2 significant digits.
   */
  private long calculateExpectedNumberSize(String numberString) {
    BigDecimal number = new BigDecimal(numberString);
    String plainString = number.stripTrailingZeros().toPlainString();
    String digitsOnly = plainString.replace("-", "").replace(".", "");
    int significantDigits = digitsOnly.length();
    return Math.max(1, Math.min(38, (significantDigits + 1) / 2));
  }
}
