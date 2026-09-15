package com.salesforce.multicloudj.docstore.aws;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Calculates the size of a DynamoDB item following AWS's exact rules.
 *
 * <p>DynamoDB item size calculation rules (per AWS documentation): - Attribute name: UTF-8 encoded
 * byte length - Strings: UTF-8 encoded byte length - Numbers: Variable length (approximately 1 byte
 * per 2 significant digits plus 1 byte, minimum 2 bytes, maximum 39 bytes) - Binary: Raw byte
 * length - Boolean: 1 byte - Null: 1 byte - Lists: 3 bytes container overhead plus 1 byte per
 * element plus element sizes - Maps: 3 bytes container overhead plus 1 byte per entry plus
 * (attribute name size + attribute value size) for each entry - String sets, number sets, binary
 * sets: Sum of element sizes
 *
 * <p>Reference: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html
 */
public class DynamoDbItemSizeCalculator {

  /** DynamoDB's hard limit for item size in bytes (400 KB). */
  public static final int ITEM_SIZE_LIMIT_BYTES = 409600; // 400 KB

  /** UTF-8 charset for string encoding. */
  private static final Charset UTF_8 = StandardCharsets.UTF_8;

  /**
   * Calculates the size of a DynamoDB item in bytes.
   *
   * @param item The DynamoDB item as a map of AttributeValue
   * @return The size in bytes
   */
  public static long calculateItemSize(Map<String, AttributeValue> item) {
    long totalSize = 0;
    for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
      // Attribute name size
      totalSize += getUtf8ByteLength(entry.getKey());
      // Attribute value size
      totalSize += calculateAttributeValueSize(entry.getValue());
    }
    return totalSize;
  }

  /**
   * Calculates the size of a single AttributeValue in bytes.
   *
   * @param value The AttributeValue
   * @return The size in bytes
   */
  private static long calculateAttributeValueSize(AttributeValue value) {
    if (value == null) {
      return 0;
    }

    // String
    if (value.s() != null) {
      return getUtf8ByteLength(value.s());
    }

    // Number
    if (value.n() != null) {
      return calculateNumberSize(value.n());
    }

    // Binary
    if (value.b() != null) {
      return value.b().asByteArray().length;
    }

    // Boolean
    if (value.bool() != null) {
      return 1;
    }

    // Null
    if (Boolean.TRUE.equals(value.nul())) {
      return 1;
    }

    // List
    if (value.l() != null && !value.l().isEmpty()) {
      long size = 3 + value.l().size();
      for (AttributeValue element : value.l()) {
        size += calculateAttributeValueSize(element);
      }
      return size;
    }

    // Map
    if (value.m() != null && !value.m().isEmpty()) {
      long size = 3 + value.m().size();
      for (Map.Entry<String, AttributeValue> entry : value.m().entrySet()) {
        size += getUtf8ByteLength(entry.getKey());
        size += calculateAttributeValueSize(entry.getValue());
      }
      return size;
    }

    // String Set
    if (value.ss() != null && !value.ss().isEmpty()) {
      long size = 0;
      for (String s : value.ss()) {
        size += getUtf8ByteLength(s);
      }
      return size;
    }

    // Number Set
    if (value.ns() != null && !value.ns().isEmpty()) {
      long size = 0;
      for (String n : value.ns()) {
        size += calculateNumberSize(n);
      }
      return size;
    }

    // Binary Set
    if (value.bs() != null && !value.bs().isEmpty()) {
      long size = 0;
      for (SdkBytes b : value.bs()) {
        size += b.asByteArray().length;
      }
      return size;
    }

    return 0;
  }

  /**
   * Calculates the size of a DynamoDB number in bytes.
   *
   * <p>DynamoDB numbers are variable-length: approximately 1 byte per 2 significant digits plus 1
   * byte, with a minimum of 2 bytes and maximum of 39 bytes.
   *
   * @param numberString The number as a string
   * @return The size in bytes
   */
  private static long calculateNumberSize(String numberString) {
    if (numberString == null || numberString.isEmpty()) {
      return 2;
    }

    try {
      BigDecimal number = new BigDecimal(numberString);
      // Get the number of significant digits
      String plainString = number.stripTrailingZeros().toPlainString();
      // Remove sign and decimal point for counting digits
      String digitsOnly = plainString.replace("-", "").replace(".", "");
      int significantDigits = digitsOnly.length();

      // DynamoDB uses approximately 1 byte per 2 significant digits
      long size = (significantDigits + 1) / 2;
      // Plus 1 byte, minimum 2 bytes, maximum 39 bytes
      return Math.max(2, Math.min(39, size + 1));
    } catch (NumberFormatException e) {
      // Fallback for malformed numbers - should not happen with valid DynamoDB AttributeValues.
      // Use the same sizing formula so behavior remains consistent with the primary path.
      long size = (numberString.length() + 1L) / 2;
      return Math.max(2, Math.min(39, size + 1));
    }
  }

  /**
   * Gets the UTF-8 byte length of a string.
   *
   * @param str The string
   * @return The byte length
   */
  private static long getUtf8ByteLength(String str) {
    if (str == null) {
      return 0;
    }
    return str.getBytes(UTF_8).length;
  }
}
