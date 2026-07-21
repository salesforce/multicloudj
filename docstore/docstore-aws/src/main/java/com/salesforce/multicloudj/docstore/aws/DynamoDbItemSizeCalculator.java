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
 * per 2 significant digits, minimum 1 byte, maximum 38 bytes) - Binary: Raw byte length - Boolean:
 * 1 byte - Null: 1 byte - Lists: Sum of element sizes - Maps: Sum of (attribute name size +
 * attribute value size) for each entry - String sets, number sets, binary sets: Sum of element
 * sizes
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
      long size = 0;
      for (AttributeValue element : value.l()) {
        size += calculateAttributeValueSize(element);
      }
      return size;
    }

    // Map
    if (value.m() != null && !value.m().isEmpty()) {
      long size = 0;
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
   * <p>DynamoDB numbers are variable-length: approximately 1 byte per 2 significant digits, with a
   * minimum of 1 byte and maximum of 38 bytes.
   *
   * @param numberString The number as a string
   * @return The size in bytes
   */
  private static long calculateNumberSize(String numberString) {
    if (numberString == null || numberString.isEmpty()) {
      return 1;
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
      // Minimum 1 byte, maximum 38 bytes
      return Math.max(1, Math.min(38, size));
    } catch (NumberFormatException e) {
      // Fallback for malformed numbers - should not happen with valid DynamoDB AttributeValues
      // This conservative estimate errs on the side of allowing slightly oversized items
      // rather than falsely rejecting valid ones
      return Math.min(38, Math.max(1, numberString.length() / 2));
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
