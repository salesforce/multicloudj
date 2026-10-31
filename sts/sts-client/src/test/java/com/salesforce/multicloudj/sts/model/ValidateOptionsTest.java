package com.salesforce.multicloudj.sts.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ValidateOptionsTest {

  @Test
  void emptyByDefault() {
    ValidateOptions options = ValidateOptions.builder().build();
    assertTrue(options.getExpectedCustomHeaders().isEmpty());
  }

  @Test
  void addsSingleAndBulkHeaders() {
    Map<String, String> bulk = new LinkedHashMap<>();
    bulk.put("x-a", "1");
    bulk.put("x-b", "2");

    ValidateOptions options =
        ValidateOptions.builder()
            .withExpectedCustomHeader("x-c", "3")
            .withExpectedCustomHeaders(bulk)
            .build();

    Map<String, String> headers = options.getExpectedCustomHeaders();
    assertEquals(3, headers.size());
    assertEquals("1", headers.get("x-a"));
    assertEquals("2", headers.get("x-b"));
    assertEquals("3", headers.get("x-c"));
  }

  @Test
  void nullBulkHeadersIsIgnored() {
    ValidateOptions options =
        ValidateOptions.builder().withExpectedCustomHeaders(null).build();
    assertTrue(options.getExpectedCustomHeaders().isEmpty());
  }

  @Test
  void returnedMapIsUnmodifiable() {
    ValidateOptions options =
        ValidateOptions.builder().withExpectedCustomHeader("x-a", "1").build();
    assertThrows(
        UnsupportedOperationException.class,
        () -> options.getExpectedCustomHeaders().put("x-b", "2"));
  }

  @Test
  void expectedHeaderNamesAreLowercased() {
    ValidateOptions options =
        ValidateOptions.builder().withExpectedCustomHeader("X-Vault-Server-ID", "v").build();

    assertEquals(Map.of("x-vault-server-id", "v"), options.getExpectedCustomHeaders());
  }

  @Test
  void caseInsensitiveDuplicateNamesAreRejected() {
    ValidateOptions.Builder builder =
        ValidateOptions.builder()
            .withExpectedCustomHeader("X-A", "1")
            .withExpectedCustomHeader("x-a", "2");

    assertThrows(InvalidArgumentException.class, builder::build);
  }

  @Test
  void blankNameOrNullValueIsRejected() {
    ValidateOptions.Builder blankName = ValidateOptions.builder().withExpectedCustomHeader("", "v");
    ValidateOptions.Builder nullValue =
        ValidateOptions.builder().withExpectedCustomHeader("x-a", null);

    assertThrows(InvalidArgumentException.class, blankName::build);
    assertThrows(InvalidArgumentException.class, nullValue::build);
  }

  @Test
  void nullNameIsRejected() {
    ValidateOptions.Builder builder = ValidateOptions.builder().withExpectedCustomHeader(null, "v");

    assertThrows(InvalidArgumentException.class, builder::build);
  }
}
