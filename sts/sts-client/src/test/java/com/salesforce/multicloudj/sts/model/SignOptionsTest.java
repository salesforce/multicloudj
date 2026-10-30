package com.salesforce.multicloudj.sts.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SignOptionsTest {

  @Test
  void customHeaderNamesAreLowercased() {
    SignOptions options =
        SignOptions.builder().withCustomHeader("X-Vault-AWS-IAM-Server-ID", "v").build();

    assertEquals(Map.of("x-vault-aws-iam-server-id", "v"), options.getCustomHeaders());
  }

  @Test
  void insertionOrderIsPreserved() {
    SignOptions options =
        SignOptions.builder()
            .withCustomHeader("X-B", "1")
            .withCustomHeader("X-A", "2")
            .withCustomHeader("X-C", "3")
            .build();

    assertEquals(List.of("x-b", "x-a", "x-c"), List.copyOf(options.getCustomHeaders().keySet()));
  }

  @Test
  void caseInsensitiveDuplicateNamesAreRejected() {
    SignOptions.Builder builder =
        SignOptions.builder().withCustomHeader("X-A", "1").withCustomHeader("x-a", "2");

    assertThrows(InvalidArgumentException.class, builder::build);
  }

  @Test
  void blankNameOrNullValueIsRejected() {
    SignOptions.Builder blankName = SignOptions.builder().withCustomHeader("  ", "v");
    SignOptions.Builder nullValue = SignOptions.builder().withCustomHeader("x-a", null);

    assertThrows(InvalidArgumentException.class, blankName::build);
    assertThrows(InvalidArgumentException.class, nullValue::build);
  }
}
