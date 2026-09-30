package com.salesforce.multicloudj.blob.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import org.junit.jupiter.api.Test;

/** Tests for {@link ObjectRetentionConfig}. */
class ObjectRetentionConfigTest {

  @Test
  void builderProducesConfigWithAllFields() {
    Instant date = Instant.parse("2030-12-31T00:00:00Z");

    ObjectRetentionConfig cfg =
        ObjectRetentionConfig.builder()
            .mode(RetentionMode.GOVERNANCE)
            .retainUntilDate(date)
            .bypassGovernanceRetention(Boolean.TRUE)
            .build();

    assertEquals(RetentionMode.GOVERNANCE, cfg.getMode());
    assertEquals(date, cfg.getRetainUntilDate());
    assertEquals(Boolean.TRUE, cfg.getBypassGovernanceRetention());
  }

  @Test
  void modeIsNullableSentinelMeaningPreserveCurrent() {
    // null mode is a valid sentinel meaning "preserve the object's current retention mode".
    // Provider impls translate this to: fetch current mode, use it.
    ObjectRetentionConfig cfg =
        ObjectRetentionConfig.builder()
            .retainUntilDate(Instant.parse("2030-12-31T00:00:00Z"))
            .build();

    assertNull(cfg.getMode());
  }

  @Test
  void bypassGovernanceRetentionIsNullableTriState() {
    // Boolean (not boolean) preserves "not specified" — providers treat null as no-bypass.
    ObjectRetentionConfig cfg =
        ObjectRetentionConfig.builder()
            .retainUntilDate(Instant.parse("2030-12-31T00:00:00Z"))
            .build();

    assertNull(cfg.getBypassGovernanceRetention());
  }
}
