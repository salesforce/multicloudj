package com.salesforce.multicloudj.blob.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * Direct unit tests for {@link ObjectRetentionRules#resolveAndValidate}. Tests the shared
 * rules helper in isolation without going through any provider implementation.
 */
class ObjectRetentionRulesTest {

  private static final Instant INITIAL = Instant.parse("2030-01-01T00:00:00Z");
  private static final Instant LATER = Instant.parse("2030-06-01T00:00:00Z");
  private static final Instant EARLIER = Instant.parse("2029-06-01T00:00:00Z");

  private ObjectRetentionConfig cfg(RetentionMode mode, Instant date, Boolean bypass) {
    return ObjectRetentionConfig.builder()
        .mode(mode)
        .retainUntilDate(date)
        .bypassGovernanceRetention(bypass)
        .build();
  }

  // ---- No current retention -------------------------------------------------------

  @Test
  void noCurrentRetention_throws() {
    FailedPreconditionException ex =
        assertThrows(
            FailedPreconditionException.class,
            () ->
                ObjectRetentionRules.resolveAndValidate(
                    null, null, cfg(RetentionMode.GOVERNANCE, LATER, null)));
    assertEquals(ObjectRetentionRules.NO_CURRENT_RETENTION_MSG, ex.getMessage());
  }

  // ---- GOVERNANCE mode -------------------------------------------------------

  @Test
  void governance_extending_succeeds() {
    RetentionMode result =
        ObjectRetentionRules.resolveAndValidate(
            RetentionMode.GOVERNANCE, INITIAL, cfg(RetentionMode.GOVERNANCE, LATER, null));
    assertEquals(RetentionMode.GOVERNANCE, result);
  }

  @Test
  void governance_shortening_withBypass_succeeds() {
    RetentionMode result =
        ObjectRetentionRules.resolveAndValidate(
            RetentionMode.GOVERNANCE, LATER, cfg(RetentionMode.GOVERNANCE, EARLIER, Boolean.TRUE));
    assertEquals(RetentionMode.GOVERNANCE, result);
  }

  @Test
  void governance_shortening_withoutBypass_throws() {
    FailedPreconditionException ex =
        assertThrows(
            FailedPreconditionException.class,
            () ->
                ObjectRetentionRules.resolveAndValidate(
                    RetentionMode.GOVERNANCE,
                    LATER,
                    cfg(RetentionMode.GOVERNANCE, EARLIER, Boolean.FALSE)));
    assertEquals(ObjectRetentionRules.GOVERNANCE_SHORTEN_NO_BYPASS_MSG, ex.getMessage());
  }

  @Test
  void governance_shortening_bypassNull_throws() {
    assertThrows(
        FailedPreconditionException.class,
        () ->
            ObjectRetentionRules.resolveAndValidate(
                RetentionMode.GOVERNANCE, LATER, cfg(RetentionMode.GOVERNANCE, EARLIER, null)));
  }

  @Test
  void governance_sameDate_notShortening_succeeds() {
    RetentionMode result =
        ObjectRetentionRules.resolveAndValidate(
            RetentionMode.GOVERNANCE, INITIAL, cfg(RetentionMode.GOVERNANCE, INITIAL, null));
    assertEquals(RetentionMode.GOVERNANCE, result);
  }

  // ---- COMPLIANCE mode -------------------------------------------------------

  @Test
  void compliance_extending_succeeds() {
    RetentionMode result =
        ObjectRetentionRules.resolveAndValidate(
            RetentionMode.COMPLIANCE, INITIAL, cfg(RetentionMode.COMPLIANCE, LATER, null));
    assertEquals(RetentionMode.COMPLIANCE, result);
  }

  @Test
  void compliance_shortening_throws_evenWithBypass() {
    FailedPreconditionException ex =
        assertThrows(
            FailedPreconditionException.class,
            () ->
                ObjectRetentionRules.resolveAndValidate(
                    RetentionMode.COMPLIANCE,
                    LATER,
                    cfg(RetentionMode.COMPLIANCE, EARLIER, Boolean.TRUE)));
    assertEquals(ObjectRetentionRules.COMPLIANCE_SHORTEN_MSG, ex.getMessage());
  }

  @Test
  void compliance_shortening_withoutBypass_throws() {
    assertThrows(
        FailedPreconditionException.class,
        () ->
            ObjectRetentionRules.resolveAndValidate(
                RetentionMode.COMPLIANCE, LATER, cfg(RetentionMode.COMPLIANCE, EARLIER, null)));
  }

  // ---- Mode transitions -------------------------------------------------------

  @Test
  void modeUpgrade_governanceToCompliance_withBypass_succeeds() {
    RetentionMode result =
        ObjectRetentionRules.resolveAndValidate(
            RetentionMode.GOVERNANCE, INITIAL, cfg(RetentionMode.COMPLIANCE, LATER, Boolean.TRUE));
    assertEquals(RetentionMode.COMPLIANCE, result);
  }

  @Test
  void modeUpgrade_governanceToCompliance_withoutBypass_throws() {
    FailedPreconditionException ex =
        assertThrows(
            FailedPreconditionException.class,
            () ->
                ObjectRetentionRules.resolveAndValidate(
                    RetentionMode.GOVERNANCE,
                    INITIAL,
                    cfg(RetentionMode.COMPLIANCE, LATER, null)));
    assertEquals(ObjectRetentionRules.MODE_UPGRADE_NO_BYPASS_MSG, ex.getMessage());
  }

  @Test
  void modeDowngrade_complianceToGovernance_throws() {
    FailedPreconditionException ex =
        assertThrows(
            FailedPreconditionException.class,
            () ->
                ObjectRetentionRules.resolveAndValidate(
                    RetentionMode.COMPLIANCE,
                    INITIAL,
                    cfg(RetentionMode.GOVERNANCE, LATER, Boolean.TRUE)));
    assertEquals(ObjectRetentionRules.MODE_DOWNGRADE_MSG, ex.getMessage());
  }

  // ---- Null mode (preserve current) -------------------------------------------------------

  @Test
  void modeNull_preservesGovernance() {
    RetentionMode result =
        ObjectRetentionRules.resolveAndValidate(
            RetentionMode.GOVERNANCE, INITIAL, cfg(null, LATER, null));
    assertEquals(RetentionMode.GOVERNANCE, result);
  }

  @Test
  void modeNull_preservesCompliance() {
    RetentionMode result =
        ObjectRetentionRules.resolveAndValidate(
            RetentionMode.COMPLIANCE, INITIAL, cfg(null, LATER, null));
    assertEquals(RetentionMode.COMPLIANCE, result);
  }

  @Test
  void modeNull_shorteningGovernance_withBypass_succeeds() {
    RetentionMode result =
        ObjectRetentionRules.resolveAndValidate(
            RetentionMode.GOVERNANCE, LATER, cfg(null, EARLIER, Boolean.TRUE));
    assertEquals(RetentionMode.GOVERNANCE, result);
  }

  @Test
  void modeNull_shorteningCompliance_throws() {
    assertThrows(
        FailedPreconditionException.class,
        () ->
            ObjectRetentionRules.resolveAndValidate(
                RetentionMode.COMPLIANCE, LATER, cfg(null, EARLIER, Boolean.TRUE)));
  }
}
