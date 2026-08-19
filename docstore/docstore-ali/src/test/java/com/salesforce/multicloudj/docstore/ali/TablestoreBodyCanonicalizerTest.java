package com.salesforce.multicloudj.docstore.ali;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.Condition;
import com.alicloud.openservices.tablestore.model.DeleteRowRequest;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.github.tomakehurst.wiremock.matching.EqualToJsonPattern;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/** Verifies the canonicalizer makes column order irrelevant while preserving content. */
class TablestoreBodyCanonicalizerTest {

  private static byte[] putBody(String table, String pkName, String pkVal, String[] order) {
    PrimaryKey pk =
        PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(pkName, PrimaryKeyValue.fromString(pkVal))
            .build();
    RowPutChange change = new RowPutChange(table, pk);
    for (String col : order) {
      switch (col) {
        case "i":
          change.addColumn("i", ColumnValue.fromLong(121));
          break;
        case "b":
          change.addColumn("b", ColumnValue.fromBoolean(true));
          break;
        case "f":
          change.addColumn("f", ColumnValue.fromDouble(12.66));
          break;
        case "bytes":
          change.addColumn("bytes", ColumnValue.fromString("randomString"));
          break;
        case "DocstoreRevision":
          change.addColumn("DocstoreRevision", ColumnValue.fromString("123"));
          break;
        default:
          throw new IllegalArgumentException(col);
      }
    }
    return OTSProtocolBuilder.buildPutRowRequest(new PutRowRequest(change)).toByteArray();
  }

  @Test
  void differentColumnOrderCanonicalizesEqual() {
    // Same table, PK, columns, values -- but added in two different orders.
    byte[] a =
        putBody(
            "docstore_test_1",
            "pName",
            "LeoPut",
            new String[] {"DocstoreRevision", "f", "b", "bytes", "i"});
    byte[] b =
        putBody(
            "docstore_test_1",
            "pName",
            "LeoPut",
            new String[] {"DocstoreRevision", "i", "b", "bytes", "f"});

    // Raw wire bytes differ (this is exactly the flakiness we saw)...
    assertFalse(Arrays.equals(a, b), "raw bodies should differ due to column order");

    // ...and the canonical JSON forms match semantically under equalToJson (which ignores object
    // key order). Note: the raw JSON bytes need NOT be byte-identical -- order-tolerance comes from
    // the matcher, not from a byte-canonical output.
    String ca =
        new String(TablestoreBodyCanonicalizer.canonicalize("/PutRow", a), StandardCharsets.UTF_8);
    String cb =
        new String(TablestoreBodyCanonicalizer.canonicalize("/PutRow", b), StandardCharsets.UTF_8);
    assertTrue(
        new EqualToJsonPattern(ca, true, false).match(cb).isExactMatch(),
        "canonical JSON forms must match regardless of column order");
  }

  @Test
  void differentValuesCanonicalizeDifferent() {
    byte[] a =
        putBody("docstore_test_1", "pName", "LeoPut", new String[] {"i", "b", "f", "bytes"});
    // Different partition key value -> must NOT collapse to the same canonical form.
    byte[] b =
        putBody("docstore_test_1", "pName", "OtherKey", new String[] {"i", "b", "f", "bytes"});

    byte[] ca = TablestoreBodyCanonicalizer.canonicalize("/PutRow", a);
    byte[] cb = TablestoreBodyCanonicalizer.canonicalize("/PutRow", b);
    assertFalse(Arrays.equals(ca, cb), "different PK must yield different canonical forms");
  }

  @Test
  void recordAndReplayPairingMatchesAcrossColumnOrder() {
    // Simulates the full harness pairing: the record transformer stores a binaryEqualTo built from
    // the canonical form of the recorded body; the replay filter canonicalizes the incoming request
    // before matching. Different column order on each side must still match.
    byte[] recordedRaw =
        putBody(
            "docstore_test_1",
            "pName",
            "LeoPut",
            new String[] {"DocstoreRevision", "f", "b", "bytes", "i"});
    byte[] replayRaw =
        putBody(
            "docstore_test_1",
            "pName",
            "LeoPut",
            new String[] {"i", "bytes", "b", "f", "DocstoreRevision"});

    // Record side: stub matcher is equalToJson(canonical(recorded)).
    String recordedCanonicalJson =
        new String(
            TablestoreBodyCanonicalizer.canonicalize("/PutRow", recordedRaw),
            StandardCharsets.UTF_8);
    EqualToJsonPattern stub = new EqualToJsonPattern(recordedCanonicalJson, true, false);
    // Replay side: incoming request body is canonicalized before matching.
    String replayCanonicalJson =
        new String(
            TablestoreBodyCanonicalizer.canonicalize("/PutRow", replayRaw), StandardCharsets.UTF_8);

    assertTrue(
        stub.match(replayCanonicalJson).isExactMatch(),
        "canonicalized replay body must match the canonicalized recorded stub");
  }

  @Test
  void malformedBodyOnRecognizedUrlReturnsOriginal() {
    // A body that is sent to /PutRow but lacks the mandatory row field (e.g. it contains only
    // unknown fields) must be returned unchanged, not collapsed into {"op":"PutRow","table":""}
    // which would cause unrelated malformed requests to match the same WireMock stub.
    // Wire-encode an unknown field (field 15, bytes "abc") — no row or table_name fields.
    byte[] malformed = new byte[] {(byte) 0x7a, 0x03, 0x61, 0x62, 0x63};
    byte[] out = TablestoreBodyCanonicalizer.canonicalize("/PutRow", malformed);
    assertArrayEquals(
        malformed, out, "malformed body missing row field must be returned unchanged");
  }

  @Test
  void nonCanonicalizableUrlReturnsOriginal() {
    byte[] body = putBody("docstore_test_1", "pName", "LeoPut", new String[] {"i"});
    byte[] out = TablestoreBodyCanonicalizer.canonicalize("/SQLQuery", body);
    assertArrayEquals(body, out, "non-write URLs must be returned unchanged");
  }

  // Builds a DeleteRow request body for the given table, pk name+value, and optional condition.
  private static byte[] deleteBody(
      String table, String pkName, String pkVal, Condition condition) {
    PrimaryKey pk =
        PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(pkName, PrimaryKeyValue.fromString(pkVal))
            .build();
    RowDeleteChange change = new RowDeleteChange(table, pk);
    if (condition != null) {
      change.setCondition(condition);
    }
    return OTSProtocolBuilder.buildDeleteRowRequest(new DeleteRowRequest(change)).toByteArray();
  }

  @Test
  void deleteRowCanonicalizesCorrectly() {
    // A DeleteRow carries only primary-key cells (no data cells). Verify the canonicalizer extracts
    // table + pk correctly and produces a valid canonical JSON.
    byte[] body = deleteBody("docstore_test_2", "Game", "test-game", null);

    String canonical =
        new String(
            TablestoreBodyCanonicalizer.canonicalize("/DeleteRow", body), StandardCharsets.UTF_8);

    assertTrue(canonical.contains("\"DeleteRow\""), "op must be DeleteRow");
    assertTrue(canonical.contains("\"docstore_test_2\""), "table name must be present");
    assertTrue(canonical.contains("\"test-game\""), "pk value must be present");
    // The SDK serialises a condition field even for unconditional deletes; the canonical form
    // must include it so that bare-delete stubs differ from revision-conditional ones.
    assertTrue(
        canonical.contains("\"condition\""),
        "bare delete must still carry the condition field");
  }

  @Test
  void conditionFieldDistinguishesRevisions() {
    // A revision-conditional delete carries a Condition with a SingleColumnValueCondition on the
    // revision column. Deletes with different revision conditions must produce different canonical
    // forms so that WireMock stubs from different revision epochs do not match each other.
    SingleColumnValueCondition revCondA =
        new SingleColumnValueCondition(
            "DocstoreRevision",
            SingleColumnValueCondition.CompareOperator.EQUAL,
            ColumnValue.fromString("rev-abc"));
    revCondA.setPassIfMissing(false);
    Condition conditionA = new Condition();
    conditionA.setColumnCondition(revCondA);

    SingleColumnValueCondition revCondB =
        new SingleColumnValueCondition(
            "DocstoreRevision",
            SingleColumnValueCondition.CompareOperator.EQUAL,
            ColumnValue.fromString("rev-xyz"));
    revCondB.setPassIfMissing(false);
    Condition conditionB = new Condition();
    conditionB.setColumnCondition(revCondB);

    byte[] bodyA = deleteBody("docstore_test_1", "pName", "LeoDel", conditionA);
    byte[] bodyB = deleteBody("docstore_test_1", "pName", "LeoDel", conditionB);

    String canonicalA =
        new String(
            TablestoreBodyCanonicalizer.canonicalize("/DeleteRow", bodyA), StandardCharsets.UTF_8);

    // The condition bytes must be present in the canonical form.
    assertTrue(
        canonicalA.contains("\"condition\""), "condition field must appear in canonical form");

    // Deletes with different revision conditions must produce different canonical forms.
    assertFalse(
        Arrays.equals(
            TablestoreBodyCanonicalizer.canonicalize("/DeleteRow", bodyA),
            TablestoreBodyCanonicalizer.canonicalize("/DeleteRow", bodyB)),
        "different revision conditions must yield different canonical forms");

    // The primary scenario: a bare (unconditional) delete must differ from a revision-conditional
    // delete. This is what prevents stale-revision stubs from matching unconditional deletes.
    byte[] bareBody = deleteBody("docstore_test_1", "pName", "LeoDel", null);
    assertFalse(
        Arrays.equals(
            TablestoreBodyCanonicalizer.canonicalize("/DeleteRow", bareBody),
            TablestoreBodyCanonicalizer.canonicalize("/DeleteRow", bodyA)),
        "bare delete must differ from revision-conditional delete");
  }
}
