package com.salesforce.multicloudj.docstore.ali;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alicloud.openservices.tablestore.core.protocol.PlainBufferBuilder;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.matching.EqualToJsonPattern;
import com.google.protobuf.CodedOutputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/** Verifies the canonicalizer makes column order irrelevant while preserving content. */
class TablestoreBodyCanonicalizerTest {

  /**
   * Builds a PutRow wire body: outer envelope (fields 1=table_name, 2=row, 3=condition) encoded
   * with {@link CodedOutputStream}; inner row bytes from {@link
   * PlainBufferBuilder#buildRowPutChangeWithHeader}. A minimal condition field is always included
   * because the real SDK always emits one and the canonicalizer requires all three fields.
   */
  private static byte[] putBody(String table, String pkName, String pkVal, String[] order)
      throws Exception {
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
    return encodeWriteRequest(
        table,
        PlainBufferBuilder.buildRowPutChangeWithHeader(change),
        buildMinimalConditionBytes(null));
  }

  /**
   * Builds a DeleteRow wire body: outer envelope (fields 1=table_name, 2=primary_key, 3=condition)
   * encoded with {@link CodedOutputStream}; inner pk bytes from {@link
   * PlainBufferBuilder#buildRowDeleteChangeWithHeader}. A condition field is always included
   * because the real SDK always emits one and the canonicalizer requires it. Pass {@code null} for
   * revisionValue for a bare (unconditional) delete.
   */
  private static byte[] deleteBody(
      String table, String pkName, String pkVal, String revisionValue) throws Exception {
    PrimaryKey pk =
        PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(pkName, PrimaryKeyValue.fromString(pkVal))
            .build();
    RowDeleteChange change = new RowDeleteChange(table, pk);
    return encodeWriteRequest(
        table,
        PlainBufferBuilder.buildRowDeleteChangeWithHeader(change),
        buildMinimalConditionBytes(revisionValue));
  }

  /**
   * Encodes a PutRow / DeleteRow outer proto envelope: field 1=table_name (string),
   * field 2=row or primary_key (bytes), field 3=condition (bytes).
   */
  private static byte[] encodeWriteRequest(
      String tableName, byte[] rowBytes, byte[] conditionBytes) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CodedOutputStream out = CodedOutputStream.newInstance(baos);
    out.writeString(1, tableName);
    out.writeByteArray(2, rowBytes);
    out.writeByteArray(3, conditionBytes);
    out.flush();
    return baos.toByteArray();
  }

  /**
   * Builds a minimal Condition proto for test purposes.
   *
   * <p>The Condition proto has {@code row_existence} (field 1, enum) and {@code column_condition}
   * (field 2, bytes). {@code row_existence = IGNORE} (value {@code 0}) is the proto default and is
   * not written to the wire, so an unconditional delete produces empty condition bytes. When a
   * revision value is provided it is written as a string in field 2, making each distinct revision
   * produce distinct condition bytes — which is all the canonicalizer's hex-passthrough requires.
   */
  private static byte[] buildMinimalConditionBytes(String revisionValue) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CodedOutputStream out = CodedOutputStream.newInstance(baos);
    // row_existence = IGNORE = 0 (proto default; not written to the wire)
    if (revisionValue != null) {
      out.writeString(2, revisionValue); // distinguishing payload — any distinct bytes suffice
    }
    out.flush();
    return baos.toByteArray();
  }

  @Test
  void differentColumnOrderCanonicalizesEqual() throws Exception {
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
  void differentValuesCanonicalizeDifferent() throws Exception {
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
  void recordAndReplayPairingMatchesAcrossColumnOrder() throws Exception {
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
    // A body that is sent to /PutRow but lacks the mandatory fields (table_name, row, condition)
    // must be returned unchanged, not collapsed into a partial canonical form that would cause
    // unrelated malformed requests to match the same WireMock stub.
    // Wire-encode an unknown field (field 15, bytes "abc") — no required fields present.
    byte[] malformed = new byte[] {(byte) 0x7a, 0x03, 0x61, 0x62, 0x63};
    byte[] out = TablestoreBodyCanonicalizer.canonicalize("/PutRow", malformed);
    assertArrayEquals(
        malformed, out, "malformed body missing required fields must be returned unchanged");
  }

  @Test
  void knownFieldWithWrongWireTypeReturnsOriginal() throws Exception {
    // Field 1 (table_name) is normally wire type 2 (length-delimited, tag = 10).
    // Encoding it as wire type 0 (varint, tag = 8) produces a tag the canonicalizer
    // does not recognise and routes through skipField — table_name stays null and the
    // body is returned unchanged. This pins the exact-tag dispatch behaviour: a known
    // field number arriving with the wrong wire type is not misread as table_name.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CodedOutputStream out = CodedOutputStream.newInstance(baos);
    out.writeUInt64(1, 42L); // field 1, varint — wrong wire type for table_name
    out.writeByteArray(2, new byte[] {0x01}); // row field, plausible bytes
    out.writeByteArray(3, new byte[] {0x01}); // condition field, plausible bytes
    out.flush();
    byte[] body = baos.toByteArray();
    assertArrayEquals(
        body,
        TablestoreBodyCanonicalizer.canonicalize("/PutRow", body),
        "known field with wrong wire type must be unrecognised — body returned unchanged");
  }

  @Test
  void nonCanonicalizableUrlReturnsOriginal() throws Exception {
    byte[] body = putBody("docstore_test_1", "pName", "LeoPut", new String[] {"i"});
    byte[] out = TablestoreBodyCanonicalizer.canonicalize("/SQLQuery", body);
    assertArrayEquals(body, out, "non-write URLs must be returned unchanged");
  }

  @Test
  void deleteRowCanonicalizesCorrectly() throws Exception {
    // A DeleteRow carries only primary-key cells (no data cells). Verify the canonicalizer extracts
    // table + pk correctly and produces a structurally valid canonical JSON.
    byte[] body = deleteBody("docstore_test_2", "Game", "test-game", null);

    JsonNode root =
        new ObjectMapper()
            .readTree(TablestoreBodyCanonicalizer.canonicalize("/DeleteRow", body));

    JsonNode opNode = root.get("op");
    assertTrue(opNode != null && "DeleteRow".equals(opNode.asText()), "op must be DeleteRow");
    JsonNode tableNode = root.get("table");
    assertTrue(
        tableNode != null && "docstore_test_2".equals(tableNode.asText()),
        "table must be docstore_test_2");
    JsonNode rows = root.get("rows");
    assertTrue(rows != null && rows.isArray() && rows.size() > 0, "rows array must be present");
    assertTrue(
        "test-game".equals(root.get("rows").get(0).get("pk").get("Game").get("v").asText()),
        "pk value must be present under rows[0].pk");
    // A condition field is always included in the request; the canonical form must carry it so
    // that bare-delete stubs differ from revision-conditional ones.
    assertFalse(
        root.path("condition").isMissingNode(), "bare delete must carry the condition field");
  }

  @Test
  void conditionFieldDistinguishesRevisions() throws Exception {
    // Deletes with different revision values (encoded as a distinguishing field in the condition)
    // must produce different canonical forms so that WireMock stubs from different revision epochs
    // do not match each other.
    byte[] bodyA = deleteBody("docstore_test_1", "pName", "LeoDel", "rev-abc");
    byte[] bodyB = deleteBody("docstore_test_1", "pName", "LeoDel", "rev-xyz");

    JsonNode rootA =
        new ObjectMapper()
            .readTree(TablestoreBodyCanonicalizer.canonicalize("/DeleteRow", bodyA));

    // The condition bytes must appear as a non-empty hex string at the top level.
    assertFalse(
        rootA.path("condition").isMissingNode(), "condition field must appear in canonical form");
    assertFalse(
        rootA.path("condition").asText().isEmpty(), "condition hex value must be non-empty");

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
