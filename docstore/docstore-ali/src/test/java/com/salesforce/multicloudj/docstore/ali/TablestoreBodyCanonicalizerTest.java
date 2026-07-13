package com.salesforce.multicloudj.docstore.ali;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.github.tomakehurst.wiremock.matching.BinaryEqualToPattern;
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

    // ...but canonical forms are identical.
    byte[] ca = TablestoreBodyCanonicalizer.canonicalize("/PutRow", a);
    byte[] cb = TablestoreBodyCanonicalizer.canonicalize("/PutRow", b);
    assertArrayEquals(ca, cb, "canonical forms must be equal regardless of column order");
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

    // Record side: stub matcher is binaryEqualTo(canonical(recorded)).
    BinaryEqualToPattern stub =
        new BinaryEqualToPattern(
            TablestoreBodyCanonicalizer.canonicalize("/PutRow", recordedRaw));
    // Replay side: incoming request body is canonicalized before matching.
    byte[] replayCanonical = TablestoreBodyCanonicalizer.canonicalize("/PutRow", replayRaw);

    assertTrue(
        stub.match(replayCanonical).isExactMatch(),
        "canonicalized replay body must match the canonicalized recorded stub");
  }

  @Test
  void nonCanonicalizableUrlReturnsOriginal() {
    byte[] body = putBody("docstore_test_1", "pName", "LeoPut", new String[] {"i"});
    byte[] out = TablestoreBodyCanonicalizer.canonicalize("/SQLQuery", body);
    assertArrayEquals(body, out, "non-write URLs must be returned unchanged");
  }
}
