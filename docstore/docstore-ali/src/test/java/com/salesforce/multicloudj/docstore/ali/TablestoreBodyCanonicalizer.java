package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.PlainBufferCell;
import com.alicloud.openservices.tablestore.core.protocol.PlainBufferCodedInputStream;
import com.alicloud.openservices.tablestore.core.protocol.PlainBufferInputStream;
import com.alicloud.openservices.tablestore.core.protocol.PlainBufferRow;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Produces an order-independent canonical byte form of an Alibaba Tablestore write-request body
 * (PutRow / DeleteRow).
 *
 * <p>Why this exists: the Tablestore driver serializes a row's data columns by iterating a {@link
 * java.util.HashMap}, whose iteration order is perturbed run-to-run (the conformance test builds
 * docs via {@code Map.of(...)}, whose iteration order is salted per JVM run). The wire body is a
 * binary PlainBuffer protobuf, so WireMock can only match it byte-for-byte ({@code binaryEqualTo}).
 * Two requests with identical columns in a different order produce different bytes, so a recorded
 * stub intermittently fails to match the equivalent replayed request. This is the binary analog of
 * the order-tolerance that {@code equalToJson} gives the JSON providers (AWS/GCP).
 *
 * <p>This class parses the request into (table, primary-key cells, data cells, condition) and emits
 * a deterministic textual representation with the data cells SORTED by name. Applying it to both
 * the recorded stub body (at record time) and the incoming request (at replay time) makes
 * WireMock's ordinary {@code binaryEqualTo} match regardless of column order.
 *
 * <p>It never re-serializes PlainBuffer (so there is no CRC recomputation); it only ever reads,
 * emitting its own canonical form. If the body is not a recognized Tablestore write request it
 * returns the original bytes unchanged, so non-write ops and other providers are unaffected.
 */
public final class TablestoreBodyCanonicalizer {

  private TablestoreBodyCanonicalizer() {}

  /** URL paths whose bodies carry an order-sensitive PlainBuffer row. */
  public static boolean isCanonicalizableUrl(String url) {
    if (url == null) {
      return false;
    }
    // Strip any query string.
    int q = url.indexOf('?');
    String path = q >= 0 ? url.substring(0, q) : url;
    return path.endsWith("/PutRow") || path.endsWith("/DeleteRow");
  }

  /**
   * Returns an order-independent canonical byte form of the given Tablestore write-request body, or
   * the original bytes unchanged if it cannot be parsed as one.
   */
  public static byte[] canonicalize(String url, byte[] body) {
    if (body == null || body.length == 0 || !isCanonicalizableUrl(url)) {
      return body;
    }
    try {
      int q = url.indexOf('?');
      String path = q >= 0 ? url.substring(0, q) : url;
      StringBuilder sb = new StringBuilder();
      if (path.endsWith("/PutRow")) {
        OtsInternalApi.PutRowRequest req = OtsInternalApi.PutRowRequest.parseFrom(body);
        sb.append("PutRow\n");
        sb.append("table=").append(req.getTableName()).append('\n');
        appendRow(sb, req.getRow());
        if (req.hasCondition()) {
          // The serialized column-condition filter is itself order-stable (single revision column),
          // so include its bytes verbatim to preserve precondition semantics in the match.
          sb.append("condition=")
              .append(base16(req.getCondition().toByteArray()))
              .append('\n');
        }
      } else {
        OtsInternalApi.DeleteRowRequest req = OtsInternalApi.DeleteRowRequest.parseFrom(body);
        sb.append("DeleteRow\n");
        sb.append("table=").append(req.getTableName()).append('\n');
        appendRow(sb, req.getPrimaryKey());
        if (req.hasCondition()) {
          sb.append("condition=")
              .append(base16(req.getCondition().toByteArray()))
              .append('\n');
        }
      }
      return sb.toString().getBytes(StandardCharsets.UTF_8);
    } catch (Exception e) {
      // Not a parseable write request (or an SDK-shape we don't handle) -> leave it untouched.
      return body;
    }
  }

  private static void appendRow(StringBuilder sb, ByteString rowBytes) throws Exception {
    PlainBufferCodedInputStream in =
        new PlainBufferCodedInputStream(new PlainBufferInputStream(rowBytes.toByteArray()));
    List<PlainBufferRow> rows = in.readRowsWithHeader();
    for (PlainBufferRow row : rows) {
      // Primary-key cells: order is schema-defined and stable, keep as-is.
      sb.append("pk=[");
      for (PlainBufferCell cell : row.getPrimaryKey()) {
        sb.append(cellString(cell)).append(';');
      }
      sb.append("]\n");

      // Data cells: SORT by canonical string so column order is irrelevant.
      List<String> dataCells = new ArrayList<>();
      if (row.getCells() != null) {
        for (PlainBufferCell cell : row.getCells()) {
          dataCells.add(cellString(cell));
        }
      }
      Collections.sort(dataCells);
      sb.append("cells=[");
      for (String c : dataCells) {
        sb.append(c).append(';');
      }
      sb.append("]\n");
      sb.append("deleteMarker=").append(row.hasDeleteMarker()).append('\n');
    }
  }

  private static String cellString(PlainBufferCell cell) {
    StringBuilder c = new StringBuilder();
    c.append(cell.getCellName()).append('=');
    if (cell.isPk() && cell.getPkCellValue() != null) {
      c.append("PK:").append(cell.getPkCellValue().toString());
    } else if (cell.hasCellValue() && cell.getCellValue() != null) {
      c.append(cell.getCellValue().getType())
          .append(':')
          .append(cell.getCellValue().toString());
    } else {
      // Cell present with no value (e.g. a delete-column op): encode its type marker.
      c.append("op:").append(cell.getCellType());
    }
    return c.toString();
  }

  private static String base16(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(Character.forDigit((b >> 4) & 0xF, 16));
      sb.append(Character.forDigit(b & 0xF, 16));
    }
    return sb.toString();
  }
}
