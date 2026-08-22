package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.core.protocol.PlainBufferCell;
import com.alicloud.openservices.tablestore.core.protocol.PlainBufferCodedInputStream;
import com.alicloud.openservices.tablestore.core.protocol.PlainBufferInputStream;
import com.alicloud.openservices.tablestore.core.protocol.PlainBufferRow;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import java.util.List;

/**
 * Produces an order-independent canonical <b>JSON</b> form of an Alibaba Tablestore write-request
 * body (PutRow / DeleteRow), suitable for matching with WireMock's {@code equalToJson}.
 *
 * <p>Why this exists: the Tablestore driver serializes a row's data columns by iterating a {@link
 * java.util.HashMap}, whose iteration order is perturbed run-to-run (the conformance test builds
 * docs via {@code Map.of(...)}, whose iteration order is salted per JVM run). The wire body is a
 * binary PlainBuffer protobuf, so WireMock records it byte-for-byte and column reordering breaks
 * replay matching.
 *
 * <p>This class parses the request into (op, table, primary-key cells, data cells, condition) and
 * emits a JSON document where the data cells are a JSON <b>object keyed by column name</b>. Applied
 * to both the recorded stub body (record time) and the incoming request (replay time), WireMock's
 * {@code equalToJson} then matches regardless of column order — JSON object keys are unordered.
 *
 * <p><b>Type fidelity:</b> {@code equalToJson} normalizes JSON numbers ({@code 121} equals {@code
 * 121.0}), which would conflate distinct Tablestore column types (INTEGER vs DOUBLE). To preserve
 * the type distinction each value is emitted as a tagged object {@code {"t":"<ColumnType>","v":
 * "<stringified value>"}} with the value rendered as a JSON string, so matching is exact on both
 * type and value.
 *
 * <p>It never re-serializes PlainBuffer (no CRC recomputation); it only reads. If the body is not a
 * recognized Tablestore write request it returns the original bytes unchanged, so non-write ops and
 * other providers are unaffected.
 *
 * <p><b>Implementation note:</b> only the narrow outer proto envelope (table_name, row /
 * primary_key, condition) is decoded, using {@link CodedInputStream} with exact wire tags
 * ({@code fieldNumber << 3 | wireType}). A field that arrives with an unexpected wire type is
 * routed to {@code skipField} rather than being misread. The inner PlainBuffer bytes are then
 * parsed by {@link PlainBufferCodedInputStream} to extract individual cells.
 */
public final class TablestoreBodyCanonicalizer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // PutRow and DeleteRow share the same outer field layout:
  //   field 1 = table_name (string)
  //   field 2 = row / primary_key (bytes — PlainBuffer encoded)
  //   field 3 = condition (bytes)
  // Wire tag = (fieldNumber << 3) | wireType; WIRETYPE_LENGTH_DELIMITED = 2.
  private static final int TABLE_NAME_TAG = (1 << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED; // 10
  private static final int ROW_TAG        = (2 << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED; // 18
  private static final int CONDITION_TAG  = (3 << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED; // 26

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
   * Returns an order-independent canonical JSON form (UTF-8 bytes) of the given Tablestore
   * write-request body, or the original bytes unchanged if it cannot be parsed as one.
   */
  public static byte[] canonicalize(String url, byte[] body) {
    if (body == null || body.length == 0 || !isCanonicalizableUrl(url)) {
      return body;
    }
    try {
      int q = url.indexOf('?');
      String path = q >= 0 ? url.substring(0, q) : url;
      boolean isPut = path.endsWith("/PutRow");

      String tableName = null;
      byte[] rowBytes = null;
      byte[] conditionBytes = null;

      // Decode the outer proto envelope field by field. TABLE_NAME_TAG, ROW_TAG, and CONDITION_TAG
      // encode both the field number and wire type, so a field that arrives with a different wire
      // type (e.g. a malformed body) falls through to skipField rather than being misread.
      CodedInputStream cin = CodedInputStream.newInstance(body);
      int tag;
      while ((tag = cin.readTag()) != 0) {
        if (tag == TABLE_NAME_TAG) {
          tableName = cin.readString();
        } else if (tag == ROW_TAG) {
          rowBytes = cin.readByteArray();
        } else if (tag == CONDITION_TAG) {
          conditionBytes = cin.readByteArray();
        } else {
          // skipField returns false for an end-group tag — treat as unparseable.
          if (!cin.skipField(tag)) {
            return body;
          }
        }
      }

      // All three fields are required in every valid Tablestore write request. Absent or empty
      // condition bytes indicate a malformed body — the real SDK always serializes Condition as
      // a proto2 required-field message (at minimum the 2-byte IGNORE encoding 08 00). Return
      // the body unchanged so distinct malformed bodies do not collapse into the same stub JSON.
      if (tableName == null || rowBytes == null || conditionBytes == null
          || conditionBytes.length == 0) {
        return body;
      }

      ObjectNode root = MAPPER.createObjectNode();
      root.put("op", isPut ? "PutRow" : "DeleteRow");
      root.put("table", tableName);
      appendRow(root, rowBytes);
      // The serialized condition is order-stable (single revision column); include its bytes as
      // hex to preserve precondition semantics in the stub match.
      root.put("condition", base16(conditionBytes));
      return MAPPER.writeValueAsBytes(root);
    } catch (Exception e) {
      // Not a parseable write request (or an SDK-shape we don't handle) -> leave it untouched.
      return body;
    }
  }

  private static void appendRow(ObjectNode root, byte[] rowBytes) throws Exception {
    PlainBufferCodedInputStream in =
        new PlainBufferCodedInputStream(new PlainBufferInputStream(rowBytes));
    List<PlainBufferRow> rows = in.readRowsWithHeader();

    // A write request carries a single row; represent PK and cells as keyed objects so
    // equalToJson ignores column ordering. (If multiple rows ever appear, index them.)
    ArrayNode rowsNode = root.putArray("rows");
    for (PlainBufferRow row : rows) {
      ObjectNode rowNode = rowsNode.addObject();

      // Primary-key cells: schema-ordered and stable, but key by name for consistency.
      ObjectNode pk = rowNode.putObject("pk");
      for (PlainBufferCell cell : row.getPrimaryKey()) {
        pk.set(cell.getCellName(), cellValue(cell));
      }

      // Data cells: keyed by column name -> equalToJson ignores their order.
      ObjectNode cells = rowNode.putObject("cells");
      if (row.getCells() != null) {
        for (PlainBufferCell cell : row.getCells()) {
          cells.set(cell.getCellName(), cellValue(cell));
        }
      }
      rowNode.put("deleteMarker", row.hasDeleteMarker());
    }
  }

  /** Emits a type-tagged value node: {@code {"t":"<type>","v":"<stringified value>"}}. */
  private static ObjectNode cellValue(PlainBufferCell cell) {
    ObjectNode node = MAPPER.createObjectNode();
    if (cell.isPk() && cell.getPkCellValue() != null) {
      node.put("t", "PK:" + cell.getPkCellValue().getType());
      node.put("v", cell.getPkCellValue().toString());
    } else if (cell.hasCellValue() && cell.getCellValue() != null) {
      node.put("t", cell.getCellValue().getType().toString());
      node.put("v", cell.getCellValue().toString());
    } else {
      // Cell present with no value (e.g. a delete-column op): encode its type marker.
      node.put("t", "op");
      node.put("v", Byte.toString(cell.getCellType()));
    }
    return node;
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
