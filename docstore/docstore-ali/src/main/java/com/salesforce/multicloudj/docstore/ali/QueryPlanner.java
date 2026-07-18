package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.filter.ColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.docstore.driver.Filter;
import com.salesforce.multicloudj.docstore.driver.FilterOperation;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.commons.lang3.ObjectUtils;

/**
 * Translates a docstore query's filter set into the primitives a Tablestore GetRange request needs:
 * a primary-key range {@code [inclusiveStart, exclusiveEnd)}, a scan {@link Direction}, and a
 * server-side column {@link ColumnValueFilter}.
 *
 * <p>Walks the target's (base table or secondary index) ordered key-column list rather than
 * assuming a fixed partition/sort shape.
 *
 * <p><b>Two-layer design.</b>
 *
 * <ul>
 *   <li><b>Column filter = correctness.</b> Every predicate is (also) emitted as a column filter,
 *       so the rows returned are always exactly the matching set, regardless of how tight the key
 *       bounds are. This is the safety net under the fiddly bound arithmetic.
 *   <li><b>Key range bounds = efficiency.</b> A best-effort seek so the server scans as few rows
 *       as possible. Bounds NEVER over-exclude a matching row (the hard invariant). Where the
 *       primitive cannot represent an operator exactly, the bound is widened (looser, scans a
 *       little extra) and the column filter trims the surplus — never made tighter than correct.
 * </ul>
 *
 * <p><b>Composite-key rule</b> (the standard prefix rule for range scans over a composite key): an
 * equality prefix, then at most one range column, then an unconstrained ({@code INF}) tail.
 * Predicates that cannot be folded
 * into that structure (e.g. {@code IN}/{@code NOT_IN}, or an equality after a gap) still appear in
 * the column filter, so they remain enforced.
 *
 * <p><b>Boundary representation.</b> For a non-terminal range column we can realize any of
 * {@code >, >=, <, <=} exactly using an {@code INF_MIN}/{@code INF_MAX} tail fill (there is a
 * trailing slot to nudge). For the terminal (last) PK column there is no trailing slot, so the two
 * operators whose exactness needs one — forward {@code >} and {@code <=} (and the backward mirrors)
 * — are widened by one boundary group and the column filter removes the extra rows.
 */
public final class QueryPlanner {

  /** The translated GetRange inputs. {@code columnFilter} may be null (no predicates). */
  @Getter
  public static final class Plan {
    private final PrimaryKey inclusiveStartPrimaryKey;
    private final PrimaryKey exclusiveEndPrimaryKey;
    private final Direction direction;
    private final ColumnValueFilter columnFilter;

    Plan(
        PrimaryKey inclusiveStartPrimaryKey,
        PrimaryKey exclusiveEndPrimaryKey,
        Direction direction,
        ColumnValueFilter columnFilter) {
      this.inclusiveStartPrimaryKey = inclusiveStartPrimaryKey;
      this.exclusiveEndPrimaryKey = exclusiveEndPrimaryKey;
      this.direction = direction;
      this.columnFilter = columnFilter;
    }
  }

  // A single primary-key column's range constraint, in ascending terms.
  private static final class Bound {
    PrimaryKeyValue value;
    boolean inclusive;
  }

  // What constrains one primary-key column. equality wins over range if both somehow present.
  private static final class ColumnConstraint {
    PrimaryKeyValue equality;
    Bound lower;
    Bound upper;

    boolean hasEquality() {
      return equality != null;
    }

    boolean hasRange() {
      return lower != null || upper != null;
    }
  }

  private QueryPlanner() {}

  /**
   * Builds the GetRange plan for a query resolved to a table/index with the given ordered
   * primary-key columns.
   *
   * @param pkColumns the target table/index primary-key column names, in key order (never empty).
   * @param filters the query filters (ANDed). May be empty (full-range scan).
   * @param orderAscending scan direction: ascending -> FORWARD, descending -> BACKWARD.
   */
  public static Plan plan(List<String> pkColumns, List<Filter> filters, boolean orderAscending) {
    if (ObjectUtils.isEmpty(pkColumns)) {
      throw new InvalidArgumentException("primary-key column list must not be empty");
    }

    // Per-PK-column constraints extracted from the filters. Non-key filters are ignored here (they
    // only contribute to the column filter, built separately below).
    Map<String, ColumnConstraint> pkConstraints = extractPkConstraints(pkColumns, filters);

    int n = pkColumns.size();

    // Equality prefix: leading contiguous run of columns with an equality constraint.
    int eqPrefixLen = 0;
    while (eqPrefixLen < n && pkConstraints.get(pkColumns.get(eqPrefixLen)).hasEquality()) {
      eqPrefixLen++;
    }

    // Determine the single range column and the equality-prefix length that precedes it.
    //  - Genuine range: the column right after the equality prefix carries a range predicate.
    //  - Full equality (prefix covers all N columns): demote the LAST equality column to a
    //    degenerate inclusive range [v, v]. Otherwise a full-equality key would produce
    //    start == end == the exact tuple, i.e. the empty half-open range [X, X), which wrongly
    //    excludes the matching row. Demotion routes it through the terminal-range path (inclusive
    //    start at v, end widened open) and the column filter trims back to exactly v.
    // Default: no usable range column (e.g. empty filters, or an equality after a gap) -> a fully
    // open trailing tail from eqPrefixLen onward. The two branches below override when a genuine or
    // demoted range column exists.
    int rc = -1;
    int eqLen = eqPrefixLen;
    Bound lower = null;
    Bound upper = null;
    if (eqPrefixLen < n && pkConstraints.get(pkColumns.get(eqPrefixLen)).hasRange()) {
      rc = eqPrefixLen;
      ColumnConstraint rangeCol = pkConstraints.get(pkColumns.get(rc));
      lower = rangeCol.lower;
      upper = rangeCol.upper;
    } else if (eqPrefixLen == n) {
      rc = n - 1;
      eqLen = n - 1;
      PrimaryKeyValue v = pkConstraints.get(pkColumns.get(rc)).equality;
      lower = bound(v, true);
      upper = bound(v, true);
    }
    boolean terminal = rc == n - 1;

    PrimaryKeyValue[] eqPrefix = new PrimaryKeyValue[eqLen];
    for (int i = 0; i < eqLen; i++) {
      eqPrefix[i] = pkConstraints.get(pkColumns.get(i)).equality;
    }

    PrimaryKey start;
    PrimaryKey end;
    Direction direction;
    if (orderAscending) {
      direction = Direction.FORWARD;
      start = forwardStart(pkColumns, eqPrefix, rc, terminal, lower);
      end = forwardEnd(pkColumns, eqPrefix, rc, terminal, upper);
    } else {
      direction = Direction.BACKWARD;
      start = backwardStart(pkColumns, eqPrefix, rc, terminal, upper);
      end = backwardEnd(pkColumns, eqPrefix, rc, terminal, lower);
    }

    ColumnValueFilter columnFilter = buildColumnFilter(filters);
    return new Plan(start, end, direction, columnFilter);
  }

  // Bound tuple builders. Each returns a full N-column PrimaryKey. The "cells" array is filled
  // position by position: equality prefix, then the range column (rc), then the INF tail.

  // FORWARD inclusiveStart = the inclusive LOWER boundary; includes rows >= this tuple.
  private static PrimaryKey forwardStart(
      List<String> pk, PrimaryKeyValue[] eqPrefix, int rc, boolean terminal, Bound lower) {
    PrimaryKeyValue[] cells = newCells(pk.size(), eqPrefix);
    if (rc < 0) {
      // No range column: open the lower end (start from the smallest key in the prefix group).
      fillTail(cells, eqPrefix.length, PrimaryKeyValue.INF_MIN);
    } else if (lower == null) {
      // Range column has only an upper bound: lower end is open.
      fillTail(cells, rc, PrimaryKeyValue.INF_MIN);
    } else {
      cells[rc] = lower.value;
      if (lower.inclusive) {
        // >= L : start at (L, INF_MIN...) — includes the entire L group.
        fillTail(cells, rc + 1, PrimaryKeyValue.INF_MIN);
      } else if (!terminal) {
        // > L (non-terminal): start at (L, INF_MAX...) — skips the entire L group exactly.
        fillTail(cells, rc + 1, PrimaryKeyValue.INF_MAX);
      }
      // > L (terminal): cell[rc]=L, inclusive-start includes L (loose by one row); filter drops L.
    }
    return build(pk, cells);
  }

  // FORWARD exclusiveEnd = the exclusive UPPER boundary; includes rows < this tuple.
  private static PrimaryKey forwardEnd(
      List<String> pk, PrimaryKeyValue[] eqPrefix, int rc, boolean terminal, Bound upper) {
    PrimaryKeyValue[] cells = newCells(pk.size(), eqPrefix);
    if (rc < 0) {
      // No range column: open the upper end (end past the largest key in the prefix group).
      fillTail(cells, eqPrefix.length, PrimaryKeyValue.INF_MAX);
    } else if (upper == null) {
      // Range column has only a lower bound: upper end is open.
      fillTail(cells, rc, PrimaryKeyValue.INF_MAX);
    } else if (upper.inclusive) {
      // <= U
      if (!terminal) {
        // end at (U, INF_MAX...) — exclusive-end includes all real U rows exactly.
        cells[rc] = upper.value;
        fillTail(cells, rc + 1, PrimaryKeyValue.INF_MAX);
      } else {
        // terminal <= U: an exclusive end at U would drop the valid U row (over-exclusion).
        // Widen the upper end fully open; the column filter trims rows > U.
        fillTail(cells, rc, PrimaryKeyValue.INF_MAX);
      }
    } else {
      // < U : end at (U, INF_MIN...) — exclusive-end includes rows strictly below U exactly.
      cells[rc] = upper.value;
      fillTail(cells, rc + 1, PrimaryKeyValue.INF_MIN);
    }
    return build(pk, cells);
  }

  // BACKWARD inclusiveStart = the inclusive UPPER boundary (scan starts high, descends);
  // includes rows <= this tuple.
  private static PrimaryKey backwardStart(
      List<String> pk, PrimaryKeyValue[] eqPrefix, int rc, boolean terminal, Bound upper) {
    PrimaryKeyValue[] cells = newCells(pk.size(), eqPrefix);
    if (rc < 0) {
      fillTail(cells, eqPrefix.length, PrimaryKeyValue.INF_MAX);
    } else if (upper == null) {
      // Only a lower bound: start from the top of the prefix group.
      fillTail(cells, rc, PrimaryKeyValue.INF_MAX);
    } else {
      cells[rc] = upper.value;
      if (upper.inclusive) {
        // <= U : inclusive-start at (U, INF_MAX...) includes all real U rows (terminal: (U) too).
        fillTail(cells, rc + 1, PrimaryKeyValue.INF_MAX);
      } else if (!terminal) {
        // < U (non-terminal): inclusive-start at (U, INF_MIN...) includes rows strictly below U.
        fillTail(cells, rc + 1, PrimaryKeyValue.INF_MIN);
      }
      // < U (terminal): cell[rc]=U, inclusive-start includes U (loose); filter drops U.
    }
    return build(pk, cells);
  }

  // BACKWARD exclusiveEnd = the exclusive LOWER boundary; includes rows > this tuple.
  private static PrimaryKey backwardEnd(
      List<String> pk, PrimaryKeyValue[] eqPrefix, int rc, boolean terminal, Bound lower) {
    PrimaryKeyValue[] cells = newCells(pk.size(), eqPrefix);
    if (rc < 0) {
      fillTail(cells, eqPrefix.length, PrimaryKeyValue.INF_MIN);
    } else if (lower == null) {
      // Only an upper bound: end at the bottom of the prefix group.
      fillTail(cells, rc, PrimaryKeyValue.INF_MIN);
    } else if (lower.inclusive) {
      // >= L
      if (!terminal) {
        // end at (L, INF_MIN...) — exclusive-end includes real rows >= L exactly.
        cells[rc] = lower.value;
        fillTail(cells, rc + 1, PrimaryKeyValue.INF_MIN);
      } else {
        // terminal >= L: an exclusive end at L would drop the valid L row (over-exclusion).
        // Widen the lower end fully open; the column filter trims rows < L.
        fillTail(cells, rc, PrimaryKeyValue.INF_MIN);
      }
    } else {
      // > L : end at (L, INF_MAX...) — exclusive-end includes rows strictly above L exactly.
      cells[rc] = lower.value;
      fillTail(cells, rc + 1, PrimaryKeyValue.INF_MAX);
    }
    return build(pk, cells);
  }

  private static PrimaryKeyValue[] newCells(int n, PrimaryKeyValue[] eqPrefix) {
    PrimaryKeyValue[] cells = new PrimaryKeyValue[n];
    System.arraycopy(eqPrefix, 0, cells, 0, eqPrefix.length);
    return cells;
  }

  // Fill positions [fromInclusive, n) with the given INF sentinel.
  private static void fillTail(PrimaryKeyValue[] cells, int fromInclusive, PrimaryKeyValue inf) {
    for (int i = fromInclusive; i < cells.length; i++) {
      cells[i] = inf;
    }
  }

  private static PrimaryKey build(List<String> pkColumns, PrimaryKeyValue[] cells) {
    PrimaryKeyBuilder b = PrimaryKeyBuilder.createPrimaryKeyBuilder();
    for (int i = 0; i < pkColumns.size(); i++) {
      b.addPrimaryKeyColumn(pkColumns.get(i), cells[i]);
    }
    return b.build();
  }

  private static Map<String, ColumnConstraint> extractPkConstraints(
      List<String> pkColumns, List<Filter> filters) {
    Map<String, ColumnConstraint> map = new LinkedHashMap<>();
    for (String col : pkColumns) {
      map.put(col, new ColumnConstraint());
    }
    if (filters == null) {
      return map;
    }
    for (Filter f : filters) {
      ColumnConstraint c = map.get(f.getFieldPath());
      if (c == null) {
        continue; // non-key filter: contributes only to the column filter
      }
      PrimaryKeyValue pkv = toPrimaryKeyValue(f.getValue());
      if (pkv == null) {
        // Value type not representable as a primary key (e.g. Double/Boolean). Leave this column
        // unconstrained in the bounds; the column filter still enforces it.
        continue;
      }
      switch (f.getOp()) {
        case EQUAL:
          c.equality = pkv;
          break;
        case GREATER_THAN:
          c.lower = bound(pkv, false);
          break;
        case GREATER_THAN_OR_EQUAL_TO:
          c.lower = bound(pkv, true);
          break;
        case LESS_THAN:
          c.upper = bound(pkv, false);
          break;
        case LESS_THAN_OR_EQUAL_TO:
          c.upper = bound(pkv, true);
          break;
        default:
          // IN / NOT_IN: not expressible as a contiguous key range; column filter handles it.
          break;
      }
    }
    return map;
  }

  private static Bound bound(PrimaryKeyValue value, boolean inclusive) {
    Bound b = new Bound();
    b.value = value;
    b.inclusive = inclusive;
    return b;
  }

  /**
   * Builds the correctness-layer column filter: EVERY predicate becomes a
   * {@link ColumnValueFilter}, ANDed together — including predicates on primary-key columns. This
   * is deliberate: it makes the returned rows exact regardless of how loose the key bounds are,
   * with no dependency on the bounds being tight or on any column carrying an equality. (Some of
   * these filters are redundant with an exact bound — a harmless server-side re-check we may
   * optimize away later.) {@code IN} expands to an {@code OR} of equals; {@code NOT_IN} to a
   * {@code NOT} of that {@code OR}. Returns null when there are no predicates.
   */
  static ColumnValueFilter buildColumnFilter(List<Filter> filters) {
    if (filters == null) {
      return null;
    }
    List<ColumnValueFilter> parts = new ArrayList<>();
    for (Filter f : filters) {
      parts.add(toColumnValueFilter(f));
    }
    if (parts.isEmpty()) {
      return null;
    }
    if (parts.size() == 1) {
      return parts.get(0);
    }
    CompositeColumnValueFilter and =
        new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.AND);
    for (ColumnValueFilter p : parts) {
      and.addFilter(p);
    }
    return and;
  }

  private static ColumnValueFilter toColumnValueFilter(Filter f) {
    switch (f.getOp()) {
      case IN:
        return inFilter(f.getFieldPath(), f.getValue());
      case NOT_IN:
        CompositeColumnValueFilter not =
            new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.NOT);
        not.addFilter(inFilter(f.getFieldPath(), f.getValue()));
        return not;
      default:
        SingleColumnValueFilter scv =
            new SingleColumnValueFilter(
                f.getFieldPath(), toCompareOperator(f.getOp()), toColumnValue(f.getValue()));
        scv.setPassIfMissing(false);
        return scv;
    }
  }

  // IN -> OR(=v0, =v1, ...). A single-element collection collapses to a bare
  // SingleColumnValueFilter (the SDK's OR composite requires >= 2 children). An empty
  // collection is rejected: the Query API
  // does not validate against it (its element loop simply doesn't run), and there is no meaningful
  // GetRange filter for "IN ()"/"NOT IN ()", so fail clearly rather than silently mis-scan. The
  // message is operator-neutral because NOT_IN routes through here too.
  private static ColumnValueFilter inFilter(String field, Object value) {
    List<ColumnValueFilter> equals = new ArrayList<>();
    if (value instanceof Iterable) {
      for (Object element : (Iterable<?>) value) {
        equals.add(equalFilter(field, element));
      }
    } else {
      equals.add(equalFilter(field, value));
    }
    if (equals.isEmpty()) {
      throw new InvalidArgumentException(
          "IN / NOT_IN filter on '" + field + "' requires at least one value");
    }
    if (equals.size() == 1) {
      return equals.get(0);
    }
    CompositeColumnValueFilter or =
        new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.OR);
    for (ColumnValueFilter e : equals) {
      or.addFilter(e);
    }
    return or;
  }

  private static SingleColumnValueFilter equalFilter(String field, Object value) {
    SingleColumnValueFilter scv =
        new SingleColumnValueFilter(
            field, SingleColumnValueFilter.CompareOperator.EQUAL, toColumnValue(value));
    scv.setPassIfMissing(false);
    return scv;
  }

  private static SingleColumnValueFilter.CompareOperator toCompareOperator(FilterOperation op) {
    switch (op) {
      case EQUAL:
        return SingleColumnValueFilter.CompareOperator.EQUAL;
      case GREATER_THAN:
        return SingleColumnValueFilter.CompareOperator.GREATER_THAN;
      case GREATER_THAN_OR_EQUAL_TO:
        return SingleColumnValueFilter.CompareOperator.GREATER_EQUAL;
      case LESS_THAN:
        return SingleColumnValueFilter.CompareOperator.LESS_THAN;
      case LESS_THAN_OR_EQUAL_TO:
        return SingleColumnValueFilter.CompareOperator.LESS_EQUAL;
      default:
        throw new InvalidArgumentException("Operator not expressible as a column filter: " + op);
    }
  }

  // Maps a filter value to a PrimaryKeyValue, or null when the type cannot be a primary key
  // (Tablestore PK columns are only STRING / INTEGER / BINARY).
  static PrimaryKeyValue toPrimaryKeyValue(Object value) {
    if (value instanceof Long) {
      return PrimaryKeyValue.fromLong((Long) value);
    }
    if (value instanceof Integer) {
      return PrimaryKeyValue.fromLong((Integer) value);
    }
    if (value instanceof String) {
      return PrimaryKeyValue.fromString((String) value);
    }
    if (value instanceof byte[]) {
      return PrimaryKeyValue.fromBinary((byte[]) value);
    }
    return null;
  }

  static ColumnValue toColumnValue(Object value) {
    if (value instanceof Boolean) {
      return ColumnValue.fromBoolean((Boolean) value);
    }
    if (value instanceof Long) {
      return ColumnValue.fromLong((Long) value);
    }
    if (value instanceof Integer) {
      return ColumnValue.fromLong((Integer) value);
    }
    if (value instanceof Double) {
      return ColumnValue.fromDouble((Double) value);
    }
    if (value instanceof Float) {
      return ColumnValue.fromDouble((Float) value);
    }
    if (value instanceof String) {
      return ColumnValue.fromString((String) value);
    }
    if (value instanceof byte[]) {
      return ColumnValue.fromBinary((byte[]) value);
    }
    throw new InvalidArgumentException(
        "Unsupported filter value type: " + (value == null ? "null" : value.getClass().getName()));
  }
}
