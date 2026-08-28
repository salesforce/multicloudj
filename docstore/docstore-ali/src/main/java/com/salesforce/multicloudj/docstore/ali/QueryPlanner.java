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

    // True iff every filter was folded into the key range, so the column filter only trims an O(1)
    // terminal boundary rather than dropping a non-trivial number of scanned rows. When true, a
    // fixed per-page GetRange row cap (offset + limit) is accurate; when false, such a cap would
    // force the driving iterator to re-scan page after page (the filter drops most of what each
    // capped page scans), so the caller leaves the page cap off. See computeKeyRangeTight.
    private final boolean keyRangeTight;

    Plan(
        PrimaryKey inclusiveStartPrimaryKey,
        PrimaryKey exclusiveEndPrimaryKey,
        Direction direction,
        ColumnValueFilter columnFilter,
        boolean keyRangeTight) {
      this.inclusiveStartPrimaryKey = inclusiveStartPrimaryKey;
      this.exclusiveEndPrimaryKey = exclusiveEndPrimaryKey;
      this.direction = direction;
      this.columnFilter = columnFilter;
      this.keyRangeTight = keyRangeTight;
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
    // Full-equality means every PK column carried an equality, so the last equality was demoted to
    // the degenerate [v, v] terminal range above. In that case rc == n - 1 carries an equality (not
    // a genuine range), which both the redundant-equality drop and the key-range-tight check treat
    // specially.
    boolean fullEquality = eqPrefixLen == n;

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

    ColumnValueFilter columnFilter = buildColumnFilter(filters, pkColumns, eqPrefix);
    boolean tight = computeKeyRangeTight(pkColumns, filters, eqLen, rc, fullEquality);
    return new Plan(start, end, direction, columnFilter, tight);
  }

  // True iff EVERY filter was folded into the key range: the range bounds capture the whole
  // predicate set, so the column filter only trims an O(1) terminal boundary group (one full-key
  // row, since a folded range is always the terminal or an exact non-terminal). A filter is folded
  // when it is an EQUAL on a used-equality-prefix column, the single range op on the range column,
  // or the demoted full-equality on the terminal column. Anything else — a non-key predicate,
  // IN/NOT_IN, a non-PK-typed value on a key column, an equality after a gap, or a second range —
  // is enforced only by the column filter, which then drops a non-trivial number of scanned rows.
  //
  // Empty filters are trivially tight: with no predicate to drop, a page's setLimit(N) returns
  // exactly N rows, so the fixed per-page cap is accurate. (An open, everything-in-range scan where
  // scanned == matched.)
  private static boolean computeKeyRangeTight(
      List<String> pkColumns, List<Filter> filters, int eqLen, int rc, boolean fullEquality) {
    if (filters == null || filters.isEmpty()) {
      return true;
    }
    // A genuine range column can fold only ONE lower and ONE upper bound into the key range:
    // extractPkConstraints keeps just the last predicate per side, so any additional same-side
    // predicate on the range column supplies no key bound and is enforced only by the column
    // filter. That filter may be selective (e.g. B>100 AND B>0 retains only the weaker B>0 as the
    // bound while B>100 drops the (0,100] rows the range scanned), so the plan is NOT tight and the
    // per-page cap must stay off. Skip when rc carries the demoted full-equality (not a range).
    if (rc >= 0 && !fullEquality && hasRepeatedSameSideBound(pkColumns.get(rc), filters)) {
      return false;
    }
    for (Filter f : filters) {
      if (!isFilterFolded(f, pkColumns, eqLen, rc, fullEquality)) {
        return false;
      }
    }
    return true;
  }

  // Whether the range column has more than one predicate on the same side (>= 2 lower bounds
  // GT/GE, or >= 2 upper bounds LT/LE). Only one lower and one upper can be the retained (folded)
  // key bound; any extra same-side predicate is column-filter-only and may be selective.
  private static boolean hasRepeatedSameSideBound(String rangeColumn, List<Filter> filters) {
    int lowerCount = 0;
    int upperCount = 0;
    for (Filter f : filters) {
      if (!rangeColumn.equals(f.getFieldPath())) {
        continue;
      }
      switch (f.getOp()) {
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL_TO:
          lowerCount++;
          break;
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL_TO:
          upperCount++;
          break;
        default:
          break;
      }
    }
    return lowerCount >= 2 || upperCount >= 2;
  }

  // Whether a single filter was folded into the key range (see computeKeyRangeTight). Mirrors the
  // fold decisions plan() makes when deriving eqLen / rc: only the used equality prefix, the single
  // range column, and the demoted full-equality terminal are folded.
  private static boolean isFilterFolded(
      Filter f, List<String> pkColumns, int eqLen, int rc, boolean fullEquality) {
    int idx = pkColumns.indexOf(f.getFieldPath());
    if (idx < 0) {
      return false; // non-key predicate: enforced only by the column filter
    }
    if (toPrimaryKeyValue(f.getValue()) == null) {
      return false; // value cannot be a key bound (e.g. a Double against a key column)
    }
    switch (f.getOp()) {
      case EQUAL:
        // Folded as part of the used equality prefix, or as the demoted full-equality terminal
        // column (rc, which is the last key column in that case).
        return idx < eqLen || (fullEquality && idx == rc);
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL_TO:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL_TO:
        // A range is folded only when it IS the single range column, and never in the full-equality
        // case (where rc carries the demoted equality, not a range).
        return idx == rc && !fullEquality;
      default:
        return false; // IN / NOT_IN: not expressible as a contiguous key range
    }
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
        // The value's type cannot be a Tablestore primary key (STRING/INTEGER/BINARY) -- e.g. a
        // Double or Boolean compared against a key column. This is NOT swallowed: the predicate is
        // still emitted as a column filter (the correctness layer), which enforces it exactly; we
        // only skip using it as a key range bound (the seek-optimization layer). Bounds must never
        // over-exclude, so an unrepresentable value simply leaves this column's range open.
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
   * Builds the correctness-layer column filter: each predicate becomes a
   * {@link ColumnValueFilter}, ANDed together, so the returned rows are exact regardless of how
   * loose the key bounds are, with no dependency on the bounds being tight.
   *
   * <p><b>Redundant-equality elision.</b> An {@code EQUAL} predicate on a used-equality-prefix
   * column ({@code pkColumns[i]} for {@code i} in {@code [0, eqLen)}) is provably redundant: the
   * key range pins that column to that exact value on both the inclusive start and the exclusive
   * end, so every scanned row already carries it. Such a predicate is dropped rather than
   * re-checked server-side. The elision is value-guarded — it fires only when the predicate's value
   * equals the value actually pinned into the range ({@code eqPrefix[i]}), so a (degenerate) second
   * equality on the same column with a different value is kept and still enforced. Everything else
   * is retained: range predicates (they trim widened terminal bounds), the demoted full-equality
   * terminal column (it trims the {@code [v, v]} widening), {@code IN}/{@code NOT_IN}, non-key
   * predicates, and an equality after a gap (unconsumed, so still needed).
   *
   * <p>{@code IN} expands to an {@code OR} of equals; {@code NOT_IN} to a {@code NOT} of that
   * {@code OR}. Returns null when nothing remains after elision (no predicates, or every predicate
   * was a redundant prefix equality).
   *
   * @param pkColumns the target's ordered primary-key column names.
   * @param eqPrefix the values pinned into the key range for the used equality prefix; its length
   *     is the used equality-prefix length ({@code eqLen}), and {@code eqPrefix[i]} is the value
   *     pinned for {@code pkColumns[i]}.
   */
  static ColumnValueFilter buildColumnFilter(
      List<Filter> filters, List<String> pkColumns, PrimaryKeyValue[] eqPrefix) {
    if (filters == null) {
      return null;
    }
    List<ColumnValueFilter> parts = new ArrayList<>();
    for (Filter f : filters) {
      if (isRedundantPrefixEquality(f, pkColumns, eqPrefix)) {
        continue; // pinned exactly by the key-range equality prefix; the re-check would be a no-op
      }
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

  // Whether this predicate is an EQUAL on a used-equality-prefix column whose value is exactly the
  // one pinned into the key range for that column. Such a predicate is redundant: the range fixes
  // that column to that value on both bounds, so every scanned row already satisfies it. The
  // value guard keeps the elision provably safe — a differing value on the same prefix column
  // (a degenerate contradictory query) is NOT dropped, so the column filter still rejects the rows
  // the range would otherwise return. The demoted full-equality terminal column is not part of the
  // prefix ({@code eqLen == n - 1} there), so its equality is never elided here.
  private static boolean isRedundantPrefixEquality(
      Filter f, List<String> pkColumns, PrimaryKeyValue[] eqPrefix) {
    if (f.getOp() != FilterOperation.EQUAL) {
      return false;
    }
    int idx = pkColumns.indexOf(f.getFieldPath());
    if (idx < 0 || idx >= eqPrefix.length) {
      return false;
    }
    PrimaryKeyValue pkv = toPrimaryKeyValue(f.getValue());
    return pkv != null && pkv.equals(eqPrefix[idx]);
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
