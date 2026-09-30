package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.filter.ColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.salesforce.multicloudj.docstore.driver.Filter;
import com.salesforce.multicloudj.docstore.driver.FilterOperation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Correctness tests for {@link QueryPlanner}, verified with an in-memory model of Tablestore
 * GetRange semantics.
 *
 * <p>Two layers are checked:
 *
 * <ul>
 *   <li><b>No-exclusion invariant</b> (the original bound-arithmetic guard): the planner's key
 *       range must never exclude a row that satisfies the query predicates, i.e.
 *       {@code expected ⊆ inRange}. A matching row dropped by the bounds is a correctness bug.
 *   <li><b>Exact result set</b> (the stronger invariant, which also validates the option-3
 *       redundant-equality elision and the key-range-tight gate): evaluating the ACTUAL emitted
 *       column filter tree over the in-range rows must reproduce the true predicate answer exactly,
 *       i.e. {@code {row : inRange(row) AND columnFilterHolds(row)}} must equal
 *       {@code {row : allPredicatesHold(row)}}.
 *       Nulling the column filter, or over-dropping a non-redundant predicate, makes a case fail
 *       with surplus/missing rows.
 * </ul>
 *
 * <p>Range membership is evaluated with the SDK's own {@link PrimaryKey#compareTo} (which handles
 * INF_MIN/INF_MAX), so the model reflects real server behavior:
 *
 * <ul>
 *   <li>FORWARD: {@code start <= row < end}
 *   <li>BACKWARD: {@code end < row <= start}
 * </ul>
 *
 * <p>Coverage spans primary-key arity {1,2,3,4}, every range operator at terminal and non-terminal
 * positions, equality prefixes, full equality, IN/NOT_IN, non-key predicates, equality-after-a-gap,
 * unanchored key predicates, second ranges, non-PK-typed key values, and empty filters — plus the
 * {@code keyRangeTight} classification and the option-3 elision for each.
 */
class QueryPlannerTest {

  // A test row: its full primary-key tuple plus every column's raw value (key columns AND non-key
  // attributes) for predicate / column-filter evaluation.
  private static final class Row {
    final PrimaryKey pk;
    final Map<String, Object> values;

    // Key-only row: the values are the primary-key column values in key order.
    Row(List<String> pkCols, List<Object> pkVals) {
      this(pkCols, toMap(pkCols, pkVals));
    }

    // Row carrying arbitrary named columns; pkCols selects which of them form the primary key.
    Row(List<String> pkCols, Map<String, Object> values) {
      this.values = values;
      PrimaryKeyBuilder b = PrimaryKeyBuilder.createPrimaryKeyBuilder();
      for (String c : pkCols) {
        b.addPrimaryKeyColumn(c, pkv(values.get(c)));
      }
      this.pk = b.build();
    }

    Object valOf(String col) {
      return values.get(col);
    }
  }

  private static Map<String, Object> toMap(List<String> cols, List<Object> vals) {
    Map<String, Object> m = new LinkedHashMap<>();
    for (int i = 0; i < cols.size(); i++) {
      m.put(cols.get(i), vals.get(i));
    }
    return m;
  }

  private static PrimaryKeyValue pkv(Object v) {
    if (v instanceof Integer) {
      return PrimaryKeyValue.fromLong((Integer) v);
    }
    if (v instanceof Long) {
      return PrimaryKeyValue.fromLong((Long) v);
    }
    return PrimaryKeyValue.fromString(String.valueOf(v));
  }

  // in-memory model of GetRange range membership

  private static boolean inRange(Row row, QueryPlanner.Plan plan) {
    PrimaryKey start = plan.getInclusiveStartPrimaryKey();
    PrimaryKey end = plan.getExclusiveEndPrimaryKey();
    if (plan.getDirection() == Direction.FORWARD) {
      return start.compareTo(row.pk) <= 0 && row.pk.compareTo(end) < 0;
    } else {
      return row.pk.compareTo(start) <= 0 && end.compareTo(row.pk) < 0;
    }
  }

  // predicate evaluation (the "true answer")

  @SuppressWarnings("unchecked")
  private static boolean predicateHolds(Row row, Filter f) {
    Object rowVal = row.valOf(f.getFieldPath());
    switch (f.getOp()) {
      case EQUAL:
        return cmp(rowVal, f.getValue()) == 0;
      case GREATER_THAN:
        return cmp(rowVal, f.getValue()) > 0;
      case GREATER_THAN_OR_EQUAL_TO:
        return cmp(rowVal, f.getValue()) >= 0;
      case LESS_THAN:
        return cmp(rowVal, f.getValue()) < 0;
      case LESS_THAN_OR_EQUAL_TO:
        return cmp(rowVal, f.getValue()) <= 0;
      case IN:
        for (Object o : (Iterable<Object>) f.getValue()) {
          if (cmp(rowVal, o) == 0) {
            return true;
          }
        }
        return false;
      case NOT_IN:
        for (Object o : (Iterable<Object>) f.getValue()) {
          if (cmp(rowVal, o) == 0) {
            return false;
          }
        }
        return true;
      default:
        throw new IllegalArgumentException("unhandled op " + f.getOp());
    }
  }

  private static int cmp(Object a, Object b) {
    if (a instanceof Number && b instanceof Number) {
      return Long.compare(((Number) a).longValue(), ((Number) b).longValue());
    }
    return String.valueOf(a).compareTo(String.valueOf(b));
  }

  private static boolean allPredicatesHold(Row row, List<Filter> filters) {
    for (Filter f : filters) {
      if (!predicateHolds(row, f)) {
        return false;
      }
    }
    return true;
  }

  // Interprets the ACTUAL column-filter tree the planner emitted, exactly as Tablestore would:
  // SingleColumnValueFilter compares the row's value (missing -> passIfMissing), and
  // CompositeColumnValueFilter combines its children with AND / OR / NOT. A null filter passes
  // every row (no server-side filtering).
  private static boolean columnFilterHolds(ColumnValueFilter filter, Row row) {
    if (filter == null) {
      return true;
    }
    if (filter instanceof SingleColumnValueFilter) {
      return singleHolds((SingleColumnValueFilter) filter, row);
    }
    CompositeColumnValueFilter comp = (CompositeColumnValueFilter) filter;
    List<ColumnValueFilter> subs = comp.getSubFilters();
    switch (comp.getOperationType()) {
      case AND:
        for (ColumnValueFilter s : subs) {
          if (!columnFilterHolds(s, row)) {
            return false;
          }
        }
        return true;
      case OR:
        for (ColumnValueFilter s : subs) {
          if (columnFilterHolds(s, row)) {
            return true;
          }
        }
        return false;
      case NOT:
        return !columnFilterHolds(subs.get(0), row);
      default:
        throw new IllegalStateException("unhandled logic op " + comp.getOperationType());
    }
  }

  private static boolean singleHolds(SingleColumnValueFilter f, Row row) {
    Object rowVal = row.valOf(f.getColumnName());
    if (rowVal == null) {
      return f.isPassIfMissing();
    }
    int c = cmp(rowVal, f.getColumnValue().getValue());
    switch (f.getOperator()) {
      case EQUAL:
        return c == 0;
      case NOT_EQUAL:
        return c != 0;
      case GREATER_THAN:
        return c > 0;
      case GREATER_EQUAL:
        return c >= 0;
      case LESS_THAN:
        return c < 0;
      case LESS_EQUAL:
        return c <= 0;
      default:
        throw new IllegalStateException("unhandled compare op " + f.getOperator());
    }
  }

  // assertions

  /** Asserts every predicate-satisfying row falls within the planner's key range (no exclusion). */
  private void assertNoExclusion(
      List<String> pkCols, List<Row> universe, List<Filter> filters, boolean asc) {
    QueryPlanner.Plan plan = QueryPlanner.plan(pkCols, filters, asc);
    for (Row row : universe) {
      if (allPredicatesHold(row, filters)) {
        Assertions.assertTrue(
            inRange(row, plan),
            () ->
                "Row "
                    + row.values
                    + " satisfies "
                    + describe(filters)
                    + " (asc="
                    + asc
                    + ") but was EXCLUDED by range ["
                    + plan.getInclusiveStartPrimaryKey()
                    + ", "
                    + plan.getExclusiveEndPrimaryKey()
                    + ") dir="
                    + plan.getDirection());
      }
    }
    // Direction must reflect orderAscending.
    Assertions.assertEquals(asc ? Direction.FORWARD : Direction.BACKWARD, plan.getDirection());
  }

  /**
   * Asserts the planner reproduces the EXACT predicate answer: evaluating the emitted column filter
   * over the in-range rows equals {@code allPredicatesHold}. Also asserts scan direction. Returns
   * the plan so callers can add key-range-tight / elision assertions.
   */
  private QueryPlanner.Plan assertExactResultSet(
      List<String> pkCols, List<Row> universe, List<Filter> filters, boolean asc) {
    QueryPlanner.Plan plan = QueryPlanner.plan(pkCols, filters, asc);
    for (Row row : universe) {
      boolean expected = allPredicatesHold(row, filters);
      boolean actual = inRange(row, plan) && columnFilterHolds(plan.getColumnFilter(), row);
      Assertions.assertEquals(
          expected,
          actual,
          () ->
              "Row "
                  + row.values
                  + " under "
                  + describe(filters)
                  + " (asc="
                  + asc
                  + "): expected in-result="
                  + expected
                  + " but planner yields "
                  + actual
                  + " [inRange="
                  + inRange(row, plan)
                  + ", columnFilterHolds="
                  + columnFilterHolds(plan.getColumnFilter(), row)
                  + "]");
    }
    Assertions.assertEquals(asc ? Direction.FORWARD : Direction.BACKWARD, plan.getDirection());
    return plan;
  }

  private static String describe(List<Filter> filters) {
    StringBuilder sb = new StringBuilder();
    for (Filter f : filters) {
      sb.append(f.getFieldPath()).append(' ').append(f.getOp()).append(' ').append(f.getValue())
          .append("; ");
    }
    return sb.toString();
  }

  private static Filter filter(String field, FilterOperation op, Object value) {
    return new Filter(field, op, value);
  }

  // Collects every SingleColumnValueFilter in the (possibly composite) filter tree.
  private static List<SingleColumnValueFilter> singles(ColumnValueFilter f) {
    List<SingleColumnValueFilter> out = new ArrayList<>();
    collectSingles(f, out);
    return out;
  }

  private static void collectSingles(ColumnValueFilter f, List<SingleColumnValueFilter> out) {
    if (f == null) {
      return;
    }
    if (f instanceof SingleColumnValueFilter) {
      out.add((SingleColumnValueFilter) f);
      return;
    }
    for (ColumnValueFilter s : ((CompositeColumnValueFilter) f).getSubFilters()) {
      collectSingles(s, out);
    }
  }

  // Whether the emitted column filter contains any SingleColumnValueFilter on the given column.
  private static boolean hasSingleOn(ColumnValueFilter f, String col) {
    for (SingleColumnValueFilter s : singles(f)) {
      if (s.getColumnName().equals(col)) {
        return true;
      }
    }
    return false;
  }

  // universes

  private List<Row> universe1(List<String> cols, Object[] a) {
    List<Row> rows = new ArrayList<>();
    for (Object x : a) {
      rows.add(new Row(cols, List.of(x)));
    }
    return rows;
  }

  private List<Row> universe2(List<String> cols, Object[] a, Object[] b) {
    List<Row> rows = new ArrayList<>();
    for (Object x : a) {
      for (Object y : b) {
        rows.add(new Row(cols, List.of(x, y)));
      }
    }
    return rows;
  }

  private List<Row> universe3(List<String> cols, Object[] a, Object[] b, Object[] c) {
    List<Row> rows = new ArrayList<>();
    for (Object x : a) {
      for (Object y : b) {
        for (Object z : c) {
          rows.add(new Row(cols, List.of(x, y, z)));
        }
      }
    }
    return rows;
  }

  private List<Row> universe4(
      List<String> cols, Object[] a, Object[] b, Object[] c, Object[] d) {
    List<Row> rows = new ArrayList<>();
    for (Object w : a) {
      for (Object x : b) {
        for (Object y : c) {
          for (Object z : d) {
            rows.add(new Row(cols, List.of(w, x, y, z)));
          }
        }
      }
    }
    return rows;
  }

  private static final FilterOperation[] RANGE_OPS = {
    FilterOperation.GREATER_THAN,
    FilterOperation.GREATER_THAN_OR_EQUAL_TO,
    FilterOperation.LESS_THAN,
    FilterOperation.LESS_THAN_OR_EQUAL_TO
  };

  // ---------------------------------------------------------------------------------------------
  // no-exclusion coverage (original bound-arithmetic guard)
  // ---------------------------------------------------------------------------------------------

  @Test
  void singleColumn_stringRangeOps_bothDirections() {
    List<String> pk = List.of("A");
    List<Row> u = universe1(pk, new Object[] {"a1", "a2", "a3"});
    for (FilterOperation op : RANGE_OPS) {
      for (boolean asc : new boolean[] {true, false}) {
        assertNoExclusion(pk, u, List.of(filter("A", op, "a2")), asc);
      }
    }
  }

  @Test
  void singleColumn_intRangeOps_bothDirections() {
    List<String> pk = List.of("A");
    List<Row> u = universe1(pk, new Object[] {1, 2, 3, 4});
    for (FilterOperation op : RANGE_OPS) {
      for (boolean asc : new boolean[] {true, false}) {
        assertNoExclusion(pk, u, List.of(filter("A", op, 2)), asc);
      }
    }
  }

  @Test
  void singleColumn_equality() {
    List<String> pk = List.of("A");
    List<Row> u = universe1(pk, new Object[] {"a1", "a2", "a3"});
    assertNoExclusion(pk, u, List.of(filter("A", FilterOperation.EQUAL, "a2")), true);
    assertNoExclusion(pk, u, List.of(filter("A", FilterOperation.EQUAL, "a2")), false);
  }

  @Test
  void twoColumn_partitionEq_terminalSortRange() {
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {"a1", "a2"}, new Object[] {1, 2, 3});
    for (FilterOperation op : RANGE_OPS) {
      for (boolean asc : new boolean[] {true, false}) {
        assertNoExclusion(
            pk,
            u,
            List.of(filter("A", FilterOperation.EQUAL, "a1"), filter("B", op, 2)),
            asc);
      }
    }
  }

  @Test
  void twoColumn_partitionEqOnly() {
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {"a1", "a2"}, new Object[] {1, 2, 3});
    assertNoExclusion(pk, u, List.of(filter("A", FilterOperation.EQUAL, "a1")), true);
    assertNoExclusion(pk, u, List.of(filter("A", FilterOperation.EQUAL, "a1")), false);
  }

  @Test
  void threeColumn_eqPrefix_middleRange_bothDirections() {
    List<String> pk = List.of("A", "B", "C");
    List<Row> u =
        universe3(pk, new Object[] {"a1", "a2"}, new Object[] {1, 2, 3}, new Object[] {"c1", "c2"});
    for (FilterOperation op : RANGE_OPS) {
      for (boolean asc : new boolean[] {true, false}) {
        assertNoExclusion(
            pk,
            u,
            List.of(filter("A", FilterOperation.EQUAL, "a1"), filter("B", op, 2)),
            asc);
      }
    }
  }

  @Test
  void threeColumn_eqPrefixLen2_terminalRange_bothDirections() {
    List<String> pk = List.of("A", "B", "C");
    List<Row> u =
        universe3(
            pk, new Object[] {"a1", "a2"}, new Object[] {1, 2}, new Object[] {"c1", "c2", "c3"});
    for (FilterOperation op : RANGE_OPS) {
      for (boolean asc : new boolean[] {true, false}) {
        assertNoExclusion(
            pk,
            u,
            List.of(
                filter("A", FilterOperation.EQUAL, "a1"),
                filter("B", FilterOperation.EQUAL, 1),
                filter("C", op, "c2")),
            asc);
      }
    }
  }

  @Test
  void threeColumn_fullEquality() {
    List<String> pk = List.of("A", "B", "C");
    List<Row> u =
        universe3(pk, new Object[] {"a1", "a2"}, new Object[] {1, 2}, new Object[] {"c1", "c2"});
    assertNoExclusion(
        pk,
        u,
        List.of(
            filter("A", FilterOperation.EQUAL, "a1"),
            filter("B", FilterOperation.EQUAL, 1),
            filter("C", FilterOperation.EQUAL, "c1")),
        true);
  }

  @Test
  void inAndNotIn_onKeyColumn_doNotExclude() {
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {"a1", "a2", "a3"}, new Object[] {1, 2});
    assertNoExclusion(
        pk, u, List.of(filter("A", FilterOperation.IN, Arrays.asList("a1", "a3"))), true);
    assertNoExclusion(
        pk, u, List.of(filter("A", FilterOperation.NOT_IN, Arrays.asList("a1", "a3"))), true);
  }

  @Test
  void noFilters_fullScan_includesEverything() {
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {"a1", "a2"}, new Object[] {1, 2});
    assertNoExclusion(pk, u, List.of(), true);
    assertNoExclusion(pk, u, List.of(), false);
  }

  // ---------------------------------------------------------------------------------------------
  // exact-result-set + keyRangeTight + option-3 elision, one per shape
  // ---------------------------------------------------------------------------------------------

  @Test
  void exact_singleColEquality_notTight_keepsDemotedTerminal() {
    // Single-column equality is the demoted full-equality terminal (eqLen=0), so the equality is
    // NOT elided (no prefix to pin it) -- it stays as the terminal trim. Being a demoted [v,v]
    // terminal, the scanned-away end is widened fully open, so the plan is NOT tight.
    List<String> pk = List.of("A");
    List<Row> u = universe1(pk, new Object[] {"a1", "a2", "a3"});
    for (boolean asc : new boolean[] {true, false}) {
      QueryPlanner.Plan plan =
          assertExactResultSet(pk, u, List.of(filter("A", FilterOperation.EQUAL, "a2")), asc);
      Assertions.assertFalse(
          plan.isKeyRangeTight(), "a demoted full-equality terminal is open-ended, not tight");
      Assertions.assertTrue(hasSingleOn(plan.getColumnFilter(), "A"), "demoted terminal kept");
    }
  }

  @Test
  void exact_twoColEqualityPrefix_partial_tight_dropsBothPrefixEqualities() {
    // A=a1 & B=b1 is an equality prefix of length 2 over a 3-col key (C unconstrained). Both prefix
    // equalities are pinned by the range and elided, leaving no column filter.
    List<String> pk = List.of("A", "B", "C");
    List<Row> u =
        universe3(pk, new Object[] {"a1", "a2"}, new Object[] {"b1", "b2"}, new Object[] {1, 2});
    List<Filter> filters =
        List.of(filter("A", FilterOperation.EQUAL, "a1"), filter("B", FilterOperation.EQUAL, "b1"));
    for (boolean asc : new boolean[] {true, false}) {
      QueryPlanner.Plan plan = assertExactResultSet(pk, u, filters, asc);
      Assertions.assertTrue(plan.isKeyRangeTight(), "pure equality prefix is tight");
      Assertions.assertNull(
          plan.getColumnFilter(), "both prefix equalities are redundant and elided");
    }
  }

  @Test
  void exact_threeColEqualityPrefix_partial_tight_dropsAllPrefixEqualities() {
    // A=a1 & B=b1 & C=c1 is an equality prefix of length 3 over a 4-col key (D unconstrained). All
    // three prefix equalities are elided.
    List<String> pk = List.of("A", "B", "C", "D");
    List<Row> u =
        universe4(
            pk,
            new Object[] {"a1", "a2"},
            new Object[] {"b1", "b2"},
            new Object[] {"c1", "c2"},
            new Object[] {1, 2});
    List<Filter> filters =
        List.of(
            filter("A", FilterOperation.EQUAL, "a1"),
            filter("B", FilterOperation.EQUAL, "b1"),
            filter("C", FilterOperation.EQUAL, "c1"));
    for (boolean asc : new boolean[] {true, false}) {
      QueryPlanner.Plan plan = assertExactResultSet(pk, u, filters, asc);
      Assertions.assertTrue(plan.isKeyRangeTight(), "3-col equality prefix is tight");
      Assertions.assertNull(plan.getColumnFilter(), "all three prefix equalities elided");
    }
  }

  @Test
  void exact_fullEquality_notTight_dropsPrefixKeepsDemotedTerminal() {
    // A=a1 & B=b1 & C=c1 fully covers a 3-col key: A and B are the used prefix (elided), C is the
    // demoted terminal ([c1,c1] widened open) and MUST be kept to trim the terminal widening.
    // Because that terminal end is open in both directions, the plan is NOT tight.
    List<String> pk = List.of("A", "B", "C");
    List<Row> u =
        universe3(pk, new Object[] {"a1", "a2"}, new Object[] {"b1", "b2"}, new Object[] {"c1",
            "c2"});
    List<Filter> filters =
        List.of(
            filter("A", FilterOperation.EQUAL, "a1"),
            filter("B", FilterOperation.EQUAL, "b1"),
            filter("C", FilterOperation.EQUAL, "c1"));
    for (boolean asc : new boolean[] {true, false}) {
      QueryPlanner.Plan plan = assertExactResultSet(pk, u, filters, asc);
      Assertions.assertFalse(
          plan.isKeyRangeTight(), "full equality opens the demoted terminal end, not tight");
      Assertions.assertFalse(hasSingleOn(plan.getColumnFilter(), "A"), "prefix A elided");
      Assertions.assertFalse(hasSingleOn(plan.getColumnFilter(), "B"), "prefix B elided");
      Assertions.assertTrue(
          hasSingleOn(plan.getColumnFilter(), "C"), "demoted terminal C kept for trimming");
    }
  }

  @Test
  void exact_eqPrefix_terminalRange_tightnessDependsOnBoundaryExactness() {
    // A=a1 & B <op> 2: equality prefix + a TERMINAL range. The equality is always elided and the
    // range is always kept (it trims the terminal boundary), and the exact result set holds for
    // every shape. Tightness, however, splits by whether the terminal bound can be represented
    // exactly at the last PK column:
    //  - forward '<=' (LESS_THAN_OR_EQUAL_TO) and backward '>=' (GREATER_THAN_OR_EQUAL_TO) widen
    //    the scanned-away end fully open, so the column filter trims an unbounded tail: NOT tight.
    //  - forward >,>=,< and backward <,<=,> pin the boundary exactly or loosen by at most one row:
    //    tight.
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {"a1", "a2"}, new Object[] {1, 2, 3});
    for (FilterOperation op : RANGE_OPS) {
      for (boolean asc : new boolean[] {true, false}) {
        List<Filter> filters =
            List.of(filter("A", FilterOperation.EQUAL, "a1"), filter("B", op, 2));
        QueryPlanner.Plan plan = assertExactResultSet(pk, u, filters, asc);
        Assertions.assertFalse(hasSingleOn(plan.getColumnFilter(), "A"), "prefix A elided");
        Assertions.assertTrue(hasSingleOn(plan.getColumnFilter(), "B"), "range on B kept");
        boolean openEnded =
            (asc && op == FilterOperation.LESS_THAN_OR_EQUAL_TO)
                || (!asc && op == FilterOperation.GREATER_THAN_OR_EQUAL_TO);
        if (openEnded) {
          Assertions.assertFalse(
              plan.isKeyRangeTight(),
              "terminal " + op + " (asc=" + asc + ") opens the scanned-away end -> not tight");
        } else {
          Assertions.assertTrue(
              plan.isKeyRangeTight(),
              "terminal " + op + " (asc=" + asc + ") has an exact boundary -> tight");
        }
      }
    }
  }

  @Test
  void exact_eqPrefix_nonTerminalRange_tight_dropsEqualityKeepsRange() {
    // A=a1 & B <op> 2 over a 3-col key: a MIDDLE (non-terminal) range (C unconstrained). Bounds are
    // exact for a non-terminal range, but the range predicate is still retained (never elided).
    List<String> pk = List.of("A", "B", "C");
    List<Row> u =
        universe3(pk, new Object[] {"a1", "a2"}, new Object[] {1, 2, 3}, new Object[] {"c1", "c2"});
    for (FilterOperation op : RANGE_OPS) {
      for (boolean asc : new boolean[] {true, false}) {
        List<Filter> filters =
            List.of(filter("A", FilterOperation.EQUAL, "a1"), filter("B", op, 2));
        QueryPlanner.Plan plan = assertExactResultSet(pk, u, filters, asc);
        Assertions.assertTrue(plan.isKeyRangeTight(), "eq-prefix + non-terminal range is tight");
        Assertions.assertFalse(hasSingleOn(plan.getColumnFilter(), "A"), "prefix A elided");
        Assertions.assertTrue(hasSingleOn(plan.getColumnFilter(), "B"), "range on B kept");
      }
    }
  }

  @Test
  void exact_inOnKeyColumn_notTight_keepsMembershipFilter() {
    // IN on a key column cannot narrow the key range, so the whole partition is scanned and the OR
    // membership filter drops non-members -> NOT tight, filter retained.
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {"a1", "a2", "a3"}, new Object[] {1, 2});
    List<Filter> filters = List.of(filter("A", FilterOperation.IN, Arrays.asList("a1", "a3")));
    for (boolean asc : new boolean[] {true, false}) {
      QueryPlanner.Plan plan = assertExactResultSet(pk, u, filters, asc);
      Assertions.assertFalse(plan.isKeyRangeTight(), "IN on a key column is not tight");
      Assertions.assertNotNull(plan.getColumnFilter(), "IN membership must be enforced");
    }
  }

  @Test
  void exact_notInOnKeyColumn_notTight_keepsMembershipFilter() {
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {"a1", "a2", "a3"}, new Object[] {1, 2});
    List<Filter> filters = List.of(filter("A", FilterOperation.NOT_IN, Arrays.asList("a1", "a3")));
    for (boolean asc : new boolean[] {true, false}) {
      QueryPlanner.Plan plan = assertExactResultSet(pk, u, filters, asc);
      Assertions.assertFalse(plan.isKeyRangeTight(), "NOT_IN on a key column is not tight");
      Assertions.assertNotNull(plan.getColumnFilter(), "NOT_IN membership must be enforced");
    }
  }

  @Test
  void exact_partitionEqPlusNonKeyPredicate_notTight_dropsEqKeepsNonKey() {
    // The #18 shape: A=a1 (partition key) AND glitch=true (a NON-key attribute). The partition
    // equality is elided, but the non-key predicate is enforced only by the column filter and drops
    // most scanned rows -> NOT tight (so the page cap must be turned off).
    List<String> pk = List.of("A", "B");
    List<Row> u = new ArrayList<>();
    for (Object a : new Object[] {"a1", "a2"}) {
      for (Object b : new Object[] {1, 2}) {
        for (Object g : new Object[] {true, false}) {
          Map<String, Object> m = new LinkedHashMap<>();
          m.put("A", a);
          m.put("B", b);
          m.put("glitch", g);
          u.add(new Row(pk, m));
        }
      }
    }
    List<Filter> filters =
        List.of(
            filter("A", FilterOperation.EQUAL, "a1"),
            filter("glitch", FilterOperation.EQUAL, true));
    for (boolean asc : new boolean[] {true, false}) {
      QueryPlanner.Plan plan = assertExactResultSet(pk, u, filters, asc);
      Assertions.assertFalse(plan.isKeyRangeTight(), "a non-key predicate makes it not tight");
      Assertions.assertFalse(hasSingleOn(plan.getColumnFilter(), "A"), "prefix A elided");
      Assertions.assertTrue(hasSingleOn(plan.getColumnFilter(), "glitch"), "non-key kept");
    }
  }

  @Test
  void exact_loneNonKeyPredicate_notTight_keepsFilter() {
    // A predicate on a non-key attribute only: no key range constraint at all, filter enforces it.
    List<String> pk = List.of("A", "B");
    List<Row> u = new ArrayList<>();
    for (Object a : new Object[] {"a1", "a2"}) {
      for (Object b : new Object[] {1, 2}) {
        for (Object z : new Object[] {"zz", "yy"}) {
          Map<String, Object> m = new LinkedHashMap<>();
          m.put("A", a);
          m.put("B", b);
          m.put("Z", z);
          u.add(new Row(pk, m));
        }
      }
    }
    List<Filter> filters = List.of(filter("Z", FilterOperation.EQUAL, "zz"));
    QueryPlanner.Plan plan = assertExactResultSet(pk, u, filters, true);
    Assertions.assertFalse(plan.isKeyRangeTight(), "a lone non-key predicate is not tight");
    Assertions.assertTrue(hasSingleOn(plan.getColumnFilter(), "Z"), "non-key predicate kept");
  }

  @Test
  void exact_equalityAfterGap_notTight_dropsPrefixKeepsGapEquality() {
    // pk [A,B,C], filters A=1 & C=3: B is a gap, so only A is the used prefix. A is elided; the
    // gap equality on C is UNCONSUMED and must be kept in the column filter.
    List<String> pk = List.of("A", "B", "C");
    List<Row> u = universe3(pk, new Object[] {1, 2}, new Object[] {1, 2}, new Object[] {3, 4});
    List<Filter> filters =
        List.of(filter("A", FilterOperation.EQUAL, 1), filter("C", FilterOperation.EQUAL, 3));
    for (boolean asc : new boolean[] {true, false}) {
      QueryPlanner.Plan plan = assertExactResultSet(pk, u, filters, asc);
      Assertions.assertFalse(plan.isKeyRangeTight(), "an equality after a gap is not tight");
      Assertions.assertFalse(hasSingleOn(plan.getColumnFilter(), "A"), "prefix A elided");
      Assertions.assertTrue(
          hasSingleOn(plan.getColumnFilter(), "C"), "gap equality on C kept (unconsumed)");
    }
  }

  @Test
  void exact_loneUnanchoredKeyPredicate_notTight_keepsFilter() {
    // A lone equality on the SORT key with no partition anchor: the leading key column has no
    // constraint, so nothing is folded (open range) and the equality is enforced by the filter.
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {"a1", "a2"}, new Object[] {4, 5, 6});
    List<Filter> filters = List.of(filter("B", FilterOperation.EQUAL, 5));
    for (boolean asc : new boolean[] {true, false}) {
      QueryPlanner.Plan plan = assertExactResultSet(pk, u, filters, asc);
      Assertions.assertFalse(plan.isKeyRangeTight(), "an unanchored sort-key eq is not tight");
      Assertions.assertTrue(hasSingleOn(plan.getColumnFilter(), "B"), "unanchored equality kept");
    }
  }

  @Test
  void exact_secondRange_notTight_keepsBothRanges() {
    // pk [A,B], filters A>1 & B<5: A is the single range column; B is a SECOND range, unfoldable
    // into the key bounds, so it is enforced only by the column filter -> NOT tight.
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {1, 2, 3}, new Object[] {4, 5, 6});
    List<Filter> filters =
        List.of(
            filter("A", FilterOperation.GREATER_THAN, 1),
            filter("B", FilterOperation.LESS_THAN, 5));
    for (boolean asc : new boolean[] {true, false}) {
      QueryPlanner.Plan plan = assertExactResultSet(pk, u, filters, asc);
      Assertions.assertFalse(plan.isKeyRangeTight(), "a second range column is not tight");
      Assertions.assertTrue(hasSingleOn(plan.getColumnFilter(), "A"), "range on A kept");
      Assertions.assertTrue(hasSingleOn(plan.getColumnFilter(), "B"), "second range on B kept");
    }
  }

  @Test
  void exact_repeatedLowerBoundsOnRangeColumn_notTight_bothOrders() {
    // A=1 & B>100 & B>0: B is the terminal range column, but extractPkConstraints keeps only the
    // LAST lower bound, so exactly one > predicate becomes the folded key bound and the other is
    // column-filter-only (and selective) -> NOT tight, order-independently. The exact result set
    // (only B>100 rows) must still hold no matter which of the two bounds the range retained.
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {1, 2}, new Object[] {0, 50, 100, 150, 200});
    List<List<Filter>> orders =
        List.of(
            List.of(
                filter("A", FilterOperation.EQUAL, 1),
                filter("B", FilterOperation.GREATER_THAN, 100),
                filter("B", FilterOperation.GREATER_THAN, 0)),
            List.of(
                filter("A", FilterOperation.EQUAL, 1),
                filter("B", FilterOperation.GREATER_THAN, 0),
                filter("B", FilterOperation.GREATER_THAN, 100)));
    for (List<Filter> filters : orders) {
      for (boolean asc : new boolean[] {true, false}) {
        QueryPlanner.Plan plan = assertExactResultSet(pk, u, filters, asc);
        Assertions.assertFalse(
            plan.isKeyRangeTight(), "two lower bounds on the range column are not tight");
      }
    }
  }

  @Test
  void exact_repeatedUpperBoundsOnRangeColumn_notTight_bothOrders() {
    // A=1 & B<100 & B<200: two upper bounds on the terminal range column; only the last is folded,
    // so the plan is NOT tight in both orders. Exact result set (only B<100 rows) still holds.
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {1, 2}, new Object[] {50, 100, 150, 200, 250});
    List<List<Filter>> orders =
        List.of(
            List.of(
                filter("A", FilterOperation.EQUAL, 1),
                filter("B", FilterOperation.LESS_THAN, 100),
                filter("B", FilterOperation.LESS_THAN, 200)),
            List.of(
                filter("A", FilterOperation.EQUAL, 1),
                filter("B", FilterOperation.LESS_THAN, 200),
                filter("B", FilterOperation.LESS_THAN, 100)));
    for (List<Filter> filters : orders) {
      for (boolean asc : new boolean[] {true, false}) {
        QueryPlanner.Plan plan = assertExactResultSet(pk, u, filters, asc);
        Assertions.assertFalse(
            plan.isKeyRangeTight(), "two upper bounds on the range column are not tight");
      }
    }
  }

  @Test
  void exact_repeatedMixedInclusiveSameSideBound_notTight() {
    // Mixed inclusive/exclusive on the SAME side (B>100 & B>=0) still counts as two lower bounds:
    // only one folds, the other is column-filter-only -> NOT tight. Exact set (B>100) holds.
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {1, 2}, new Object[] {0, 50, 100, 150, 200});
    List<Filter> filters =
        List.of(
            filter("A", FilterOperation.EQUAL, 1),
            filter("B", FilterOperation.GREATER_THAN, 100),
            filter("B", FilterOperation.GREATER_THAN_OR_EQUAL_TO, 0));
    for (boolean asc : new boolean[] {true, false}) {
      QueryPlanner.Plan plan = assertExactResultSet(pk, u, filters, asc);
      Assertions.assertFalse(
          plan.isKeyRangeTight(), "mixed inclusive/exclusive same-side bounds are not tight");
    }
  }

  @Test
  void exact_singleLowerAndUpperBound_stillTight() {
    // Control: one lower + one upper (A=1 & B>10 & B<100) is a normal bounded range -- exactly one
    // bound per side folds into the key range -> still TIGHT. The repeated-same-side gate must not
    // over-fire on a genuine two-sided range. Exact set (B in (10,100)) holds.
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {1, 2}, new Object[] {5, 10, 50, 100, 150});
    List<Filter> filters =
        List.of(
            filter("A", FilterOperation.EQUAL, 1),
            filter("B", FilterOperation.GREATER_THAN, 10),
            filter("B", FilterOperation.LESS_THAN, 100));
    for (boolean asc : new boolean[] {true, false}) {
      QueryPlanner.Plan plan = assertExactResultSet(pk, u, filters, asc);
      Assertions.assertTrue(
          plan.isKeyRangeTight(), "a single lower + single upper bound is still tight");
    }
  }

  @Test
  void exact_plainSingleRange_stillTight() {
    // Control: a plain single range (A=1 & B>10) remains tight -- unchanged by the same-side gate.
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {1, 2}, new Object[] {5, 10, 50, 100});
    List<Filter> filters =
        List.of(filter("A", FilterOperation.EQUAL, 1), filter("B", FilterOperation.GREATER_THAN,
            10));
    for (boolean asc : new boolean[] {true, false}) {
      QueryPlanner.Plan plan = assertExactResultSet(pk, u, filters, asc);
      Assertions.assertTrue(plan.isKeyRangeTight(), "a plain single range is still tight");
    }
  }

  @Test
  void exact_nonPkTypedKeyValue_notTight_dropsEqKeepsNonPkTyped() {
    // A=a1 (partition eq) & B=2.0 (a Double against key column B). A Double cannot be a Tablestore
    // primary-key value, so B is NOT folded into the range (left open) -> NOT tight; the predicate
    // is enforced only by the column filter. A is still a valid prefix equality and is elided.
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {"a1", "a2"}, new Object[] {1, 2, 3});
    List<Filter> filters =
        List.of(
            filter("A", FilterOperation.EQUAL, "a1"), filter("B", FilterOperation.EQUAL, 2.0));
    for (boolean asc : new boolean[] {true, false}) {
      QueryPlanner.Plan plan = assertExactResultSet(pk, u, filters, asc);
      Assertions.assertFalse(
          plan.isKeyRangeTight(), "a non-PK-typed value on a key column is not tight");
      Assertions.assertFalse(hasSingleOn(plan.getColumnFilter(), "A"), "prefix A elided");
      Assertions.assertTrue(hasSingleOn(plan.getColumnFilter(), "B"), "non-PK-typed pred kept");
    }
  }

  @Test
  void exact_noFilters_tight_noColumnFilter() {
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {"a1", "a2"}, new Object[] {1, 2});
    for (boolean asc : new boolean[] {true, false}) {
      QueryPlanner.Plan plan = assertExactResultSet(pk, u, List.of(), asc);
      Assertions.assertTrue(plan.isKeyRangeTight(), "an empty filter set is trivially tight");
      Assertions.assertNull(plan.getColumnFilter(), "no predicates -> no column filter");
    }
  }

  @Test
  void exact_conflictingDuplicateEquality_valueGuardPreventsOverDrop() {
    // Degenerate contradiction A=1 AND A=2 on a 2-col key: the range pins A to the LAST value (2),
    // so A=2 is redundant (elided) but A=1 is NOT (its value differs from the pinned one) and MUST
    // be kept. A naive "drop any equality on a prefix column" would drop BOTH, leaving the range's
    // A=2 rows wrongly in the result. The exact-set invariant (empty result here) proves the
    // value-guarded elision does not over-drop.
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {1, 2, 3}, new Object[] {5, 6});
    List<Filter> filters =
        List.of(filter("A", FilterOperation.EQUAL, 1), filter("A", FilterOperation.EQUAL, 2));
    for (boolean asc : new boolean[] {true, false}) {
      QueryPlanner.Plan plan = assertExactResultSet(pk, u, filters, asc);
      Assertions.assertTrue(
          hasSingleOn(plan.getColumnFilter(), "A"),
          "the mismatched-value equality must be kept, not over-dropped");
    }
  }

  @Test
  void nonKeyFilter_doesNotBreakRange() {
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {"a1", "a2"}, new Object[] {1, 2, 3});
    // A=a1, B>1 on the key, plus a non-key predicate that the universe rows don't carry.
    // The non-key filter can't be evaluated on rows here (valOf returns null), so restrict the
    // assertion to key predicates by omitting the non-key filter from the universe check but still
    // passing it to the planner to ensure it doesn't corrupt the bounds.
    QueryPlanner.Plan plan =
        QueryPlanner.plan(
            pk,
            List.of(
                filter("A", FilterOperation.EQUAL, "a1"),
                filter("B", FilterOperation.GREATER_THAN, 1),
                filter("Z", FilterOperation.EQUAL, "ignored")),
            true);
    for (Row row : u) {
      boolean keyMatch = cmp(row.valOf("A"), "a1") == 0 && cmp(row.valOf("B"), 1) > 0;
      if (keyMatch) {
        Assertions.assertTrue(inRange(row, plan), () -> "key-matching row excluded: " + row.values);
      }
    }
  }
}
