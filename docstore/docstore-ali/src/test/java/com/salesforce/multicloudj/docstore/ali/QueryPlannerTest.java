package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.salesforce.multicloudj.docstore.driver.Filter;
import com.salesforce.multicloudj.docstore.driver.FilterOperation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Correctness tests for {@link QueryPlanner}, verified with an in-memory model of
 * Tablestore GetRange semantics.
 *
 * <p>The hard invariant under test: <b>the planner's key range must never exclude a row that
 * satisfies the query predicates.</b> Since the column filter is the correctness layer, a valid
 * result is precisely {@code inRange ∩ predicatesHold}; for that to equal {@code predicatesHold}
 * (the true answer), every predicate-satisfying row must fall in the range. So the test asserts
 * {@code expected ⊆ inRange} — a matching row dropped by the bounds is a correctness bug.
 *
 * <p>Range membership is evaluated with the SDK's own {@link PrimaryKey#compareTo} (which handles
 * INF_MIN/INF_MAX), so the model reflects real server behavior:
 *
 * <ul>
 *   <li>FORWARD: {@code start <= row < end}
 *   <li>BACKWARD: {@code end < row <= start}
 * </ul>
 *
 * <p>Coverage spans primary-key arity {1,2,3}, every range operator at terminal and non-terminal
 * positions, equality prefixes, ASC/DESC, and IN/NOT_IN — the cases a conformance-only test set
 * would miss.
 */
class QueryPlannerTest {

  // A test row: its full primary-key tuple plus the raw values (for predicate evaluation).
  private static final class Row {
    final PrimaryKey pk;
    final List<String> cols;
    final List<Object> vals;

    Row(List<String> cols, List<Object> vals) {
      this.cols = cols;
      this.vals = vals;
      PrimaryKeyBuilder b = PrimaryKeyBuilder.createPrimaryKeyBuilder();
      for (int i = 0; i < cols.size(); i++) {
        b.addPrimaryKeyColumn(cols.get(i), pkv(vals.get(i)));
      }
      this.pk = b.build();
    }

    Object valOf(String col) {
      int i = cols.indexOf(col);
      return i < 0 ? null : vals.get(i);
    }
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

  // the core assertion

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
                    + row.vals
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

  private static final FilterOperation[] RANGE_OPS = {
    FilterOperation.GREATER_THAN,
    FilterOperation.GREATER_THAN_OR_EQUAL_TO,
    FilterOperation.LESS_THAN,
    FilterOperation.LESS_THAN_OR_EQUAL_TO
  };

  // single-column PK

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

  // two-column PK (partition eq + terminal sort range)

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

  // three-column PK (eq prefix + MIDDLE range = the trailing-INF trick)

  @Test
  void threeColumn_eqPrefix_middleRange_bothDirections() {
    List<String> pk = List.of("A", "B", "C");
    List<Row> u =
        universe3(pk, new Object[] {"a1", "a2"}, new Object[] {1, 2, 3}, new Object[] {"c1", "c2"});
    // A = a1 (eq prefix len 1), B <op> 2 (MIDDLE range, position 1 of 3 -> non-terminal).
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
    // A = a1, B = 1 (eq prefix len 2), C <op> c2 (TERMINAL range, position 2 of 3).
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

  // IN / NOT_IN (no key bounds; must not exclude)

  @Test
  void inAndNotIn_onKeyColumn_doNotExclude() {
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {"a1", "a2", "a3"}, new Object[] {1, 2});
    assertNoExclusion(
        pk, u, List.of(filter("A", FilterOperation.IN, Arrays.asList("a1", "a3"))), true);
    assertNoExclusion(
        pk, u, List.of(filter("A", FilterOperation.NOT_IN, Arrays.asList("a1", "a3"))), true);
  }

  // non-key filter present (must not affect key bounds correctness)

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
      boolean keyMatch =
          cmp(row.valOf("A"), "a1") == 0 && cmp(row.valOf("B"), 1) > 0;
      if (keyMatch) {
        Assertions.assertTrue(inRange(row, plan), () -> "key-matching row excluded: " + row.vals);
      }
    }
  }

  // empty filters = full scan

  @Test
  void noFilters_fullScan_includesEverything() {
    List<String> pk = List.of("A", "B");
    List<Row> u = universe2(pk, new Object[] {"a1", "a2"}, new Object[] {1, 2});
    assertNoExclusion(pk, u, List.of(), true);
    assertNoExclusion(pk, u, List.of(), false);
  }
}
