package com.salesforce.multicloudj.docstore.ali;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.Row;
import com.salesforce.multicloudj.docstore.driver.Document;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Behavioral tests for {@link AliDocumentIterator}: page-walking, limit, client-side offset, resume
 * (inclusive-start skip), and pagination-token emptiness. The underlying {@link QueryRunner} is
 * mocked to hand back scripted pages, so no live client is needed.
 */
class AliDocumentIteratorTest {

  private PrimaryKey pk(String player) {
    return PrimaryKeyBuilder.createPrimaryKeyBuilder()
        .addPrimaryKeyColumn("Game", PrimaryKeyValue.fromString("Zombie DMV"))
        .addPrimaryKeyColumn("Player", PrimaryKeyValue.fromString(player))
        .build();
  }

  private Row row(String player, long score) {
    return new Row(pk(player), new Column[] {new Column("Score", ColumnValue.fromLong(score))});
  }

  /**
   * Scripts the mocked runner to return {@code pages} in order (appending each page's rows to the
   * caller-supplied list), returning the given next-cursor per page. The last page returns null.
   */
  private QueryRunner runnerReturning(List<List<Row>> pages, List<PrimaryKey> nextCursors) {
    QueryRunner runner = mock(QueryRunner.class);
    int[] call = {0};
    when(runner.run(any(), any()))
        .thenAnswer(
            inv -> {
              int i = call[0]++;
              @SuppressWarnings("unchecked")
              List<Row> sink = (List<Row>) inv.getArgument(1);
              if (i < pages.size()) {
                sink.addAll(pages.get(i));
                return nextCursors.get(i);
              }
              return null;
            });
    return runner;
  }

  private List<String> collectPlayers(AliDocumentIterator iter) {
    List<String> players = new ArrayList<>();
    while (iter.hasNext()) {
      Map<String, Object> sink = new HashMap<>();
      iter.next(new Document(sink));
      players.add((String) sink.get("Player"));
    }
    return players;
  }

  @Test
  void singlePageReturnsAllThenStops() {
    QueryRunner runner =
        runnerReturning(
            List.of(List.of(row("billie", 111), row("fran", 33))),
            java.util.Arrays.asList((PrimaryKey) null));
    AliDocumentIterator iter = new AliDocumentIterator(runner, 0, 0, null);
    Assertions.assertEquals(List.of("billie", "fran"), collectPlayers(iter));
    Assertions.assertThrows(
        NoSuchElementException.class, () -> iter.next(new Document(new HashMap<>())));
  }

  @Test
  void loopsAcrossPagesFollowingCursor() {
    QueryRunner runner =
        runnerReturning(
            List.of(List.of(row("billie", 111)), List.of(row("fran", 33))),
            java.util.Arrays.asList(pk("fran"), (PrimaryKey) null));
    AliDocumentIterator iter = new AliDocumentIterator(runner, 0, 0, null);
    Assertions.assertEquals(List.of("billie", "fran"), collectPlayers(iter));
  }

  // A filtered page can be EMPTY yet non-final (0 matches + a live nextStartPrimaryKey), because
  // GetRange's limit caps rows scanned, not matched. The iterator must keep fetching across such
  // pages, not stop at the first empty one.
  @Test
  void skipsFilteredEmptyPageThenReturnsLaterMatches() {
    QueryRunner runner =
        runnerReturning(
            java.util.Arrays.asList(
                java.util.Collections.<Row>emptyList(), // page 1: all scanned rows filtered out
                List.of(row("fran", 33))), // page 2: real matches
            java.util.Arrays.asList(pk("cursor1"), (PrimaryKey) null));
    AliDocumentIterator iter = new AliDocumentIterator(runner, 0, 0, null);
    Assertions.assertEquals(List.of("fran"), collectPlayers(iter));
  }

  @Test
  void skipsMultipleConsecutiveFilteredEmptyPages() {
    QueryRunner runner =
        runnerReturning(
            java.util.Arrays.asList(
                java.util.Collections.<Row>emptyList(),
                java.util.Collections.<Row>emptyList(),
                List.of(row("mel", 190))),
            java.util.Arrays.asList(pk("c1"), pk("c2"), (PrimaryKey) null));
    AliDocumentIterator iter = new AliDocumentIterator(runner, 0, 0, null);
    Assertions.assertEquals(List.of("mel"), collectPlayers(iter));
  }

  // Offset skipping must also cross filtered-empty pages (same root cause as #1).
  @Test
  void offsetSkipCrossesFilteredEmptyPage() {
    QueryRunner runner =
        runnerReturning(
            java.util.Arrays.asList(
                List.of(row("billie", 111)), // page 1: 1 match (this is the one we skip)
                java.util.Collections.<Row>emptyList(), // page 2: filtered empty, live cursor
                List.of(row("fran", 33))), // page 3: the row we should return
            java.util.Arrays.asList(pk("c1"), pk("c2"), (PrimaryKey) null));
    AliDocumentIterator iter = new AliDocumentIterator(runner, 1, 0, null);
    Assertions.assertEquals(List.of("fran"), collectPlayers(iter));
  }

  @Test
  void limitStopsEarlyWithinPage() {
    QueryRunner runner =
        runnerReturning(
            List.of(List.of(row("billie", 111), row("fran", 33), row("mel", 190))),
            java.util.Arrays.asList((PrimaryKey) null));
    AliDocumentIterator iter = new AliDocumentIterator(runner, 0, 2, null);
    Assertions.assertEquals(List.of("billie", "fran"), collectPlayers(iter));
  }

  @Test
  void offsetSkipsWithinPage() {
    QueryRunner runner =
        runnerReturning(
            List.of(List.of(row("billie", 111), row("fran", 33), row("mel", 190))),
            java.util.Arrays.asList((PrimaryKey) null));
    // offset 1, no limit: skip billie, return fran + mel.
    AliDocumentIterator iter = new AliDocumentIterator(runner, 1, 0, null);
    Assertions.assertEquals(List.of("fran", "mel"), collectPlayers(iter));
  }

  @Test
  void offsetPlusLimit() {
    QueryRunner runner =
        runnerReturning(
            List.of(
                List.of(row("billie", 111), row("fran", 33), row("mel", 190), row("pat", 120))),
            java.util.Arrays.asList((PrimaryKey) null));
    // offset 1, limit 2: skip billie, return fran + mel, stop before pat.
    AliDocumentIterator iter = new AliDocumentIterator(runner, 1, 2, null);
    Assertions.assertEquals(List.of("fran", "mel"), collectPlayers(iter));
  }

  @Test
  void offsetSpansPageBoundary() {
    QueryRunner runner =
        runnerReturning(
            List.of(List.of(row("billie", 111)), List.of(row("fran", 33), row("mel", 190))),
            java.util.Arrays.asList(pk("fran"), (PrimaryKey) null));
    // offset 2 crosses from page 1 (billie) into page 2 (fran) -> return only mel.
    AliDocumentIterator iter = new AliDocumentIterator(runner, 2, 0, null);
    Assertions.assertEquals(List.of("mel"), collectPlayers(iter));
  }

  @Test
  void offsetBeyondResultsYieldsNothing() {
    QueryRunner runner =
        runnerReturning(
            List.of(List.of(row("billie", 111), row("fran", 33))),
            java.util.Arrays.asList((PrimaryKey) null));
    AliDocumentIterator iter = new AliDocumentIterator(runner, 5, 0, null);
    Assertions.assertEquals(List.of(), collectPlayers(iter));
    Assertions.assertTrue(iter.getPaginationToken().isEmpty());
  }

  @Test
  void resumeSkipsInclusiveStartRow() {
    // Resuming at 'fran' (inclusive start): server returns fran (already seen) + pat; skip fran.
    QueryRunner runner =
        runnerReturning(
            List.of(List.of(row("fran", 33), row("pat", 120))),
            java.util.Arrays.asList((PrimaryKey) null));
    AliDocumentIterator iter = new AliDocumentIterator(runner, 0, 0, pk("fran"));
    Assertions.assertEquals(List.of("pat"), collectPlayers(iter));
  }

  @Test
  void paginationTokenIsLastConsumedRowKey() {
    QueryRunner runner =
        runnerReturning(
            List.of(List.of(row("billie", 111), row("fran", 33))),
            java.util.Arrays.asList(pk("mel"))); // server cursor exists, but limit stops us first
    AliDocumentIterator iter = new AliDocumentIterator(runner, 0, 1, null);
    Assertions.assertTrue(iter.hasNext());
    iter.next(new Document(new HashMap<>()));
    AliPaginationToken token = (AliPaginationToken) iter.getPaginationToken();
    Assertions.assertFalse(token.isEmpty());
    String tokenPlayer =
        token.getNextStartPrimaryKey().getPrimaryKeyColumn("Player").getValue().asString();
    Assertions.assertEquals("billie", tokenPlayer);
  }

  @Test
  void emptyResultHasEmptyToken() {
    QueryRunner runner =
        runnerReturning(List.of(List.of()), java.util.Arrays.asList((PrimaryKey) null));
    AliDocumentIterator iter = new AliDocumentIterator(runner, 0, 0, null);
    Assertions.assertFalse(iter.hasNext());
    Assertions.assertTrue(iter.getPaginationToken().isEmpty());
  }

  @Test
  void stopHaltsIteration() {
    QueryRunner runner =
        runnerReturning(
            List.of(List.of(row("billie", 111), row("fran", 33))),
            java.util.Arrays.asList((PrimaryKey) null));
    AliDocumentIterator iter = new AliDocumentIterator(runner, 0, 0, null);
    Assertions.assertTrue(iter.hasNext());
    iter.stop();
    Assertions.assertFalse(iter.hasNext());
  }
}
