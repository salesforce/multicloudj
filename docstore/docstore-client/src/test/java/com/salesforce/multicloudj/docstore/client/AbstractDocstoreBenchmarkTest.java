package com.salesforce.multicloudj.docstore.client;

import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.ActionList;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
import com.salesforce.multicloudj.docstore.driver.FilterOperation;
import com.salesforce.multicloudj.docstore.driver.PaginationToken;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractDocstoreBenchmarkTest {

  private static final Logger logger =
      LoggerFactory.getLogger(AbstractDocstoreBenchmarkTest.class);

  // Pre-seeded test data
  protected List<String> documentKeys;
  protected List<Player> testPlayers;
  protected List<HighScore> testHighScores;
  protected DocStoreClient docStoreClient;
  protected DocStoreClient queryDocStoreClient; // For composite key table queries

  // Keys created during benchmark invocations, cleaned up in teardown.
  // ConcurrentHashMap.newKeySet() uses bucket-level locking — no global monitor contention
  // under @Threads(4) unlike Collections.synchronizedSet().
  private final Set<String> benchmarkCreatedKeys = ConcurrentHashMap.newKeySet();

  // Pre-filtered key lists to avoid O(n) stream allocation in the benchmark hot path.
  private List<String> smallPlayerKeys;
  private List<String> largePlayerKeys;

  /**
   * Batch size parameter for the batch-mode benchmarks (benchmarkBatchPut, benchmarkBatchGet).
   * Three points span the typical request-batch range; lets us see scaling behaviour without
   * exploding the suite runtime.
   */
  @Param({"10", "50", "100"})
  public int batchSize;

  // Harness interface
  public interface Harness extends AutoCloseable {
    AbstractDocStore createDocStore() throws Exception; // Single key table

    AbstractDocStore createQueryDocStore() throws Exception; // Composite key table for queries
  }

  protected Harness harness;

  protected abstract Harness createHarness();

  protected abstract String getProviderId();

  /**
   * Reads a required config value from OS environment first, then from -D system properties.
   * Fails fast with a clear error if neither is set — avoids silent misconfiguration producing
   * garbage benchmark results.
   */
  protected static String requireEnv(String name) {
    String value = System.getenv(name);
    if (StringUtils.isBlank(value)) {
      value = System.getProperty(name);
    }
    if (StringUtils.isBlank(value)) {
      throw new IllegalStateException("Required environment variable not set: " + name);
    }
    return value;
  }

  @Setup(Level.Trial)
  public void setupBenchmark() throws Exception {
    try {
      harness = createHarness();

      documentKeys = new ArrayList<>();
      testPlayers = new ArrayList<>();
      testHighScores = new ArrayList<>();

      // Setup single key table
      AbstractDocStore docStore = harness.createDocStore();
      docStoreClient = new DocStoreClient(docStore);

      // Setup composite key table for queries
      AbstractDocStore queryDocStore = harness.createQueryDocStore();
      queryDocStoreClient = new DocStoreClient(queryDocStore);

      cleanupTestData();
      generateTestPlayers();
      generateTestHighScores();
      setupTestData();

      // Pre-filter key lists once so hot-path lookups are O(1)
      smallPlayerKeys = Collections.unmodifiableList(
          documentKeys.stream()
              .filter(k -> k.contains("BenchmarkSmall"))
              .collect(Collectors.toList()));
      largePlayerKeys = Collections.unmodifiableList(
          documentKeys.stream()
              .filter(k -> k.contains("BenchmarkLarge"))
              .collect(Collectors.toList()));
    } catch (Exception e) {
      throw new RuntimeException("Failed to setup benchmark", e);
    }
  }

  @TearDown(Level.Trial)
  public void teardownBenchmark() throws Exception {
    try {
      if (!benchmarkCreatedKeys.isEmpty() && docStoreClient != null) {
        for (String key : benchmarkCreatedKeys) {
          try {
            Player p = new Player();
            p.setPName(key);
            docStoreClient.delete(new Document(p));
          } catch (Exception e) {
            logger.warn("Failed to delete benchmark key {}: {}", key, e.getMessage());
          }
        }
        benchmarkCreatedKeys.clear();
      }

      cleanupTestData();

      if (docStoreClient != null) {
        docStoreClient.close();
      }

      if (queryDocStoreClient != null) {
        queryDocStoreClient.close();
      }

      if (harness != null) {
        harness.close();
      }
    } catch (Exception e) {
      throw new RuntimeException("Error closing harness", e);
    }
  }

  private void generateTestPlayers() {
    String[] baseNames = {
      "John", "Taylor", "Leo", "Frank", "David", "King", "Queen", "Tiger", "Lion", "Louis"
    };

    int smallDocSize = 900;
    for (int i = 0; i < 100; i++) {
      String baseName = baseNames[i % baseNames.length];
      String pName = baseName + "BenchmarkSmall" + i;
      Player player = createPlayer(pName, i, smallDocSize);
      documentKeys.add(pName);
      testPlayers.add(player);
    }

    // Large players - ~100KB document size
    int largeDocSize = 99900;
    for (int i = 0; i < 5; i++) {
      String baseName = baseNames[i % baseNames.length];
      String pName = baseName + "BenchmarkLarge" + i;
      Player player = createPlayer(pName, i, largeDocSize);
      documentKeys.add(pName);
      testPlayers.add(player);
    }
  }

  // 200 HighScore docs (5 games × 10 players × 4 rounds). Larger dataset gives query planners
  // a path that reflects real-world workloads — at 15 rows, providers can pick degenerate plans
  // (e.g., DynamoDB skipping index lookups for full-table scan) that don't represent customer
  // behavior. Score values are deterministic but well-distributed so range/orderBy benchmarks
  // see meaningful spread.
  private void generateTestHighScores() {
    String[] players = {
      "pat", "mel", "andy", "fran", "billie", "alex", "sam", "chris", "jamie", "taylor"
    };
    for (int g = 0; g < games.length; g++) {
      for (int p = 0; p < players.length; p++) {
        for (int r = 0; r < 4; r++) {
          int score = 10 + (g * 73 + p * 17 + r * 5) % 390;
          int magic = (g * 11 + p * 3 + r) % 30;
          int taken = (g * 7 + p * 2 + r) % 8;
          long fld1 = (long) score * 1000L;
          float fld2 = score / 4.0f;
          double fld3 = score / 100.0;
          testHighScores.add(
              new HighScore(
                  games[g],
                  players[p] + r,
                  score,
                  "2024-0" + (g + 1) + "-" + (10 + p),
                  score > 200,
                  (byte) magic,
                  (short) taken,
                  fld1,
                  fld2,
                  fld3));
        }
      }
    }
  }

  private void setupTestData() {
    try {
      // Use batchPut to seed all data in 2 RPCs instead of 140 serial puts
      List<Document> playerDocs =
          testPlayers.stream().map(Document::new).collect(Collectors.toList());
      docStoreClient.batchPut(playerDocs);

      List<Document> scoreDocs =
          testHighScores.stream().map(Document::new).collect(Collectors.toList());
      queryDocStoreClient.batchPut(scoreDocs);
    } catch (Exception e) {
      throw new RuntimeException("Failed to setup test data", e);
    }
  }

  private void cleanupTestData() {
    // Cleanup Player data from single key table
    if (documentKeys != null && docStoreClient != null) {
      try {
        ActionList actionList = docStoreClient.getActions();
        for (String key : documentKeys) {
          try {
            if (key.contains("Benchmark")) {
              Player deletePlayer = new Player();
              deletePlayer.setPName(key);
              actionList.delete(new Document(deletePlayer));
            } else {
              Map<String, Object> deleteDoc = new HashMap<>();
              deleteDoc.put("pName", key);
              actionList.delete(new Document(deleteDoc));
            }
          } catch (Exception e) {
            // Continue cleanup on error
          }
        }
        actionList.run();
      } catch (Exception e) {
        // Continue cleanup on error
      }
    }

    // Cleanup HighScore data from composite key table
    if (testHighScores != null && queryDocStoreClient != null) {
      try {
        ActionList actionList = queryDocStoreClient.getActions();
        for (HighScore highScore : testHighScores) {
          try {
            HighScore deleteScore = new HighScore();
            deleteScore.setGame(highScore.getGame());
            deleteScore.setPlayer(highScore.getPlayer());
            actionList.delete(new Document(deleteScore));
          } catch (Exception e) {
            // Continue cleanup on error
          }
        }
        actionList.run();
      } catch (Exception e) {
        // Continue cleanup on error
      }
    }
  }

  /**
   * Single-document put baseline. Renamed from benchmarkSingleActionPut, which executed 10 puts
   * in a loop per invocation — that conflated batch effects with single-op cost. One put per
   * invocation gives the true per-op latency and ops/s baseline.
   */
  @Benchmark
  @Threads(4)
  public void benchmarkPut(Blackhole bh) {
    final String baseKey = "benchmarkput-player-";
    final int docSize = 500;
    try {
      String key = baseKey + ThreadLocalRandom.current().nextLong();
      benchmarkCreatedKeys.add(key);
      Player player = createPlayer(key, 0, docSize);
      docStoreClient.put(new Document(player));
      bh.consume(player.getPName());
    } catch (Exception e) {
      throw new RuntimeException("Benchmark put failed", e);
    }
  }

  /**
   * Single-document get from pre-seeded data. Replaces the old benchmarkSingleActionGet which
   * embedded 10 puts before the get, conflating put + get in the measurement. This version
   * reads only — every invocation is a single get RPC, so the number reflects get latency.
   */
  @Benchmark
  @Threads(4)
  public void benchmarkGet(Blackhole bh) {
    try {
      String key = smallPlayerKeys.get(ThreadLocalRandom.current().nextInt(smallPlayerKeys.size()));
      Player getPlayer = new Player();
      getPlayer.setPName(key);
      Document doc = new Document(getPlayer);
      docStoreClient.get(doc);
      bh.consume(doc.getField("pName"));
    } catch (Exception e) {
      throw new RuntimeException("Benchmark get failed", e);
    }
  }

  /**
   * Batch put across {@code @Param batchSize} ∈ {10, 50, 100}. Renamed from benchmarkActionListPut
   * which was fixed at batch=50; the parameter exposes the batch-size scaling curve.
   */
  @Benchmark
  @Threads(4)
  public void benchmarkBatchPut(Blackhole bh) {
    final String baseKey = "benchmarkbatchput-player-";
    final int docSize = 200;

    try {
      List<Document> documents = new ArrayList<>();
      for (int i = 0; i < batchSize; i++) {
        String key = baseKey + ThreadLocalRandom.current().nextLong();
        benchmarkCreatedKeys.add(key);
        Player player = createPlayer(key, i, docSize);
        documents.add(new Document(player));
      }

      docStoreClient.batchPut(documents);
      bh.consume(documents.size());
    } catch (Exception e) {
      throw new RuntimeException("Benchmark batch put failed", e);
    }
  }

  /**
   * Batch get from pre-seeded data across {@code @Param batchSize} ∈ {10, 50, 100}. Replaces the
   * old benchmarkActionListGet which embedded 100 puts before the batchGet — that pattern caused
   * suite hangs (~2000 orphan docs accumulating per run on Firestore) and conflated put + get in
   * the measurement. Reading only; every invocation is one batchGet RPC.
   *
   * <p>Keys are sampled without replacement because the SDK's ActionList rejects duplicate-key
   * actions in a single batch (see {@code ActionList.toDriverActions}). The pre-seeded pool has
   * 100 keys, so all three batch sizes (10/50/100) fit. We do an O(n) shuffle of a fresh copy
   * each invocation — adds GC pressure but avoids the duplicate-key error and keeps the
   * randomness signal across iterations.
   */
  @Benchmark
  @Threads(4)
  public void benchmarkBatchGet(Blackhole bh) {
    try {
      List<String> shuffled = new ArrayList<>(smallPlayerKeys);
      Collections.shuffle(shuffled, ThreadLocalRandom.current());
      List<Document> documents = new ArrayList<>(batchSize);
      for (int i = 0; i < batchSize; i++) {
        Player getPlayer = new Player();
        getPlayer.setPName(shuffled.get(i));
        documents.add(new Document(getPlayer));
      }
      docStoreClient.batchGet(documents);
      bh.consume(documents.size());
    } catch (Exception e) {
      throw new RuntimeException("Benchmark batch get failed", e);
    }
  }

  /** Write-Read-Delete Benchmark */
  @Benchmark
  @Threads(4)
  public void benchmarkWriteReadDelete(Blackhole bh) {
    final String baseKey = "writereaddeletebenchmark-player-";
    final int docSize = 500;

    try {
      String key = baseKey + ThreadLocalRandom.current().nextLong();
      Player player = createPlayer(key, ThreadLocalRandom.current().nextInt(1000), docSize);

      Document writeDoc = new Document(player);
      docStoreClient.put(writeDoc);
      bh.consume(player.getPName());

      Player readPlayer = new Player();
      readPlayer.setPName(key);
      Document doc = new Document(readPlayer);
      docStoreClient.get(doc);
      bh.consume(doc.getField("pName"));

      docStoreClient.delete(doc);

    } catch (Exception e) {
      throw new RuntimeException("Benchmark write-read-delete failed", e);
    }
  }

  /**
   * Single-document get of a large (~100 KB) Player. Pairs with benchmarkGet (small ~900 B) to
   * bracket the document-size range. The middle "medium" benchmark was dropped — small + large
   * already capture the scaling trend.
   */
  @Benchmark
  @Threads(1)
  public void benchmarkGetLarge(Blackhole bh) {
    benchmarkGetByList(bh, largePlayerKeys, "Large");
  }

  /** Benchmark atomic writes */
  @Benchmark
  @Threads(1)
  public void benchmarkAtomicWrites(Blackhole bh) {
    try {
      List<Player> initialPlayers = new ArrayList<>();
      ActionList actions = docStoreClient.getActions();

      for (int i = 0; i < 5; i++) {
        String key = "AtomicDoc" + ThreadLocalRandom.current().nextLong();
        benchmarkCreatedKeys.add(key);
        Player player = createPlayer(key, i, 100);
        initialPlayers.add(player);
        actions.create(new Document(player));
      }
      actions.run();

      // Perform atomic operations
      actions = docStoreClient.getActions();

      // Regular operations
      Player getPlayer = new Player();
      getPlayer.setPName(initialPlayers.get(0).getPName());
      actions.get(new Document(getPlayer));

      actions.delete(new Document(initialPlayers.get(1)));

      // Enable atomic writes for remaining operations
      actions.enableAtomicWrites();

      // Update remaining players atomically
      for (int i = 2; i < initialPlayers.size(); i++) {
        Player updatePlayer = initialPlayers.get(i);
        updatePlayer.setS("atomically_updated_" + i);
        actions.put(new Document(updatePlayer));
      }

      actions.run();
      bh.consume(initialPlayers.size());

    } catch (Exception e) {
      throw new RuntimeException("Benchmark atomic writes failed", e);
    }
  }

  /** Query Benchmark 1: Partition Key Queries */
  @Benchmark
  @Threads(2)
  public void benchmarkPartitionKeyQuery(Blackhole bh) {
    DocumentIterator iter = null;
    try {
      iter = queryDocStoreClient.query().where("Game", FilterOperation.EQUAL, game2).get();

      int count = 0;
      while (iter.hasNext()) {
        HighScore score = new HighScore();
        iter.next(new Document(score));
        count++;
        bh.consume(score.getGame());
      }
      bh.consume(count);
    } catch (Exception e) {
      throw new RuntimeException("Benchmark partition key query failed", e);
    } finally {
      if (iter != null) {
        iter.stop();
      }
    }
  }

  /** Composite Key Queries */
  @Benchmark
  @Threads(2)
  public void benchmarkCompositeKeyQuery(Blackhole bh) {
    DocumentIterator iter = null;
    try {
      iter =
          queryDocStoreClient
              .query()
              .where("Game", FilterOperation.EQUAL, game1)
              .where("Player", FilterOperation.EQUAL, "andy")
              .get();

      int count = 0;
      while (iter.hasNext()) {
        HighScore score = new HighScore();
        iter.next(new Document(score));
        count++;
        bh.consume(score.getScore());
      }
      bh.consume(count);
    } catch (Exception e) {
      throw new RuntimeException("Benchmark composite key query failed", e);
    } finally {
      if (iter != null) {
        iter.stop();
      }
    }
  }

  /** Range Queries */
  @Benchmark
  @Threads(1)
  public void benchmarkRangeQuery(Blackhole bh) {
    DocumentIterator iter = null;
    try {
      iter = queryDocStoreClient.query().where("Score", FilterOperation.GREATER_THAN, 100).get();

      int count = 0;
      while (iter.hasNext()) {
        HighScore score = new HighScore();
        iter.next(new Document(score));
        count++;
        bh.consume(score.getScore());
      }
      bh.consume(count);
    } catch (Exception e) {
      throw new RuntimeException("Benchmark range query failed", e);
    } finally {
      if (iter != null) {
        iter.stop();
      }
    }
  }

  /** Query with limit clause — measures cost of bounding result set size at the cloud. */
  @Benchmark
  @Threads(2)
  public void benchmarkQueryWithLimit(Blackhole bh) {
    DocumentIterator iter = null;
    try {
      iter =
          queryDocStoreClient
              .query()
              .where("Game", FilterOperation.EQUAL, game1)
              .limit(10)
              .get();

      int count = 0;
      while (iter.hasNext()) {
        HighScore score = new HighScore();
        iter.next(new Document(score));
        count++;
        bh.consume(score.getScore());
      }
      bh.consume(count);
    } catch (Exception e) {
      throw new RuntimeException("Benchmark query with limit failed", e);
    } finally {
      if (iter != null) {
        iter.stop();
      }
    }
  }

  /**
   * Query with orderBy — fundamental cross-cloud variance: provider plans range over indexed vs.
   * non-indexed sorts very differently. orderBy field must appear in a where clause (see
   * Query#orderBy javadoc for cross-substrate consistency). Uses descending order
   * ("leaderboard"-style highest score first). Requires Firestore composite index
   * (Game ASC, Score DESC) on the composite-key collection.
   */
  @Benchmark
  @Threads(1)
  public void benchmarkQueryWithOrderBy(Blackhole bh) {
    DocumentIterator iter = null;
    try {
      iter =
          queryDocStoreClient
              .query()
              .where("Game", FilterOperation.EQUAL, game1)
              .where("Score", FilterOperation.GREATER_THAN, 0)
              .orderBy("Score", false)
              .get();

      int count = 0;
      while (iter.hasNext()) {
        HighScore score = new HighScore();
        iter.next(new Document(score));
        count++;
        bh.consume(score.getScore());
      }
      bh.consume(count);
    } catch (Exception e) {
      throw new RuntimeException("Benchmark query with orderBy failed", e);
    } finally {
      if (iter != null) {
        iter.stop();
      }
    }
  }

  /**
   * Two-page fetch using paginationToken — measures cursor overhead across providers. Different
   * providers (DynamoDB LastEvaluatedKey, Firestore startAfter cursor, etc.) produce
   * behaviorally significant cross-cloud differences here.
   */
  @Benchmark
  @Threads(1)
  public void benchmarkQueryWithPagination(Blackhole bh) {
    DocumentIterator firstPage = null;
    DocumentIterator secondPage = null;
    try {
      firstPage =
          queryDocStoreClient
              .query()
              .where("Game", FilterOperation.EQUAL, game1)
              .limit(20)
              .get();

      int firstCount = 0;
      while (firstPage.hasNext()) {
        HighScore score = new HighScore();
        firstPage.next(new Document(score));
        firstCount++;
        bh.consume(score.getScore());
      }

      PaginationToken token = firstPage.getPaginationToken();
      // Release the first cursor before opening the second so we never hold both at once.
      firstPage.stop();
      firstPage = null;
      if (token != null) {
        secondPage =
            queryDocStoreClient
                .query()
                .where("Game", FilterOperation.EQUAL, game1)
                .limit(20)
                .paginationToken(token)
                .get();

        int secondCount = 0;
        while (secondPage.hasNext()) {
          HighScore score = new HighScore();
          secondPage.next(new Document(score));
          secondCount++;
          bh.consume(score.getScore());
        }
        bh.consume(secondCount);
      }
      bh.consume(firstCount);
    } catch (Exception e) {
      throw new RuntimeException("Benchmark query with pagination failed", e);
    } finally {
      if (firstPage != null) {
        firstPage.stop();
      }
      if (secondPage != null) {
        secondPage.stop();
      }
    }
  }

  /**
   * Get with field projection — partial-field read. Each provider implements projection
   * differently (DynamoDB ProjectionExpression, Firestore DocumentMask), so this quantifies the
   * wire-transfer benefit of asking for fewer fields.
   */
  @Benchmark
  @Threads(4)
  public void benchmarkGetWithProjection(Blackhole bh) {
    try {
      String key = smallPlayerKeys.get(ThreadLocalRandom.current().nextInt(smallPlayerKeys.size()));
      Player getPlayer = new Player();
      getPlayer.setPName(key);
      Document doc = new Document(getPlayer);
      docStoreClient.get(doc, "pName", "i");
      bh.consume(doc.getField("pName"));
    } catch (Exception e) {
      throw new RuntimeException("Benchmark get with projection failed", e);
    }
  }

  /** Helper — get from a pre-filtered, pre-computed key list (O(1), no allocation) */
  private void benchmarkGetByList(Blackhole bh, List<String> keys, String label) {
    try {
      String key = keys.get(ThreadLocalRandom.current().nextInt(keys.size()));
      Player getPlayer = new Player();
      getPlayer.setPName(key);
      Document doc = new Document(getPlayer);
      docStoreClient.get(doc);
      bh.consume(doc.getField("pName"));
    } catch (Exception e) {
      throw new RuntimeException("Benchmark get " + label + " failed", e);
    }
  }

  /**
   * Create a Player with varied data types and a string of specific size. `size` parameter is for
   * the size of the 's' field in characters.
   */
  protected Player createPlayer(String pName, int baseValue, int size) {
    char[] chars = new char[size];
    java.util.Arrays.fill(chars, 'A');
    String largeString = new String(chars);

    return new Player(
        pName,
        baseValue,
        (float) (baseValue + ThreadLocalRandom.current().nextFloat() * 100),
        baseValue % 2 == 0,
        largeString,
        "randomString".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  @EnabledIfSystemProperty(named = "runBenchmarks", matches = "true")
  public void runBenchmarks() throws RunnerException {
    List<String> forwardedArgs = new ArrayList<>();
    for (String key : System.getProperties().stringPropertyNames()) {
      if (key.startsWith("DOCSTORE_BENCHMARK_")) {
        forwardedArgs.add("-D" + key + "=" + System.getProperty(key));
      }
    }

    Options opt =
        new OptionsBuilder()
            .include(".*" + this.getClass().getName() + ".*")
            .forks(1)
            .resultFormat(ResultFormatType.JSON)
            .result("target/jmh-docstore-results-" + getProviderId() + ".json")
            .jvmArgsAppend(forwardedArgs.toArray(new String[0]))
            .build();

    new Runner(opt).run();
  }

  private static final String game1 = "Praise All Monsters";
  private static final String game2 = "Zombie DMV";
  private static final String game3 = "Days Gone";
  private static final String game4 = "Space Drifter";
  private static final String game5 = "Neon Abyss";
  private static final String[] games = {game1, game2, game3, game4, game5};
}
