package com.salesforce.multicloudj.docstore.client;

import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.Action;
import com.salesforce.multicloudj.docstore.driver.ActionKind;
import com.salesforce.multicloudj.docstore.driver.ActionList;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Disabled;


@Disabled
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractDocstoreBenchmarkTest {

    // Test data
    protected List<String> documentKeys;
    protected List<Player> testPlayers;
    protected Random random;
    protected DocStoreClient docStoreClient;

    private final AtomicInteger nextPutId = new AtomicInteger(0);
    private final AtomicInteger nextGetId = new AtomicInteger(0);
    private final AtomicInteger nextCreateId = new AtomicInteger(0);
    private final AtomicInteger nextReplaceId = new AtomicInteger(0);
    private final AtomicInteger nextBatchPutId = new AtomicInteger(0);
    private final AtomicInteger nextBatchGetId = new AtomicInteger(0);
    private final AtomicInteger nextWriteReadDeleteId = new AtomicInteger(0);

    // Harness interface
    public interface Harness extends AutoCloseable {
        AbstractDocStore createDocStore() throws Exception;
        void cleanup() throws Exception;
    }

    protected Harness harness;

    protected abstract Harness createHarness();

    @Setup(Level.Trial)
    public void setupBenchmark() throws Exception {
        try {
            harness = createHarness();
            
            documentKeys = new ArrayList<>();
            testPlayers = new ArrayList<>();
            random = new Random(42); 

            AbstractDocStore docStore = harness.createDocStore();
            docStoreClient = new DocStoreClient(docStore);
            cleanupTestData();
            generateTestPlayers();
            setupTestData();
        } catch (Exception e) {
            throw new RuntimeException("Failed to setup benchmark", e);
        }
    }

    @TearDown(Level.Trial)
    public void teardownBenchmark() throws Exception {
        try {
            cleanupTestData();

            if (docStoreClient != null) {
                docStoreClient.close();
            }

            if (harness != null) {
                harness.close();
            }
        } catch (Exception e) {
            throw new RuntimeException("Error closing harness", e);
        }
    }

    private void generateTestPlayers() {
        String[] baseNames = {"John", "Taylor", "Leo", "Frank", "David", "King", "Queen", "Tiger", "Lion", "Louis"};
        
        // Small players - ~1KB document size
        // For Player object with pName, score, floatValue, isActive, and s, 
        // a 's' field of ~900 characters will make the document around 1KB.
        int smallDocSize = 900; // Characters for the 's' field
        for (int i = 0; i < 100; i++) {
            String baseName = baseNames[i % baseNames.length];
            String pName = baseName + "BenchmarkSmall" + i;
            Player player = createPlayer(pName, i, smallDocSize); 
            documentKeys.add(pName);
            testPlayers.add(player);
        }

        // Medium players - ~10KB document size
        // For Player object, an 's' field of ~9900 characters will make the document around 10KB.
        int mediumDocSize = 9900; // Characters for the 's' field
        for (int i = 0; i < 20; i++) {
            String baseName = baseNames[i % baseNames.length];
            String pName = baseName + "BenchmarkMedium" + i;
            Player player = createPlayer(pName, i, mediumDocSize);
            documentKeys.add(pName);
            testPlayers.add(player);
        }

        // Large players - ~100KB document size
        // For Player object, an 's' field of ~99900 characters will make the document around 100KB.
        int largeDocSize = 99900; // Characters for the 's' field
        for (int i = 0; i < 5; i++) {
            String baseName = baseNames[i % baseNames.length];
            String pName = baseName + "BenchmarkLarge" + i;
            Player player = createPlayer(pName, i, largeDocSize); 
            documentKeys.add(pName);
            testPlayers.add(player);
        }
    }

    private void setupTestData() {
        try {
            for (Player player : testPlayers) {
                Document doc = new Document(player);
                docStoreClient.put(doc);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to setup test data", e);
        }
    }

    private void cleanupTestData() {
        if (documentKeys == null || docStoreClient == null) {
            return;
        }
        
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
                    // Log the exception if needed, but continue cleanup
                }
            }
            actionList.run();
        } catch (Exception e) {
            // Log the exception if needed, but continue cleanup
        }
    }

    /**
     * Single Action Put
     */
    @Benchmark
    @Threads(4)
    public void benchmarkSingleActionPut(Blackhole bh) {
        benchmarkSingleActionPut(bh, 10);
    }

    private void benchmarkSingleActionPut(Blackhole bh, int n) {
        final String baseKey = "benchmarksingleaction-put-player-";
        final int docSize = 500; // Example size for single put

        try {
            for (int i = 0; i < n; i++) {
                String key = baseKey + nextPutId.incrementAndGet();
                Player player = createPlayer(key, i, docSize);

                Document doc = new Document(player);
                docStoreClient.put(doc);
                bh.consume(player.getPName());
            }
        } catch (Exception e) {
            throw new RuntimeException("Benchmark single action put failed", e);
        }
    }

    /**
     * Single Action Get
     */
    @Benchmark
    @Threads(4)
    public void benchmarkSingleActionGet(Blackhole bh) {
        benchmarkSingleActionGet(bh, 10);
    }

    private void benchmarkSingleActionGet(Blackhole bh, int n) {
        final String baseKey = "benchmarksingleaction-get-player-";
        final int docSize = 500; 

        try {
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                String key = baseKey + nextGetId.incrementAndGet();
                keys.add(key);
                Player player = createPlayer(key, i, docSize);

                Document doc = new Document(player);
                docStoreClient.put(doc);
            }

            for (String key : keys) {
                Player getPlayer = new Player();
                getPlayer.setPName(key);
                Document doc = new Document(getPlayer);
                docStoreClient.get(doc);
                bh.consume(doc.getField("pName"));
            }
        } catch (Exception e) {
            throw new RuntimeException("Benchmark single action get failed", e);
        }
    }

    /**
     * Batch Action Put
     */
    @Benchmark
    @Threads(4)
    public void benchmarkActionListPut(Blackhole bh) {
        benchmarkActionListPut(bh, 50);
    }

    private void benchmarkActionListPut(Blackhole bh, int n) {
        final String baseKey = "benchmarkactionlist-put-player-";
        final int docSize = 200; // Example size for batch put

        try {
            List<Document> documents = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                String key = baseKey + nextBatchPutId.incrementAndGet();
                Player player = createPlayer(key, i, docSize);
                documents.add(new Document(player));
            }

            docStoreClient.batchPut(documents);
            bh.consume(documents.size());
        } catch (Exception e) {
            throw new RuntimeException("Benchmark action list put failed", e);
        }
    }

    /**
     * Batch Action Get
     */
    @Benchmark
    @Threads(4)
    public void benchmarkActionListGet(Blackhole bh) {
        benchmarkActionListGet(bh, 100);
    }

    private void benchmarkActionListGet(Blackhole bh, int n) {
        final String baseKey = "benchmarkactionlist-get-player-";
        final int docSize = 200; // Example size for batch get

        try {
            List<Document> documents = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                String key = baseKey + nextBatchGetId.incrementAndGet();
                Player player = createPlayer(key, i, docSize);
                Document doc = new Document(player);
                docStoreClient.put(doc);
                
                Player getPlayer = new Player();
                getPlayer.setPName(key);
                documents.add(new Document(getPlayer));
            }
            docStoreClient.batchGet(documents);
            bh.consume(documents.size());
        } catch (Exception e) {
            throw new RuntimeException("Benchmark action list get failed", e);
        }
    }

    /**
     * Write-Read-Delete Benchmark
     */
    @Benchmark
    @Threads(4)
    public void benchmarkWriteReadDelete(Blackhole bh) {
        final String baseKey = "writereaddeletebenchmark-player-";
        final int docSize = 500; // Example size for W-R-D

        try {
            String key = baseKey + nextWriteReadDeleteId.incrementAndGet();
            Player player = createPlayer(key, random.nextInt(1000), docSize);
            
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
     * Benchmark gets using test data
     */
    @Benchmark
    public void benchmarkGetFromTestData(Blackhole bh) {
        try {
            String key = getRandomPlayerKey();
            Player getPlayer = new Player();
            getPlayer.setPName(key);
            Document doc = new Document(getPlayer);
            docStoreClient.get(doc);
            bh.consume(doc.getField("pName"));
        } catch (Exception e) {
            throw new RuntimeException("Benchmark get from test data failed", e);
        }
    }

    /**
     * Benchmark operations by data size
     */
    @Benchmark
    @Threads(4)
    public void benchmarkGetSmall(Blackhole bh) {
        benchmarkGetByPrefix(bh, "BenchmarkSmall");
    }

    @Benchmark
    @Threads(2)
    public void benchmarkGetMedium(Blackhole bh) {
        benchmarkGetByPrefix(bh, "BenchmarkMedium");
    }

    @Benchmark
    @Threads(1)
    public void benchmarkGetLarge(Blackhole bh) {
        benchmarkGetByPrefix(bh, "BenchmarkLarge");
    }

    /**
     * Benchmark atomic writes
     */
    @Benchmark
    @Threads(1)
    public void benchmarkAtomicWrites(Blackhole bh) {
        try {
            List<Player> initialPlayers = new ArrayList<>();
            ActionList actions = docStoreClient.getActions();
            
            for (int i = 0; i < 5; i++) {
                String key = "AtomicDoc" + UUID.randomUUID().toString().substring(0, 8);
                Player player = createPlayer(key, i, 100); // Small size for atomic writes example
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

    /**
     * Helper method for getting players by prefix
     */
    private void benchmarkGetByPrefix(Blackhole bh, String prefix) {
        try {
            String key = getRandomPlayerKeyWithPrefix(prefix);
            Player getPlayer = new Player();
            getPlayer.setPName(key);
            Document doc = new Document(getPlayer);
            docStoreClient.get(doc);
            bh.consume(doc.getField("pName"));
        } catch (Exception e) {
            throw new RuntimeException("Benchmark get " + prefix + " failed", e);
        }
    }

    /**
     * Create a Player with varied data types and a string of specified size.
     * The `size` parameter is for the size of the 's' field in characters.
     */
    protected Player createPlayer(String pName, int baseValue, int size) {
        char[] chars = new char[size];
        java.util.Arrays.fill(chars, 'A');
        String largeString = new String(chars);

        return new Player(pName, baseValue, (float) (baseValue + random.nextFloat() * 100), 
                         baseValue % 2 == 0, largeString);
    }

    /**
     * Get a random player key from pre-populated test data 
     */
    protected String getRandomPlayerKey() {
        List<String> playerKeys = documentKeys.stream()
                .filter(key -> key.contains("Benchmark"))
                .collect(Collectors.toList());
        
        if (playerKeys.isEmpty()) {
            return "FallbackBenchmarkPlayer";
        }
        return playerKeys.get(random.nextInt(playerKeys.size()));
    }

    /**
     * Get a random player key with specific prefix
     */
    protected String getRandomPlayerKeyWithPrefix(String prefix) {
        List<String> filteredKeys = documentKeys.stream()
                .filter(key -> key.contains(prefix)) 
                .collect(Collectors.toList()); 

        if (filteredKeys.isEmpty()) {
            return prefix + "fallback";
        }

        return filteredKeys.get(random.nextInt(filteredKeys.size()));
    }

    @Test
    public void runBenchmarks() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + this.getClass().getName() + ".*")
                .forks(1)
                .warmupIterations(3)
                .measurementIterations(5)
                .resultFormat(ResultFormatType.JSON)
                .result("target/jmh-results.json")
                .build();

        new Runner(opt).run();
    }
}