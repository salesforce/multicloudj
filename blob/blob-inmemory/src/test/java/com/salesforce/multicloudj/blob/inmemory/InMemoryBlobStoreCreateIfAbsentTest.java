package com.salesforce.multicloudj.blob.inmemory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InMemoryBlobStoreCreateIfAbsentTest {

  private static final String BUCKET = "create-if-absent-bucket";
  private static final String KEY = "marker";

  private InMemoryBlobStore store;

  @BeforeEach
  void setUp() {
    InMemoryBlobStore.clearStorage();
    InMemoryBlobStore.createBucket(BUCKET);
    store = new InMemoryBlobStore.Builder().withBucket(BUCKET).withRegion("local").build();
  }

  @AfterEach
  void tearDown() {
    InMemoryBlobStore.clearStorage();
  }

  @Test
  void uploadWithoutConditionStillOverwrites() {
    store.upload(request(false, Map.of()), bytes("first"));
    store.upload(request(false, Map.of()), bytes("second"));

    assertArrayEquals(bytes("second"), download());
  }

  @Test
  void createIfAbsentRejectsExistingBlobAndPreservesWinningState() {
    store.upload(request(true, Map.of("writer", "first")), bytes("first"));

    assertThrows(
        ResourceAlreadyExistsException.class,
        () -> store.upload(request(true, Map.of("writer", "second")), bytes("second")));

    assertArrayEquals(bytes("first"), download());
    BlobMetadata metadata = store.getMetadata(KEY, null);
    assertEquals("first", metadata.getMetadata().get("writer"));
  }

  @Test
  void concurrentCreateIfAbsentHasExactlyOneWinner() throws Exception {
    int writerCount = 16;
    ExecutorService executor = Executors.newFixedThreadPool(writerCount);
    CountDownLatch ready = new CountDownLatch(writerCount);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<byte[]>> attempts = new ArrayList<>();

    try {
      for (int writer = 0; writer < writerCount; writer++) {
        byte[] payload = bytes("writer-" + writer);
        attempts.add(
            executor.submit(
                () -> {
                  ready.countDown();
                  start.await();
                  try {
                    store.upload(request(true, Map.of()), payload);
                    return payload;
                  } catch (ResourceAlreadyExistsException expected) {
                    return null;
                  }
                }));
      }

      assertTrue(ready.await(10, java.util.concurrent.TimeUnit.SECONDS));
      start.countDown();

      List<byte[]> winners = new ArrayList<>();
      for (Future<byte[]> attempt : attempts) {
        byte[] winner = attempt.get();
        if (winner != null) {
          winners.add(winner);
        }
      }

      assertEquals(1, winners.size());
      assertArrayEquals(winners.get(0), download());
    } finally {
      start.countDown();
      executor.shutdownNow();
    }
  }

  private static UploadRequest request(boolean createIfAbsent, Map<String, String> metadata) {
    return UploadRequest.builder()
        .withKey(KEY)
        .withCreateIfAbsent(createIfAbsent)
        .withMetadata(metadata)
        .build();
  }

  private byte[] download() {
    ByteArray output = new ByteArray();
    store.download(DownloadRequest.builder().withKey(KEY).build(), output);
    return output.getBytes();
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
