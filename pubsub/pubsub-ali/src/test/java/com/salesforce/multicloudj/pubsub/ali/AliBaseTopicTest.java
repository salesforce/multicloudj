package com.salesforce.multicloudj.pubsub.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.aliyun.mns.model.serialize.queue.MessageListSerializer;
import com.salesforce.multicloudj.pubsub.driver.Message;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class AliBaseTopicTest {

  @Test
  void encodedWireSizeDoesNotOverflowForNearMaxLength() {
    // A raw length near Integer.MAX_VALUE would overflow the 32-bit `4 * ((n + 2) / 3)` into a
    // negative, understated size that slips past the single-message size check. Computed in long,
    // encodedWireSize must return a correct positive size far above the per-request limit, so such
    // a body is rejected rather than silently accepted.
    long wireSize = AliBaseTopic.encodedWireSize(Integer.MAX_VALUE);

    long expected =
        4L * ((Integer.MAX_VALUE + 2L) / 3L) + AliBaseTopic.MESSAGE_ENVELOPE_OVERHEAD_BYTES;
    assertEquals(expected, wireSize);
    assertTrue(wireSize > 0, "wire size must not overflow to a negative value");
    assertTrue(
        wireSize > AliBaseTopic.MAX_BATCH_BYTE_SIZE,
        "a near-2 GB body must measure well above the per-request limit");
  }

  @Test
  void estimatedRequestSizeIsAtLeastActualSerializedSize() throws Exception {
    // The byte guard estimates each batch's request size as FIXED_REQUEST_OVERHEAD_BYTES plus the
    // sum of measureWireSize(message) over the batch. This proves that estimate is a true upper
    // bound on the request the SMQ SDK actually serializes (MessageListSerializer,
    // exactly what batchPutMessage transmits) for representative batches: a tiny single message, a
    // single message near the per-message limit, and a full 16-message batch near the packing
    // boundary. Pure client-side serialization: no network, no credentials.
    try (AliSmqQueue topic = new AliSmqQueue()) {
      assertEstimateIsUpperBound(topic, uniform(1, 3));
      assertEstimateIsUpperBound(topic, uniform(1, 48_000));
      assertEstimateIsUpperBound(topic, uniform(16, 3_000));
    }
  }

  @Test
  void everySubBatchFromSplitBySizeSerializesWithinTheLimit() throws Exception {
    // An over-limit logical batch is packed by splitBySize into sub-batches; each sub-batch, once
    // serialized by the SMQ SDK, must stay within the documented 64 KB per-request limit. Three
    // ~20 KB bodies (each ~26 KB base64) force a split into more than one sub-batch.
    try (AliSmqQueue topic = new AliSmqQueue()) {
      List<Message> batch = uniform(3, 20_000);
      List<List<Message>> subBatches = topic.splitBySize(batch);
      assertTrue(
          subBatches.size() > 1, "the over-limit batch must split into multiple sub-batches");
      for (List<Message> subBatch : subBatches) {
        assertTrue(
            serializedLength(topic, subBatch) <= AliBaseTopic.MAX_BATCH_BYTE_SIZE,
            "each serialized sub-batch must stay within the 64 KB per-request limit");
      }
    }
  }

  private static void assertEstimateIsUpperBound(AliSmqQueue topic, List<Message> messages)
      throws Exception {
    long estimate = AliBaseTopic.FIXED_REQUEST_OVERHEAD_BYTES;
    for (Message message : messages) {
      estimate += topic.measureWireSize(message);
    }
    long actual = serializedLength(topic, messages);
    assertTrue(
        estimate >= actual,
        "estimated request size ("
            + estimate
            + ") must be an upper bound on the actual serialized size ("
            + actual
            + ") for a batch of "
            + messages.size()
            + " message(s)");
  }

  /** Serializes the batch exactly as batchPutMessage does and returns the byte length. */
  private static long serializedLength(AliSmqQueue topic, List<Message> messages)
      throws Exception {
    List<com.aliyun.mns.model.Message> smqMessages = new ArrayList<>();
    for (Message message : messages) {
      smqMessages.add(topic.toSmqMessage(message));
    }
    MessageListSerializer serializer = new MessageListSerializer();
    try (InputStream in = serializer.serialize(smqMessages, "UTF-8")) {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      byte[] chunk = new byte[8192];
      int read;
      while ((read = in.read(chunk)) != -1) {
        buffer.write(chunk, 0, read);
      }
      return buffer.size();
    }
  }

  private static List<Message> uniform(int count, int rawLength) {
    List<Message> messages = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      messages.add(Message.builder().withBody(bodyOf(rawLength)).build());
    }
    return messages;
  }

  private static byte[] bodyOf(int rawLength) {
    byte[] body = new byte[rawLength];
    for (int i = 0; i < rawLength; i++) {
      body[i] = (byte) ('A' + (i % 26));
    }
    return body;
  }
}
