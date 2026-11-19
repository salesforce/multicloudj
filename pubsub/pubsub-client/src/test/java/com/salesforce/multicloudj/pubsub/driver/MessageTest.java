package com.salesforce.multicloudj.pubsub.driver;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Map;
import org.junit.jupiter.api.Test;

/** Unit tests for Message system metadata. */
public class MessageTest {

  @Test
  public void testGetSystemMetadata_DefaultIsNull() {
    Message message = Message.builder().withBody("body").build();

    assertNull(message.getSystemMetadata());
  }

  @Test
  public void testWithSystemMetadata_ReturnsMapAsIs() {
    Map<String, String> source = Map.of(Message.SystemMetadataKeys.DELIVERY_COUNTER, "2");

    Message message = Message.builder().withSystemMetadata(source).build();

    assertSame(source, message.getSystemMetadata());
  }

  @Test
  public void testWithSystemMetadata_NullStaysNull() {
    Message message = Message.builder().withSystemMetadata(null).build();

    assertNull(message.getSystemMetadata());
  }
}
