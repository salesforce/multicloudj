package com.salesforce.multicloudj.pubsub.ali;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.multicloudj.pubsub.client.SubscriptionClient;
import com.salesforce.multicloudj.pubsub.client.TopicClient;
import org.junit.jupiter.api.Test;

/**
 * Verifies the Alibaba pubsub providers are discoverable through the public client builders, i.e.
 * that the {@code @AutoService} registrations on {@link AliSmqQueue} and {@link AliSubscription}
 * are present so Java's {@code ServiceLoader} can find them by provider id.
 *
 * <p>{@code TopicClient.builder(id)} and {@code SubscriptionClient.builder(id)} resolve the
 * provider in their constructor via {@code ServiceLoader} and throw an
 * {@link IllegalArgumentException} when no registered provider matches the id. These tests
 * therefore fail if the {@code @AutoService} annotation is removed from either provider, which is
 * the exact regression this coverage guards against - the other unit tests build the concrete
 * {@code Builder} directly and never exercise discovery.
 */
public class AliPubsubProviderDiscoveryTest {

  @Test
  void topicClientBuilderDiscoversAliSmqQueueProvider() {
    // Resolving the "alismqqueue" id through the public builder requires the ServiceLoader
    // registration on AliSmqQueue; without it the constructor throws IllegalArgumentException.
    assertNotNull(assertDoesNotThrow(() -> TopicClient.builder("alismqqueue")));
  }

  @Test
  void subscriptionClientBuilderDiscoversAliProvider() {
    // Resolving the "ali" id through the public builder requires the ServiceLoader registration on
    // AliSubscription; without it the constructor throws IllegalArgumentException.
    assertNotNull(assertDoesNotThrow(() -> SubscriptionClient.builder("ali")));
  }

  @Test
  void unregisteredProviderIdIsNotDiscovered() {
    // Negative control: an id no provider registers throws, so the positive cases above genuinely
    // depend on the @AutoService registration and are not vacuously passing.
    assertThrows(
        IllegalArgumentException.class, () -> TopicClient.builder("unregistered-provider"));
    assertThrows(
        IllegalArgumentException.class,
        () -> SubscriptionClient.builder("unregistered-provider"));
  }
}
