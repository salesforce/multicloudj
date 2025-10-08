package com.salesforce.multicloudj.pubsub.client;

import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * This class will attempt to find a PubSub provider in the classpath by providerId.
 * 
 * <p>ProviderSupplier uses Java's ServiceLoader mechanism to automatically discover
 * topic and subscription provider implementations at runtime. This enables the
 * multicloud SDK's plugin architecture where new cloud providers can be added
 * without modifying existing code.
 * 
 * <p>Provider implementations register themselves using the {@code @AutoService}
 * annotation and are automatically discovered when their JARs are on the classpath.
 */
public class ProviderSupplier {

    private ProviderSupplier() {
        // Utility class - prevent instantiation
    }

    /**
     * Finds a topic provider by provider ID.
     *
     * @param providerId Id of the provider to be found (e.g., "aws", "gcp", "ali")
     * @return Builder object for the topic provider
     * @throws IllegalArgumentException if no provider is found for the given ID
     */
    static AbstractTopic.Builder<?> findTopicProviderBuilder(String providerId) {
        ServiceLoader<AbstractTopic> services = ServiceLoader.load(AbstractTopic.class);
        List<AbstractTopic> all = new ArrayList<>();
        for (AbstractTopic service : services) {
            all.add(service);
        }

        for (AbstractTopic provider : all) {
            if (provider.getProviderId().equals(providerId)) {
                return createTopicBuilderInstance(provider);
            }
        }
        throw new IllegalArgumentException("No Topic provider found for providerId: " + providerId);
    }

    /**
     * Finds a subscription provider by provider ID.
     *
     * @param providerId Id of the provider to be found (e.g., "aws", "gcp", "ali")
     * @return Builder object for the subscription provider
     * @throws IllegalArgumentException if no provider is found for the given ID
     */
    static AbstractSubscription.Builder<?> findSubscriptionProviderBuilder(String providerId) {
        ServiceLoader<AbstractSubscription> services = ServiceLoader.load(AbstractSubscription.class);
        List<AbstractSubscription> all = new ArrayList<>();
        for (AbstractSubscription service : services) {
            all.add(service);
        }

        for (AbstractSubscription provider : all) {
            if (provider.getProviderId().equals(providerId)) {
                return createSubscriptionBuilderInstance(provider);
            }
        }
        throw new IllegalArgumentException("No Subscription provider found for providerId: " + providerId);
    }

    /**
     * Creates a topic builder instance using reflection.
     * 
     * @param provider The topic provider instance
     * @return A new builder instance for the provider
     * @throws RuntimeException if the builder creation fails
     */
    private static AbstractTopic.Builder<?> createTopicBuilderInstance(AbstractTopic<?> provider) {
        try {
            return (AbstractTopic.Builder<?>) provider.getClass().getMethod("builder").invoke(provider);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topic builder for provider: " + provider.getClass().getName(), e);
        }
    }

    /**
     * Creates a subscription builder instance using reflection.
     * 
     * @param provider The subscription provider instance
     * @return A new builder instance for the provider
     * @throws RuntimeException if the builder creation fails
     */
    private static AbstractSubscription.Builder<?> createSubscriptionBuilderInstance(AbstractSubscription<?> provider) {
        try {
            return (AbstractSubscription.Builder<?>) provider.getClass().getMethod("builder").invoke(provider);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create subscription builder for provider: " + provider.getClass().getName(), e);
        }
    }
} 