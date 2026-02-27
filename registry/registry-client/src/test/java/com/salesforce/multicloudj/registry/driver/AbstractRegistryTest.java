package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.registry.model.Platform;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AbstractRegistryTest {

    private static final String REGISTRY_ENDPOINT = "https://registry.example.com";

    @Test
    void testConstructor_Throws_WhenRegistryEndpointNotConfigured() {
        InvalidArgumentException empty = assertThrows(InvalidArgumentException.class,
                () -> new TestRegistry.Builder()
                        .providerId("test")
                        .withRegistryEndpoint("")
                        .build());
        assertTrue(empty.getMessage().contains("Registry endpoint is not configured"));

        assertThrows(InvalidArgumentException.class,
                () -> new TestRegistry.Builder()
                        .providerId("test")
                        .build());
    }

    @Test
    void testConstructor_SetsTargetPlatform_WhenPlatformSet() {
        Platform custom = Platform.builder()
                .architecture("arm64")
                .operatingSystem("linux")
                .build();

        TestRegistry registry = new TestRegistry.Builder()
                .providerId("test")
                .withRegistryEndpoint(REGISTRY_ENDPOINT)
                .withPlatform(custom)
                .build();

        assertSame(custom, registry.getTargetPlatform());
    }

    static class TestRegistry extends AbstractRegistry {

        TestRegistry(Builder builder) {
            super(builder);
        }

        @Override
        public Builder builder() {
            return new Builder().withRegistryEndpoint(registryEndpoint).providerId(providerId);
        }

        @Override
        public String getAuthUsername() {
            return "test-user";
        }

        @Override
        public String getAuthToken() {
            return "test-token";
        }

        @Override
        protected OciRegistryClient getOciClient() {
            return null;
        }

        @Override
        public Class<? extends SubstrateSdkException> getException(Throwable t) {
            return SubstrateSdkException.class;
        }

        @Override
        public void close() {
            // no-op
        }

        static class Builder extends AbstractRegistry.Builder<TestRegistry, Builder> {

            Builder() {
                providerId("test");
            }

            @Override
            public Builder self() {
                return this;
            }

            @Override
            public TestRegistry build() {
                return new TestRegistry(this);
            }
        }
    }
}
