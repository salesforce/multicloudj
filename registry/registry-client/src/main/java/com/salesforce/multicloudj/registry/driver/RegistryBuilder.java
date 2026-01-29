package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.common.provider.SdkProvider;
import com.salesforce.multicloudj.common.service.SdkService;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import lombok.Getter;

import java.net.URI;

/**
 * Base builder class for registry implementations.
 * Stores common configuration fields shared by all providers.
 */
@Getter
public abstract class RegistryBuilder<T extends SdkService> implements SdkProvider.Builder<T> {

    private String providerId;
    private String repository;
    private String region;
    private String registryEndpoint;  // User-provided registry endpoint
    private URI proxyEndpoint;  // Proxy endpoint for HTTP client
    private CredentialsOverrider credentialsOverrider;
    private Platform platform;  // User-specified platform for multi-arch selection

    public RegistryBuilder<T> providerId(String providerId) {
        this.providerId = providerId;
        return this;
    }

    /**
     * Method to supply repository name
     * @param repository Repository name
     * @return An instance of self
     */
    public RegistryBuilder<T> withRepository(String repository) {
        this.repository = repository;
        return this;
    }

    /**
     * Method to supply region
     * @param region Region
     * @return An instance of self
     */
    public RegistryBuilder<T> withRegion(String region) {
        this.region = region;
        return this;
    }

    /**
     * Method to supply registry endpoint
     * @param registryEndpoint Registry endpoint (e.g., "https://123456789.dkr.ecr.us-east-1.amazonaws.com")
     * @return An instance of self
     */
    public RegistryBuilder<T> withRegistryEndpoint(String registryEndpoint) {
        this.registryEndpoint = registryEndpoint;
        return this;
    }

    /**
     * Method to supply a proxy endpoint override
     * @param proxyEndpoint The proxy endpoint override
     * @return An instance of self
     */
    public RegistryBuilder<T> withProxyEndpoint(URI proxyEndpoint) {
        this.proxyEndpoint = proxyEndpoint;
        return this;
    }

    /**
     * Method to supply credentialsOverrider
     * @param credentialsOverrider CredentialsOverrider
     * @return An instance of self
     */
    public RegistryBuilder<T> withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
        this.credentialsOverrider = credentialsOverrider;
        return this;
    }

    /**
     * Method to supply target platform for multi-arch image selection
     * @param platform Platform object
     * @return An instance of self
     */
    public RegistryBuilder<T> withPlatform(Platform platform) {
        this.platform = platform;
        return this;
    }
}
