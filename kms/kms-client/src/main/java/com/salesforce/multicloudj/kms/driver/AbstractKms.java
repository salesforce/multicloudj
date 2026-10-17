package com.salesforce.multicloudj.kms.driver;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.common.provider.SdkProvider;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.kms.model.CreateKeyRequest;
import com.salesforce.multicloudj.kms.model.CreateKeyResponse;
import com.salesforce.multicloudj.kms.model.DecryptRequest;
import com.salesforce.multicloudj.kms.model.DecryptResponse;
import com.salesforce.multicloudj.kms.model.Effect;
import com.salesforce.multicloudj.kms.model.EncryptRequest;
import com.salesforce.multicloudj.kms.model.EncryptResponse;
import com.salesforce.multicloudj.kms.model.ExpirationModel;
import com.salesforce.multicloudj.kms.model.GetImportParametersRequest;
import com.salesforce.multicloudj.kms.model.ImportKeyMaterialRequest;
import com.salesforce.multicloudj.kms.model.ImportParameters;
import com.salesforce.multicloudj.kms.model.KeyAction;
import com.salesforce.multicloudj.kms.model.KeyGrant;
import com.salesforce.multicloudj.kms.model.KeyPolicy;
import com.salesforce.multicloudj.kms.model.KeyPrincipal;
import com.salesforce.multicloudj.kms.model.KeyRef;
import com.salesforce.multicloudj.kms.model.PolicyCapabilities;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * Base class for key management provider implementations. Public operations validate their input
 * and then delegate to the provider's {@code do*} methods.
 */
public abstract class AbstractKms implements KeyManagement, AutoCloseable {

  @Getter private final String providerId;
  @Getter protected final String region;
  protected final URI endpoint;

  /**
   * Provider resource that keys are created in and short key ids are resolved against, for
   * providers that group keys under a parent resource.
   */
  @Getter protected final String keyContainer;

  protected final CredentialsOverrider credentialsOverrider;
  protected final RetryConfig retryConfig;
  protected final Properties properties;

  protected AbstractKms(Builder<?, ?> builder) {
    this.providerId = builder.getProviderId();
    this.region = builder.getRegion();
    this.endpoint = builder.getEndpoint();
    this.keyContainer = builder.getKeyContainer();
    this.credentialsOverrider = builder.getCredentialsOverrider();
    this.retryConfig = builder.getRetryConfig();
    this.properties = builder.getProperties();
  }

  /** {@inheritDoc} */
  @Override
  public CreateKeyResponse createKey(CreateKeyRequest request) {
    requireNonNull(request, "request");
    validateTags(request.getTags());
    return doCreateKey(request);
  }

  /** {@inheritDoc} */
  @Override
  public void enableKey(KeyRef keyRef) {
    validateKeyRef(keyRef);
    doEnableKey(keyRef);
  }

  /** {@inheritDoc} */
  @Override
  public void disableKey(KeyRef keyRef) {
    validateKeyRef(keyRef);
    doDisableKey(keyRef);
  }

  /** {@inheritDoc} */
  @Override
  public ImportParameters getImportParameters(GetImportParametersRequest request) {
    requireNonNull(request, "request");
    validateKeyRef(request.getKeyRef());
    requireNonNull(request.getWrappingAlgorithm(), "wrappingAlgorithm");
    return doGetImportParameters(request);
  }

  /** {@inheritDoc} */
  @Override
  public KeyRef importKeyMaterial(ImportKeyMaterialRequest request) {
    requireNonNull(request, "request");
    validateKeyRef(request.getKeyRef());
    if (StringUtils.isBlank(request.getImportToken())) {
      throw new InvalidArgumentException("importToken is required");
    }
    requireNonEmpty(request.getWrappedKeyMaterial(), "wrappedKeyMaterial");
    requireNonNull(request.getExpirationModel(), "expirationModel");
    if (request.getExpirationModel() == ExpirationModel.KEY_MATERIAL_EXPIRES
        && request.getValidTo() == null) {
      throw new InvalidArgumentException("validTo is required when key material expires");
    }
    return doImportKeyMaterial(request);
  }

  /** {@inheritDoc} */
  @Override
  public void setKeyPolicy(KeyRef keyRef, KeyPolicy policy) {
    validateKeyRef(keyRef);
    validatePolicy(policy);
    doSetKeyPolicy(keyRef, policy);
  }

  /** {@inheritDoc} */
  @Override
  public KeyPolicy getKeyPolicy(KeyRef keyRef) {
    validateKeyRef(keyRef);
    return doGetKeyPolicy(keyRef);
  }

  /** {@inheritDoc} */
  @Override
  public void tagKey(KeyRef keyRef, Map<String, String> tags) {
    validateKeyRef(keyRef);
    if (tags == null || tags.isEmpty()) {
      throw new InvalidArgumentException("tags must contain at least one entry");
    }
    validateTags(tags);
    doTagKey(keyRef, tags);
  }

  /** {@inheritDoc} */
  @Override
  public EncryptResponse encrypt(EncryptRequest request) {
    requireNonNull(request, "request");
    validateKeyRef(request.getKeyRef());
    requireNonEmpty(request.getPlaintext(), "plaintext");
    return doEncrypt(request);
  }

  /** {@inheritDoc} */
  @Override
  public DecryptResponse decrypt(DecryptRequest request) {
    requireNonNull(request, "request");
    if (request.getKeyRef() != null) {
      validateKeyRef(request.getKeyRef());
    }
    requireNonEmpty(request.getCiphertext(), "ciphertext");
    return doDecrypt(request);
  }

  protected abstract CreateKeyResponse doCreateKey(CreateKeyRequest request);

  protected abstract void doEnableKey(KeyRef keyRef);

  protected abstract void doDisableKey(KeyRef keyRef);

  protected abstract ImportParameters doGetImportParameters(GetImportParametersRequest request);

  protected abstract KeyRef doImportKeyMaterial(ImportKeyMaterialRequest request);

  /**
   * Replaces the key's policy. Called only with policies that fit {@link #getPolicyCapabilities()}.
   */
  protected abstract void doSetKeyPolicy(KeyRef keyRef, KeyPolicy policy);

  protected abstract KeyPolicy doGetKeyPolicy(KeyRef keyRef);

  protected abstract void doTagKey(KeyRef keyRef, Map<String, String> tags);

  protected EncryptResponse doEncrypt(EncryptRequest request) {
    throw new UnSupportedOperationException("encrypt is not supported by " + getProviderId());
  }

  protected DecryptResponse doDecrypt(DecryptRequest request) {
    throw new UnSupportedOperationException("decrypt is not supported by " + getProviderId());
  }

  private void validatePolicy(KeyPolicy policy) {
    requireNonNull(policy, "policy");
    if (policy.getGrants().isEmpty()) {
      throw new InvalidArgumentException("policy must contain at least one grant");
    }
    PolicyCapabilities capabilities = getPolicyCapabilities();
    for (KeyGrant grant : policy.getGrants()) {
      if (grant.getPrincipals().isEmpty()) {
        throw new InvalidArgumentException("every grant must name at least one principal");
      }
      if (grant.getActions().isEmpty()) {
        throw new InvalidArgumentException("every grant must contain at least one action");
      }
      requireNonNull(grant.getEffect(), "effect");
      if (grant.getEffect() == Effect.DENY && !capabilities.isSupportsDeny()) {
        throw new UnSupportedOperationException(
            getProviderId() + " key policies cannot express DENY grants");
      }
      if (!grant.getConditions().isEmpty() && !capabilities.isSupportsConditions()) {
        throw new UnSupportedOperationException(
            getProviderId() + " key policies cannot express grant conditions");
      }
      for (KeyPrincipal principal : grant.getPrincipals()) {
        if (StringUtils.isBlank(principal.getIdentifier())) {
          throw new InvalidArgumentException("principal identifier is required");
        }
        if (!capabilities.getSupportedPrincipalTypes().contains(principal.getType())) {
          throw new UnSupportedOperationException(
              getProviderId()
                  + " key policies cannot express principal type "
                  + principal.getType());
        }
      }
      for (KeyAction action : grant.getActions()) {
        if (!capabilities.getSupportedActions().contains(action)) {
          throw new UnSupportedOperationException(
              getProviderId() + " key policies cannot express action " + action);
        }
      }
    }
  }

  private static void validateKeyRef(KeyRef keyRef) {
    requireNonNull(keyRef, "keyRef");
    if (StringUtils.isBlank(keyRef.getKeyId())) {
      throw new InvalidArgumentException("keyRef.keyId is required");
    }
  }

  private static void validateTags(Map<String, String> tags) {
    for (Map.Entry<String, String> tag : tags.entrySet()) {
      if (StringUtils.isBlank(tag.getKey())) {
        throw new InvalidArgumentException("tag names cannot be blank");
      }
      if (tag.getValue() == null) {
        throw new InvalidArgumentException("tag " + tag.getKey() + " has no value");
      }
    }
  }

  private static void requireNonNull(Object value, String name) {
    if (value == null) {
      throw new InvalidArgumentException(name + " cannot be null");
    }
  }

  private static void requireNonEmpty(byte[] value, String name) {
    if (value == null || value.length == 0) {
      throw new InvalidArgumentException(name + " cannot be empty");
    }
  }

  /**
   * Builder for provider implementations of {@link AbstractKms}.
   *
   * @param <A> the provider implementation built
   * @param <T> the concrete builder type
   */
  @Getter
  public abstract static class Builder<A extends AbstractKms, T extends Builder<A, T>>
      implements Provider.Builder, SdkProvider.Builder<A> {
    private String providerId;
    private String region;
    private URI endpoint;
    private String keyContainer;
    private CredentialsOverrider credentialsOverrider;
    private RetryConfig retryConfig;
    private Properties properties = new Properties();

    @Override
    public T providerId(String providerId) {
      this.providerId = providerId;
      return self();
    }

    /**
     * Method to supply region
     *
     * @param region Region
     * @return An instance of self
     */
    public T withRegion(String region) {
      this.region = region;
      return self();
    }

    /**
     * Method to supply an endpoint override
     *
     * @param endpoint The endpoint to set.
     * @return An instance of self
     */
    public T withEndpoint(URI endpoint) {
      this.endpoint = endpoint;
      return self();
    }

    /**
     * Method to supply the provider resource that keys are created in and short key ids are
     * resolved against. Providers that do not group keys under a parent resource ignore it.
     *
     * @param keyContainer Fully qualified native name of the key container
     * @return An instance of self
     */
    public T withKeyContainer(String keyContainer) {
      this.keyContainer = keyContainer;
      return self();
    }

    /**
     * Method to supply credentialsOverrider
     *
     * @param credentialsOverrider CredentialsOverrider
     * @return An instance of self
     */
    public T withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
      this.credentialsOverrider = credentialsOverrider;
      return self();
    }

    /**
     * Method to supply retry configuration
     *
     * @param retryConfig The retry configuration to use for retrying failed requests
     * @return An instance of self
     * @throws InvalidArgumentException if the configuration has invalid values for its mode
     */
    public T withRetryConfig(RetryConfig retryConfig) {
      if (retryConfig != null) {
        retryConfig.validate();
      }
      this.retryConfig = retryConfig;
      return self();
    }

    /**
     * Method to supply provider-specific settings that have no portable builder method.
     *
     * @param properties Provider-specific settings
     * @return An instance of self
     */
    @Override
    public T withProperties(Properties properties) {
      this.properties = properties;
      return self();
    }

    public abstract T self();

    /**
     * Builds and returns an instance of AbstractKms.
     *
     * @return An instance of AbstractKms.
     */
    @Override
    public abstract A build();
  }
}
