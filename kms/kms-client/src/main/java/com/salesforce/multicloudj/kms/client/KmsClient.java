package com.salesforce.multicloudj.kms.client;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.kms.driver.AbstractKms;
import com.salesforce.multicloudj.kms.model.CreateKeyRequest;
import com.salesforce.multicloudj.kms.model.CreateKeyResponse;
import com.salesforce.multicloudj.kms.model.DecryptRequest;
import com.salesforce.multicloudj.kms.model.DecryptResponse;
import com.salesforce.multicloudj.kms.model.EncryptRequest;
import com.salesforce.multicloudj.kms.model.EncryptResponse;
import com.salesforce.multicloudj.kms.model.GetImportParametersRequest;
import com.salesforce.multicloudj.kms.model.ImportKeyMaterialRequest;
import com.salesforce.multicloudj.kms.model.ImportParameters;
import com.salesforce.multicloudj.kms.model.KeyPolicy;
import com.salesforce.multicloudj.kms.model.KeyRef;
import com.salesforce.multicloudj.kms.model.PolicyCapabilities;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

/**
 * Entry point for client code to manage keys in a cloud key management service in a
 * substrate-agnostic way.
 *
 * <p>Usage example:
 *
 * <pre>
 * KmsClient client = KmsClient.builder(providerId)
 *     .withRegion(region)
 *     .withKeyContainer(keyContainer)
 *     .build();
 *
 * CreateKeyResponse created = client.createKey(CreateKeyRequest.builder()
 *     .keyName("orders-root-key")
 *     .description("Root key for order data")
 *     .build());
 * client.tagKey(created.getKeyRef(), Map.of("team", "orders"));
 * </pre>
 */
public class KmsClient implements AutoCloseable {
  protected AbstractKms kms;

  protected KmsClient(AbstractKms kms) {
    this.kms = kms;
  }

  /**
   * Creates a new KmsClientBuilder for the specified provider.
   *
   * @param providerId the ID of the provider such as "aws", "gcp", or "ali"
   * @return a new KmsClientBuilder instance
   */
  public static KmsClientBuilder builder(String providerId) {
    return new KmsClientBuilder(providerId);
  }

  /**
   * Creates a symmetric key.
   *
   * @param request the key's configuration
   * @return a reference to the new key and its initial state
   * @throws SubstrateSdkException if the operation fails
   */
  public CreateKeyResponse createKey(CreateKeyRequest request) {
    try {
      return kms.createKey(request);
    } catch (Throwable t) {
      throw kms.mapException(t);
    }
  }

  /**
   * Enables a key, or only the version named by {@link KeyRef#getVersionId()} when set.
   *
   * @param keyRef the key to enable
   * @throws SubstrateSdkException if the operation fails
   */
  public void enableKey(KeyRef keyRef) {
    try {
      kms.enableKey(keyRef);
    } catch (Throwable t) {
      throw kms.mapException(t);
    }
  }

  /**
   * Disables a key, or only the version named by {@link KeyRef#getVersionId()} when set.
   *
   * @param keyRef the key to disable
   * @throws SubstrateSdkException if the operation fails
   */
  public void disableKey(KeyRef keyRef) {
    try {
      kms.disableKey(keyRef);
    } catch (Throwable t) {
      throw kms.mapException(t);
    }
  }

  /**
   * Starts an import session for a key created with external origin.
   *
   * @param request the key and the wrapping configuration
   * @return the public key and token to use with {@link #importKeyMaterial}
   * @throws SubstrateSdkException if the operation fails
   */
  public ImportParameters getImportParameters(GetImportParametersRequest request) {
    try {
      return kms.getImportParameters(request);
    } catch (Throwable t) {
      throw kms.mapException(t);
    }
  }

  /**
   * Imports wrapped key material into a key created with external origin.
   *
   * @param request the key, import token and wrapped material
   * @return a reference to the key, including the version created for the material where the
   *     provider versions keys
   * @throws SubstrateSdkException if the operation fails
   */
  public KeyRef importKeyMaterial(ImportKeyMaterialRequest request) {
    try {
      return kms.importKeyMaterial(request);
    } catch (Throwable t) {
      throw kms.mapException(t);
    }
  }

  /**
   * Replaces the access policy attached to a key.
   *
   * @param keyRef the key whose policy is replaced
   * @param policy the new policy
   * @throws SubstrateSdkException if the operation fails, including {@code
   *     UnSupportedOperationException} when the policy uses anything outside {@link
   *     #getPolicyCapabilities()}
   */
  public void setKeyPolicy(KeyRef keyRef, KeyPolicy policy) {
    try {
      kms.setKeyPolicy(keyRef, policy);
    } catch (Throwable t) {
      throw kms.mapException(t);
    }
  }

  /**
   * Reads the access policy attached to a key. Native policy entries that the {@link KeyPolicy}
   * model cannot express are omitted.
   *
   * @param keyRef the key whose policy is read
   * @return the key's policy
   * @throws SubstrateSdkException if the operation fails
   */
  public KeyPolicy getKeyPolicy(KeyRef keyRef) {
    try {
      return kms.getKeyPolicy(keyRef);
    } catch (Throwable t) {
      throw kms.mapException(t);
    }
  }

  /**
   * Adds or overwrites tags on a key. Existing tags with other names are kept.
   *
   * @param keyRef the key to tag
   * @param tags tag names and values
   * @throws SubstrateSdkException if the operation fails
   */
  public void tagKey(KeyRef keyRef, Map<String, String> tags) {
    try {
      kms.tagKey(keyRef, tags);
    } catch (Throwable t) {
      throw kms.mapException(t);
    }
  }

  /**
   * Encrypts a small payload directly with a key.
   *
   * @param request the key, plaintext and encryption context
   * @return the ciphertext
   * @throws SubstrateSdkException if the operation fails
   */
  public EncryptResponse encrypt(EncryptRequest request) {
    try {
      return kms.encrypt(request);
    } catch (Throwable t) {
      throw kms.mapException(t);
    }
  }

  /**
   * Decrypts a ciphertext produced by {@link #encrypt}.
   *
   * @param request the ciphertext and encryption context
   * @return the plaintext
   * @throws SubstrateSdkException if the operation fails
   */
  public DecryptResponse decrypt(DecryptRequest request) {
    try {
      return kms.decrypt(request);
    } catch (Throwable t) {
      throw kms.mapException(t);
    }
  }

  /**
   * Describes the subset of the {@link KeyPolicy} model the provider enforces.
   *
   * @return the provider's policy capabilities
   */
  public PolicyCapabilities getPolicyCapabilities() {
    return kms.getPolicyCapabilities();
  }

  @Override
  public void close() throws Exception {
    kms.close();
  }

  /** Builder class for KmsClient. */
  public static class KmsClientBuilder {
    private final AbstractKms.Builder<?, ?> kmsBuilder;

    /**
     * Constructor for KmsClientBuilder.
     *
     * @param providerId the ID of the provider such as "aws", "gcp", or "ali"
     */
    public KmsClientBuilder(String providerId) {
      this.kmsBuilder = ProviderSupplier.findProviderBuilder(providerId);
    }

    /**
     * Method to supply region
     *
     * @param region Region
     * @return An instance of self
     */
    public KmsClientBuilder withRegion(String region) {
      this.kmsBuilder.withRegion(region);
      return this;
    }

    /**
     * Method to supply an endpoint override
     *
     * @param endpoint The endpoint override
     * @return An instance of self
     */
    public KmsClientBuilder withEndpoint(URI endpoint) {
      this.kmsBuilder.withEndpoint(endpoint);
      return this;
    }

    /**
     * Method to supply the provider resource that keys are created in and short key ids are
     * resolved against. Providers that do not group keys under a parent resource ignore it.
     *
     * @param keyContainer Fully qualified native name of the key container
     * @return An instance of self
     */
    public KmsClientBuilder withKeyContainer(String keyContainer) {
      this.kmsBuilder.withKeyContainer(keyContainer);
      return this;
    }

    /**
     * Method to supply credentialsOverrider
     *
     * @param credentialsOverrider CredentialsOverrider
     * @return An instance of self
     */
    public KmsClientBuilder withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
      this.kmsBuilder.withCredentialsOverrider(credentialsOverrider);
      return this;
    }

    /**
     * Method to supply retry configuration
     *
     * @param retryConfig The retry configuration to use for retrying failed requests
     * @return An instance of self
     */
    public KmsClientBuilder withRetryConfig(RetryConfig retryConfig) {
      this.kmsBuilder.withRetryConfig(retryConfig);
      return this;
    }

    /**
     * Method to supply provider-specific settings that have no portable builder method.
     *
     * @param properties Provider-specific settings
     * @return An instance of self
     */
    public KmsClientBuilder withProperties(Properties properties) {
      this.kmsBuilder.withProperties(properties);
      return this;
    }

    /**
     * Builds and returns an instance of KmsClient.
     *
     * @return An instance of KmsClient.
     */
    public KmsClient build() {
      return new KmsClient(kmsBuilder.build());
    }
  }
}
