package com.salesforce.multicloudj.kms.driver;

import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.provider.Provider;
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
import java.util.Map;

/** Portable control-plane operations on keys held by a cloud key management service. */
public interface KeyManagement extends Provider {

  /**
   * Creates a symmetric key.
   *
   * @param request the key's configuration
   * @return a reference to the new key and its initial state
   */
  CreateKeyResponse createKey(CreateKeyRequest request);

  /**
   * Enables a key so it can be used for cryptographic operations. When {@link
   * KeyRef#getVersionId()} is set only that version is enabled.
   *
   * @param keyRef the key to enable
   */
  void enableKey(KeyRef keyRef);

  /**
   * Disables a key so it can no longer be used for cryptographic operations. When {@link
   * KeyRef#getVersionId()} is set only that version is disabled.
   *
   * @param keyRef the key to disable
   */
  void disableKey(KeyRef keyRef);

  /**
   * Starts an import session for a key created with external origin.
   *
   * @param request the key and the wrapping configuration
   * @return the public key and token to use with {@link #importKeyMaterial}
   */
  ImportParameters getImportParameters(GetImportParametersRequest request);

  /**
   * Imports wrapped key material into a key created with external origin.
   *
   * @param request the key, import token and wrapped material
   * @return a reference to the key, including the version created for the material where the
   *     provider versions keys
   */
  KeyRef importKeyMaterial(ImportKeyMaterialRequest request);

  /**
   * Replaces the access policy attached to a key.
   *
   * @param keyRef the key whose policy is replaced
   * @param policy the new policy
   * @throws UnSupportedOperationException if the policy uses anything outside {@link
   *     #getPolicyCapabilities()}
   */
  void setKeyPolicy(KeyRef keyRef, KeyPolicy policy);

  /**
   * Reads the access policy attached to a key. Native policy entries that the {@link KeyPolicy}
   * model cannot express are omitted.
   *
   * @param keyRef the key whose policy is read
   * @return the key's policy
   */
  KeyPolicy getKeyPolicy(KeyRef keyRef);

  /**
   * Adds or overwrites tags on a key. Existing tags with other names are kept.
   *
   * @param keyRef the key to tag
   * @param tags tag names and values
   */
  void tagKey(KeyRef keyRef, Map<String, String> tags);

  /**
   * Describes the subset of the {@link KeyPolicy} model this provider enforces.
   *
   * @return the provider's policy capabilities
   */
  PolicyCapabilities getPolicyCapabilities();

  /**
   * Encrypts a small payload directly with a key.
   *
   * @param request the key, plaintext and encryption context
   * @return the ciphertext
   * @throws UnSupportedOperationException if the provider does not offer direct encryption
   */
  default EncryptResponse encrypt(EncryptRequest request) {
    throw new UnSupportedOperationException("encrypt is not supported by " + getProviderId());
  }

  /**
   * Decrypts a ciphertext produced by {@link #encrypt}.
   *
   * @param request the ciphertext and encryption context
   * @return the plaintext
   * @throws UnSupportedOperationException if the provider does not offer direct decryption
   */
  default DecryptResponse decrypt(DecryptRequest request) {
    throw new UnSupportedOperationException("decrypt is not supported by " + getProviderId());
  }
}
