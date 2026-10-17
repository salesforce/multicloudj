package com.salesforce.multicloudj.kms.client;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.kms.driver.AbstractKms;
import com.salesforce.multicloudj.kms.model.CreateKeyRequest;
import com.salesforce.multicloudj.kms.model.CreateKeyResponse;
import com.salesforce.multicloudj.kms.model.GetImportParametersRequest;
import com.salesforce.multicloudj.kms.model.ImportKeyMaterialRequest;
import com.salesforce.multicloudj.kms.model.ImportParameters;
import com.salesforce.multicloudj.kms.model.KeyPolicy;
import com.salesforce.multicloudj.kms.model.KeyRef;
import com.salesforce.multicloudj.kms.model.KeyState;
import com.salesforce.multicloudj.kms.model.PolicyCapabilities;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** In-memory provider that records the driver calls it receives. */
public class TestKms extends AbstractKms {
  public static final String PROVIDER_ID = "test";

  final List<String> calls = new ArrayList<>();
  PolicyCapabilities capabilities = PolicyCapabilities.builder().supportsDeny(true).build();
  RuntimeException failure;

  public TestKms() {
    this(new Builder());
  }

  public TestKms(Builder builder) {
    super(builder);
  }

  @Override
  public Builder builder() {
    return new Builder();
  }

  RetryConfig configuredRetryConfig() {
    return retryConfig;
  }

  Properties configuredProperties() {
    return properties;
  }

  @Override
  public SubstrateSdkException mapException(Throwable t) {
    if (t instanceof SubstrateSdkException) {
      return (SubstrateSdkException) t;
    }
    return new UnknownException(t);
  }

  @Override
  public PolicyCapabilities getPolicyCapabilities() {
    return capabilities;
  }

  @Override
  protected CreateKeyResponse doCreateKey(CreateKeyRequest request) {
    record("createKey");
    return CreateKeyResponse.builder()
        .keyRef(KeyRef.builder().providerId(getProviderId()).keyId("key-1").build())
        .keyState(KeyState.ENABLED)
        .build();
  }

  @Override
  protected void doEnableKey(KeyRef keyRef) {
    record("enableKey");
  }

  @Override
  protected void doDisableKey(KeyRef keyRef) {
    record("disableKey");
  }

  @Override
  protected ImportParameters doGetImportParameters(GetImportParametersRequest request) {
    record("getImportParameters");
    return ImportParameters.builder().keyRef(request.getKeyRef()).importToken("token").build();
  }

  @Override
  protected KeyRef doImportKeyMaterial(ImportKeyMaterialRequest request) {
    record("importKeyMaterial");
    return request.getKeyRef();
  }

  @Override
  protected void doSetKeyPolicy(KeyRef keyRef, KeyPolicy policy) {
    record("setKeyPolicy");
  }

  @Override
  protected KeyPolicy doGetKeyPolicy(KeyRef keyRef) {
    record("getKeyPolicy");
    return KeyPolicy.builder().build();
  }

  @Override
  protected void doTagKey(KeyRef keyRef, Map<String, String> tags) {
    record("tagKey");
  }

  @Override
  public void close() {
    record("close");
  }

  private void record(String call) {
    calls.add(call);
    if (failure != null) {
      throw failure;
    }
  }

  public static class Builder extends AbstractKms.Builder<TestKms, Builder> {
    public Builder() {
      providerId(PROVIDER_ID);
    }

    @Override
    public Builder self() {
      return this;
    }

    @Override
    public TestKms build() {
      return new TestKms(this);
    }
  }
}
