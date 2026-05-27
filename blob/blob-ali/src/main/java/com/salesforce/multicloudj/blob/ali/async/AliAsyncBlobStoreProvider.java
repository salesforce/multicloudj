package com.salesforce.multicloudj.blob.ali.async;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStoreProvider;
import com.salesforce.multicloudj.common.ali.AliConstants;

@AutoService(AsyncBlobStoreProvider.class)
public class AliAsyncBlobStoreProvider implements AsyncBlobStoreProvider {

  @Override
  public AliAsyncBlobStore.Builder builder() {
    return AliAsyncBlobStore.builder();
  }

  @Override
  public String getProviderId() {
    return AliConstants.PROVIDER_ID;
  }
}
