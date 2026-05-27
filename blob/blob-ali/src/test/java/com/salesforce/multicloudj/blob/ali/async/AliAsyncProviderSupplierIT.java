package com.salesforce.multicloudj.blob.ali.async;

import com.salesforce.multicloudj.blob.async.client.AsyncProviderSupplierIT;
import com.salesforce.multicloudj.common.ali.AliConstants;

public class AliAsyncProviderSupplierIT extends AsyncProviderSupplierIT {

  @Override
  protected String getSubstrate() {
    return AliConstants.PROVIDER_ID;
  }
}
