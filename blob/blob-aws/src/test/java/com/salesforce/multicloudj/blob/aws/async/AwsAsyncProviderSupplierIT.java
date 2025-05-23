package com.salesforce.multicloudj.blob.aws.async;

import com.salesforce.multicloudj.blob.async.client.AsyncProviderSupplierIT;
import com.salesforce.multicloudj.common.aws.AwsConstants;

public class AwsAsyncProviderSupplierIT extends AsyncProviderSupplierIT {

    @Override
    protected String getSubstrate() {
        return AwsConstants.PROVIDER_ID;
    }
}
