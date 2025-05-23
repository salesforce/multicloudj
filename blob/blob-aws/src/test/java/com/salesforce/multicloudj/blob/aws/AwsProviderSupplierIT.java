package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.blob.client.ProviderSupplierIT;
import com.salesforce.multicloudj.common.aws.AwsConstants;

public class AwsProviderSupplierIT extends ProviderSupplierIT {

    @Override
    protected String getSubstrate() {
        return AwsConstants.PROVIDER_ID;
    }
}
