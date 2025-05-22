package com.salesforce.multicloudj.blob.ali;

import com.salesforce.multicloudj.blob.client.ProviderSupplierIT;
import com.salesforce.multicloudj.common.ali.AliConstants;

public class AliProviderSupplierIT extends ProviderSupplierIT {

    @Override
    protected String getSubstrate() {
        return AliConstants.PROVIDER_ID;
    }
}
