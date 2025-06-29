package com.salesforce.multicloudj.blob.gcp;

import com.salesforce.multicloudj.blob.client.ProviderSupplierIT;
import com.salesforce.multicloudj.common.gcp.GcpConstants;

public class GcpProviderSupplierIT extends ProviderSupplierIT {

    @Override
    protected String getSubstrate() {
        return GcpConstants.PROVIDER_ID;
    }
}
