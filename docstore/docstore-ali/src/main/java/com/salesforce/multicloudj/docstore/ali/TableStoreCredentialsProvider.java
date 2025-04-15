package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProviderFactory;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.StsCredentials;

public class TableStoreCredentialsProvider {
    public static CredentialsProvider getCredentialsProvider(CredentialsOverrider overrider, String region) {
        if (overrider == null || overrider.getType() == null) {
            return null;
        }

        switch (overrider.getType()) {
            case SESSION:
                StsCredentials stsCredentials = overrider.getSessionCredentials();
                return CredentialsProviderFactory.newDefaultCredentialProvider(
                        stsCredentials.getAccessKeyId(),
                        stsCredentials.getAccessKeySecret(),
                        stsCredentials.getSecurityToken()
                );
            case ASSUME_ROLE:
                // TODD: We need to make changes in the upstream to support assume role credentials provider

        }

        return null;
    }
}
