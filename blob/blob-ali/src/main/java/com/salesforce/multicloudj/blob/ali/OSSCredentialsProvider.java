package com.salesforce.multicloudj.blob.ali;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.CredentialsProviderFactory;
import com.aliyun.oss.common.auth.STSAssumeRoleSessionCredentialsProvider;
import com.aliyuncs.auth.DefaultCredentialsProvider;
import com.aliyuncs.profile.DefaultProfile;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.StsCredentials;

public class OSSCredentialsProvider {
    public static CredentialsProvider getCredentialsProvider(CredentialsOverrider overrider, String region) {
        if (overrider == null || overrider.getType() == null) {
            return null;
        }

        switch (overrider.getType()) {
            case SESSION:
                StsCredentials stsCredentials = overrider.getSessionCredentials();
                CredentialsProviderFactory factory = new CredentialsProviderFactory();
                return factory.newDefaultCredentialProvider(
                        stsCredentials.getAccessKeyId(),
                        stsCredentials.getAccessKeySecret(),
                        stsCredentials.getSecurityToken()
                );
            case ASSUME_ROLE:
                String assumeRole = overrider.getRole();
                DefaultCredentialsProvider provider;
                try {
                    provider = new DefaultCredentialsProvider();
                } catch (com.aliyuncs.exceptions.ClientException | ClientException e) {
                    throw new RuntimeException(e);
                }
                DefaultProfile clientProfile = DefaultProfile.getProfile(region);
                return new STSAssumeRoleSessionCredentialsProvider(provider, assumeRole, clientProfile);

        }

        return null;
    }
}
