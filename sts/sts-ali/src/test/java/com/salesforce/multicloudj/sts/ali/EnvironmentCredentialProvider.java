package com.salesforce.multicloudj.sts.ali;

import com.aliyuncs.auth.AlibabaCloudCredentialsProvider;
import com.aliyuncs.auth.BasicSessionCredentials;
import com.aliyuncs.utils.AuthUtils;
import org.apache.commons.lang3.StringUtils;

public class EnvironmentCredentialProvider implements AlibabaCloudCredentialsProvider {
    @Override
    public BasicSessionCredentials getCredentials() {
        if (!"default".equals(AuthUtils.getClientType())) {
            return null;
        }

        String accessKeyId = System.getenv().getOrDefault("ALIBABA_CLOUD_ACCESS_KEY_ID", "FAKE_ACCESS_KEY");
        String accessKeySecret = System.getenv().getOrDefault("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "FAKE_SECRET");
        String securityToken = System.getenv().getOrDefault("ALIBABA_CLOUD_SECURITY_TOKEN", "FAKE_SECURITY_TOKEN");
        if (StringUtils.isEmpty(accessKeyId) || StringUtils.isEmpty(accessKeySecret) || StringUtils.isEmpty(securityToken)) {
            return null;
        }

        return new BasicSessionCredentials(accessKeyId, accessKeySecret, securityToken);
    }
}
