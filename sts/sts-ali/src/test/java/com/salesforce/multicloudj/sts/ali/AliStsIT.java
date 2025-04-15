package com.salesforce.multicloudj.sts.ali;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.http.HttpClientConfig;
import com.aliyuncs.profile.DefaultProfile;
import com.salesforce.multicloudj.common.util.ali.TestsUtilAli;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.sts.driver.AbstractSts;
import com.salesforce.multicloudj.sts.client.AbstractStsIT;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;


public class AliStsIT extends AbstractStsIT {
    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    public static class HarnessImpl implements AbstractStsIT.Harness {
        int port = ThreadLocalRandom.current().nextInt(1000, 10000);
        DefaultAcsClient client;
        @Override
        public AbstractSts<?> createStsDriver(boolean longTermCredentials) {
            HttpClientConfig config = HttpClientConfig.getDefault();
            config.setHttpsProxy(String.format("https://%s:%d", TestsUtil.WIREMOCK_HOST, port));
            config.setX509TrustManagers(TestsUtilAli.createTrustManager());

            DefaultProfile profile = DefaultProfile.getProfile("cn-shanghai");
            profile.setHttpClientConfig(config);
            profile.setCredentialsProvider(new EnvironmentCredentialProvider());

            client = new DefaultAcsClient(profile);
            return new AliSts().builder().build(client);
        }

        @Override
        public String getRoleName() {
            return "acs:ram::1936276257662232:role/chameleon-test-role";
        }

        @Override
        public String getStsEndpoint() {
            return "https://sts.cn-shanghai.aliyuncs.com";
        }

        @Override
        public String getProviderId() {
            return "ali";
        }

        @Override
        public int getPort() {
            return port;
        }

        @Override
        public List<String> getWiremockExtensions() {
            return List.of();
        }

        /**
         * Conformance tests cannot validate this functionality until expose through acs client
         * see: {@link AliSts#getAccessTokenFromProvider(GetAccessTokenRequest)}}
         */
        @Override
        public boolean supportsGetAccessToken() {
            return false;
        }


        @Override
        public void close() {
            client.shutdown();
        }
    }
}
