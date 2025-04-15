package com.salesforce.multicloudj.common.util.ali;

import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;

public class TestsUtilAli {

    public static X509TrustManager[] createTrustManager() {

        return new X509TrustManager[]{new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) {

            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) {
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        }};
    }
}