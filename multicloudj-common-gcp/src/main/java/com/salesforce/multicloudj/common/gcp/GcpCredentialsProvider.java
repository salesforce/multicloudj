package com.salesforce.multicloudj.common.gcp;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.StsCredentials;

import java.io.IOException;
import java.util.List;

public class GcpCredentialsProvider {

    public static Credentials getCredentials(CredentialsOverrider overrider) {
        if (overrider == null || overrider.getType() == null) {
            return null;
        }
        switch (overrider.getType()) {
            case SESSION:
                StsCredentials stsCredentials = overrider.getSessionCredentials();
                return GoogleCredentials.newBuilder()
                        .setAccessToken(AccessToken.newBuilder()
                                .setTokenValue(stsCredentials.getSecurityToken())
                                .build())
                        .build();
            case ASSUME_ROLE:
                GoogleCredentials sourceCredentials = null;
                try {
                    sourceCredentials = GoogleCredentials.getApplicationDefault();
                } catch (IOException e) {
                    throw new SubstrateSdkException("Could not find default credentials", e);
                }
                String targetServiceAccount = overrider.getRole();
                return ImpersonatedCredentials.create(
                        sourceCredentials,
                        targetServiceAccount,
                        null,
                        List.of("https://www.googleapis.com/auth/cloud-platform"),
                        overrider.getDurationSeconds()==null ? 0 : overrider.getDurationSeconds()
                );
        }
        return null;
    }
}
