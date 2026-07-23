package com.salesforce.multicloudj.common.gcp;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdentityPoolCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

public class GcpCredentialsProvider {

  // Google Security Token Service endpoint used to exchange a federated subject token
  // for a short-lived GCP access token under Workload Identity Federation.
  private static final String STS_TOKEN_URL = "https://sts.googleapis.com/v1/token";

  private static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";

  public static Credentials getCredentials(CredentialsOverrider overrider) {
    if (overrider == null || overrider.getType() == null) {
      return null;
    }
    switch (overrider.getType()) {
      case SESSION:
        StsCredentials stsCredentials = overrider.getSessionCredentials();
        return GoogleCredentials.newBuilder()
            .setAccessToken(
                AccessToken.newBuilder().setTokenValue(stsCredentials.getSecurityToken()).build())
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
            List.of(CLOUD_PLATFORM_SCOPE),
            overrider.getDurationSeconds() == null ? 0 : overrider.getDurationSeconds());
      case ASSUME_ROLE_WEB_IDENTITY:
        if (overrider.getWebIdentityTokenSupplier() == null) {
          throw new IllegalArgumentException(
              "webIdentityTokenSupplier must be provided for ASSUME_ROLE_WEB_IDENTITY");
        }
        // Workload Identity Federation exchanges an external subject token for a short-lived GCP
        // access token. IdentityPoolCredentials invokes the supplier on every token refresh, so
        // the caller's supplier is responsible for returning a fresh token. The audience is the
        // full Workload Identity Pool provider resource name, carried in the overrider's role
        // field. The subject token type is inferred from the token the supplier returns so that
        // both OIDC/JWT tokens and pre-signed GetCallerIdentity tokens are supported.
        Supplier<String> tokenSupplier = overrider.getWebIdentityTokenSupplier();
        ExternalAccountCredentials.SubjectTokenTypes subjectTokenType =
            inferSubjectTokenType(tokenSupplier.get());
        return IdentityPoolCredentials.newBuilder()
            .setSubjectTokenSupplier(context -> tokenSupplier.get())
            .setAudience(overrider.getRole())
            .setSubjectTokenType(subjectTokenType)
            .setTokenUrl(STS_TOKEN_URL)
            .setScopes(List.of(CLOUD_PLATFORM_SCOPE))
            .build();
      default:
        return null;
    }
  }

  /**
   * Infers the Workload Identity Federation subject token type from the token the supplier returns.
   * GCP also supports the direct SigV4 signed request for workload identity pool.
   *
   * <p>A pre-signed GetCallerIdentity subject token is a URL-encoded JSON envelope, so it begins
   * with an encoded opening brace ("%7B") or, if the caller supplied it unencoded, a literal "{".
   * Any other value is treated as an OIDC/JWT subject token. The subject token type must match the
   * format that GCP STS receives or the token exchange is rejected.
   */
  private static ExternalAccountCredentials.SubjectTokenTypes inferSubjectTokenType(String token) {
    if (token != null) {
      String trimmed = token.trim();
      if (trimmed.startsWith("{") || trimmed.regionMatches(true, 0, "%7B", 0, 3)) {
        return ExternalAccountCredentials.SubjectTokenTypes.AWS4;
      }
    }
    return ExternalAccountCredentials.SubjectTokenTypes.JWT;
  }
}
