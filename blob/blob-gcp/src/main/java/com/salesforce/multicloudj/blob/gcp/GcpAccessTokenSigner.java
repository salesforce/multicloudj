package com.salesforce.multicloudj.blob.gcp;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.UrlEncodedContent;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.gson.GsonFactory;
import com.google.auth.ServiceAccountSigner;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Signs V4 URLs for credentials that hold only an OAuth2 access token. The token's account email is
 * resolved through tokeninfo and the URL is signed remotely by IAM {@code signBlob}, so the token
 * needs the {@code userinfo.email} scope, must not be downscoped, and the account needs
 * {@code iam.serviceAccounts.signBlob} on itself.
 */
final class GcpAccessTokenSigner {

  static final String TOKEN_INFO_URL = "https://oauth2.googleapis.com/tokeninfo";
  private static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";

  private record CachedSigner(String tokenValue, ServiceAccountSigner signer) {}

  // Only the latest token's signer is kept so rotated tokens do not accumulate.
  private volatile CachedSigner cached;

  /** Returns a signer when the credentials are a bare access token, otherwise empty. */
  Optional<ServiceAccountSigner> signerFor(ServiceOptions<?, ?> options) {
    // Subclasses (service account, user, impersonated, external account) can sign or refresh
    // themselves, so only the plain GoogleCredentials type is handled here.
    if (options == null
        || options.getCredentials() == null
        || options.getCredentials().getClass() != GoogleCredentials.class) {
      return Optional.empty();
    }
    GoogleCredentials credentials = (GoogleCredentials) options.getCredentials();
    AccessToken accessToken = credentials.getAccessToken();
    if (accessToken == null || accessToken.getTokenValue() == null) {
      return Optional.empty();
    }
    String tokenValue = accessToken.getTokenValue();

    CachedSigner current = cached;
    if (current != null && current.tokenValue().equals(tokenValue)) {
      return Optional.of(current.signer());
    }
    return Optional.of(buildSigner(options, credentials, tokenValue));
  }

  // Synchronized so concurrent callers with a new token resolve the signer once and share it.
  private synchronized ServiceAccountSigner buildSigner(
      ServiceOptions<?, ?> options, GoogleCredentials credentials, String tokenValue) {
    CachedSigner current = cached;
    if (current != null && current.tokenValue().equals(tokenValue)) {
      return current.signer();
    }

    HttpTransportFactory transportFactory =
        options.getTransportOptions() instanceof HttpTransportOptions httpOptions
            ? httpOptions.getHttpTransportFactory()
            : HttpTransportOptions.newBuilder().build().getHttpTransportFactory();
    ServiceAccountSigner signer = ImpersonatedCredentials.newBuilder()
        .setSourceCredentials(credentials)
        .setTargetPrincipal(lookupEmail(transportFactory, tokenValue))
        .setScopes(List.of(CLOUD_PLATFORM_SCOPE))
        .setHttpTransportFactory(transportFactory)
        .build();
    cached = new CachedSigner(tokenValue, signer);
    return signer;
  }

  private static String lookupEmail(HttpTransportFactory transportFactory, String tokenValue) {
    GenericJson tokenInfo;
    try {
      // The token goes in the POST body rather than the URL query string. The client library
      // redacts only the Authorization header when it logs requests, so logging is disabled
      // to keep the body's token out of application logs.
      HttpRequest request = transportFactory.create().createRequestFactory().buildPostRequest(
          new GenericUrl(TOKEN_INFO_URL),
          new UrlEncodedContent(Map.of("access_token", tokenValue)));
      request.setLoggingEnabled(false);
      request.setParser(new JsonObjectParser(GsonFactory.getDefaultInstance()));
      tokenInfo = request.execute().parseAs(GenericJson.class);
    } catch (IOException e) {
      throw new SubstrateSdkException("Failed to resolve the access token's account email", e);
    }
    if (!(tokenInfo.get("email") instanceof String email) || email.isEmpty()) {
      throw new InvalidArgumentException(
          "Cannot sign URL with access-token credentials: tokeninfo did not return an account "
              + "email. Use a service account access token that is not downscoped and was "
              + "minted with the 'userinfo.email' scope in addition to 'cloud-platform'.");
    }
    return email;
  }
}
