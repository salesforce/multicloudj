package com.salesforce.multicloudj.sts;

import static com.salesforce.multicloudj.sts.Curl.requestToCurl;

import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageResponse;
import com.salesforce.multicloudj.examples.AppConfig;
import com.salesforce.multicloudj.sts.client.StsClient;
import com.salesforce.multicloudj.sts.client.StsUtilities;
import com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.CredentialScope;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.GetCallerIdentityRequest;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.net.URI;
import java.net.http.HttpRequest;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    assumeRole();
    assumeRoleWebIdentityCredentialsOverrider();
    getCallerIdentity();
    nativeAuthSignerUtilityWithStsCredentials();
    nativeAuthSignerUtilityWithDefaultCredentials();
  }

  public static void assumeRole() {
    StsClient client =
        StsClient.builder(provider()).withRegion(AppConfig.get("region")).build();

    // Create a cloud-agnostic credential scope with condition
    CredentialScope.AvailabilityCondition condition =
        CredentialScope.AvailabilityCondition.builder()
            .resourcePrefix("storage://my-bucket/documents/")
            .title("Limit to documents folder")
            .description("Only allow access to objects in the documents folder")
            .build();

    CredentialScope.ScopeRule rule =
        CredentialScope.ScopeRule.builder()
            .availableResource("storage://my-bucket/*")
            .availablePermission("storage:GetObject")
            .availablePermission("storage:PutObject")
            .availabilityCondition(condition)
            .build();

    CredentialScope credentialScope = CredentialScope.builder().rule(rule).build();

    AssumedRoleRequest request =
        AssumedRoleRequest.newBuilder()
            .withRole(AppConfig.get("sts.role"))
            .withSessionName("my-session")
            .withCredentialScope(credentialScope)
            .build();
    StsCredentials stsCredentials = client.getAssumeRoleCredentials(request);

    logger.info("AccessKeyId: {}", stsCredentials.getAccessKeyId());
  }

  public static void assumeRoleWebIdentityCredentialsOverrider() {
    String audience = AppConfig.get("sts.federation.audience");
    String identityProvider = AppConfig.get("sts.federation.identity.provider");
    Supplier<String> tokenSupplier =
        () -> {
          // The identity token comes from the configured identity provider and is exchanged for
          // credentials of the federation role on the configured sts provider.
          StsClient identityClient = StsClient.builder(identityProvider).build();
          CallerIdentity identity =
              identityClient.getCallerIdentity(
                  GetCallerIdentityRequest.builder().aud(audience).build());
          return identity.getCloudResourceName();
        };

    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
            .withRole(AppConfig.get("sts.federation.role"))
            .withWebIdentityTokenSupplier(tokenSupplier)
            .build();
    BucketClient bucketClient =
        BucketClient.builder(provider())
            .withRegion(AppConfig.get("region"))
            .withBucket(AppConfig.get("sts.federation.bucket"))
            .withCredentialsOverrider(overrider)
            .build();
    ListBlobsPageResponse r =
        bucketClient.listPage(ListBlobsPageRequest.builder().withMaxResults(1).build());
    logger.info("s");
  }

  private static void getCallerIdentity() {
    // An identity from the configured identity provider assumes the federation role on the
    // configured sts provider via web identity.
    String region = AppConfig.get("region");
    StsClient identityClient =
        StsClient.builder(AppConfig.get("sts.federation.identity.provider"))
            .withRegion(region)
            .build();
    CallerIdentity identity = identityClient.getCallerIdentity();
    StsClient client = StsClient.builder(provider()).withRegion(region).build();
    StsCredentials credentials =
        client.getAssumeRoleWithWebIdentityCredentials(
            AssumeRoleWebIdentityRequest.builder()
                .webIdentityToken(identity.getCloudResourceName())
                .role(AppConfig.get("sts.federation.role"))
                .build());
    logger.info(
        "AccountId: {}, UserId: {}, ResourceName: {}, AccessKeyId: {}",
        identity.getAccountId(),
        identity.getUserId(),
        identity.getCloudResourceName(),
        credentials.getAccessKeyId());
  }

  public static void nativeAuthSignerUtilityWithStsCredentials() {
    String region = AppConfig.get("region");

    StsClient client = StsClient.builder(provider()).withRegion(region).build();
    AssumedRoleRequest request =
        AssumedRoleRequest.newBuilder()
            .withRole(AppConfig.get("sts.role"))
            .withSessionName("my-session")
            .build();
    StsCredentials stsCredentials = client.getAssumeRoleCredentials(request);

    HttpRequest fakeRequest = requestToSign();
    CredentialsOverrider credsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(stsCredentials)
            .build();
    StsUtilities stsUtil =
        StsUtilities.builder(provider())
            .withRegion(region)
            .withCredentialsOverrider(credsOverrider)
            .build();

    SignedAuthRequest newSignedAuthRequest = stsUtil.newCloudNativeAuthSignedRequest(fakeRequest);

    logger.info(
        "nativeAuthSignerUtilityWithStsCredentials curl request:\n{}",
        requestToCurl(newSignedAuthRequest.getRequest()));
  }

  public static void nativeAuthSignerUtilityWithDefaultCredentials() {
    String region = AppConfig.get("region");

    HttpRequest fakeRequest = requestToSign();
    StsUtilities stsUtil = StsUtilities.builder(provider()).withRegion(region).build();
    SignedAuthRequest newSignedAuthRequest = stsUtil.newCloudNativeAuthSignedRequest(fakeRequest);

    logger.info(
        "nativeAuthSignerUtilityWithDefaultCredentials curl request:\n{}",
        requestToCurl(newSignedAuthRequest.getRequest()));
  }

  /** Builds the sample request to sign from {@code sts.sign.url} and {@code sts.sign.body}. */
  private static HttpRequest requestToSign() {
    String body = AppConfig.getOptional("sts.sign.body");
    HttpRequest.BodyPublisher bodyPublisher =
        body == null
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(body);
    return HttpRequest.newBuilder()
        .POST(bodyPublisher)
        .uri(URI.create(AppConfig.get("sts.sign.url")))
        .build();
  }

  private static String provider() {
    // Never hardcode the provider id; resolve it from configuration (see examples.properties)
    return AppConfig.provider();
  }
}
