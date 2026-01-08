package com.salesforce.multicloudj.sts;

import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageResponse;
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

import static com.salesforce.multicloudj.sts.curl.requestToCurl;


public class Main {

    static String provider = "gcp";

    public static void main(String[] args) {
        assumeRole();
        //assumeRoleWebIdentityCredentialsOverrider();
        //getCallerIdentity();
        //nativeAuthSignerUtilityWithStsCredentials();
        //nativeAuthSignerUtilityWithDefaultCredentials();
    }

    public static void assumeRole() {
        StsClient client = StsClient.builder(provider).withRegion("us-west-2").build();

        // Create a cloud-agnostic credential scope with condition
        CredentialScope.AvailabilityCondition condition = CredentialScope.AvailabilityCondition.builder()
                .resourcePrefix("storage://my-bucket/documents/")
                .title("Limit to documents folder")
                .description("Only allow access to objects in the documents folder")
                .build();

        CredentialScope.ScopeRule rule = CredentialScope.ScopeRule.builder()
                .availableResource("storage://my-bucket/*")
                .availablePermission("storage:GetObject")
                .availablePermission("storage:PutObject")
                .availabilityCondition(condition)
                .build();

        CredentialScope credentialScope = CredentialScope.builder()
                .rule(rule)
                .build();

        AssumedRoleRequest request = AssumedRoleRequest.newBuilder()
                .withRole("chameleon@substrate-sdk-gcp-poc1.iam.gserviceaccount.com")
                .withSessionName("my-session")
                .withCredentialScope(credentialScope)
                .build();
        StsCredentials stsCredentials = client.getAssumeRoleCredentials(request);

        System.out.println(stsCredentials.getAccessKeyId());
    }

    public static void assumeRoleWebIdentityCredentialsOverrider() {
        Supplier<String> tokenSupplier = () -> {
            StsClient clientGcp = StsClient.builder("gcp").build();
            CallerIdentity identity = clientGcp.getCallerIdentity(GetCallerIdentityRequest.builder().aud("multicloudj").build());
            return identity.getCloudResourceName();
        };

        CredentialsOverrider overrider = new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
                .withRole("arn:aws:iam::654654370895:role/chameleon-web")
                .withWebIdentityTokenSupplier(tokenSupplier)
                .build();
        BucketClient bucketClient = BucketClient.builder(provider)
                .withRegion("us-west-2").withBucket("chameleon-jclouds")
                .withCredentialsOverrider(overrider)
                .build();
        ListBlobsPageResponse r=bucketClient.listPage(ListBlobsPageRequest.builder().withMaxResults(1).build());
        System.out.println("s");
    }

    private static void getCallerIdentity() {
        StsClient client = StsClient.builder("gcp").withRegion("us-west-2").build();
        CallerIdentity identity = client.getCallerIdentity();
        StsClient client2 = StsClient.builder("aws").withRegion("us-west-2").build();
        StsCredentials credentials = client2.getAssumeRoleWithWebIdentityCredentials(AssumeRoleWebIdentityRequest.builder()
                .webIdentityToken(identity.getCloudResourceName()).role("arn:aws:iam::654654370895:role/chameleon-web").build());
        System.out.printf("\nAccountId: %s,UserId: %s,ResourceName: %s\n",
                identity.getAccountId(), identity.getUserId(), identity.getCloudResourceName(), credentials.getAccessKeyId());
    }

    public static void nativeAuthSignerUtilityWithStsCredentials() {
        String region = "us-west-2";
        String service = "sts";
        String apiName = "GetCallerIdentity";
        String apiVersion = "2011-06-15";

        StsClient client = StsClient.builder(provider).withRegion(region).build();
        AssumedRoleRequest request = AssumedRoleRequest.newBuilder()
                .withRole("arn:aws:iam::<account>:role/<role-name>")
                .withSessionName("my-session")
                .build();
        StsCredentials stsCredentials = client.getAssumeRoleCredentials(request);

        // create a sample bodypublisher
        HttpRequest.BodyPublisher body = HttpRequest.BodyPublishers.ofString("Action=" + apiName + "&Version=" + apiVersion);

        // the request object we want to sign
        HttpRequest fakeRequest = HttpRequest.newBuilder()
                .POST(body)
                .uri(URI.create("https://" + service + "." + region + ".amazonaws.com:443"))
                .build();
        CredentialsOverrider credsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(stsCredentials).build();
        StsUtilities stsUtil = StsUtilities.builder(provider)
                .withRegion(region)
                .withCredentialsOverrider(credsOverrider)
                .build();

        SignedAuthRequest newSignedAuthRequest = stsUtil.newCloudNativeAuthSignedRequest(fakeRequest);

        System.out.println("nativeAuthSignerUtilityWithStsCredentials curl request:\n" + requestToCurl(newSignedAuthRequest.getRequest()));
    }

    public static void nativeAuthSignerUtilityWithDefaultCredentials() {
        String region = "us-west-2";
        String service = "ec2";

        // create a sample bodypublisher
        HttpRequest.BodyPublisher body = HttpRequest.BodyPublishers.noBody();

        // the request object we want to sign
        HttpRequest fakeRequest = HttpRequest.newBuilder()
                .POST(body)
                .uri(URI.create("https://" + service + "." + region + ".amazonaws.com:443"))
                .build();
        StsUtilities stsUtil = StsUtilities.builder(provider)
                .withRegion(region)
                .build();
        SignedAuthRequest newSignedAuthRequest = stsUtil.newCloudNativeAuthSignedRequest(fakeRequest);

        System.out.println("nativeAuthSignerUtilityWithDefaultCredentials curl request:\n" + requestToCurl(newSignedAuthRequest.getRequest()));
    }
}