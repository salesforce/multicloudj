package com.salesforce.multicloudj.sts;

import com.salesforce.multicloudj.sts.client.StsClient;
import com.salesforce.multicloudj.sts.client.StsUtilities;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;

import java.net.URI;
import java.net.http.HttpRequest;

import static com.salesforce.multicloudj.sts.curl.requestToCurl;


public class Main {

    static String provider = "aws";

    public static void main(String[] args) {
        assumeRole();
        getCallerIdentity();
        nativeAuthSignerUtilityWithStsCredentials();
        nativeAuthSignerUtilityWithDefaultCredentials();
    }

    public static void assumeRole() {
        StsClient client = StsClient.builder(provider).withRegion("us-west-2").build();
        AssumedRoleRequest request = AssumedRoleRequest.newBuilder()
                .withRole("arn:aws:iam::<account>:role/<role-name>")
                .withSessionName("my-session")
                .build();
        StsCredentials stsCredentials = client.getAssumeRoleCredentials(request);

        System.out.println(stsCredentials.getAccessKeyId());
    }

    public static void getCallerIdentity() {
        StsClient client = StsClient.builder(provider).withRegion("us-west-2").build();
        CallerIdentity identity = client.getCallerIdentity();

        System.out.printf("\nAccountId: %s,UserId: %s,ResourceName: %s\n",
                identity.getAccountId(), identity.getUserId(), identity.getCloudResourceName());
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