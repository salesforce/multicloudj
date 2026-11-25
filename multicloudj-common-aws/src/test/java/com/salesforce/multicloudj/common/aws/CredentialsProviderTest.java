package com.salesforce.multicloudj.common.aws;

import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

public class CredentialsProviderTest {

  @Test
  public void testAssumeRoleCredentialsProvider() {
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE).withRole("testRole").build();
    AwsCredentialsProvider awsCredsProvider =
        CredentialsProvider.getCredentialsProvider(credentialsOverrider, Region.AF_SOUTH_1);
    Assertions.assertNotNull(awsCredsProvider);
    Assertions.assertInstanceOf(StsAssumeRoleCredentialsProvider.class, awsCredsProvider);
  }

  @Test
  public void testBasicCredentialsProvider() {
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(new StsCredentials("a", "b", null))
            .build();
    AwsCredentialsProvider awsCredsProvider =
        CredentialsProvider.getCredentialsProvider(credentialsOverrider, Region.AF_SOUTH_1);
    Assertions.assertNotNull(awsCredsProvider);
    Assertions.assertInstanceOf(AwsBasicCredentials.class, awsCredsProvider.resolveCredentials());
  }

  @Test
  public void testAssumeRoleCredentialsProviderWithSessionName() {
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE)
            .withRole("testRole")
            .withSessionName("customSession")
            .withDurationSeconds(1)
            .build();
    AwsCredentialsProvider awsCredsProvider =
        CredentialsProvider.getCredentialsProvider(credentialsOverrider, Region.AF_SOUTH_1);
    Assertions.assertNotNull(awsCredsProvider);
    Assertions.assertInstanceOf(StsAssumeRoleCredentialsProvider.class, awsCredsProvider);
  }

  @Test
  public void testAssumeRoleWebIdentityCredentialsProvider() {
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
            .withRole("testRole")
            .withWebIdentityTokenSupplier(() -> "mockWebIdentityToken")
            .build();
    AwsCredentialsProvider awsCredsProvider =
        CredentialsProvider.getCredentialsProvider(credentialsOverrider, Region.AF_SOUTH_1);
    Assertions.assertNotNull(awsCredsProvider);
    Assertions.assertInstanceOf(RefreshingWebIdentityProvider.class, awsCredsProvider);
  }

  @Test
  public void testAssumeRoleWebIdentityFactoryLambdaInvocation() {
    // Create a spy to verify the token supplier is called by the lambda
    final boolean[] tokenSupplierCalled = {false};

    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
            .withRole("arn:aws:iam::123456789012:role/TestRole")
            .withWebIdentityTokenSupplier(
                () -> {
                  tokenSupplierCalled[0] = true;
                  return "mockToken123";
                })
            .withSessionName("testSession")
            .withDurationSeconds(3600)
            .build();

    AwsCredentialsProvider provider =
        CredentialsProvider.getCredentialsProvider(credentialsOverrider, Region.US_EAST_1);

    Assertions.assertNotNull(provider);
    Assertions.assertInstanceOf(RefreshingWebIdentityProvider.class, provider);

    // The token supplier should NOT have been called yet (lazy initialization)
    Assertions.assertFalse(
        tokenSupplierCalled[0],
        "Token supplier should not be called until credentials are resolved");

    // Now invoke the lambda by attempting to resolve credentials
    // This will fail because we don't have real AWS credentials, but it proves the lambda is
    // invoked
    try {
      provider.resolveCredentials();
    } catch (Exception e) {
      // Expected to fail, but the token supplier should have been called by the lambda
    }

    // Verify the lambda was invoked and called the token supplier
    Assertions.assertTrue(
        tokenSupplierCalled[0], "Token supplier should have been called when lambda was invoked");
  }

  @Test
  public void testStaticCredentialsProvider() {
    StsCredentials stsCredentials = new StsCredentials("key", "secret", "token");
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(stsCredentials)
            .build();
    AwsCredentialsProvider awsCredsProvider =
        CredentialsProvider.getCredentialsProvider(credentialsOverrider, Region.AF_SOUTH_1);
    Assertions.assertNotNull(awsCredsProvider);
    Assertions.assertInstanceOf(StaticCredentialsProvider.class, awsCredsProvider);
  }

  @Test
  public void testFixedSessionCredentialsWithATokenStayStatic() {
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(new StsCredentials("key", "secret", "token"))
            .build();

    AwsCredentialsProvider awsCredsProvider =
        CredentialsProvider.getCredentialsProvider(credentialsOverrider, Region.AF_SOUTH_1);

    Assertions.assertInstanceOf(StaticCredentialsProvider.class, awsCredsProvider);
    AwsSessionCredentials resolved =
        Assertions.assertInstanceOf(
            AwsSessionCredentials.class, awsCredsProvider.resolveCredentials());
    Assertions.assertEquals("key", resolved.accessKeyId());
    Assertions.assertEquals("secret", resolved.secretAccessKey());
    Assertions.assertEquals("token", resolved.sessionToken());
  }

  @Test
  public void testSessionWithASupplierUsesTheRefreshableProvider() {
    AtomicInteger supplierCalls = new AtomicInteger();
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentialsSupplier(
                () -> {
                  supplierCalls.incrementAndGet();
                  return new StsCredentials("key", "secret", "token");
                })
            .withRefreshInterval(Duration.ofHours(1))
            .build();

    AwsCredentialsProvider awsCredsProvider =
        CredentialsProvider.getCredentialsProvider(credentialsOverrider, Region.AF_SOUTH_1);

    Assertions.assertInstanceOf(RefreshableSessionCredentialsProvider.class, awsCredsProvider);
    Assertions.assertEquals(0, supplierCalls.get());
    Assertions.assertEquals("key", awsCredsProvider.resolveCredentials().accessKeyId());
    Assertions.assertEquals(1, supplierCalls.get());
  }

  @Test
  public void testSessionOverriderWithASupplierSharesOneRefresherAcrossProviders() {
    AtomicInteger supplierCalls = new AtomicInteger();
    CredentialsOverrider credentialsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentialsSupplier(
                () -> {
                  supplierCalls.incrementAndGet();
                  return new StsCredentials("key", "secret", "token");
                })
            .withRefreshInterval(Duration.ofHours(1))
            .build();

    AwsCredentialsProvider first =
        CredentialsProvider.getCredentialsProvider(credentialsOverrider, Region.AF_SOUTH_1);
    AwsCredentialsProvider second =
        CredentialsProvider.getCredentialsProvider(credentialsOverrider, Region.US_EAST_1);
    first.resolveCredentials();
    second.resolveCredentials();

    Assertions.assertEquals(1, supplierCalls.get());
  }

  @Test
  public void testNullOverriderAndNullTypeStillReturnNull() {
    Assertions.assertNull(CredentialsProvider.getCredentialsProvider(null, Region.AF_SOUTH_1));
    Assertions.assertNull(
        CredentialsProvider.getCredentialsProvider(
            new CredentialsOverrider.Builder(null).build(), Region.AF_SOUTH_1));
  }
}
