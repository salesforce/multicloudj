package com.salesforce.multicloudj.sts;

import com.salesforce.multicloudj.sts.client.StsUtilities;
import com.salesforce.multicloudj.sts.client.StsVerifier;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.SignOptions;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import com.salesforce.multicloudj.sts.model.ValidateOptions;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * End-to-end Cloud Native Auth (CNA) example.
 *
 * <p>CNA lets a workload prove <em>its own</em> cloud identity to another service without any
 * shared secret. The flow has two halves:
 *
 * <ol>
 *   <li><b>Sign</b> (client side): {@link StsUtilities} signs the caller's cloud credentials into a
 *       portable, self-contained {@code signedIdentity} string. The client sends this string to the
 *       server as its proof of identity.
 *   <li><b>Validate</b> (server side): {@link StsVerifier} takes the {@code signedIdentity},
 *       proves it against the substrate, and returns the verified {@link CallerIdentity} — the
 *       caller's cloud resource name, user id, and account.
 * </ol>
 *
 * <p>Optional custom headers are signed by the client and asserted by the verifier, so the server
 * can bind additional context (a request source, a tenant id, a federation target, ...) into the
 * signed proof and reject anything that does not match.
 *
 * <p>Run this against real credentials for {@code PROVIDER}/{@code REGION}. For {@code aws} the
 * verifier replays the presigned {@code GetCallerIdentity} request against AWS STS; the default
 * credential chain must resolve. Custom header names are lowercase because AWS SigV4 signs
 * canonical (lowercase) header names.
 */
public class CloudNativeAuth {

  private static final String PROVIDER = "aws";
  private static final String REGION = "us-east-2";

  public static void main(String[] args) {
    // Custom headers the client signs into its identity and the server asserts on validation.
    Map<String, String> customHeaders = new LinkedHashMap<>();
    customHeaders.put("x-request-source", "cloud-native-auth-example");
    customHeaders.put("x-tenant-id", "tenant-42");

    // ── Sign (client side) ──────────────────────────────────────────────
    //
    // StsUtilities produces a portable signedIdentity from the caller's own cloud credentials.
    StsUtilities signer = StsUtilities.builder(PROVIDER).withRegion(REGION).build();

    SignOptions signOptions = SignOptions.builder().withCustomHeaders(customHeaders).build();

    // No service request to hash here, so a bare identity assertion is signed.
    SignedAuthRequest signed = signer.newCloudNativeAuthSignedRequest(null, signOptions);
    String signedIdentity = signed.getSignedIdentity();

    System.out.println("Signed auth request created successfully");
    System.out.println("  SignedIdentity: " + preview(signedIdentity));

    // ── Validate (server side) ──────────────────────────────────────────
    //
    // StsVerifier proves the signedIdentity and returns who the caller is.
    StsVerifier verifier = StsVerifier.builder(PROVIDER).withRegion(REGION).build();

    ValidateOptions validateOptions =
        ValidateOptions.builder().withExpectedCustomHeaders(customHeaders).build();

    CallerIdentity identity = verifier.validateSignedAuthRequest(signedIdentity, validateOptions);

    System.out.println();
    System.out.println("Validation succeeded");
    System.out.println("  CloudResourceName: " + identity.getCloudResourceName());
    System.out.println("  UserId           : " + identity.getUserId());
    System.out.println("  Account          : " + identity.getAccountId());
  }

  /** Shortens the signed identity for display; the full string can be several hundred bytes. */
  private static String preview(String value) {
    if (value == null) {
      return "(none)";
    }
    return value.length() <= 50 ? value : value.substring(0, 50) + "...";
  }
}
