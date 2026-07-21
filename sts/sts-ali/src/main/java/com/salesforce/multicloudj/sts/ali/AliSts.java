package com.salesforce.multicloudj.sts.ali;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.auth.BasicSessionCredentials;
import com.aliyuncs.auth.DefaultCredentialsProvider;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.http.HttpClientConfig;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.sts.model.v20150401.AssumeRoleRequest;
import com.aliyuncs.sts.model.v20150401.AssumeRoleResponse;
import com.aliyuncs.sts.model.v20150401.AssumeRoleWithOIDCRequest;
import com.aliyuncs.sts.model.v20150401.AssumeRoleWithOIDCResponse;
import com.aliyuncs.sts.model.v20150401.GetCallerIdentityRequest;
import com.aliyuncs.sts.model.v20150401.GetCallerIdentityResponse;
import com.aliyuncs.utils.EnvironmentUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.driver.AbstractSts;
import com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.CredentialScope;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@AutoService(AbstractSts.class)
public class AliSts extends AbstractSts {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private IAcsClient stsClient;

  public AliSts(Builder builder) {
    super(builder);

    DefaultProfile clientProfile;
    if (builder.getEndpoint() != null) {
      clientProfile = getClientProfile(builder.getEndpoint());
    } else {
      clientProfile = DefaultProfile.getProfile(builder.getRegion());
    }

    // Configure proxy if any proxy settings are provided
    if (builder.getProxyEndpoint() != null
        || builder.getUseSystemPropertyProxyValues() != null
        || builder.getUseEnvironmentVariableProxyValues() != null) {
      HttpClientConfig httpClientConfig = buildHttpClientConfig(builder);
      clientProfile.setHttpClientConfig(httpClientConfig);
    }

    // Workaround for SDK limitation: When environment variables should be disabled,
    // set EnvironmentUtils to empty strings to prevent auto-detection during requests
    if (Boolean.FALSE.equals(builder.getUseEnvironmentVariableProxyValues())) {
      EnvironmentUtils.setHttpProxy("");
      EnvironmentUtils.setHttpsProxy("");
      EnvironmentUtils.setNoProxy("");
    }

    this.stsClient = new DefaultAcsClient(clientProfile);
  }

  public AliSts(Builder builder, IAcsClient stsClient) {
    super(builder);
    this.stsClient = stsClient;
  }

  public AliSts() {
    super(new Builder());
  }

  // Alibaba STS client doesn't directly support the endpoints override but
  // have the chained endpoint resolver which resolved endpoint based on the
  // IClientProfile.
  static DefaultProfile getClientProfile(URI endpoint) {
    DefaultProfile profile;
    // Regex pattern to match the first part and the region part
    // <product>.<region>.aliyuncs.com
    // product can be sts or sts-vpc
    String regex = "^([^.]+)\\.([^.]+)\\.([^.]+)\\.([^.]+)$";
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(endpoint.getHost());

    if (matcher.matches()) {
      String product = matcher.group(1);
      String region = matcher.group(2);
      profile = DefaultProfile.getProfile(region);
      if (product.endsWith("-vpc")) {
        profile.enableUsingVpcEndpoint();
      }
    } else {
      throw new InvalidArgumentException("The endpoint is invalid");
    }

    return profile;
  }

  /**
   * Builds HttpClientConfig with proxy configuration.
   *
   * <p><b>SDK Limitation:</b> The Alibaba SDK (aliyun-java-sdk-core 4.7.2) does not provide API
   * methods to control proxy auto-detection from system properties or environment variables. The
   * SDK automatically reads:
   *
   * <ul>
   *   <li>Environment variables: HTTP_PROXY, HTTPS_PROXY, NO_PROXY (via EnvironmentUtils)
   *   <li>System properties: NOT read by Alibaba SDK (does not call useSystemProperties())
   * </ul>
   *
   * <p><b>Behavior by Configuration:</b>
   *
   * <ul>
   *   <li>Explicit proxyEndpoint: Used for both HTTP and HTTPS, overrides all auto-detection
   *   <li>useSystemPropertyProxyValues=false: HONORED (no-op, SDK does not read sys props anyway)
   *   <li>useEnvironmentVariableProxyValues=false: WORKAROUND - Sets EnvironmentUtils to empty
   *       strings to prevent env var reading
   *   <li>Default (all null): SDK automatically reads environment variables
   * </ul>
   *
   * <p><b>Workaround Implementation:</b> When useEnvironmentVariableProxyValues=false, the
   * constructor calls EnvironmentUtils.setHttpProxy("") to override the SDK's environment variable
   * reading. This is not a true disable (SDK limitation), but prevents proxy usage in practice.
   *
   * @param builder The builder containing proxy configuration
   * @return Configured HttpClientConfig
   */
  static HttpClientConfig buildHttpClientConfig(Builder builder) {
    HttpClientConfig clientConfig = HttpClientConfig.getDefault();

    // Priority 1: Explicit proxy endpoint (overrides system properties and env vars)
    if (builder.getProxyEndpoint() != null) {
      String proxyUrl = builder.getProxyEndpoint().toString();
      // Set both HTTP and HTTPS proxy to the same endpoint
      // Alibaba SDK determines which to use based on the target endpoint protocol
      clientConfig.setHttpProxy(proxyUrl);
      clientConfig.setHttpsProxy(proxyUrl);
    }

    // Priority 2 & 3: System properties and environment variables
    // System properties: NOT read by Alibaba SDK (no useSystemProperties() call)
    // Environment variables: Handled by EnvironmentUtils workaround in constructor
    //   - When useEnvironmentVariableProxyValues=false, constructor sets EnvironmentUtils to ""
    //   - When null/true, SDK automatically reads HTTP_PROXY/HTTPS_PROXY via EnvironmentUtils

    return clientConfig;
  }

  @Override
  public Builder builder() {
    return new Builder();
  }

  @Override
  protected StsCredentials getSTSCredentialsWithAssumeRole(AssumedRoleRequest request) {
    AssumeRoleRequest roleRequest = new AssumeRoleRequest();
    roleRequest.setRoleArn(request.getRole());
    roleRequest.setRoleSessionName(request.getSessionName());
    if (request.getExpiration() > 0) {
      roleRequest.setDurationSeconds((long) request.getExpiration());
    }
    // If a credential scope is supplied, downscope the assumed role with an inline RAM policy.
    if (request.getCredentialScope() != null) {
      roleRequest.setPolicy(convertToRamPolicy(request.getCredentialScope()));
    }

    try {
      AssumeRoleResponse response = stsClient.getAcsResponse(roleRequest);
      AssumeRoleResponse.Credentials credentials = response.getCredentials();
      return new StsCredentials(
          credentials.getAccessKeyId(),
          credentials.getAccessKeySecret(),
          credentials.getSecurityToken());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Translates a cloud-agnostic {@link CredentialScope} into an Alibaba RAM policy document (JSON)
   * suitable for the AssumeRole {@code Policy} parameter.
   *
   * <p>Each scope rule becomes up to two RAM statements, because OSS restricts object access and
   * list access differently:
   *
   * <ul>
   *   <li><b>Object operations</b> ({@code oss:GetObject}/{@code PutObject}/{@code DeleteObject})
   *       are restricted to a key prefix by encoding the prefix directly in the resource ARN
   *       ({@code acs:oss:*:*:bucket/prefix/*}).
   *   <li><b>List</b> ({@code oss:ListObjects}) must target the bucket resource
   *       ({@code acs:oss:*:*:bucket}); the prefix is enforced with an {@code oss:Prefix} condition
   *       because OSS evaluates the listable scope from the request prefix parameter, not the ARN.
   * </ul>
   *
   * <p>Region and account fields use the {@code *} wildcard: the bucket name is the meaningful
   * selector for OSS, and the policy is intersected with an already-assumed role for that account.
   */
  private String convertToRamPolicy(CredentialScope credentialScope) {
    List<Map<String, Object>> statements = new ArrayList<>();
    for (CredentialScope.ScopeRule rule : credentialScope.getRules()) {
      String bucket = extractBucket(rule.getAvailableResource());
      String prefix = extractPrefix(rule.getAvailabilityCondition(), bucket);

      List<String> objectActions = new ArrayList<>();
      List<String> listActions = new ArrayList<>();
      for (String permission : rule.getAvailablePermissions()) {
        String action = toOssAction(permission);
        if ("oss:ListObjects".equals(action)) {
          listActions.add(action);
        } else {
          objectActions.add(action);
        }
      }

      if (!objectActions.isEmpty()) {
        Map<String, Object> statement = new LinkedHashMap<>();
        statement.put("Effect", "Allow");
        statement.put("Action", objectActions);
        statement.put("Resource", objectResourceArn(bucket, prefix));
        statements.add(statement);
      }

      if (!listActions.isEmpty()) {
        Map<String, Object> statement = new LinkedHashMap<>();
        statement.put("Effect", "Allow");
        statement.put("Action", listActions);
        statement.put("Resource", "acs:oss:*:*:" + bucket);
        if (prefix != null && !prefix.isEmpty()) {
          Map<String, Object> stringLike = new LinkedHashMap<>();
          stringLike.put("oss:Prefix", List.of(prefix, prefix + "*"));
          Map<String, Object> condition = new LinkedHashMap<>();
          condition.put("StringLike", stringLike);
          statement.put("Condition", condition);
        }
        statements.add(statement);
      }
    }

    // Throw on a scope with no statements so the caller gets an actionable error instead of an
    // opaque AccessDenied: OSS treats an empty inline policy as deny-all.
    if (statements.isEmpty()) {
      throw new InvalidArgumentException(
          "credential scope produced no RAM statements; supply at least one rule with a resource "
              + "and permissions");
    }

    Map<String, Object> policy = new LinkedHashMap<>();
    policy.put("Version", "1");
    policy.put("Statement", statements);
    return toJsonString(policy);
  }

  // Extracts the bucket name from a cloud-agnostic resource, e.g. "storage://my-bucket" or
  // "storage://my-bucket/*" -> "my-bucket".
  private String extractBucket(String availableResource) {
    String remainder = availableResource.substring("storage://".length());
    int slash = remainder.indexOf('/');
    return slash >= 0 ? remainder.substring(0, slash) : remainder;
  }

  // Extracts the object-key prefix from the availability condition, relative to the bucket, e.g.
  // "storage://my-bucket/documents/" -> "documents/". Returns null when no prefix is scoped (no
  // condition or no resourcePrefix). A resourcePrefix that IS present but does not name this rule's
  // bucket is rejected rather than ignored: silently returning null would widen the grant to the
  // whole bucket (the unsafe direction), defeating the downscoping.
  private String extractPrefix(CredentialScope.AvailabilityCondition condition, String bucket) {
    if (condition == null || condition.getResourcePrefix() == null) {
      return null;
    }
    String path = condition.getResourcePrefix().substring("storage://".length());
    String bucketSegment = bucket + "/";
    if (!path.startsWith(bucketSegment)) {
      throw new InvalidArgumentException(
          "credential scope resourcePrefix must be under the rule's bucket 'storage://"
              + bucket
              + "/'. Found: "
              + condition.getResourcePrefix());
    }
    return path.substring(bucketSegment.length());
  }

  // Builds the OSS object resource ARN, encoding the key prefix when one is scoped:
  // "acs:oss:*:*:bucket/prefix/*", or "acs:oss:*:*:bucket/*" for the whole bucket.
  private String objectResourceArn(String bucket, String prefix) {
    if (prefix != null && !prefix.isEmpty()) {
      return "acs:oss:*:*:" + bucket + "/" + prefix + "*";
    }
    return "acs:oss:*:*:" + bucket + "/*";
  }

  // Maps a cloud-agnostic "storage:<Action>" permission to its OSS action. Object actions map by
  // suffix (GetObject/PutObject/DeleteObject -> oss:GetObject/...); the list action is renamed
  // (ListBucket -> oss:ListObjects, the RAM action behind the OSS GetBucket API).
  private String toOssAction(String permission) {
    String action = permission.substring("storage:".length());
    if ("ListBucket".equals(action)) {
      return "oss:ListObjects";
    }
    return "oss:" + action;
  }

  private String toJsonString(Map<String, Object> map) {
    try {
      return OBJECT_MAPPER.writeValueAsString(map);
    } catch (JsonProcessingException e) {
      throw new InvalidArgumentException("scoped credentials is not in right format", e);
    }
  }

  @Override
  protected CallerIdentity getCallerIdentityFromProvider(
      com.salesforce.multicloudj.sts.model.GetCallerIdentityRequest request) {
    GetCallerIdentityRequest callerIdentityRequest = new GetCallerIdentityRequest();
    try {
      GetCallerIdentityResponse response = stsClient.getAcsResponse(callerIdentityRequest);
      return new CallerIdentity(
          response.getPrincipalId(), response.getArn(), response.getAccountId());
    } catch (ClientException e) {
      throw new RuntimeException(e);
    }
  }

  /*
   * TODO: The correct implementation of GetAccessToken is to use the acs
   * request GenerateSessionAccessKey. This is only available to japan region
   * so far. We should follow up with alibaba if this feature will be
   * available to cn-shanghai.
   */
  @Override
  protected StsCredentials getAccessTokenFromProvider(GetAccessTokenRequest request) {
    try {

      DefaultCredentialsProvider credentialsProvider = new DefaultCredentialsProvider();
      BasicSessionCredentials credentials =
          (BasicSessionCredentials) credentialsProvider.getCredentials();
      return new StsCredentials(
          credentials.getAccessKeyId(),
          credentials.getAccessKeySecret(),
          credentials.getSessionToken());
    } catch (ClientException e) {
      throw new RuntimeException(e);
    }
  }

  // Env var the Aliyun SDK's own OIDCCredentialsProvider reads for the OIDC provider ARN when one
  // is not supplied explicitly; mirrored here so callers can configure it out-of-band.
  static final String OIDC_PROVIDER_ARN_ENV = "ALIBABA_CLOUD_OIDC_PROVIDER_ARN";

  @Override
  protected StsCredentials getSTSCredentialsWithAssumeRoleWebIdentity(
      AssumeRoleWebIdentityRequest request) {
    // AssumeRoleWithOIDC requires the OIDC provider ARN to be named explicitly on the request; it
    // cannot be derived from the token. Take it from the request, else fall back to the env var the
    // Aliyun SDK itself uses, else reject.
    String providerArn = request.getWebIdentityProviderArn();
    if (providerArn == null || providerArn.isEmpty()) {
      providerArn = System.getenv(OIDC_PROVIDER_ARN_ENV);
    }
    if (providerArn == null || providerArn.isEmpty()) {
      throw new InvalidArgumentException(
          "webIdentityProviderArn is required for AssumeRoleWithOIDC; set it on the request or via "
              + "the " + OIDC_PROVIDER_ARN_ENV + " environment variable");
    }

    AssumeRoleWithOIDCRequest oidcRequest = new AssumeRoleWithOIDCRequest();
    oidcRequest.setRoleArn(request.getRole());
    oidcRequest.setOIDCProviderArn(providerArn);
    oidcRequest.setOIDCToken(request.getWebIdentityToken());
    oidcRequest.setRoleSessionName(request.getSessionName());
    if (request.getExpiration() > 0) {
      oidcRequest.setDurationSeconds((long) request.getExpiration());
    }

    try {
      AssumeRoleWithOIDCResponse response = stsClient.getAcsResponse(oidcRequest);
      AssumeRoleWithOIDCResponse.Credentials credentials = response.getCredentials();
      return new StsCredentials(
          credentials.getAccessKeyId(),
          credentials.getAccessKeySecret(),
          credentials.getSecurityToken());
    } catch (ClientException e) {
      throw mapException(e);
    }
  }

  @Override
  public SubstrateSdkException mapException(Throwable t) {
    Class<? extends SubstrateSdkException> exceptionClass = UnknownException.class;
    // ali-yun has a wierd chain where ServerException extends the ClientException
    if (t instanceof ClientException) {
      String errorCode = ((ClientException) t).getErrCode();
      exceptionClass = ERROR_MAPPING.getOrDefault(errorCode, UnknownException.class);
    }
    // aliyuncs ClientException has no status/retry signal; rely on type-default retryability.
    return ExceptionHandler.build(exceptionClass, t, null);
  }

  // The common error codes as source of truth is here:
  // https://docs.ali.amazon.com/STS/latest/APIReference/CommonErrors.html
  private static final Map<String, Class<? extends SubstrateSdkException>> ERROR_MAPPING =
      Map.of(
          "SDK.InvalidAccessKeySecret", InvalidArgumentException.class,
          "InvalidSecurityToken.MismatchWithAccessKey", InvalidArgumentException.class,
          "InvalidSecurityToken.Malformed", InvalidArgumentException.class,
          "InvalidAccessKeyId.NotFound", InvalidArgumentException.class,
          "SignatureNotMatch", UnAuthorizedException.class,
          "Unauthorized", UnAuthorizedException.class,
          "InternalServerError", UnknownException.class,
          "ServerBusy", UnknownException.class,
          "MissingRoleSessionName", InvalidArgumentException.class
          // Add more mappings as needed
          );

  public static class Builder extends AbstractSts.Builder<AliSts, Builder> {
    protected Builder() {
      providerId("ali");
    }

    @Override
    public Builder self() {
      return this;
    }

    @Override
    public AliSts build() {
      return new AliSts(this);
    }

    public AliSts build(DefaultAcsClient stsClient) {
      return new AliSts(this, stsClient);
    }
  }
}
