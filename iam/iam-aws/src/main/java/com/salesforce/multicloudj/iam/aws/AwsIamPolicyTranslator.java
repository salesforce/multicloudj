package com.salesforce.multicloudj.iam.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.iam.model.Action;
import com.salesforce.multicloudj.iam.model.ComputeActions;
import com.salesforce.multicloudj.iam.model.ConditionOperator;
import com.salesforce.multicloudj.iam.model.IamActions;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.Statement;
import com.salesforce.multicloudj.iam.model.StorageActions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/**
 * Translates substrate-neutral PolicyDocument to AWS IAM policy format.
 *
 * <p>This translator converts substrate-neutral actions, resources, and conditions to AWS-specific
 * IAM policy JSON format according to the translation rules defined in PolicyDocument
 * documentation.
 *
 * <p>Translation rules:
 *
 * <ul>
 *   <li>Actions: storage:GetObject → s3:GetObject, compute:CreateInstance → ec2:RunInstances
 *   <li>Resources: storage://bucket-name/* → arn:aws:s3:::bucket-name/*
 *   <li>Conditions: stringEquals → StringEquals (capitalize first letter)
 *   <li>Principals: Wrap in {"AWS": "principal"} or {"Service": "principal"}
 * </ul>
 */
public class AwsIamPolicyTranslator {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String SERVICE_PRINCIPAL_SUFFIX = ".amazonaws.com";

  // Action mappings: substrate-neutral → AWS
  private static final Map<Action, String> ACTION_MAPPINGS =
      Map.ofEntries(
          // Storage actions
          Map.entry(StorageActions.GET_OBJECT, "s3:GetObject"),
          Map.entry(StorageActions.PUT_OBJECT, "s3:PutObject"),
          Map.entry(StorageActions.DELETE_OBJECT, "s3:DeleteObject"),
          Map.entry(StorageActions.LIST_BUCKET, "s3:ListBucket"),
          Map.entry(StorageActions.GET_BUCKET_LOCATION, "s3:GetBucketLocation"),
          Map.entry(StorageActions.CREATE_BUCKET, "s3:CreateBucket"),
          Map.entry(StorageActions.DELETE_BUCKET, "s3:DeleteBucket"),

          // Compute actions
          Map.entry(ComputeActions.CREATE_INSTANCE, "ec2:RunInstances"),
          Map.entry(ComputeActions.DELETE_INSTANCE, "ec2:TerminateInstances"),
          Map.entry(ComputeActions.START_INSTANCE, "ec2:StartInstances"),
          Map.entry(ComputeActions.STOP_INSTANCE, "ec2:StopInstances"),
          Map.entry(ComputeActions.DESCRIBE_INSTANCES, "ec2:DescribeInstances"),
          Map.entry(ComputeActions.GET_INSTANCE, "ec2:DescribeInstances"),

          // IAM actions
          Map.entry(IamActions.ASSUME_ROLE, "sts:AssumeRole"),
          Map.entry(IamActions.CREATE_ROLE, "iam:CreateRole"),
          Map.entry(IamActions.DELETE_ROLE, "iam:DeleteRole"),
          Map.entry(IamActions.GET_ROLE, "iam:GetRole"),
          Map.entry(IamActions.ATTACH_ROLE_POLICY, "iam:AttachRolePolicy"),
          Map.entry(IamActions.DETACH_ROLE_POLICY, "iam:DetachRolePolicy"),
          Map.entry(IamActions.PUT_ROLE_POLICY, "iam:PutRolePolicy"),
          Map.entry(IamActions.GET_ROLE_POLICY, "iam:GetRolePolicy"));

  // Condition operator mappings: substrate-neutral → AWS
  private static final Map<ConditionOperator, String> CONDITION_MAPPINGS =
      Map.ofEntries(
          Map.entry(ConditionOperator.STRING_EQUALS, "StringEquals"),
          Map.entry(ConditionOperator.STRING_NOT_EQUALS, "StringNotEquals"),
          Map.entry(ConditionOperator.STRING_LIKE, "StringLike"),
          Map.entry(ConditionOperator.STRING_NOT_LIKE, "StringNotLike"),
          Map.entry(ConditionOperator.NUMERIC_EQUALS, "NumericEquals"),
          Map.entry(ConditionOperator.NUMERIC_NOT_EQUALS, "NumericNotEquals"),
          Map.entry(ConditionOperator.NUMERIC_LESS_THAN, "NumericLessThan"),
          Map.entry(ConditionOperator.NUMERIC_LESS_THAN_EQUALS, "NumericLessThanEquals"),
          Map.entry(ConditionOperator.NUMERIC_GREATER_THAN, "NumericGreaterThan"),
          Map.entry(ConditionOperator.NUMERIC_GREATER_THAN_EQUALS, "NumericGreaterThanEquals"),
          Map.entry(ConditionOperator.DATE_EQUALS, "DateEquals"),
          Map.entry(ConditionOperator.DATE_NOT_EQUALS, "DateNotEquals"),
          Map.entry(ConditionOperator.DATE_LESS_THAN, "DateLessThan"),
          Map.entry(ConditionOperator.DATE_LESS_THAN_EQUALS, "DateLessThanEquals"),
          Map.entry(ConditionOperator.DATE_GREATER_THAN, "DateGreaterThan"),
          Map.entry(ConditionOperator.DATE_GREATER_THAN_EQUALS, "DateGreaterThanEquals"),
          Map.entry(ConditionOperator.BOOL, "Bool"),
          Map.entry(ConditionOperator.IP_ADDRESS, "IpAddress"),
          Map.entry(ConditionOperator.NOT_IP_ADDRESS, "NotIpAddress"));
  public static final String DEFAULT_VERSION = "2012-10-17";

  /**
   * Translates a substrate-neutral PolicyDocument to AWS IAM policy JSON string.
   *
   * @param policyDocument the substrate-neutral policy document
   * @return AWS IAM policy JSON string
   * @throws SubstrateSdkException if translation fails
   */
  public static String translateToAwsPolicy(PolicyDocument policyDocument) {
    Map<String, Object> awsPolicy = new LinkedHashMap<>();
    // Default to AWS IAM policy version if not provided
    String version =
        StringUtils.isNotBlank(policyDocument.getVersion())
            ? policyDocument.getVersion()
            : DEFAULT_VERSION;
    awsPolicy.put("Version", version);

    List<Map<String, Object>> awsStatements = new ArrayList<>();
    for (Statement statement : policyDocument.getStatements()) {
      awsStatements.add(translateStatement(statement));
    }
    awsPolicy.put("Statement", awsStatements);

    try {
      return OBJECT_MAPPER.writeValueAsString(awsPolicy);
    } catch (JsonProcessingException e) {
      throw new SubstrateSdkException("Failed to serialize AWS IAM policy to JSON", e);
    }
  }

  /**
   * Translates a single statement from substrate-neutral to AWS format.
   *
   * @param statement the substrate-neutral statement
   * @return AWS IAM statement as a map
   * @throws SubstrateSdkException if translation fails
   */
  private static Map<String, Object> translateStatement(Statement statement) {
    if (statement.getEffect() == null) {
      throw new InvalidArgumentException("Effect is required for AWS IAM policy statement");
    }

    Map<String, Object> awsStatement = new LinkedHashMap<>();

    // Add Sid if present
    String sid = statement.getSid();
    if (StringUtils.isNotBlank(sid)) {
      awsStatement.put("Sid", sid);
    }

    // Add Effect
    awsStatement.put("Effect", statement.getEffect().getValue());

    // Translate and add Principals if present
    List<String> principals = statement.getPrincipals();
    if (principals != null && !principals.isEmpty()) {
      awsStatement.put("Principal", translatePrincipals(principals));
    }

    // Translate and add Actions
    List<String> awsActions =
        statement.getActions().stream()
            .map(AwsIamPolicyTranslator::translateAction)
            .collect(Collectors.toList());

    if (awsActions.size() == 1) {
      awsStatement.put("Action", awsActions.get(0));
    } else {
      awsStatement.put("Action", awsActions);
    }

    // Translate and add Resources if present
    if (statement.getResources() != null && !statement.getResources().isEmpty()) {
      List<String> awsResources =
          statement.getResources().stream()
              .map(AwsIamPolicyTranslator::translateResource)
              .collect(Collectors.toList());

      if (awsResources.size() == 1) {
        awsStatement.put("Resource", awsResources.get(0));
      } else {
        awsStatement.put("Resource", awsResources);
      }
    }

    // Translate and add Conditions if present
    if (statement.getConditions() != null && !statement.getConditions().isEmpty()) {
      awsStatement.put("Condition", translateConditions(statement.getConditions()));
    }

    return awsStatement;
  }

  /**
   * Translates substrate-neutral action to AWS action. Supports wildcard actions like storage:*,
   * compute:*, iam:*.
   *
   * @param action the substrate-neutral action
   * @return AWS action
   * @throws SubstrateSdkException if action is unknown
   */
  private static String translateAction(Action action) {
    // Handle wildcard actions (e.g., storage:*, compute:*, iam:*)
    if (action.isWildcard()) {
      String service = action.getService();
      switch (service) {
        case "storage":
          return "s3:*";
        case "compute":
          return "ec2:*";
        case "iam":
          return "iam:*";
        default:
          throw new InvalidArgumentException(
              "Unknown substrate-neutral service for wildcard action: "
                  + action.toActionString()
                  + ". "
                  + "Supported wildcard services: storage:*, compute:*, iam:*");
      }
    }

    // Handle specific actions
    String awsAction = ACTION_MAPPINGS.get(action);
    if (awsAction == null) {
      throw new InvalidArgumentException(
          "Unknown substrate-neutral action: "
              + action.toActionString()
              + ". "
              + "Supported actions: "
              + ACTION_MAPPINGS.keySet().stream()
                  .map(Action::toActionString)
                  .collect(Collectors.joining(", "))
              + ", or wildcard actions: storage:*, compute:*, iam:*");
    }
    return awsAction;
  }

  /**
   * Translates substrate-neutral resource URI to AWS ARN.
   *
   * @param resource the substrate-neutral resource URI
   * @return AWS ARN
   * @throws SubstrateSdkException if resource format is invalid
   */
  private static String translateResource(String resource) {
    // Handle wildcard
    if ("*".equals(resource)) {
      return "*";
    }

    // Handle storage:// URIs
    if (resource.startsWith("storage://")) {
      String path = resource.substring("storage://".length());
      return "arn:aws:s3:::" + path;
    }

    // If already an ARN, return as-is
    if (resource.startsWith("arn:")) {
      return resource;
    }

    throw new InvalidArgumentException(
        "Unknown resource format: "
            + resource
            + ". "
            + "Supported formats: storage://bucket/key, arn:aws:..., or *");
  }

  /**
   * Translates substrate-neutral principals to AWS principal format.
   *
   * @param principals list of substrate-neutral principals
   * @return AWS principal object
   */
  private static Map<String, Object> translatePrincipals(List<String> principals) {
    Map<String, Object> principalMap = new LinkedHashMap<>();
    List<String> awsPrincipals = new ArrayList<>();
    List<String> servicePrincipals = new ArrayList<>();

    for (String principal : principals) {
      if (principal.endsWith(SERVICE_PRINCIPAL_SUFFIX)) {
        servicePrincipals.add(principal);
      } else {
        awsPrincipals.add(principal);
      }
    }

    if (!awsPrincipals.isEmpty()) {
      principalMap.put("AWS", awsPrincipals.size() == 1 ? awsPrincipals.get(0) : awsPrincipals);
    }
    if (!servicePrincipals.isEmpty()) {
      principalMap.put(
          "Service", servicePrincipals.size() == 1 ? servicePrincipals.get(0) : servicePrincipals);
    }

    return principalMap;
  }

  /**
   * Translates substrate-neutral conditions to AWS condition format.
   *
   * @param conditions substrate-neutral conditions
   * @return AWS conditions
   * @throws SubstrateSdkException if condition operator is unsupported
   */
  private static Map<String, Map<String, Object>> translateConditions(
      Map<ConditionOperator, Map<String, Object>> conditions) {
    Map<String, Map<String, Object>> awsConditions = new LinkedHashMap<>();

    for (Map.Entry<ConditionOperator, Map<String, Object>> entry : conditions.entrySet()) {
      ConditionOperator operator = entry.getKey();
      String awsOperator = CONDITION_MAPPINGS.get(operator);

      if (awsOperator == null) {
        throw new InvalidArgumentException(
            "Unsupported condition operator: "
                + operator.getValue()
                + ". "
                + "Supported operators: "
                + CONDITION_MAPPINGS.keySet().stream()
                    .map(ConditionOperator::getValue)
                    .collect(Collectors.joining(", ")));
      }

      awsConditions.put(awsOperator, entry.getValue());
    }

    return awsConditions;
  }
}
