package com.salesforce.multicloudj.iam.aws;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.Statement;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Translates substrate-neutral PolicyDocument to AWS IAM policy format.
 * 
 * <p>This translator converts substrate-neutral actions, resources, and conditions
 * to AWS-specific IAM policy JSON format according to the translation rules defined
 * in PolicyDocument documentation.
 * 
 * <p>Translation rules:
 * <ul>
 *   <li>Actions: storage:GetObject → s3:GetObject, compute:CreateInstance → ec2:RunInstances</li>
 *   <li>Resources: storage://bucket-name/* → arn:aws:s3:::bucket-name/*</li>
 *   <li>Conditions: stringEquals → StringEquals (capitalize first letter)</li>
 *   <li>Principals: Wrap in {"AWS": "principal"} or {"Service": "principal"}</li>
 * </ul>
 */
public class AwsIamPolicyTranslator {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String SERVICE_PRINCIPAL_SUFFIX = ".amazonaws.com";

    // Action mappings: substrate-neutral → AWS
    private static final Map<String, String> ACTION_MAPPINGS = Map.ofEntries(
        // Storage actions
        Map.entry("storage:GetObject", "s3:GetObject"),
        Map.entry("storage:PutObject", "s3:PutObject"),
        Map.entry("storage:DeleteObject", "s3:DeleteObject"),
        Map.entry("storage:ListBucket", "s3:ListBucket"),
        Map.entry("storage:GetBucketLocation", "s3:GetBucketLocation"),
        Map.entry("storage:CreateBucket", "s3:CreateBucket"),
        Map.entry("storage:DeleteBucket", "s3:DeleteBucket"),
        
        // Compute actions
        Map.entry("compute:CreateInstance", "ec2:RunInstances"),
        Map.entry("compute:DeleteInstance", "ec2:TerminateInstances"),
        Map.entry("compute:StartInstance", "ec2:StartInstances"),
        Map.entry("compute:StopInstance", "ec2:StopInstances"),
        Map.entry("compute:DescribeInstances", "ec2:DescribeInstances"),
        Map.entry("compute:GetInstance", "ec2:DescribeInstances"),
        
        // IAM actions
        Map.entry("iam:AssumeRole", "sts:AssumeRole"),
        Map.entry("iam:CreateRole", "iam:CreateRole"),
        Map.entry("iam:DeleteRole", "iam:DeleteRole"),
        Map.entry("iam:GetRole", "iam:GetRole"),
        Map.entry("iam:AttachRolePolicy", "iam:AttachRolePolicy"),
        Map.entry("iam:DetachRolePolicy", "iam:DetachRolePolicy"),
        Map.entry("iam:PutRolePolicy", "iam:PutRolePolicy"),
        Map.entry("iam:GetRolePolicy", "iam:GetRolePolicy")
    );

    // Condition operator mappings: substrate-neutral → AWS
    private static final Map<String, String> CONDITION_MAPPINGS = Map.ofEntries(
        Map.entry("stringEquals", "StringEquals"),
        Map.entry("stringNotEquals", "StringNotEquals"),
        Map.entry("stringLike", "StringLike"),
        Map.entry("stringNotLike", "StringNotLike"),
        Map.entry("numericEquals", "NumericEquals"),
        Map.entry("numericNotEquals", "NumericNotEquals"),
        Map.entry("numericLessThan", "NumericLessThan"),
        Map.entry("numericLessThanEquals", "NumericLessThanEquals"),
        Map.entry("numericGreaterThan", "NumericGreaterThan"),
        Map.entry("numericGreaterThanEquals", "NumericGreaterThanEquals"),
        Map.entry("dateEquals", "DateEquals"),
        Map.entry("dateNotEquals", "DateNotEquals"),
        Map.entry("dateLessThan", "DateLessThan"),
        Map.entry("dateLessThanEquals", "DateLessThanEquals"),
        Map.entry("dateGreaterThan", "DateGreaterThan"),
        Map.entry("dateGreaterThanEquals", "DateGreaterThanEquals"),
        Map.entry("bool", "Bool"),
        Map.entry("ipAddress", "IpAddress"),
        Map.entry("notIpAddress", "NotIpAddress")
    );

    /**
     * Translates a substrate-neutral PolicyDocument to AWS IAM policy JSON string.
     *
     * @param policyDocument the substrate-neutral policy document
     * @return AWS IAM policy JSON string
     * @throws SubstrateSdkException if translation fails
     */
    public static String translateToAwsPolicy(PolicyDocument policyDocument) {
        Map<String, Object> awsPolicy = new LinkedHashMap<>();
        awsPolicy.put("Version", policyDocument.getVersion());

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
        Map<String, Object> awsStatement = new LinkedHashMap<>();

        // Add Sid if present
        if (statement.getSid() != null && !statement.getSid().isEmpty()) {
            awsStatement.put("Sid", statement.getSid());
        }

        // Add Effect
        awsStatement.put("Effect", statement.getEffect());

        // Translate and add Principals if present
        if (statement.getPrincipals() != null && !statement.getPrincipals().isEmpty()) {
            awsStatement.put("Principal", translatePrincipals(statement.getPrincipals()));
        }

        // Translate and add Actions
        List<String> awsActions = statement.getActions().stream()
            .map(AwsIamPolicyTranslator::translateAction)
            .collect(Collectors.toList());
        
        if (awsActions.size() == 1) {
            awsStatement.put("Action", awsActions.get(0));
        } else {
            awsStatement.put("Action", awsActions);
        }

        // Translate and add Resources if present
        if (statement.getResources() != null && !statement.getResources().isEmpty()) {
            List<String> awsResources = statement.getResources().stream()
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
     * Translates substrate-neutral action to AWS action.
     * Supports wildcard actions like storage:*, compute:*, iam:*.
     *
     * @param action the substrate-neutral action
     * @return AWS action
     * @throws SubstrateSdkException if action is unknown
     */
    private static String translateAction(String action) {
        // Handle wildcard actions (e.g., storage:*, compute:*, iam:*)
        if (action.endsWith(":*")) {
            String service = action.substring(0, action.length() - 2);
            switch (service) {
                case "storage":
                    return "s3:*";
                case "compute":
                    return "ec2:*";
                case "iam":
                    return "iam:*";
                default:
                    throw new SubstrateSdkException(
                        "Unknown substrate-neutral service for wildcard action: " + action + ". " +
                        "Supported wildcard services: storage:*, compute:*, iam:*"
                    );
            }
        }
        
        // Handle specific actions
        String awsAction = ACTION_MAPPINGS.get(action);
        if (awsAction == null) {
            throw new SubstrateSdkException(
                "Unknown substrate-neutral action: " + action + ". " +
                "Supported actions: " + String.join(", ", ACTION_MAPPINGS.keySet()) + 
                ", or wildcard actions: storage:*, compute:*, iam:*"
            );
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

        throw new SubstrateSdkException(
            "Unknown resource format: " + resource + ". " +
            "Supported formats: storage://bucket/key, arn:aws:..., or *"
        );
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
            principalMap.put("Service", servicePrincipals.size() == 1 ? servicePrincipals.get(0) : servicePrincipals);
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
            Map<String, Map<String, Object>> conditions) {
        Map<String, Map<String, Object>> awsConditions = new LinkedHashMap<>();

        for (Map.Entry<String, Map<String, Object>> entry : conditions.entrySet()) {
            String operator = entry.getKey();
            String awsOperator = CONDITION_MAPPINGS.get(operator);
            
            if (awsOperator == null) {
                throw new SubstrateSdkException(
                    "Unsupported condition operator: " + operator + ". " +
                    "Supported operators: " + String.join(", ", CONDITION_MAPPINGS.keySet())
                );
            }
            
            awsConditions.put(awsOperator, entry.getValue());
        }

        return awsConditions;
    }
}
