package com.salesforce.multicloudj.iam.aws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.Statement;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class AwsIamPolicyTranslatorTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testTranslateStorageActions() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .sid("StorageAccess")
                .effect("Allow")
                .action("storage:GetObject")
                .action("storage:PutObject")
                .resource("storage://my-bucket/*")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        assertEquals("2012-10-17", policyJson.get("Version").asText());
        JsonNode statement = policyJson.get("Statement").get(0);
        assertEquals("StorageAccess", statement.get("Sid").asText());
        assertEquals("Allow", statement.get("Effect").asText());

        JsonNode actions = statement.get("Action");
        assertTrue(actions.isArray());
        assertEquals(2, actions.size());
        assertEquals("s3:GetObject", actions.get(0).asText());
        assertEquals("s3:PutObject", actions.get(1).asText());

        assertEquals("arn:aws:s3:::my-bucket/*", statement.get("Resource").asText());
    }

    @Test
    void testTranslateComputeActions() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("compute:CreateInstance")
                .action("compute:DeleteInstance")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        JsonNode actions = policyJson.get("Statement").get(0).get("Action");
        assertTrue(actions.isArray());
        assertEquals("ec2:RunInstances", actions.get(0).asText());
        assertEquals("ec2:TerminateInstances", actions.get(1).asText());
    }

    @Test
    void testTranslateIamActions() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("iam:AssumeRole")
                .action("iam:CreateRole")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        JsonNode actions = policyJson.get("Statement").get(0).get("Action");
        assertTrue(actions.isArray());
        assertEquals("sts:AssumeRole", actions.get(0).asText());
        assertEquals("iam:CreateRole", actions.get(1).asText());
    }

    @Test
    void testTranslateSingleAction() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("storage:GetObject")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        JsonNode action = policyJson.get("Statement").get(0).get("Action");
        assertFalse(action.isArray());
        assertEquals("s3:GetObject", action.asText());
    }

    @Test
    void testTranslateResourceWildcard() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("storage:GetObject")
                .resource("*")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        assertEquals("*", policyJson.get("Statement").get(0).get("Resource").asText());
    }

    @Test
    void testTranslateResourceArn() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("storage:GetObject")
                .resource("arn:aws:s3:::my-bucket/*")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        assertEquals("arn:aws:s3:::my-bucket/*", policyJson.get("Statement").get(0).get("Resource").asText());
    }

    @Test
    void testTranslateMultipleResources() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("storage:GetObject")
                .resource("storage://bucket1/*")
                .resource("storage://bucket2/*")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        JsonNode resources = policyJson.get("Statement").get(0).get("Resource");
        assertTrue(resources.isArray());
        assertEquals(2, resources.size());
        assertEquals("arn:aws:s3:::bucket1/*", resources.get(0).asText());
        assertEquals("arn:aws:s3:::bucket2/*", resources.get(1).asText());
    }

    @Test
    void testTranslatePrincipalsAwsOnly() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("storage:GetObject")
                .principal("arn:aws:iam::123456789012:user/TestUser")
                .principal("arn:aws:iam::123456789012:role/TestRole")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        JsonNode principal = policyJson.get("Statement").get(0).get("Principal");
        JsonNode awsPrincipals = principal.get("AWS");
        assertTrue(awsPrincipals.isArray());
        assertEquals(2, awsPrincipals.size());
        assertEquals("arn:aws:iam::123456789012:user/TestUser", awsPrincipals.get(0).asText());
        assertEquals("arn:aws:iam::123456789012:role/TestRole", awsPrincipals.get(1).asText());
    }

    @Test
    void testTranslatePrincipalsServiceOnly() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("storage:GetObject")
                .principal("ec2.amazonaws.com")
                .principal("lambda.amazonaws.com")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        JsonNode principal = policyJson.get("Statement").get(0).get("Principal");
        JsonNode servicePrincipals = principal.get("Service");
        assertTrue(servicePrincipals.isArray());
        assertEquals(2, servicePrincipals.size());
        assertEquals("ec2.amazonaws.com", servicePrincipals.get(0).asText());
        assertEquals("lambda.amazonaws.com", servicePrincipals.get(1).asText());
    }

    @Test
    void testTranslatePrincipalsMixed() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("storage:GetObject")
                .principal("arn:aws:iam::123456789012:user/TestUser")
                .principal("ec2.amazonaws.com")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        JsonNode principal = policyJson.get("Statement").get(0).get("Principal");
        assertEquals("arn:aws:iam::123456789012:user/TestUser", principal.get("AWS").asText());
        assertEquals("ec2.amazonaws.com", principal.get("Service").asText());
    }

    @Test
    void testTranslateConditions() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("storage:GetObject")
                .condition("stringEquals", "aws:RequestedRegion", "us-west-2")
                .condition("numericLessThan", "s3:max-keys", 100)
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        JsonNode condition = policyJson.get("Statement").get(0).get("Condition");
        assertNotNull(condition);
        assertEquals("us-west-2", condition.get("StringEquals").get("aws:RequestedRegion").asText());
        assertEquals(100, condition.get("NumericLessThan").get("s3:max-keys").asInt());
    }

    @Test
    void testTranslateMultipleStatements() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .sid("StorageRead")
                .effect("Allow")
                .action("storage:GetObject")
                .build())
            .statement(Statement.builder()
                .sid("StorageWrite")
                .effect("Allow")
                .action("storage:PutObject")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        JsonNode statements = policyJson.get("Statement");
        assertEquals(2, statements.size());
        assertEquals("StorageRead", statements.get(0).get("Sid").asText());
        assertEquals("s3:GetObject", statements.get(0).get("Action").asText());
        assertEquals("StorageWrite", statements.get(1).get("Sid").asText());
        assertEquals("s3:PutObject", statements.get(1).get("Action").asText());
    }

    @Test
    void testTranslateUnknownActionThrowsException() {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("unknown:Action")
                .build())
            .build();

        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        });

        assertTrue(exception.getMessage().contains("Unknown substrate-neutral action: unknown:Action"));
    }

    @Test
    void testTranslateUnsupportedConditionThrowsException() {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("storage:GetObject")
                .condition("unsupportedOperator", "key", "value")
                .build())
            .build();

        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        });

        assertTrue(exception.getMessage().contains("Unsupported condition operator: unsupportedOperator"));
    }

    @Test
    void testTranslateInvalidResourceFormatThrowsException() {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("storage:GetObject")
                .resource("invalid://resource")
                .build())
            .build();

        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        });

        assertTrue(exception.getMessage().contains("Unknown resource format: invalid://resource"));
    }

    @Test
    void testTranslateStatementWithoutSid() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("storage:GetObject")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        JsonNode statement = policyJson.get("Statement").get(0);
        assertNull(statement.get("Sid"));
    }

    @Test
    void testTranslateStatementWithoutPrincipals() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("storage:GetObject")
                .resource("storage://my-bucket/*")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        JsonNode statement = policyJson.get("Statement").get(0);
        assertNull(statement.get("Principal"));
    }

    @Test
    void testTranslateStatementWithoutResources() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("storage:GetObject")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        JsonNode statement = policyJson.get("Statement").get(0);
        assertNull(statement.get("Resource"));
    }

    @Test
    void testTranslateStatementWithoutConditions() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("storage:GetObject")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        JsonNode statement = policyJson.get("Statement").get(0);
        assertNull(statement.get("Condition"));
    }

    @Test
    void testTranslateAllConditionOperators() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("storage:GetObject")
                .condition("stringEquals", "key1", "value1")
                .condition("stringNotEquals", "key2", "value2")
                .condition("stringLike", "key3", "value3")
                .condition("numericLessThan", "key4", 100)
                .condition("dateGreaterThan", "key5", "2024-01-01")
                .condition("bool", "key6", true)
                .condition("ipAddress", "key7", "192.168.1.0/24")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        JsonNode condition = policyJson.get("Statement").get(0).get("Condition");
        assertNotNull(condition.get("StringEquals"));
        assertNotNull(condition.get("StringNotEquals"));
        assertNotNull(condition.get("StringLike"));
        assertNotNull(condition.get("NumericLessThan"));
        assertNotNull(condition.get("DateGreaterThan"));
        assertNotNull(condition.get("Bool"));
        assertNotNull(condition.get("IpAddress"));
    }

    @Test
    void testTranslateWildcardStorageAction() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("storage:*")
                .resource("storage://my-bucket/*")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        assertEquals("s3:*", policyJson.get("Statement").get(0).get("Action").asText());
    }

    @Test
    void testTranslateWildcardComputeAction() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("compute:*")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        assertEquals("ec2:*", policyJson.get("Statement").get(0).get("Action").asText());
    }

    @Test
    void testTranslateWildcardIamAction() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("iam:*")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        assertEquals("iam:*", policyJson.get("Statement").get(0).get("Action").asText());
    }

    @Test
    void testTranslateMixedWildcardAndSpecificActions() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("storage:*")
                .action("compute:CreateInstance")
                .action("iam:GetRole")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        JsonNode actions = policyJson.get("Statement").get(0).get("Action");
        assertTrue(actions.isArray());
        assertEquals(3, actions.size());
        assertEquals("s3:*", actions.get(0).asText());
        assertEquals("ec2:RunInstances", actions.get(1).asText());
        assertEquals("iam:GetRole", actions.get(2).asText());
    }

    @Test
    void testTranslateUnknownWildcardServiceThrowsException() {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("unknown:*")
                .build())
            .build();

        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        });

        assertTrue(exception.getMessage().contains("Unknown substrate-neutral service for wildcard action: unknown:*"));
        assertTrue(exception.getMessage().contains("Supported wildcard services: storage:*, compute:*, iam:*"));
    }

    @Test
    void testTranslateMultipleWildcardActions() throws Exception {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2012-10-17")
            .statement(Statement.builder()
                .effect("Allow")
                .action("storage:*")
                .action("compute:*")
                .action("iam:*")
                .build())
            .build();

        String awsPolicy = AwsIamPolicyTranslator.translateToAwsPolicy(policy);
        JsonNode policyJson = OBJECT_MAPPER.readTree(awsPolicy);

        JsonNode actions = policyJson.get("Statement").get(0).get("Action");
        assertTrue(actions.isArray());
        assertEquals(3, actions.size());
        assertEquals("s3:*", actions.get(0).asText());
        assertEquals("ec2:*", actions.get(1).asText());
        assertEquals("iam:*", actions.get(2).asText());
    }
}
