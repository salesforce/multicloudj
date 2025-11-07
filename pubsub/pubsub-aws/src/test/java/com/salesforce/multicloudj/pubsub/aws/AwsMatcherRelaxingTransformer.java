package com.salesforce.multicloudj.pubsub.aws;

import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.StubMappingTransformer;
import com.github.tomakehurst.wiremock.matching.ContentPattern;
import com.github.tomakehurst.wiremock.matching.MatchesJsonPathPattern;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;

import java.util.List;

/**
 * Relax the request body matching for AWS SQS operations.
 * Only verify existence of key fields, not exact JSON values.
 */
public class AwsMatcherRelaxingTransformer extends StubMappingTransformer {

    @Override
    public StubMapping transform(StubMapping stub, FileSource files, Parameters params) {
        // Clear scenario states for all operations
        stub.setScenarioName(null);
        stub.setRequiredScenarioState(null);
        stub.setNewScenarioState(null);

        String amzTarget = null;
        if (stub.getRequest().getHeaders() != null && stub.getRequest().getHeaders().containsKey("X-Amz-Target")) {
            String headerValue = stub.getRequest().getHeaders().get("X-Amz-Target").getExpected();
            if (headerValue != null) {
                amzTarget = headerValue;
            }
        }

        if (amzTarget == null) {
            return stub;
        }

        List<ContentPattern<?>> bodyPatterns = stub.getRequest().getBodyPatterns();
        if (bodyPatterns == null) {
            return stub;
        }

        // Relax only batch operations
        if (amzTarget.contains("DeleteMessageBatch")) {
            bodyPatterns.clear();
            bodyPatterns.add(new MatchesJsonPathPattern("$.Entries[*].ReceiptHandle"));
        } 
        else if (amzTarget.contains("ChangeMessageVisibilityBatch")) {
            bodyPatterns.clear();
            bodyPatterns.add(new MatchesJsonPathPattern("$.Entries[*].ReceiptHandle"));
            bodyPatterns.add(new MatchesJsonPathPattern("$.Entries[*].VisibilityTimeout"));
        }
        else if (amzTarget.contains("SendMessageBatch")) {
            // keep body integrity to preserve MD5 validation
            // just verify structure, but don't clear existing patterns
            bodyPatterns.add(new MatchesJsonPathPattern("$.Entries[*].MessageBody"));
        }
        // For ReceiveMessage — normally no body, skip
        else if (amzTarget.contains("ReceiveMessage")) {
            return stub;
        }

        return stub;
    }

    @Override
    public String getName() {
        return "relax-aws-matchers";
    }

    @Override
    public boolean applyGlobally() {
        return true;
    }
}
