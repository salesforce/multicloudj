package com.salesforce.multicloudj.pubsub.aws.util;

import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.StubMappingTransformer;
import com.github.tomakehurst.wiremock.matching.ContentPattern;
import com.github.tomakehurst.wiremock.matching.MatchesJsonPathPattern;
import com.github.tomakehurst.wiremock.matching.MultiValuePattern;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;

import java.util.List;
import java.util.Map;

/**
 * Relax the request body matching for AWS SQS DeleteMessageBatch and ChangeMessageVisibilityBatch operations.
 * Use JsonPath to only verify the existence of ReceiptHandle fields rather than exact values.
 * 
 * This is necessary because:
 * 1. ReceiptHandle values are dynamic and change with each ReceiveMessage call
 * 2. Batch size is dynamically adjusted based on throughput, causing different MaxNumberOfMessages
 *    values between record and replay, which may match different ReceiveMessage mappings
 * 3. Different ReceiveMessage mappings return different ReceiptHandle values (all from AWS during recording)
 * 4. Ack/Nack requests use the ReceiptHandle from the matched ReceiveMessage mapping, which may differ
 *    from the one recorded in the DeleteMessageBatch/ChangeMessageVisibilityBatch mapping
 */
public class AckMatcherRelaxingTransformer extends StubMappingTransformer {

    @Override
    public StubMapping transform(StubMapping stub, FileSource files, Parameters params) {
        // Check if this is a DeleteMessageBatch or ChangeMessageVisibilityBatch request
        // by examining the X-Amz-Target header or the request body
        String targetValue = extractXAmzTarget(stub);
        boolean isDeleteBatch = false;
        boolean isChangeVisibilityBatch = false;
        
        if (targetValue != null) {
            isDeleteBatch = targetValue.contains("DeleteMessageBatch");
            isChangeVisibilityBatch = targetValue.contains("ChangeMessageVisibilityBatch");
        } else {
            // Fallback: check if body contains ReceiptHandle (indicates ack/nack operation)
            List<ContentPattern<?>> bodyPatterns = stub.getRequest().getBodyPatterns();
            if (bodyPatterns != null && !bodyPatterns.isEmpty()) {
                for (ContentPattern<?> pattern : bodyPatterns) {
                    String patternStr = pattern.toString();
                    if (patternStr.contains("ReceiptHandle")) {
                        // Check if it's ChangeMessageVisibilityBatch by looking for VisibilityTimeout
                        if (patternStr.contains("VisibilityTimeout")) {
                            isChangeVisibilityBatch = true;
                        } else {
                            isDeleteBatch = true;
                        }
                        break;
                    }
                }
            }
        }
        
        if (!isDeleteBatch && !isChangeVisibilityBatch) {
            return stub;
        }
        
        // Clear scenario state to avoid sequence-dependent matching issues
        stub.setScenarioName(null);
        stub.setRequiredScenarioState(null);
        stub.setNewScenarioState(null);
        
        // Relax the body matching: only verify that ReceiptHandle fields exist,
        // not their exact values
        List<ContentPattern<?>> bodyPatterns = stub.getRequest().getBodyPatterns();
        if (bodyPatterns != null && !bodyPatterns.isEmpty()) {
            bodyPatterns.clear();
            
            // Verify that the request has the required structure:
            // - QueueUrl exists
            // - Entries array exists
            // - Each entry has ReceiptHandle (but don't check the exact value)
            bodyPatterns.add(new MatchesJsonPathPattern("$.QueueUrl"));
            bodyPatterns.add(new MatchesJsonPathPattern("$.Entries"));
            bodyPatterns.add(new MatchesJsonPathPattern("$.Entries[*]"));
            bodyPatterns.add(new MatchesJsonPathPattern("$.Entries[*].ReceiptHandle"));
            
            // For ChangeMessageVisibilityBatch, also verify VisibilityTimeout exists
            if (isChangeVisibilityBatch) {
                bodyPatterns.add(new MatchesJsonPathPattern("$.Entries[*].VisibilityTimeout"));
            }
        }
        
        return stub;
    }
    
    /**
     * Extracts the X-Amz-Target header value from the stub mapping.
     * WireMock stores headers as Map<String, MultiValuePattern>.
     * Extract from string representation of the pattern.
     */
    private String extractXAmzTarget(StubMapping stub) {
        Map<String, MultiValuePattern> headers = stub.getRequest().getHeaders();
        if (headers == null) {
            return null;
        }
        
        MultiValuePattern xAmzTargetHeader = headers.get("X-Amz-Target");
        if (xAmzTargetHeader == null) {
            return null;
        }
        
        // Extract from string representation
        String headerStr = xAmzTargetHeader.toString();
        if (headerStr.contains("equalTo(") && headerStr.contains(")")) {
            int start = headerStr.indexOf("equalTo(") + 8;
            int end = headerStr.lastIndexOf(")");
            if (start < end) {
                return headerStr.substring(start, end);
            }
        }
        
        if (headerStr.contains("AmazonSQS.")) {
            int start = headerStr.indexOf("AmazonSQS.");
            int end = headerStr.indexOf(")", start);
            if (end == -1) {
                end = headerStr.length();
            }
            return headerStr.substring(start, end);
        }
        
        return null;
    }

    @Override
    public String getName() {
        return "relax-aws-ack-matchers";
    }

    @Override
    public boolean applyGlobally() {
        return true;
    }
}

