package com.salesforce.multicloudj.pubsub.gcp.util;

import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.StubMappingTransformer;
import com.github.tomakehurst.wiremock.matching.ContentPattern;
import com.github.tomakehurst.wiremock.matching.MatchesJsonPathPattern;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;

import java.util.List;

/**
 * Relax the request body matching for ack/nack/pull operations.
 * Use JsonPath to only verify the existence.
 */
public class AckMatcherRelaxingTransformer extends StubMappingTransformer {

    @Override
    public StubMapping transform(StubMapping stub, FileSource files, Parameters params) {
        String url = stub.getRequest().getUrl();
        String urlPattern = stub.getRequest().getUrlPattern();
        String urlPath = stub.getRequest().getUrlPath();

        boolean isAck = (url != null && url.contains(":acknowledge")) ||
                (urlPattern != null && urlPattern.contains(":acknowledge")) ||
                (urlPath != null && urlPath.contains(":acknowledge"));
        boolean isMod = (url != null && url.contains(":modifyAckDeadline")) ||
                (urlPattern != null && urlPattern.contains(":modifyAckDeadline")) ||
                (urlPath != null && urlPath.contains(":modifyAckDeadline"));
        boolean isPull = (url != null && url.contains(":pull")) ||
                (urlPattern != null && urlPattern.contains(":pull")) ||
                (urlPath != null && urlPath.contains(":pull"));
        boolean isPublish = (url != null && url.contains(":publish")) ||
                (urlPattern != null && urlPattern.contains(":publish")) ||
                (urlPath != null && urlPath.contains(":publish"));

        // During recording, if the Recorder detects repeated calls to the same endpoint,
        // it will usually auto-add `scenarioName`, `requiredScenarioState`, and `newScenarioState` to those stubs.
        // The call sequence and count during recording often differ from replay
        // (due to batching, retries, and timing differences).
        stub.setScenarioName(null);
        stub.setRequiredScenarioState(null);
        stub.setNewScenarioState(null);

        if (isAck || isMod) {
            List<ContentPattern<?>> bodyPatterns = stub.getRequest().getBodyPatterns();
            if (bodyPatterns != null) {
                bodyPatterns.clear();

                bodyPatterns.add(new MatchesJsonPathPattern("$.ackIds"));
                bodyPatterns.add(new MatchesJsonPathPattern("$.ackIds[*]"));
            }
        } else if (isPull) {
            List<ContentPattern<?>> bodyPatterns = stub.getRequest().getBodyPatterns();
            if (bodyPatterns != null) {
                bodyPatterns.clear();
                bodyPatterns.add(new MatchesJsonPathPattern("$.returnImmediately"));
                bodyPatterns.add(new MatchesJsonPathPattern("$.maxMessages"));
            }
        } else if (isPublish) {
            List<ContentPattern<?>> bodyPatterns = stub.getRequest().getBodyPatterns();
            if (bodyPatterns != null) {
                bodyPatterns.clear();
                bodyPatterns.add(new MatchesJsonPathPattern("$.messages"));
                bodyPatterns.add(new MatchesJsonPathPattern("$.messages[*]"));
            }
        }

        return stub;
    }

    @Override
    public String getName() {
        return "relax-ack-matchers";
    }

    @Override
    public boolean applyGlobally() {
        return true;
    }
}
