package com.salesforce.multicloudj.iam.gcp.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.StubMappingTransformer;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.github.tomakehurst.wiremock.matching.ContentPattern;
import com.github.tomakehurst.wiremock.matching.MatchesJsonPathPattern;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;

public class IamJsonResponseTransformer extends StubMappingTransformer {
	private static final ObjectMapper objectMapper = new ObjectMapper();

	private static final Pattern EMAIL_PATTERN = Pattern.compile("([a-zA-Z0-9._%+-]+)@([a-zA-Z0-9.-]+\\.[a-zA-Z]{2,})");
	private static final Pattern USER_EMAIL_PATTERN = Pattern.compile("user:([a-zA-Z0-9._%+-]+)@([a-zA-Z0-9.-]+\\.[a-zA-Z]{2,})");
	private static final Pattern GROUP_EMAIL_PATTERN = Pattern.compile("group:([a-zA-Z0-9._%+-]+)@([a-zA-Z0-9.-]+\\.[a-zA-Z]{2,})");

	@Override
	public StubMapping transform(StubMapping stub, FileSource files, Parameters params) {
		String url = stub.getRequest().getUrl();
		String urlPattern = stub.getRequest().getUrlPattern();
		String urlPath = stub.getRequest().getUrlPath();
		boolean isSetIamPolicy = (url != null && url.contains(":setIamPolicy")) ||
				(urlPattern != null && urlPattern.contains(":setIamPolicy")) ||
				(urlPath != null && urlPath.contains(":setIamPolicy"));

		if (isSetIamPolicy) {
			List<ContentPattern<?>> bodyPatterns = stub.getRequest().getBodyPatterns();
			if (bodyPatterns != null) {
				bodyPatterns.clear();
				bodyPatterns.add(new MatchesJsonPathPattern("$.policy"));
				bodyPatterns.add(new MatchesJsonPathPattern("$.policy.bindings"));
			}
			stub.setScenarioName(null);
			stub.setRequiredScenarioState(null);
			stub.setNewScenarioState(null);
		}

		try {
			ResponseDefinition response = stub.getResponse();
			String inlineBody = response.getBody();
			String body;
			if (inlineBody != null && !inlineBody.isEmpty()) {
				body = inlineBody;
			} else if (response.getBodyFileName() != null) {
				String bodyFileName = response.getBodyFileName();
				byte[] bodyBytes = files.getBinaryFileNamed(bodyFileName).readContents();
				if (bodyBytes == null || bodyBytes.length == 0) return stub;
				body = new String(bodyBytes, StandardCharsets.UTF_8);
			} else {
				return stub;
			}

			String masked = objectMapper.writerWithDefaultPrettyPrinter()
					.writeValueAsString(maskPiiInJsonNode(objectMapper.readTree(body)));
			String bodyFileName = response.getBodyFileName();
			if (bodyFileName == null || bodyFileName.isEmpty()) {
				String stubName = stub.getName();
				bodyFileName = (stubName != null && !stubName.isEmpty())
						? stubName.replaceAll("[^a-zA-Z0-9_-]", "_") + ".json"
						: "response-" + stub.getUuid().toString().substring(0, 8) + ".json";
			}
			files.writeBinaryFile(bodyFileName, masked.getBytes(StandardCharsets.UTF_8));
			if (inlineBody != null && !inlineBody.isEmpty()) {
				stub.setResponse(aResponse().withStatus(response.getStatus()).withHeaders(response.getHeaders())
						.withBodyFile(bodyFileName).build());
			}
		} catch (Exception e) {
			throw new RuntimeException("Failed to mask PII in response body", e);
		}

		return stub;
	}

	private JsonNode maskPiiInJsonNode(JsonNode node) {
		if (node.isObject()) {
			ObjectNode result = objectMapper.createObjectNode();
			node.fields().forEachRemaining(e -> result.set(e.getKey(),
					e.getValue().isTextual() ? maskPiiInString(e.getValue().asText())
							: (e.getValue().isArray() || e.getValue().isObject()) ? maskPiiInJsonNode(e.getValue())
									: e.getValue()));
			return result;
		} else if (node.isArray()) {
			ArrayNode result = objectMapper.createArrayNode();
			node.forEach(item -> result.add(item.isTextual() ? maskPiiInString(item.asText())
					: (item.isArray() || item.isObject()) ? maskPiiInJsonNode(item) : item));
			return result;
		}
		return node.isTextual() ? maskPiiInString(node.asText()) : node;
	}

	private JsonNode maskPiiInString(String text) {
		if (text == null || text.isEmpty()) return objectMapper.valueToTree(text);
		String masked = text;
		masked = USER_EMAIL_PATTERN.matcher(masked)
				.replaceAll(mr -> "user:user@domain.com");
		masked = GROUP_EMAIL_PATTERN.matcher(masked)
				.replaceAll(mr -> "group:user@domain.com");
		masked = EMAIL_PATTERN.matcher(masked).replaceAll(mr -> {
			String domain = mr.group(2);
			if (domain.endsWith(".iam.gserviceaccount.com")) {
				return mr.group(0);
			}
			return "user@domain.com";
		});
		return objectMapper.valueToTree(masked);
	}

	@Override
	public String getName() {
		return "iam-json-response-transformer";
	}

	@Override
	public boolean applyGlobally() {
		return true;
	}
}
