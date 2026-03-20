package com.salesforce.multicloudj.registry.gcp.util;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;

import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.StubMappingTransformer;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * WireMock transformer that redacts OAuth2/bearer tokens
 * from recorded stub responses.
 *
 * <p>During record mode, token exchange endpoints return real
 * tokens in the response body. This transformer replaces those
 * sensitive values with mock placeholders so the recorded
 * fixtures can be safely committed to source control.
 *
 * <p>Redacted JSON fields:
 * {@code token}, {@code access_token}, {@code refresh_token}.
 */
public class RegistryTokenRedactingTransformer
    extends StubMappingTransformer {

  private static final String REDACTED = "REDACTED";
  private static final Set<String> TOKEN_FIELDS =
      Set.of("token", "access_token", "refresh_token");

  /** Redacts token fields in the stub's response body. */
  @Override
  public StubMapping transform(StubMapping stub,
      FileSource files, Parameters parameters) {
    try {
      ResponseDefinition response = stub.getResponse();
      String body = extractResponseBody(response, files);
      if (body == null || body.isEmpty()) {
        return stub;
      }

      JsonElement parsed;
      try {
        parsed = JsonParser.parseString(body);
      } catch (JsonSyntaxException e) {
        return stub;
      }

      if (!parsed.isJsonObject()) {
        return stub;
      }

      JsonObject json = parsed.getAsJsonObject();
      boolean modified = false;
      for (String field : TOKEN_FIELDS) {
        if (json.has(field)
            && json.get(field).isJsonPrimitive()) {
          json.addProperty(field, REDACTED);
          modified = true;
        }
      }

      if (!modified) {
        return stub;
      }

      String redacted = json.toString();
      String bodyFileName = response.getBodyFileName();
      if (bodyFileName != null && !bodyFileName.isEmpty()) {
        files.writeBinaryFile(
            bodyFileName,
            redacted.getBytes(StandardCharsets.UTF_8));
      } else {
        stub.setResponse(
            aResponse()
                .withStatus(response.getStatus())
                .withHeaders(response.getHeaders())
                .withBody(redacted)
                .build());
      }
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to redact tokens in response body", e);
    }
    return stub;
  }

  /** Returns the response body, reading from __files/ if needed. */
  private String extractResponseBody(ResponseDefinition response,
      FileSource files) {
    String inlineBody = response.getBody();
    if (inlineBody != null && !inlineBody.isEmpty()) {
      return inlineBody;
    }
    if (response.getBodyFileName() != null) {
      byte[] bodyBytes = files.getBinaryFileNamed(
          response.getBodyFileName()).readContents();
      if (bodyBytes != null && bodyBytes.length > 0) {
        return new String(bodyBytes, StandardCharsets.UTF_8);
      }
    }
    return null;
  }

  @Override
  public String getName() {
    return "registry-token-redacting-transformer";
  }

  @Override
  public boolean applyGlobally() {
    return true;
  }
}
