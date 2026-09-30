package com.salesforce.multicloudj.sts.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;

/**
 * ValidateOptions carries optional, substrate-agnostic knobs that influence how a signed cloud
 * native auth request is validated by a verifier.
 *
 * <p>All options are optional; an instance built with no customization requests the default
 * validation behavior. When expected custom headers are supplied, validation fails if any expected
 * header is missing from the signed request or has a different value.
 */
@Getter
public class ValidateOptions {

  /**
   * Key-value pairs that must match the custom headers carried by the signed request. Validation
   * fails if any expected header is missing or has a different value.
   */
  private final Map<String, String> expectedCustomHeaders;

  private ValidateOptions(Builder builder) {
    this.expectedCustomHeaders =
        Collections.unmodifiableMap(new LinkedHashMap<>(builder.expectedCustomHeaders));
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link ValidateOptions}. */
  public static class Builder {
    private final Map<String, String> expectedCustomHeaders = new LinkedHashMap<>();

    /**
     * Adds a single expected custom header that must be present in the signed request with the
     * given value.
     *
     * @param name header name
     * @param value expected header value
     * @return this builder
     */
    public Builder withExpectedCustomHeader(String name, String value) {
      this.expectedCustomHeaders.put(name, value);
      return this;
    }

    /**
     * Adds all supplied expected custom headers that must be present in the signed request.
     *
     * @param expectedCustomHeaders expected headers to add
     * @return this builder
     */
    public Builder withExpectedCustomHeaders(Map<String, String> expectedCustomHeaders) {
      if (expectedCustomHeaders != null) {
        this.expectedCustomHeaders.putAll(expectedCustomHeaders);
      }
      return this;
    }

    public ValidateOptions build() {
      return new ValidateOptions(this);
    }
  }
}
