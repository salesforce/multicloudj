package com.salesforce.multicloudj.sts.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;

/**
 * SignOptions carries optional, substrate-agnostic knobs that influence how a cloud native auth
 * request is constructed and signed.
 *
 * <p>All options are optional; an instance built with no customization requests the default signing
 * behavior. Custom headers are applied to the request that gets signed, and the exclusion flags let
 * a caller drop headers that a downstream verifier does not expect.
 */
@Getter
public class SignOptions {

  /** Additional headers to set on the request that is signed. */
  private final Map<String, String> customHeaders;

  /** When true, the request payload hash header is not added to the signed request. */
  private final boolean excludeRequestHashHeader;

  /** When true, the content type header is not added to the signed request. */
  private final boolean excludeContentTypeHeader;

  /**
   * When true, the STS action and version are signed as URL query parameters over an empty body,
   * rather than as a form-encoded request body. This produces a request whose signature validates
   * when a verifier replays it from the URL and headers alone, with no body.
   */
  private final boolean actionInQueryString;

  private SignOptions(Builder builder) {
    this.customHeaders = Collections.unmodifiableMap(new LinkedHashMap<>(builder.customHeaders));
    this.excludeRequestHashHeader = builder.excludeRequestHashHeader;
    this.excludeContentTypeHeader = builder.excludeContentTypeHeader;
    this.actionInQueryString = builder.actionInQueryString;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final Map<String, String> customHeaders = new LinkedHashMap<>();
    private boolean excludeRequestHashHeader;
    private boolean excludeContentTypeHeader;
    private boolean actionInQueryString;

    /**
     * Adds a single custom header to set on the signed request.
     *
     * @param name header name
     * @param value header value
     * @return this builder
     */
    public Builder withCustomHeader(String name, String value) {
      this.customHeaders.put(name, value);
      return this;
    }

    /**
     * Adds all supplied custom headers to set on the signed request.
     *
     * @param customHeaders headers to add
     * @return this builder
     */
    public Builder withCustomHeaders(Map<String, String> customHeaders) {
      if (customHeaders != null) {
        this.customHeaders.putAll(customHeaders);
      }
      return this;
    }

    /**
     * Controls whether the request payload hash header is excluded from the signed request.
     *
     * @param excludeRequestHashHeader true to exclude the request hash header
     * @return this builder
     */
    public Builder withExcludeRequestHashHeader(boolean excludeRequestHashHeader) {
      this.excludeRequestHashHeader = excludeRequestHashHeader;
      return this;
    }

    /**
     * Controls whether the content type header is excluded from the signed request.
     *
     * @param excludeContentTypeHeader true to exclude the content type header
     * @return this builder
     */
    public Builder withExcludeContentTypeHeader(boolean excludeContentTypeHeader) {
      this.excludeContentTypeHeader = excludeContentTypeHeader;
      return this;
    }

    /**
     * Controls whether the STS action and version are signed as URL query parameters over an empty
     * body instead of as a form-encoded request body.
     *
     * @param actionInQueryString true to sign the action and version as query parameters
     * @return this builder
     */
    public Builder withActionInQueryString(boolean actionInQueryString) {
      this.actionInQueryString = actionInQueryString;
      return this;
    }

    public SignOptions build() {
      return new SignOptions(this);
    }
  }
}
