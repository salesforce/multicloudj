package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;

/**
 * Authentication schemes supported by OCI registries.
 *
 * <p>The scheme is discovered at runtime by pinging the registry's {@code /v2/} endpoint:
 *
 * <ul>
 *   <li>HTTP 200 → {@link #ANONYMOUS} (no auth required)
 *   <li>HTTP 401 + {@code WWW-Authenticate: Basic ...} → {@link #BASIC}
 *   <li>HTTP 401 + {@code WWW-Authenticate: Bearer ...} → {@link #BEARER}
 * </ul>
 */
public enum AuthScheme {
  ANONYMOUS,
  BASIC,
  BEARER;

  /**
   * Parses an authentication scheme string (case-insensitive).
   *
   * @param scheme the scheme string from a WWW-Authenticate header
   * @return the matching AuthScheme
   * @throws InvalidArgumentException if the scheme string is null or blank
   * @throws UnSupportedOperationException if the scheme string is not a recognized value
   */
  public static AuthScheme fromString(String scheme) {
    if (scheme == null || scheme.isBlank()) {
      throw new InvalidArgumentException("Authentication scheme is blank or missing");
    }
    switch (scheme.toLowerCase()) {
      case "anonymous":
        return ANONYMOUS;
      case "basic":
        return BASIC;
      case "bearer":
        return BEARER;
      default:
        throw new UnSupportedOperationException("Unsupported authentication scheme: " + scheme);
    }
  }
}
