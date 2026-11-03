package com.salesforce.multicloudj.sts.driver;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import java.util.Locale;
import java.util.Map;

/**
 * Finds an expected custom header in what a signer produced, using the one naming rule every
 * provider verifier shares: header names are compared in lowercase, as SigV4 canonicalizes them.
 */
public final class CustomHeaderLookup {

  private CustomHeaderLookup() {}

  /**
   * Returns the value whose name equals {@code name} once both are lowercased, or null when there
   * is none. Names that are equal after lowercasing are ambiguous and are rejected rather than
   * picking one.
   *
   * @param values the signed request parameters or token claims to search
   * @param name the expected custom header name
   * @param source what {@code values} holds, used in the error message, e.g. "JWT claim"
   * @return the matching value, or null if no name matches
   * @throws InvalidArgumentException if more than one name matches
   */
  public static <V> V find(Map<String, V> values, String name, String source) {
    String lower = name.toLowerCase(Locale.ROOT);
    V match = null;
    boolean found = false;
    for (Map.Entry<String, V> entry : values.entrySet()) {
      if (entry.getKey().toLowerCase(Locale.ROOT).equals(lower)) {
        if (found) {
          throw new InvalidArgumentException(
              "custom header \"" + lower + "\" matches more than one " + source);
        }
        found = true;
        match = entry.getValue();
      }
    }
    return match;
  }
}
