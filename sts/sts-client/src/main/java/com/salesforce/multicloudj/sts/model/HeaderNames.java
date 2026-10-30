package com.salesforce.multicloudj.sts.model;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Applies the shared naming rule for custom header maps carried by sign and validate options. */
final class HeaderNames {

  private HeaderNames() {}

  /**
   * Returns an unmodifiable, insertion-ordered copy of {@code in} whose names are lowercased.
   * Header names are case-insensitive, so names that are equal after lowercasing are rejected
   * rather than silently merged.
   *
   * @param in the header names and values as supplied by the caller
   * @return the normalized headers
   * @throws InvalidArgumentException if a name is null or blank, a value is null, or two names
   *     differ only by case
   */
  static Map<String, String> normalize(Map<String, String> in) {
    Map<String, String> out = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : in.entrySet()) {
      String name = entry.getKey();
      if (name == null || name.isBlank()) {
        throw new InvalidArgumentException("custom header name must not be blank");
      }
      if (entry.getValue() == null) {
        throw new InvalidArgumentException("custom header \"" + name + "\" has a null value");
      }
      String lower = name.toLowerCase(Locale.ROOT);
      if (out.put(lower, entry.getValue()) != null) {
        throw new InvalidArgumentException(
            "custom header \"" + lower + "\" is specified more than once");
      }
    }
    return Collections.unmodifiableMap(out);
  }
}
