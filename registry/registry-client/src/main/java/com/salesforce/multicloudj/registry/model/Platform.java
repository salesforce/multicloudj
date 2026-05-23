package com.salesforce.multicloudj.registry.model;

import java.util.HashSet;
import java.util.List;
import lombok.Builder;
import lombok.Getter;

/** Target platform for multi-arch image selection (e.g. linux/amd64), used when pulling images. */
@Builder
@Getter
public class Platform {

  public static final String DEFAULT_OS = "linux";
  public static final String DEFAULT_ARCHITECTURE = "amd64";

  public static final Platform DEFAULT =
      Platform.builder().operatingSystem(DEFAULT_OS).architecture(DEFAULT_ARCHITECTURE).build();

  private final String architecture;
  private final String operatingSystem;
  private final String operatingSystemVersion;
  private final String variant;
  private final List<String> operatingSystemFeatures;

  /**
   * Checks if this platform matches the given spec platform.
   *
   * <p>Matching semantics:
   *
   * <ul>
   *   <li>Empty/null fields in the spec are treated as "match anything"
   *   <li>Matching is case-sensitive (per OCI spec)
   *   <li>OS, Architecture, Variant, and OSVersion are checked for equality
   *   <li>OSFeatures must be a superset of the spec's OSFeatures
   * </ul>
   *
   * @param spec the spec platform representing requirements to match against
   * @return true if this platform matches the spec requirements
   */
  public boolean matches(Platform spec) {
    if (spec == null) {
      return true;
    }
    return isFieldMatch(spec.operatingSystem, operatingSystem)
        && isFieldMatch(spec.architecture, architecture)
        && isFieldMatch(spec.variant, variant)
        && isFieldMatch(spec.operatingSystemVersion, operatingSystemVersion)
        && isFeaturesMatch(spec.operatingSystemFeatures, operatingSystemFeatures);
  }

  private static boolean isFieldMatch(String specValue, String actualValue) {
    return specValue == null || specValue.isEmpty() || specValue.equals(actualValue);
  }

  private static boolean isFeaturesMatch(List<String> specFeatures, List<String> actualFeatures) {
    if (specFeatures == null || specFeatures.isEmpty()) {
      return true;
    }
    if (actualFeatures == null || actualFeatures.isEmpty()) {
      return false;
    }
    return new HashSet<>(actualFeatures).containsAll(specFeatures);
  }
}
