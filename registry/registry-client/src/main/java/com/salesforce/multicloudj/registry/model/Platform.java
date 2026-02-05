package com.salesforce.multicloudj.registry.model;

import lombok.Builder;
import lombok.Getter;

/** Target platform for multi-arch image selection (e.g. linux/amd64), used when pulling images. */
@Builder
@Getter
public class Platform {

    public static final String DEFAULT_OS = "linux";
    public static final String DEFAULT_ARCHITECTURE = "amd64";

    public static final Platform DEFAULT = Platform.builder()
            .operatingSystem(DEFAULT_OS)
            .architecture(DEFAULT_ARCHITECTURE)
            .build();

    private final String architecture;
    private final String operatingSystem;
    private final String operatingSystemVersion;
    private final String variant;

    public boolean matches(Platform target) {
        if (target == null) {
            return true;
        }
        if (target.getOperatingSystem() != null && !target.getOperatingSystem().isEmpty()
                && !target.getOperatingSystem().equals(this.operatingSystem)) {
            return false;
        }
        if (target.getArchitecture() != null && !target.getArchitecture().isEmpty()
                && !target.getArchitecture().equals(this.architecture)) {
            return false;
        }
        return true;
    }
}
