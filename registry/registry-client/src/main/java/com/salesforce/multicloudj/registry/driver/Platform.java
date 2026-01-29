package com.salesforce.multicloudj.registry.driver;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class Platform {
    public static final Platform DEFAULT = Platform.builder()
        .os("linux")
        .architecture("amd64")
        .build();

    private final String architecture;
    private final String os;
    private final String osVersion;
    private final String variant;

    public boolean matches(Platform target) {
        if (target == null) {
            return true;
        }
        if (target.getOs() != null && !target.getOs().isEmpty() && !target.getOs().equals(this.os)) {
            return false;
        }
        if (target.getArchitecture() != null && !target.getArchitecture().isEmpty() && !target.getArchitecture().equals(this.architecture)) {
            return false;
        }
        return true;
    }
}
