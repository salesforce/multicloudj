package com.salesforce.multicloudj.registry.driver;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

/**
 * Represents the target OS/Architecture for a container image.
 * Similar to go-containerregistry's v1.Platform.
 * 
 * @see <a href="https://github.com/google/go-containerregistry/blob/main/pkg/v1/platform.go">go-containerregistry Platform</a>
 */
@Getter
@Builder
@ToString
public class Platform {
    /**
     * Architecture (e.g., "amd64", "arm64", "arm")
     */
    private final String architecture;
    
    /**
     * Operating system (e.g., "linux", "windows", "darwin")
     */
    private final String os;
    
    /**
     * OS version (e.g., "10.0.19041.388" for Windows)
     */
    private final String osVersion;
    
    /**
     * OS features (e.g., ["win32k"] for Windows)
     */
    private final List<String> osFeatures;
    
    /**
     * Variant (e.g., "v7", "v8" for ARM)
     */
    private final String variant;
    
    /**
     * Features (additional features)
     */
    private final List<String> features;
    
    /**
     * Default platform: linux/amd64
     * Same as go-containerregistry's defaultPlatform
     */
    public static final Platform DEFAULT = Platform.builder()
            .os("linux")
            .architecture("amd64")
            .build();
    
    /**
     * Check if this platform matches the required platform.
     * Similar to go-containerregistry's matchesPlatform function.
     * 
     * Matching rules:
     * - Architecture and OS must be identical
     * - OSVersion and Variant must match if provided in required
     * - Required platform's features must be a subset of this platform's features
     * 
     * @param required The required platform to match against
     * @return true if this platform matches the required platform
     */
    public boolean matches(Platform required) {
        if (required == null) {
            return true; // If no requirement, match any
        }
        
        // Required fields that must be identical
        if (!equalsIgnoreNull(this.architecture, required.architecture) ||
            !equalsIgnoreNull(this.os, required.os)) {
            return false;
        }
        
        // Optional fields that may be empty, but must be identical if provided
        if (required.osVersion != null && !required.osVersion.isEmpty()) {
            if (!equalsIgnoreNull(this.osVersion, required.osVersion)) {
                return false;
            }
        }
        
        if (required.variant != null && !required.variant.isEmpty()) {
            if (!equalsIgnoreNull(this.variant, required.variant)) {
                return false;
            }
        }
        
        // Verify required platform's features are a subset of this platform's features
        if (!isSubset(this.osFeatures, required.osFeatures)) {
            return false;
        }
        
        if (!isSubset(this.features, required.features)) {
            return false;
        }
        
        return true;
    }
    
    /**
     * Check if two strings are equal, treating null and empty as equivalent.
     */
    private boolean equalsIgnoreNull(String a, String b) {
        if (a == null || a.isEmpty()) {
            return (b == null || b.isEmpty());
        }
        if (b == null || b.isEmpty()) {
            return false;
        }
        return a.equals(b);
    }
    
    /**
     * Check if required list is a subset of given list.
     * Similar to go-containerregistry's isSubset function.
     */
    private boolean isSubset(List<String> given, List<String> required) {
        if (required == null || required.isEmpty()) {
            return true; // Empty requirement means no constraint
        }
        if (given == null || given.isEmpty()) {
            return false; // Required features but none given
        }
        
        // Check if all required features are in given list
        for (String req : required) {
            if (!given.contains(req)) {
                return false;
            }
        }
        return true;
    }
}
