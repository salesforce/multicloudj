package com.salesforce.multicloudj.registry.client;

import com.salesforce.multicloudj.registry.driver.Platform;
import lombok.Builder;
import lombok.Getter;

/**
 * Optional configuration for image pull operations.
 *
 * <p>This class provides additional options that can be set during image pull,
 * such as platform selection for multi-arch images.
 *
 * <p>Usage example:
 * <pre>
 * // First pull: discover it's multi-arch
 * Image img1 = client.pull("my-image:tag");
 *
 * // Discover it's multi-arch, need linux/arm64
 * PullOptions options = PullOptions.builder()
 *     .platform(Platform.builder()
 *         .os("linux")
 *         .architecture("arm64")
 *         .build())
 *     .build();
 *
 * // Re-pull with specific platform
 * Image img2 = client.pull("my-image:tag", Optional.of(options));
 * </pre>
 */
@Getter
@Builder
public class PullOptions {
    /**
     * Target platform for multi-arch image selection.
     * If not specified, defaults to linux/amd64.
     */
    private final Platform platform;
}
