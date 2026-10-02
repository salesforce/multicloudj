package com.salesforce.multicloudj.registry;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.examples.AppConfig;
import com.salesforce.multicloudj.registry.client.ContainerRegistryClient;
import com.salesforce.multicloudj.registry.model.Image;
import com.salesforce.multicloudj.registry.model.Layer;
import com.salesforce.multicloudj.registry.model.Platform;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class demonstrating Container Registry operations across different cloud providers. This
 * example shows how to use the multicloudj library to pull images from cloud container registries.
 *
 * <p>Usage: {@code java -cp examples/target/multicloudj-examples-<version>.jar
 * com.salesforce.multicloudj.registry.Main [provider-id]}. The provider id defaults to the global
 * {@code provider} and the region comes from the global {@code region}. Endpoint, repository and
 * tag come from the {@code registry.*} keys in {@code examples.properties}.
 */
public class Main {
  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  // Demo settings
  private static final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

  // Runtime configuration, resolved from examples.properties
  private final String provider;
  private final String registryEndpoint;
  private final String repository;
  private final String tag;
  private final String region;

  /** Constructor that accepts provider configuration; other values come from configuration. */
  public Main(String provider) {
    this.provider = provider;
    this.registryEndpoint = AppConfig.get("registry.endpoint");
    this.repository = AppConfig.get("registry.repository");
    this.tag = AppConfig.get("registry.tag");
    this.region = AppConfig.get("region");
  }

  public static void main(String[] args) {
    // Parse command line arguments
    String provider = parseProvider(args);
    Main main = new Main(provider);

    // Display welcome banner
    printWelcomeBanner();

    // Display configuration
    main.printConfiguration();

    main.runDemo();

    // Display completion banner
    printCompletionBanner();

    // Close reader
    try {
      reader.close();
    } catch (IOException e) {
      // Ignore
    }
  }

  /** Print a welcome banner for the demo. */
  private static void printWelcomeBanner() {
    logger.info("");
    logger.info("╔══════════════════════════════════════════════════════════════════════════════╗");
    logger.info("║           🚀 MultiCloudJ Container Registry Demo 🚀                        ║");
    logger.info("║             Cross-Cloud Container Registry Operations                       ║");
    logger.info("╚══════════════════════════════════════════════════════════════════════════════╝");
    logger.info("");
  }

  /** Print the configuration being used. */
  private void printConfiguration() {
    logger.info("📋 Configuration:");
    logger.info("   Provider:   {}", provider);
    logger.info("   Endpoint:   {}", registryEndpoint);
    logger.info("   Repository: {}", repository);
    logger.info("   Tag:        {}", tag);
    logger.info("");
    waitForEnter("Press Enter to start the demo...");
  }

  /** Print a completion banner. */
  private static void printCompletionBanner() {
    logger.info("");
    logger.info("╔══════════════════════════════════════════════════════════════════════════════╗");
    logger.info("║                    ✅ Demo Completed Successfully! ✅                       ║");
    logger.info("║                    Thanks for trying MultiCloudJ!                          ║");
    logger.info("╚══════════════════════════════════════════════════════════════════════════════╝");
    logger.info("");
    waitForEnter("Press Enter to exit...");
  }

  /** Parse provider from command line arguments, falling back to configuration. */
  private static String parseProvider(String[] args) {
    if (args.length > 0 && args[0] != null && !args[0].trim().isEmpty()) {
      return args[0].trim();
    }
    // Never hardcode the provider id; resolve it from configuration (see examples.properties)
    return AppConfig.provider();
  }

  /** Wait for user to press Enter key. */
  private static void waitForEnter(String message) {
    logger.info(message);
    try {
      reader.readLine();
    } catch (IOException e) {
      logger.info("(Continuing automatically...)");
    }
  }

  /** Display a success message with emoji. */
  private static void showSuccess(String message) {
    logger.info("✅ {}", message);
  }

  /** Display an info message with emoji. */
  private static void showInfo(String message) {
    logger.info("ℹ️  {}", message);
  }

  /** Display an error message with emoji. */
  private static void showError(String message) {
    logger.error("❌ {}", message);
  }

  /** Display a section header. */
  private static void showSectionHeader(String title) {
    logger.info("");
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    logger.info("📚 {}", title);
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    logger.info("");
  }

  /** Display a section header with pause for major transitions. */
  private static void showSectionHeaderWithPause(String title) {
    showSectionHeader(title);
    waitForEnter("Press Enter to start this section...");
  }

  /** Initialize the ContainerRegistryClient with appropriate configuration. */
  private ContainerRegistryClient initializeClient() {
    return ContainerRegistryClient.builder(provider)
        .withRegistryEndpoint(registryEndpoint)
        .withRegion(region)
        .build();
  }

  /** Main demo method that orchestrates all the registry operations. */
  private void runDemo() {
    showInfo("Starting Registry demo with provider: " + provider);

    try {
      demonstratePullImage();
      demonstratePullByDigest();
      demonstratePlatformSelection();
      demonstrateExtractFilesystem();
      demonstrateErrorHandling();

      showSuccess("Registry demo completed successfully!");
    } catch (Exception e) {
      showError("Demo failed: " + e.getMessage());
      logger.error("Demo failed", e);
    }
  }

  /** Demonstrates pulling an image by tag via {@link ContainerRegistryClient#pull(String)}. */
  private void demonstratePullImage() {
    showSectionHeaderWithPause("Pull Image by Tag");

    String imageRef = repository + ":" + tag;
    showInfo("Pulling image: " + imageRef);

    try (ContainerRegistryClient client = initializeClient()) {
      // Pull returns a lazy Image — the manifest is fetched, but layers are not yet downloaded
      Image image = client.pull(imageRef);

      showSuccess("Image pulled:");
      showInfo("  Image ref: " + image.getImageRef());
      showInfo("  Digest:    " + image.getDigest());

      // getLayers() triggers the actual blob downloads
      showInfo("Accessing layers (triggers blob downloads)...");
      List<Layer> layers = image.getLayers();
      showSuccess("Layers loaded: " + layers.size());

      for (int i = 0; i < layers.size(); i++) {
        Layer layer = layers.get(i);
        long size = layer.getSize();
        String sizeStr = size >= 0 ? " (" + size + " bytes)" : "";
        showInfo("  Layer " + (i + 1) + ": " + layer.getDigest() + sizeStr);
      }

    } catch (ResourceNotFoundException e) {
      showError("Image not found: " + e.getMessage());
    } catch (SubstrateSdkException e) {
      showError("Failed to pull image: " + e.getMessage());
    } catch (Exception e) {
      showError("Unexpected error: " + e.getMessage());
      logger.error("Pull image demo failed", e);
    }

    waitForEnter("Press Enter to continue...");
  }

  /** Demonstrates pulling an image by digest for content-addressable retrieval. */
  private void demonstratePullByDigest() {
    showSectionHeaderWithPause("Pull Image by Digest");

    String imageRef = repository + ":" + tag;
    showInfo("First, pulling by tag to obtain digest: " + imageRef);

    try (ContainerRegistryClient client = initializeClient()) {
      Image image = client.pull(imageRef);
      String digest = image.getDigest();
      showSuccess("Obtained digest: " + digest);

      // Now pull the same image by its digest
      String digestRef = repository + "@" + digest;
      showInfo("Pulling by digest: " + digestRef);
      Image imageByDigest = client.pull(digestRef);

      showSuccess("Image pulled by digest:");
      showInfo("  Digest: " + imageByDigest.getDigest());

      if (digest.equals(imageByDigest.getDigest())) {
        showSuccess("Digest verified — same image content guaranteed");
      }

    } catch (ResourceNotFoundException e) {
      showError("Image not found: " + e.getMessage());
    } catch (SubstrateSdkException e) {
      showError("Failed to pull image by digest: " + e.getMessage());
    } catch (Exception e) {
      showError("Unexpected error: " + e.getMessage());
      logger.error("Pull by digest demo failed", e);
    }

    waitForEnter("Press Enter to continue...");
  }

  /**
   * Demonstrates platform-specific image selection for multi-arch registries.
   *
   * <p>Platform fields:
   *
   * <ul>
   *   <li>{@code operatingSystem} – e.g., {@code "linux"}, {@code "windows"}
   *   <li>{@code architecture} – e.g., {@code "amd64"}, {@code "arm64"}
   *   <li>{@code variant} – optional, e.g., {@code "v8"} for arm64
   * </ul>
   *
   * <p>For single-arch images, the platform setting is ignored.
   */
  private void demonstratePlatformSelection() {
    showSectionHeaderWithPause("Platform Selection (Multi-Arch)");

    String imageRef = repository + ":" + tag;

    // Default platform (linux/amd64)
    showInfo("Pulling with default platform (linux/amd64)...");
    try (ContainerRegistryClient client = initializeClient()) {
      Image image = client.pull(imageRef);
      showSuccess("Pulled with default platform: " + image.getDigest());
    } catch (SubstrateSdkException e) {
      showError("Pull failed: " + e.getMessage());
    } catch (Exception e) {
      showError("Unexpected error: " + e.getMessage());
      logger.error("Platform selection demo failed", e);
    }

    // Explicit linux/arm64 platform
    showInfo("Pulling with explicit linux/arm64 platform...");
    Platform arm64 = Platform.builder().operatingSystem("linux").architecture("arm64").build();

    try (ContainerRegistryClient client =
        ContainerRegistryClient.builder(provider)
            .withRegistryEndpoint(registryEndpoint)
            .withRegion(region)
            .withPlatform(arm64)
            .build()) {

      Image image = client.pull(imageRef);
      showSuccess("Pulled linux/arm64 image: " + image.getDigest());

    } catch (ResourceNotFoundException e) {
      showError("arm64 variant not found: " + e.getMessage());
    } catch (SubstrateSdkException e) {
      showError("Pull failed for arm64: " + e.getMessage());
    } catch (Exception e) {
      showError("Unexpected error: " + e.getMessage());
      logger.error("Platform selection demo failed", e);
    }

    waitForEnter("Press Enter to continue...");
  }

  /**
   * Demonstrates extracting the full image filesystem via {@link
   * ContainerRegistryClient#extract(Image)}.
   */
  private void demonstrateExtractFilesystem() {
    showSectionHeaderWithPause("Extract Image Filesystem");

    String imageRef = repository + ":" + tag;
    showInfo("Pulling image and extracting filesystem: " + imageRef);

    try (ContainerRegistryClient client = initializeClient()) {
      Image image = client.pull(imageRef);
      showSuccess("Image pulled: " + image.getDigest());

      showInfo("Extracting flattened filesystem as tar stream...");
      try (InputStream tarStream = client.extract(image)) {
        // In real usage, the tar stream would be written to disk or processed further.
        // Here we just read a few bytes to confirm the stream is valid.
        byte[] header = new byte[512];
        int bytesRead = tarStream.read(header);
        showSuccess("Filesystem tar stream opened, first block read: " + bytesRead + " bytes");
      }

    } catch (ResourceNotFoundException e) {
      showError("Image not found: " + e.getMessage());
    } catch (SubstrateSdkException e) {
      showError("Failed to extract image: " + e.getMessage());
    } catch (Exception e) {
      showError("Unexpected error: " + e.getMessage());
      logger.error("Extract filesystem demo failed", e);
    }

    waitForEnter("Press Enter to continue to error handling...");
  }

  /** Demonstrates error handling for common registry error conditions. */
  private void demonstrateErrorHandling() {
    showSectionHeaderWithPause("Error Handling");

    // Case 1: Pull a non-existent tag → ResourceNotFoundException
    showInfo("Pulling a non-existent tag (expect ResourceNotFoundException)...");
    try (ContainerRegistryClient client = initializeClient()) {
      client.pull(repository + ":non-existent-tag-that-does-not-exist");
      showError("Expected ResourceNotFoundException was not thrown");
    } catch (ResourceNotFoundException e) {
      showSuccess("Correctly caught ResourceNotFoundException: " + truncate(e.getMessage(), 100));
    } catch (SubstrateSdkException e) {
      showError(
          "Unexpected SDK exception: " + e.getClass().getSimpleName() + " - " + e.getMessage());
    } catch (Exception e) {
      showError("Unexpected error: " + e.getMessage());
      logger.error("Error handling demo failed", e);
    }

    // Case 2: Pull with a blank image reference → InvalidArgumentException
    showInfo("Pulling with a blank image reference (expect InvalidArgumentException)...");
    try (ContainerRegistryClient client = initializeClient()) {
      client.pull("   ");
      showError("Expected InvalidArgumentException was not thrown");
    } catch (InvalidArgumentException e) {
      showSuccess("Correctly caught InvalidArgumentException: " + e.getMessage());
    } catch (Exception e) {
      showError("Unexpected error: " + e.getMessage());
    }

    // Case 3: Build a client without a required field → InvalidArgumentException
    showInfo("Building client without registry endpoint (expect InvalidArgumentException)...");
    try {
      ContainerRegistryClient.builder(provider).build(); // endpoint not set
      showError("Expected InvalidArgumentException was not thrown");
    } catch (InvalidArgumentException e) {
      showSuccess("Correctly caught InvalidArgumentException: " + e.getMessage());
    }
  }

  private static String truncate(String s, int maxLen) {
    if (s == null) {
      return "null";
    }
    return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
  }
}
