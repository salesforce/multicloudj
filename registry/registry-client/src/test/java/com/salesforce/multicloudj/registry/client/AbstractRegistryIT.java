package com.salesforce.multicloudj.registry.client;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import com.salesforce.multicloudj.registry.model.Image;
import java.io.InputStream;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractRegistryIT {

  // Define the Harness interface
  public interface Harness extends AutoCloseable {
    AbstractRegistry createRegistryDriver();

    String getEndpoint();

    String getProviderId();

    int getPort();

    String getTestImageRef();

    default List<String> getWiremockExtensions() {
      return List.of();
    }
  }

  protected abstract Harness createHarness();

  private Harness harness;
  protected AbstractRegistry registry;

  @BeforeAll
  public void initializeWireMockServer() {
    harness = createHarness();
    List<String> extensions = harness.getWiremockExtensions();
    TestsUtil.startWireMockServer(
        "src/test/resources", harness.getPort(), extensions.toArray(new String[0]));
  }

  @AfterAll
  public void shutdownWireMockServer() throws Exception {
    TestsUtil.stopWireMockServer();
    harness.close();
  }

  @BeforeEach
  public void setupTestEnvironment(TestInfo testInfo) {
    String testClassName = testInfo.getTestClass().map(Class::getSimpleName).orElse("Unknown");
    String testMethodName =
        testInfo.getTestMethod().map(java.lang.reflect.Method::getName).orElse("unknown");
    TestsUtil.startWireMockRecording(harness.getEndpoint(), testClassName, testMethodName);
    registry = harness.createRegistryDriver();
  }

  @AfterEach
  public void cleanupTestEnvironment() throws Exception {
    TestsUtil.stopWireMockRecording();
    if (registry != null) {
      registry.close();
    }
  }

  @Test
  public void testPull() {
    String imageRef = harness.getTestImageRef();
    Image image = registry.pull(imageRef);

    assertNotNull(image, "Pulled image should not be null");
    assertNotNull(image.getDigest(), "Image digest should not be null");
    assertNotNull(image.getLayers(), "Layers should not be null");
    assertFalse(image.getLayers().isEmpty(), "Image should have at least one layer");
  }

  @Test
  public void testExtract() throws Exception {
    String imageRef = harness.getTestImageRef();
    Image image = registry.pull(imageRef);
    assertNotNull(image);

    try (InputStream tar = registry.extract(image)) {
      assertNotNull(tar, "Extracted tar stream should not be null");
      byte[] buffer = new byte[1024];
      int totalRead = 0;
      int bytesRead;
      while ((bytesRead = tar.read(buffer)) != -1) {
        totalRead += bytesRead;
      }
      assertTrue(totalRead > 0, "Extracted tar should contain data");
    }
  }
}
