package com.salesforce.multicloudj.registry.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.registry.model.Image;
import com.salesforce.multicloudj.registry.model.Layer;
import com.salesforce.multicloudj.registry.model.Manifest;
import com.salesforce.multicloudj.registry.model.Platform;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for AbstractRegistry. */
class AbstractRegistryTest {

  private static final String REGISTRY_ENDPOINT = "https://test-registry.example.com";
  private static final String REPOSITORY = "test-repo/test-image";
  private static final String TAG = "latest";
  private static final String IMAGE_REF = REPOSITORY + ":" + TAG;
  private static final String MANIFEST_DIGEST =
      "sha256:a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
  private static final String CONFIG_DIGEST =
      "sha256:c0nf1g0000000000000000000000000000000000000000000000000000000000";
  private static final String LAYER1_DIGEST =
      "sha256:1ayer100000000000000000000000000000000000000000000000000000000000";
  private static final String LAYER2_DIGEST =
      "sha256:1ayer200000000000000000000000000000000000000000000000000000000000";
  private static final String INDEX_DIGEST =
      "sha256:1ndex000000000000000000000000000000000000000000000000000000000000";
  private static final String AMD64_DIGEST =
      "sha256:amd64000000000000000000000000000000000000000000000000000000000000";
  private static final String ARM64_DIGEST =
      "sha256:arm64000000000000000000000000000000000000000000000000000000000000";
  private static final String LINUX = "linux";
  private static final String WINDOWS = "windows";
  private static final String AMD64 = "amd64";
  private static final String ARM64 = "arm64";
  private static final String TEST_PROVIDER = "test-provider";
  private static final String PROXY_ENDPOINT = "http://proxy.example.com:8080";

  @Mock private OciHttpTransport mockOciClient;

  private TestRegistry registry;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    registry = new TestRegistry(REGISTRY_ENDPOINT, mockOciClient);
  }

  @Nested
  class PullTests {

    @Test
    void pull_singleArchImage_returnsImage() throws Exception {
      Manifest imageManifest =
          Manifest.image(
              CONFIG_DIGEST, Arrays.asList(LAYER1_DIGEST, LAYER2_DIGEST), MANIFEST_DIGEST);
      when(mockOciClient.fetchManifest(REPOSITORY, TAG)).thenReturn(imageManifest);

      Image image = registry.pull(IMAGE_REF);

      assertNotNull(image);
      assertEquals(MANIFEST_DIGEST, image.getDigest());
      assertEquals(IMAGE_REF, image.getImageRef());
      verify(mockOciClient).fetchManifest(REPOSITORY, TAG);
    }

    @Test
    void pull_multiArchIndex_selectsPlatformAndFetchesManifest() throws Exception {
      Platform linuxAmd64 = Platform.builder().operatingSystem(LINUX).architecture(AMD64).build();
      Platform linuxArm64 = Platform.builder().operatingSystem(LINUX).architecture(ARM64).build();

      List<Manifest.IndexEntry> entries =
          Arrays.asList(
              new Manifest.IndexEntry(AMD64_DIGEST, linuxAmd64),
              new Manifest.IndexEntry(ARM64_DIGEST, linuxArm64));
      Manifest indexManifest = Manifest.index(entries, INDEX_DIGEST);
      Manifest imageManifest =
          Manifest.image(CONFIG_DIGEST, Arrays.asList(LAYER1_DIGEST), AMD64_DIGEST);

      when(mockOciClient.fetchManifest(REPOSITORY, TAG)).thenReturn(indexManifest);
      when(mockOciClient.fetchManifest(REPOSITORY, AMD64_DIGEST)).thenReturn(imageManifest);

      Image image = registry.pull(IMAGE_REF);

      assertNotNull(image);
      assertEquals(AMD64_DIGEST, image.getDigest());
      verify(mockOciClient).fetchManifest(REPOSITORY, TAG);
      verify(mockOciClient).fetchManifest(REPOSITORY, AMD64_DIGEST);
    }

    @Test
    void pull_byDigest_usesDigestAsReference() throws Exception {
      String digestRef = REPOSITORY + "@" + MANIFEST_DIGEST;
      Manifest imageManifest =
          Manifest.image(CONFIG_DIGEST, Arrays.asList(LAYER1_DIGEST), MANIFEST_DIGEST);
      when(mockOciClient.fetchManifest(REPOSITORY, MANIFEST_DIGEST)).thenReturn(imageManifest);

      Image image = registry.pull(digestRef);

      assertNotNull(image);
      assertEquals(MANIFEST_DIGEST, image.getDigest());
      verify(mockOciClient).fetchManifest(REPOSITORY, MANIFEST_DIGEST);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"  ", "\t", "\n"})
    void pull_invalidImageRef_throwsInvalidArgumentException(String invalidRef) {
      assertThrows(InvalidArgumentException.class, () -> registry.pull(invalidRef));
      verifyNoInteractions(mockOciClient);
    }

    @Test
    void pull_fetchManifestFails_propagatesException() throws Exception {
      when(mockOciClient.fetchManifest(anyString(), anyString()))
          .thenThrow(new UnknownException("Network error"));
      assertThrows(UnknownException.class, () -> registry.pull(IMAGE_REF));
    }
  }

  @Nested
  class SelectPlatformFromIndexTests {

    @Test
    void selectPlatformFromIndex_matchingPlatform_returnsDigest() {
      Platform linuxAmd64 = Platform.builder().operatingSystem(LINUX).architecture(AMD64).build();
      Platform linuxArm64 = Platform.builder().operatingSystem(LINUX).architecture(ARM64).build();

      List<Manifest.IndexEntry> entries =
          Arrays.asList(
              new Manifest.IndexEntry(ARM64_DIGEST, linuxArm64),
              new Manifest.IndexEntry(AMD64_DIGEST, linuxAmd64));
      Manifest indexManifest = Manifest.index(entries, INDEX_DIGEST);

      assertEquals(AMD64_DIGEST, registry.selectPlatformFromIndex(indexManifest, linuxAmd64));
    }

    @Test
    void selectPlatformFromIndex_noMatchingPlatform_throwsUnknownException() {
      Platform linuxAmd64 = Platform.builder().operatingSystem(LINUX).architecture(AMD64).build();
      Platform windowsAmd64 =
          Platform.builder().operatingSystem(WINDOWS).architecture(AMD64).build();

      List<Manifest.IndexEntry> entries =
          Collections.singletonList(new Manifest.IndexEntry(AMD64_DIGEST, linuxAmd64));
      Manifest indexManifest = Manifest.index(entries, INDEX_DIGEST);

      UnknownException ex =
          assertThrows(
              UnknownException.class,
              () -> registry.selectPlatformFromIndex(indexManifest, windowsAmd64));
      assertTrue(ex.getMessage().contains(WINDOWS + "/" + AMD64));
      assertTrue(ex.getMessage().contains(LINUX + "/" + AMD64));
    }

    @Test
    void selectPlatformFromIndex_entryWithNullOsOrArch_formatsAsUnknown() {
      Platform targetPlatform =
          Platform.builder().operatingSystem(WINDOWS).architecture(ARM64).build();
      List<Manifest.IndexEntry> entries =
          Collections.singletonList(
              new Manifest.IndexEntry(MANIFEST_DIGEST, Platform.builder().build()));
      Manifest indexManifest = Manifest.index(entries, INDEX_DIGEST);

      UnknownException ex =
          assertThrows(
              UnknownException.class,
              () -> registry.selectPlatformFromIndex(indexManifest, targetPlatform));
      assertTrue(ex.getMessage().contains("unknown/unknown"));
    }

    @Test
    void selectPlatformFromIndex_emptyOrNullEntries_throwsUnknownException() {
      assertThrows(
          UnknownException.class,
          () ->
              registry.selectPlatformFromIndex(
                  Manifest.index(Collections.emptyList(), INDEX_DIGEST), Platform.DEFAULT));
      assertThrows(
          UnknownException.class,
          () ->
              registry.selectPlatformFromIndex(
                  Manifest.index(null, INDEX_DIGEST), Platform.DEFAULT));
    }

    @Test
    void selectPlatformFromIndex_notAnIndex_throwsInvalidArgumentException() {
      Manifest imageManifest =
          Manifest.image(CONFIG_DIGEST, Arrays.asList(LAYER1_DIGEST), MANIFEST_DIGEST);
      assertThrows(
          InvalidArgumentException.class,
          () -> registry.selectPlatformFromIndex(imageManifest, Platform.DEFAULT));
    }

    @Test
    void selectPlatformFromIndex_entryWithNullPlatform_skipsEntry() {
      Platform linuxAmd64 = Platform.builder().operatingSystem(LINUX).architecture(AMD64).build();

      List<Manifest.IndexEntry> entries =
          Arrays.asList(
              new Manifest.IndexEntry(MANIFEST_DIGEST, null),
              new Manifest.IndexEntry(AMD64_DIGEST, linuxAmd64));
      Manifest indexManifest = Manifest.index(entries, INDEX_DIGEST);

      assertEquals(AMD64_DIGEST, registry.selectPlatformFromIndex(indexManifest, linuxAmd64));
    }
  }

  @Nested
  class ExtractTests {

    @Test
    void extract_invalidInput_throwsInvalidArgumentException() {
      assertThrows(InvalidArgumentException.class, () -> registry.extract(null));

      Image nullLayersImage = mock(Image.class);
      when(nullLayersImage.getLayers()).thenReturn(null);
      assertThrows(InvalidArgumentException.class, () -> registry.extract(nullLayersImage));

      Image emptyLayersImage = mock(Image.class);
      when(emptyLayersImage.getLayers()).thenReturn(Collections.emptyList());
      assertThrows(InvalidArgumentException.class, () -> registry.extract(emptyLayersImage));
    }

    @Test
    void extract_validImage_returnsInputStream() throws Exception {
      Image mockImage = mock(Image.class);
      Layer mockLayer = mock(Layer.class);
      when(mockLayer.getUncompressed()).thenReturn(new ByteArrayInputStream(new byte[1024]));
      when(mockImage.getLayers()).thenReturn(Collections.singletonList(mockLayer));

      try (InputStream result = registry.extract(mockImage)) {
        assertNotNull(result);
      }
    }
  }

  @Nested
  class BuilderTests {

    @Test
    void builder_platformHandling() {
      TestRegistry defaultReg =
          new TestRegistry.TestBuilder().withRegistryEndpoint(REGISTRY_ENDPOINT).build();
      assertEquals(Platform.DEFAULT, defaultReg.getTargetPlatform());

      TestRegistry nullPlatformReg =
          new TestRegistry.TestBuilder()
              .withRegistryEndpoint(REGISTRY_ENDPOINT)
              .withPlatform(null)
              .build();
      assertEquals(Platform.DEFAULT, nullPlatformReg.getTargetPlatform());

      Platform customPlatform =
          Platform.builder().operatingSystem(LINUX).architecture(ARM64).build();
      TestRegistry customReg =
          new TestRegistry.TestBuilder()
              .withRegistryEndpoint(REGISTRY_ENDPOINT)
              .withPlatform(customPlatform)
              .build();
      assertEquals(customPlatform, customReg.getTargetPlatform());
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"  ", "\t"})
    void builder_withInvalidEndpoint_throwsInvalidArgumentException(String invalidEndpoint) {
      assertThrows(
          InvalidArgumentException.class,
          () -> new TestRegistry.TestBuilder().withRegistryEndpoint(invalidEndpoint).build());
    }

    @Test
    void builder_optionalFields() {
      TestRegistry reg =
          new TestRegistry.TestBuilder()
              .withRegistryEndpoint(REGISTRY_ENDPOINT)
              .withProxyEndpoint(URI.create(PROXY_ENDPOINT))
              .withCredentialsOverrider(mock(CredentialsOverrider.class))
              .providerId(TEST_PROVIDER)
              .build();

      assertNotNull(reg);
      assertEquals(TEST_PROVIDER, reg.getProviderId());
    }
  }

  static class TestRegistry extends AbstractRegistry {

    private final OciHttpTransport ociClient;

    TestRegistry(String endpoint, OciHttpTransport client) {
      super(new TestBuilder().withRegistryEndpoint(endpoint));
      this.ociClient = client;
    }

    private TestRegistry(TestBuilder builder) {
      super(builder);
      this.ociClient = null;
    }

    @Override
    public Builder<?, ?> builder() {
      return new TestBuilder();
    }

    @Override
    public String getAuthorizationHeader(AuthChallenge challenge, String repository) {
      return "Basic dGVzdHVzZXI6dGVzdHRva2Vu";
    }

    @Override
    protected OciHttpTransport getOciTransport() {
      return ociClient;
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
      return UnknownException.class;
    }

    @Override
    public void close() {
      // No-op for tests
    }

    static class TestBuilder extends Builder<TestRegistry, TestBuilder> {

      @Override
      public TestBuilder self() {
        return this;
      }

      @Override
      public TestRegistry build() {
        if (registryEndpoint == null || registryEndpoint.isBlank()) {
          throw new InvalidArgumentException("Registry endpoint is required");
        }
        return new TestRegistry(this);
      }
    }
  }
}
