package com.salesforce.multicloudj.registry.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.registry.model.Layer;
import com.salesforce.multicloudj.registry.model.Manifest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Unit tests for RemoteImage. */
@ExtendWith(MockitoExtension.class)
public class RemoteImageTest {

  private static final String REPOSITORY = "test-repo/test-image";
  private static final String IMAGE_REF = "test-repo/test-image:latest";
  private static final String MANIFEST_DIGEST = "sha256:manifest123";
  private static final String LAYER_DIGEST_1 = "sha256:layer1";
  private static final String LAYER_DIGEST_2 = "sha256:layer2";
  private static final String LAYER_DIGEST_3 = "sha256:layer3";

  @Mock private OciRegistryClient mockClient;

  @Mock private Manifest mockManifest;

  @Test
  void testGetLayers_ReturnsMultipleLayers() {
    List<String> layerDigests = Arrays.asList(LAYER_DIGEST_1, LAYER_DIGEST_2, LAYER_DIGEST_3);
    when(mockManifest.getLayerDigests()).thenReturn(layerDigests);

    RemoteImage image = new RemoteImage(mockClient, REPOSITORY, IMAGE_REF, mockManifest);

    List<Layer> layers = image.getLayers();

    assertNotNull(layers);
    assertEquals(3, layers.size());
    assertEquals(LAYER_DIGEST_1, layers.get(0).getDigest());
    assertEquals(LAYER_DIGEST_2, layers.get(1).getDigest());
    assertEquals(LAYER_DIGEST_3, layers.get(2).getDigest());

    for (Layer layer : layers) {
      assertTrue(layer instanceof RemoteLayer);
    }
  }

  @Test
  void testGetLayers_ReturnsEmptyList_WhenNoLayers() {
    List<String> layerDigests = Collections.emptyList();
    when(mockManifest.getLayerDigests()).thenReturn(layerDigests);

    RemoteImage image = new RemoteImage(mockClient, REPOSITORY, IMAGE_REF, mockManifest);

    List<Layer> layers = image.getLayers();

    assertNotNull(layers);
    assertTrue(layers.isEmpty());
  }

  @Test
  void testGetLayers_ThrowsException_WhenLayerDigestsIsNull() {
    when(mockManifest.getLayerDigests()).thenReturn(null);

    RemoteImage image = new RemoteImage(mockClient, REPOSITORY, IMAGE_REF, mockManifest);

    InvalidArgumentException exception =
        assertThrows(InvalidArgumentException.class, () -> image.getLayers());

    assertTrue(exception.getMessage().contains("missing layer digests"));
  }

  @Test
  void testGetDigest_ReturnsManifestDigest() {
    when(mockManifest.getDigest()).thenReturn(MANIFEST_DIGEST);

    RemoteImage image = new RemoteImage(mockClient, REPOSITORY, IMAGE_REF, mockManifest);

    assertEquals(MANIFEST_DIGEST, image.getDigest());
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {""})
  void testGetDigest_ThrowsException_WhenManifestDigestIsNullOrEmpty(String digest) {
    when(mockManifest.getDigest()).thenReturn(digest);

    RemoteImage image = new RemoteImage(mockClient, REPOSITORY, IMAGE_REF, mockManifest);

    InvalidArgumentException exception =
        assertThrows(InvalidArgumentException.class, () -> image.getDigest());

    assertTrue(exception.getMessage().contains("missing digest"));
  }
}
