package com.salesforce.multicloudj.examples;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Environment-specific configuration for the examples.
 *
 * <p>The examples are substrate agnostic: configuration keys never name a cloud, and the configured
 * values decide which substrate an example connects to.
 *
 * <p>Hardcoding provider ids, regions, bucket names, account ids and similar values in application
 * code is an anti-pattern: it ties the code to a single cloud and environment and defeats the
 * portability MultiCloudJ exists to provide. The examples therefore read every such value through
 * this class instead of embedding it.
 *
 * <p>A key such as {@code blob.bucket} is resolved from, in order:
 *
 * <ol>
 *   <li>the JVM system property {@code -Dblob.bucket=...}
 *   <li>the environment variable {@code BLOB_BUCKET} (upper case, {@code .} replaced by {@code _})
 *   <li>a local properties file given by {@code -Dexamples.config=/path/to/file.properties}
 *   <li>the bundled {@code examples.properties}
 * </ol>
 *
 * <p>Values that still contain a {@code <placeholder>} are treated as unset.
 */
public final class AppConfig {

  private static final String BUNDLED_RESOURCE = "/examples.properties";
  private static final String CONFIG_FILE_PROPERTY = "examples.config";
  private static final String PROVIDER_KEY = "provider";
  private static final Pattern PLACEHOLDER = Pattern.compile("<[^>]+>");
  private static final Properties FILE_PROPERTIES = loadFileProperties();

  private AppConfig() {}

  /**
   * Returns the value for {@code key}.
   *
   * @throws IllegalStateException if the key is unset, blank, or still a placeholder
   */
  public static String get(String key) {
    String value = getOptional(key);
    if (value == null) {
      throw missing(key);
    }
    return value;
  }

  /** Returns the value for {@code key}, or {@code null} if it is unset, blank, or a placeholder. */
  public static String getOptional(String key) {
    return firstSet(fromOverrides(key), fromFile(key));
  }

  /**
   * Returns the global provider id shared by every example ({@code provider}, environment variable
   * {@code PROVIDER}).
   */
  public static String provider() {
    return get(PROVIDER_KEY);
  }

  /**
   * Returns the provider id for {@code service}: the optional {@code <service>.provider} override,
   * falling back to the global {@code provider}. The override exists for services whose provider
   * ids name a backend within a cloud rather than the cloud itself.
   */
  public static String provider(String service) {
    String override = getOptional(service + "." + PROVIDER_KEY);
    return override != null ? override : provider();
  }

  private static String fromOverrides(String key) {
    return firstSet(normalize(System.getProperty(key)), normalize(System.getenv(toEnvName(key))));
  }

  private static String fromFile(String key) {
    return normalize(FILE_PROPERTIES.getProperty(key));
  }

  private static String normalize(String value) {
    if (value == null || value.trim().isEmpty() || PLACEHOLDER.matcher(value).find()) {
      return null;
    }
    return value.trim();
  }

  private static String firstSet(String... values) {
    for (String value : values) {
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  private static String toEnvName(String key) {
    return key.toUpperCase(Locale.ROOT).replace('.', '_');
  }

  private static IllegalStateException missing(String key) {
    return new IllegalStateException(
        String.format(
            "Example configuration '%s' is not set. Set -D%s=..., the %s environment variable, or"
                + " '%s' in examples.properties (or the file given by -D%s).",
            key, key, toEnvName(key), key, CONFIG_FILE_PROPERTY));
  }

  private static Properties loadFileProperties() {
    Properties properties = new Properties();
    try (InputStream in = AppConfig.class.getResourceAsStream(BUNDLED_RESOURCE)) {
      if (in != null) {
        properties.load(in);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to load " + BUNDLED_RESOURCE, e);
    }

    // A local file overlays the bundled defaults, so real values never need to be committed.
    String localFile = System.getProperty(CONFIG_FILE_PROPERTY);
    if (localFile != null && !localFile.trim().isEmpty()) {
      try (InputStream in = Files.newInputStream(Path.of(localFile.trim()))) {
        properties.load(in);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to load " + localFile, e);
      }
    }
    return properties;
  }
}
