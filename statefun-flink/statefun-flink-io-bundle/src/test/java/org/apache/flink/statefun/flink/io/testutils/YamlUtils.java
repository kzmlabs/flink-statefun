// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.io.testutils;

import java.io.IOException;
import java.net.URL;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public final class YamlUtils {

  private YamlUtils() {}

  public static JsonNode loadAsJsonFromClassResource(ClassLoader classLoader, String pathToYaml) {
    URL moduleUrl = classLoader.getResource(pathToYaml);
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    try {
      return mapper.readTree(moduleUrl);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
