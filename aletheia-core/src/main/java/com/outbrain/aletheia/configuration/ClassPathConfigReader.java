package com.outbrain.aletheia.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class ClassPathConfigReader implements ConfigReader {

  private final ClassLoader classLoader;

  public ClassPathConfigReader(final ClassLoader classLoader) {
    this.classLoader = classLoader;
  }


  @Override
  public InputStream read(URI configUri) throws IOException {
    return classLoader.getResourceAsStream(configUri.getPath());
  }
}
