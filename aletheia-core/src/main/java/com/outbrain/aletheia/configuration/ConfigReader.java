package com.outbrain.aletheia.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Interface to different sort of configuration Readers.
 */
public interface ConfigReader {
  /**
   * @param configUri configuration location
   * @return stream of the configuration. i.e. json file.
   * @throws IOException
   */
  InputStream read(final URI configUri) throws IOException;
}
