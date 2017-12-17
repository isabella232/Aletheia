package com.outbrain.aletheia.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public interface ConfigReader {
  InputStream read(final URI configUri) throws IOException;
}
