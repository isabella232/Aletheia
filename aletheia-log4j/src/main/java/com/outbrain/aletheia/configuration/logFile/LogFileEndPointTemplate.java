package com.outbrain.aletheia.configuration.logFile;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.outbrain.aletheia.configuration.endpoint.EndPointTemplate;
import com.outbrain.aletheia.datum.consumption.ConsumptionEndPoint;
import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import com.outbrain.aletheia.datum.production.logFile.LogFileProductionEndPoint;
import org.apache.commons.lang3.NotImplementedException;

/**
 * Provides log file {@link ProductionEndPoint}s and {@link ConsumptionEndPoint}s.
 */
public class LogFileEndPointTemplate implements EndPointTemplate {

  public static final String TYPE = "logFile";

  private final static String datePattern = "%d{yyyy-MM-dd-HH}.data";
  private final static String layout = "%m%n";

  private final String folder;
  private final String filename;

  public LogFileEndPointTemplate(@JsonProperty(value = "folder", required = true) final String folder,
                                 @JsonProperty(value = "filename", required = true) final String filename) {
    this.folder = folder;
    this.filename = filename;
  }

  @Override
  public ProductionEndPoint getProductionEndPoint(final String endPointName) {
    return new LogFileProductionEndPoint(filename, folder, datePattern, layout);
  }

  @Override
  public ConsumptionEndPoint getConsumptionEndPoint(final String endPointName) {
    throw  new NotImplementedException("Consuming a log file endpoint is not supported in this version.");
  }
}
