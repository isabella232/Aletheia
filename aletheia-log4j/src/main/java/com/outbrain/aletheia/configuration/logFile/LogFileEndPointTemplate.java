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
  private final boolean immediateFlush;

  public LogFileEndPointTemplate(@JsonProperty(value = "folder", required = true) final String folder,
                                 @JsonProperty(value = "filename", required = true) final String filename,
                                 @JsonProperty(value = "immediateFlush") final Boolean immediateFlush) {
    this.folder = folder;
    this.filename = filename;
    this.immediateFlush = immediateFlush != null ? immediateFlush : true;
  }

  @Override
  public ProductionEndPoint getProductionEndPoint(final String endPointName) {
    return new LogFileProductionEndPoint(filename, folder, datePattern, layout, immediateFlush);
  }

  @Override
  public ConsumptionEndPoint getConsumptionEndPoint(final String endPointName) {
    throw  new NotImplementedException("Consuming a log file endpoint is not supported in this version.");
  }
}
