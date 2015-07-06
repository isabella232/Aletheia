package com.outbrain.aletheia.configuration.hive;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.outbrain.aletheia.configuration.endpoint.EndPointTemplate;
import com.outbrain.aletheia.configuration.logFile.LogFileEndPointTemplate;
import com.outbrain.aletheia.datum.consumption.ConsumptionEndPoint;
import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import org.apache.commons.lang3.NotImplementedException;

import java.util.List;

/**
 * Provides hive {@link ProductionEndPoint}s and {@link ConsumptionEndPoint}s, this type of endpoints is based on the
 * {@link LogFileEndPointTemplate}.
 */
public class HiveTableEndPointTemplate implements EndPointTemplate {

  public static final String TYPE = "hive";

  private final String hdfsNamenodeAddress;
  private final String hiveTablePath;
  private final List<String> partitions;
  private final LogFileEndPointTemplate logFileEndPointTemplate;

  public HiveTableEndPointTemplate(@JsonProperty(value = "hdfs.namenode", required = true) final String hdfsNamenodeAddress,
                                   @JsonProperty(value = "hive.table.path", required = true) final String hiveTablePath,
                                   @JsonProperty("partitions") final List<String> partitions,
                                   @JsonProperty(value = "produce", required = true) final LogFileEndPointTemplate logFileEndPointTemplate) {
    this.hdfsNamenodeAddress = hdfsNamenodeAddress;
    this.hiveTablePath = hiveTablePath;
    this.partitions = partitions;
    this.logFileEndPointTemplate = logFileEndPointTemplate;
  }

  public String getHdfsNamenodeAddress() {
    return hdfsNamenodeAddress;
  }

  public String getHiveTablePath() {
    return hiveTablePath;
  }

  public List<String> getPartitions() {
    return partitions;
  }

  @Override
  public ProductionEndPoint getProductionEndPoint(final String endPointName) {
    return logFileEndPointTemplate.getProductionEndPoint(endPointName);
  }

  @Override
  public ConsumptionEndPoint getConsumptionEndPoint(final String endPointName) {
    throw new NotImplementedException("LogFile consumption endpoint is not supported");
  }
}
