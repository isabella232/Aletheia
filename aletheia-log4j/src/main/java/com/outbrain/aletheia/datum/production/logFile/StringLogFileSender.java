package com.outbrain.aletheia.datum.production.logFile;

import com.outbrain.aletheia.datum.production.NamedSender;
import com.outbrain.aletheia.datum.production.logFile.writer.DataFileWriterFactory;
import com.outbrain.aletheia.datum.production.logFile.writer.ExtrasRollingAppenderFactory;
import com.outbrain.aletheia.metrics.common.Counter;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringLogFileSender implements NamedSender<String> {

  private static final Logger logger = LoggerFactory.getLogger(StringLogFileSender.class);

  private final LogFileProductionEndPoint logFileDeliveryEndPoint;
  private final Logger dataLogFileWriter;
  private final Counter logWriteSuccessCount;
  private final Counter logWriteFailureCount;
  private final Counter sendDuration;

  public StringLogFileSender(final LogFileProductionEndPoint logFileDeliveryEndPoint,
                             final MetricsFactory metricFactory) {

    this.logFileDeliveryEndPoint = logFileDeliveryEndPoint;
    this.dataLogFileWriter = getLogFileWriter(logFileDeliveryEndPoint);
    logWriteSuccessCount = metricFactory.createCounter("Send.Attempts", "Success");
    sendDuration = metricFactory.createCounter("Send.Attempts", "Duration");
    logWriteFailureCount = metricFactory.createCounter("Send.Attempts", "Failure");
  }

  private Logger getLogFileWriter(final LogFileProductionEndPoint logFileDeliveryEndPoint) {

    final DataFileWriterFactory dataFileWriterFactory =
            new DataFileWriterFactory(
                    logFileDeliveryEndPoint.getFolderFullName(),
                    new ExtrasRollingAppenderFactory(
                            logFileDeliveryEndPoint.getDatePatternForExtras(),
                            logFileDeliveryEndPoint.getLayout()));

    return dataFileWriterFactory.createLogger(logFileDeliveryEndPoint.getShortFileName());
  }

  @Override
  public void send(final String line) {
    try {
      final long startTime = System.currentTimeMillis();
      final String chompedString = StringUtils.chomp(line);

      dataLogFileWriter.info(chompedString);
      logWriteSuccessCount.inc();

      final long duration = System.currentTimeMillis() - startTime;
      sendDuration.inc(duration);
    } catch (final Exception ex) {
      logger.error("failed to write data to file.", ex);
      logWriteFailureCount.inc();
    }
  }

  @Override
  public String getName() {
    return logFileDeliveryEndPoint.getName();
  }
}
