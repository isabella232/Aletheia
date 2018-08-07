package com.outbrain.aletheia.datum.production.logFile;

import com.outbrain.aletheia.datum.production.DeliveryCallback;
import com.outbrain.aletheia.datum.production.EmptyCallback;
import com.outbrain.aletheia.datum.production.NamedSender;
import com.outbrain.aletheia.datum.production.logFile.writer.DataFileWriterFactory;
import com.outbrain.aletheia.datum.production.logFile.writer.ExtrasRollingAppenderFactory;
import com.outbrain.aletheia.metrics.common.Counter;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import com.outbrain.aletheia.metrics.common.Summary;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link com.outbrain.aletheia.datum.production.NamedSender} implementation that writes data to log files.
 * Deliver with callback api is not suppoted in this implementation, callbacks will be ignored.
 */
public class StringLogFileSender implements NamedSender<String> {

  private static final Logger logger = LoggerFactory.getLogger(StringLogFileSender.class);

  private final LogFileProductionEndPoint logFileDeliveryEndPoint;
  private final org.apache.log4j.Logger dataLogFileWriter;
  private final Counter logWriteSuccessCount;
  private final Counter logWriteFailureCount;
  private final Summary sendDuration;

  public StringLogFileSender(final LogFileProductionEndPoint logFileDeliveryEndPoint,
                             final MetricsFactory metricFactory) {

    this.logFileDeliveryEndPoint = logFileDeliveryEndPoint;
    this.dataLogFileWriter = getLogFileWriter(logFileDeliveryEndPoint);
    logWriteSuccessCount = metricFactory.createCounter("sendAttemptsSuccess", "Counts successful attempts to send line");
    sendDuration = metricFactory.createSummary("sendAttemptsDuration", "Accumulated duration of the send attempts");
    logWriteFailureCount = metricFactory.createCounter("sendAttemptsFailure", "Counts failed attempts to send line");

    logger.warn("*** Please note deliver with callback API is not supported for Log Files ***");
  }

  private org.apache.log4j.Logger getLogFileWriter(final LogFileProductionEndPoint logFileDeliveryEndPoint) {

    final DataFileWriterFactory dataFileWriterFactory =
            new DataFileWriterFactory(
                    logFileDeliveryEndPoint.getFolderFullName(),
                    new ExtrasRollingAppenderFactory(
                            logFileDeliveryEndPoint.getDatePatternForExtras(),
                            logFileDeliveryEndPoint.getLayout(),
                            logFileDeliveryEndPoint.getImmediateFlush()));

    return dataFileWriterFactory.createLogger(logFileDeliveryEndPoint.getShortFileName());
  }

  @Override
  public void send(final String line) {
    send(line, EmptyCallback.getEmptyCallback());
  }

  @Override
  public void send(final String line, final DeliveryCallback deliveryCallback) {
    com.outbrain.swinfra.metrics.timing.Timer timer = sendDuration.startTimer();

    try {
      final String chompedString = StringUtils.chomp(line);

      dataLogFileWriter.info(chompedString);
      logWriteSuccessCount.inc();

    } catch (final Exception ex) {
      logger.error("failed to write data to file.", ex);
      logWriteFailureCount.inc();
    } finally {
      timer.stop();
    }
  }

  @Override
  public void close() {
  }

  @Override
  public String getName() {
    return logFileDeliveryEndPoint.getName();
  }
}
