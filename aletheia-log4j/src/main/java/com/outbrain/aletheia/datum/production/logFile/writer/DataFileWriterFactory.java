package com.outbrain.aletheia.datum.production.logFile.writer;

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class DataFileWriterFactory {

  private final Logger myLogger = LoggerFactory.getLogger(DataFileWriterFactory.class);

  private final String logFolder;

  private final ExtrasRollingAppenderFactory appenderFactory;

  public DataFileWriterFactory(final String logFolder, final ExtrasRollingAppenderFactory appenderFactory) {
    this.logFolder = logFolder;
    this.appenderFactory = appenderFactory;
  }

  private Logger createOrGetConfiguredLogger(final String shortLogFileName) {

    final org.apache.log4j.Logger logger = createOrGetLogger(shortLogFileName);

    final String appenderName = appenderNameFrom(shortLogFileName);

    if (!hasAppender(logger, appenderName)) {
      final Set<? extends Appender> appenders = appenderFactory.createNewAppender(appenderName,
                                                                                  logFolder,
                                                                                  shortLogFileName);
      logger.setLevel(Level.INFO);
      logger.setAdditivity(false);

      for (final Appender appender : appenders) {
        logger.addAppender(appender);
      }
    } else {
      myLogger.warn(String.format("Attempted to create multiple logger/appender instances for data file [%s]," +
                                          " ignoring and using the existing one.", shortLogFileName));
    }

    return LoggerFactory.getLogger(logger.getName());
  }

  private boolean hasAppender(final org.apache.log4j.Logger logger, final String appenderName) {
    return logger.getAppender(appenderName) != null;
  }

  private org.apache.log4j.Logger createOrGetLogger(final String shortLogFileName) {

    final org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();

    return rootLogger.getLoggerRepository().getLogger(loggerNameFrom(shortLogFileName));
  }

  private String loggerNameFrom(final String shortLogFileName) {
    return "programmatic_logger_" + shortLogFileName;
  }

  private String appenderNameFrom(final String shortLogFileName) {
    return "programmatic_appender_" + shortLogFileName;
  }

  public Logger createLogger(final String shortLogFileName) {
    return createOrGetConfiguredLogger(shortLogFileName);
  }
}