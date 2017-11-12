package com.outbrain.aletheia.datum.production.logFile.writer;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.EnhancedPatternLayout;
import org.apache.log4j.LogManager;
import org.apache.log4j.rolling.RollingFileAppender;
import org.apache.log4j.rolling.TimeBasedRollingPolicy;

import java.util.Collections;
import java.util.Set;

/**
 * Created by slevin on 2/6/14.
 */
public class ExtrasRollingAppenderFactory {

  private final String suffixPattern;
  private final String layout;
  private final boolean immediateFlush;

  public ExtrasRollingAppenderFactory(final String suffixPattern, final String layout, final boolean immediateFlush) {
    this.suffixPattern = suffixPattern;
    this.layout = layout;
    this.immediateFlush = immediateFlush;
  }

  private TimeBasedRollingPolicy getTimeBasedRollingPolicy(final String fileNamePattern) {
    final TimeBasedRollingPolicy policy = new TimeBasedRollingPolicy();
    policy.setFileNamePattern(fileNamePattern);
    return policy;
  }

  public Set<? extends Appender> createNewAppender(final String appenderName,
                                                   final String logFolderFullPath,
                                                   final String shortFileName) {

    final String fullFileName = FilenameUtils.concat(logFolderFullPath, shortFileName);
    final String fileNamePattern = String.format("%s.%s", fullFileName, suffixPattern);

    final RollingFileAppender rollingFileAppender = new RollingFileAppender();
    rollingFileAppender.setName(appenderName);
    rollingFileAppender.setImmediateFlush(immediateFlush);

    Runtime.getRuntime().addShutdownHook(new Thread(LogManager::shutdown));

    rollingFileAppender.setLayout(new EnhancedPatternLayout(layout));

    final TimeBasedRollingPolicy timeBasedRollingPolicy = getTimeBasedRollingPolicy(fileNamePattern);
    rollingFileAppender.setRollingPolicy(timeBasedRollingPolicy);

    rollingFileAppender.activateOptions();

    return Collections.singleton(rollingFileAppender);
  }
}
