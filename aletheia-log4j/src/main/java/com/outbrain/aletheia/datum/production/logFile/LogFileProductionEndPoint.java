package com.outbrain.aletheia.datum.production.logFile;

import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;


/**
 * Created by slevin on 8/5/14.
 */
public class LogFileProductionEndPoint implements ProductionEndPoint {

  private static final String LOG_FILE = "LogFile";

  protected final String shortFileName;
  protected final String folderFullName;
  protected final String datePatternForExtras;
  protected final String layout;

  public LogFileProductionEndPoint(final String shortFileName,
                                   final String folderFullName,
                                   final String datePatternForExtras,
                                   final String layout) {
    this.shortFileName = shortFileName;
    this.folderFullName = folderFullName;
    this.datePatternForExtras = datePatternForExtras;
    this.layout = layout;
  }

  public String getDatePatternForExtras() {
    return datePatternForExtras;
  }

  public String getLayout() {
    return layout;
  }

  public String getFolderFullName() {
    return folderFullName;
  }

  public String getShortFileName() {
    return shortFileName;
  }

  @Override
  public String getName() {
    return LOG_FILE;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}
