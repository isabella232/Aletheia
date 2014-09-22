package com.outbrain.aletheia.datum.production.logFile;

import com.outbrain.aletheia.datum.production.DatumEnvelopePeelingStringSender;
import com.outbrain.aletheia.datum.production.DatumEnvelopeSenderFactory;
import com.outbrain.aletheia.datum.production.NamedSender;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

/**
 * Created by slevin on 7/27/14.
 */
public class LogFileDatumEnvelopeSenderFactory implements DatumEnvelopeSenderFactory<LogFileProductionEndPoint> {

  @Override
  public NamedSender<DatumEnvelope> buildDatumEnvelopeSender(final LogFileProductionEndPoint productionEndPoint,
                                                             final MetricsFactory metricFactory) {
    return new DatumEnvelopePeelingStringSender(new StringLogFileSender(productionEndPoint, metricFactory));
  }
}
