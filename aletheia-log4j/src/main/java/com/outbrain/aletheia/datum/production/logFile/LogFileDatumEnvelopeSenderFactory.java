package com.outbrain.aletheia.datum.production.logFile;

import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.production.*;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

/**
 * Created by slevin on 7/27/14.
 */
public class LogFileDatumEnvelopeSenderFactory implements DatumEnvelopeSenderFactory<LogFileProductionEndPoint> {

  @Override
  public NamedSender<DatumEnvelope> buildDatumEnvelopeSender(final LogFileProductionEndPoint productionEndPoint,
                                                             final MetricsFactory metricFactory) {

    final StringLogFileSender stringLogFileSender = new StringLogFileSender(productionEndPoint, metricFactory);

    return new DatumEnvelopePeelingStringSender(new DatumKeyAwareNamedSender<String>() {
      @Override
      public String getName() {
        return stringLogFileSender.getName();
      }

      @Override
      public void send(final String data, final String key) throws SilentSenderException {
        stringLogFileSender.send(data);
      }
    });
  }
}
