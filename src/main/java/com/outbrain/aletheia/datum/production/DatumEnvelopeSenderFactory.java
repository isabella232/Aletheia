package com.outbrain.aletheia.datum.production;

import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

/**
 * Created by slevin on 7/20/14.
 */
public interface DatumEnvelopeSenderFactory<TProductionEndPoint extends ProductionEndPoint> {
  NamedSender<DatumEnvelope> buildDatumEnvelopeSender(TProductionEndPoint productionEndPoint,
                                                      MetricsFactory metricFactory);
}
