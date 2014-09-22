package com.outbrain.aletheia.datum.production;

import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

/**
 * A base interface for classes to extend in order to build a custom <code>Sender</code>,
 * for a custom <code>ProductionEndPoint</code>.
 *
 * @param <TProductionEndPoint> The type of the production endpoint this factory will be building senders for.
 */
public interface DatumEnvelopeSenderFactory<TProductionEndPoint extends ProductionEndPoint> {
  NamedSender<DatumEnvelope> buildDatumEnvelopeSender(TProductionEndPoint productionEndPoint,
                                                      MetricsFactory metricFactory);
}
