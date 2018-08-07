package com.outbrain.aletheia.datum.consumption.openers;

import com.outbrain.aletheia.breadcrumbs.BreadcrumbDispatcher;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import com.outbrain.aletheia.datum.serialization.DatumTypeVersion;
import com.outbrain.aletheia.datum.serialization.SerializedDatum;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatumEnvelopeOpener<TDomainClass> extends BaseEnvelopeOpener{

  private static final Logger logger = LoggerFactory.getLogger(DatumEnvelopeOpener.class);
private final DatumSerDe<TDomainClass> datumSerDe;


  public DatumEnvelopeOpener(final BreadcrumbDispatcher<TDomainClass> datumAuditor,
                             final DatumSerDe<TDomainClass> datumSerDe,
                             final MetricsFactory metricFactory) {
    super(datumAuditor,metricFactory);
    this.datumSerDe = datumSerDe;
  }

  @Override
  protected TDomainClass openEnvelope(final DatumEnvelope datumEnvelope) {

    logger.debug("converting datum envelope: " + datumEnvelope.toString());
    final TDomainClass datum =
            datumSerDe.deserializeDatum(new SerializedDatum(datumEnvelope.getDatumBytes(),
                    new DatumTypeVersion(
                            datumEnvelope.getDatumTypeId().toString(),
                            datumEnvelope.getDatumSchemaVersion())));
    datumAuditor.report(datum);

    return datum;
  }
}

