package com.outbrain.aletheia.datum.consumption.openers;

import com.outbrain.aletheia.breadcrumbs.BreadcrumbDispatcher;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class IdentityEnvelopeOpener extends BaseEnvelopeOpener<DatumEnvelope> {

  private static final Logger logger = LoggerFactory.getLogger(IdentityEnvelopeOpener.class);

  public IdentityEnvelopeOpener(BreadcrumbDispatcher<DatumEnvelope> datumAuditor, MetricsFactory metricFactory) {
    super(datumAuditor, metricFactory);
  }

  public void close() throws IOException {
    datumAuditor.close();
  }

  @Override
  protected DatumEnvelope openEnvelope(DatumEnvelope datumEnvelope) {

    logger.debug("auditing datum envelope: " + datumEnvelope.toString());
    datumAuditor.report(datumEnvelope);
    return datumEnvelope;
  }
}

