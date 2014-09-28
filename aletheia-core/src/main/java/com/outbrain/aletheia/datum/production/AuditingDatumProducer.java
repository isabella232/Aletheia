package com.outbrain.aletheia.datum.production;

import com.google.common.base.Predicate;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbDispatcher;
import com.outbrain.aletheia.datum.envelope.DatumEnvelopeBuilder;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.metrics.common.Counter;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import com.outbrain.aletheia.metrics.common.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@code Breadcrumb} generating, {@code DatumEnvelope} transportation based
 * {@code DatumProducer} implementation.
 */
public class AuditingDatumProducer<TDomainClass> implements DatumProducer<TDomainClass> {

  private static final Logger logger = LoggerFactory.getLogger(AuditingDatumProducer.class);

  private final Timer deliverDurationTimer;
  private final Counter deliverRequestSuccessCounter;
  private final Counter filteredCounter;

  private final BreadcrumbDispatcher<TDomainClass> datumAuditor;
  private final Sender<DatumEnvelope> envelopeSender;
  private final DatumEnvelopeBuilder<TDomainClass> datumEnvelopeBuilder;
  private final Predicate<TDomainClass> filter;
  private final MetricsFactory metricFactory;

  public AuditingDatumProducer(final DatumEnvelopeBuilder<TDomainClass> datumEnvelopeBuilder,
                               final Sender<DatumEnvelope> envelopeSender,
                               final Predicate<TDomainClass> filter,
                               final BreadcrumbDispatcher<TDomainClass> datumAuditor,
                               final MetricsFactory metricFactory) {

    this.datumAuditor = datumAuditor;
    this.envelopeSender = envelopeSender;
    this.datumEnvelopeBuilder = datumEnvelopeBuilder;
    this.filter = filter;
    this.metricFactory = metricFactory;

    deliverDurationTimer = metricFactory.createTimer("Deliver.Requests", "Duration");
    filteredCounter = metricFactory.createCounter("Deliver.Requests", "Filtered");
    deliverRequestSuccessCounter = metricFactory.createCounter("Deliver.Requests.Attempts", "Success");
  }


  public void deliver(final TDomainClass datum) {

    final Timer.Context timerContext = deliverDurationTimer.time();

    try {

      if (!filter.apply(datum)) {
        filteredCounter.inc();
        return;
      }

      datumAuditor.report(datum);

      final DatumEnvelope datumEnvelope = datumEnvelopeBuilder.buildEnvelope(datum);

      envelopeSender.send(datumEnvelope);

      deliverRequestSuccessCounter.inc();

    } catch (final SilentSenderException e) {
      metricFactory.createCounter("Deliver.Requests.Attempts.Failures." + SilentSenderException.class.getSimpleName(),
                                  e.getCause().getClass().getSimpleName())
                   .inc();
    } catch (final Exception e) {
      metricFactory.createCounter("Deliver.Requests.Attempts.Failures", e.getCause().getClass().getSimpleName())
                   .inc();
      logger.error("Could not deliver datum." + datum, e);
    } finally {
      timerContext.stop();
    }
  }
}
