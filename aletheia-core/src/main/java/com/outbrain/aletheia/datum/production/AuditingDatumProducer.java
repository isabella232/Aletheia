package com.outbrain.aletheia.datum.production;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbDispatcher;
import com.outbrain.aletheia.datum.envelope.DatumEnvelopeBuilder;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.metrics.MoreExceptionUtils;
import com.outbrain.aletheia.metrics.common.Counter;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import com.outbrain.aletheia.metrics.common.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * A {@link com.outbrain.aletheia.breadcrumbs.Breadcrumb} generating, {@link DatumEnvelope} transportation based
 * {@link DatumProducer} implementation.
 */
public class AuditingDatumProducer<TDomainClass> implements DatumProducer<TDomainClass> {

  private static final Logger logger = LoggerFactory.getLogger(AuditingDatumProducer.class);

  private static final String DELIVER_REQUESTS_ATTEMPTS_FAILURES = "Deliver.Requests.Attempts.Failures";
  private static final long LOG_SUPPRESS_INTERVAL_MS = 60 * 1000;

  private final Timer deliverDurationTimer;
  private final Counter deliverRequestSuccessCounter;
  private final Counter filteredCounter;

  private final BreadcrumbDispatcher<TDomainClass> datumAuditor;
  private final Sender<DatumEnvelope> envelopeSender;
  private final DatumEnvelopeBuilder<TDomainClass> datumEnvelopeBuilder;
  private final Predicate<TDomainClass> filter;
  private final MetricsFactory metricFactory;

  private long lastExceptionLoggedTime = 0;

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

    // Create counter for QueueFullExceptions
    metricFactory.createCounter(Joiner.on(".").join(DELIVER_REQUESTS_ATTEMPTS_FAILURES,
            SilentSenderException.class.getSimpleName()), "QueueFullException");
  }

  public void deliver(final TDomainClass datum) {
    deliver(datum, EmptyCallback.getEmptyCallback());
  }

  @Override
  public void deliver(final TDomainClass datum, final DeliveryCallback deliveryCallback) {
    final Timer.Context timerContext = deliverDurationTimer.time();

    try {

      if (!filter.apply(datum)) {
        filteredCounter.inc();
        return;
      }

      datumAuditor.report(datum);

      final DatumEnvelope datumEnvelope = datumEnvelopeBuilder.buildEnvelope(datum);

      envelopeSender.send(datumEnvelope, deliveryCallback);

      deliverRequestSuccessCounter.inc();

    } catch (final SilentSenderException e) {
      metricFactory.createCounter(Joiner.on(".").join(DELIVER_REQUESTS_ATTEMPTS_FAILURES,
              SilentSenderException.class.getSimpleName()),
              MoreExceptionUtils.getType(e)).inc();

      // Log with suppression
      final long nowTime = System.currentTimeMillis();
      if (lastExceptionLoggedTime + LOG_SUPPRESS_INTERVAL_MS < nowTime) {
        lastExceptionLoggedTime = nowTime;
        logger.error("Datum send failed with exception:", e);
      }
    } catch (final Exception e) {
      metricFactory.createCounter(DELIVER_REQUESTS_ATTEMPTS_FAILURES, MoreExceptionUtils.getType(e)).inc();
      logger.error("Could not deliver datum." + datum, e);
    } finally {
      timerContext.stop();
    }
  }

  @Override
  public void close() throws IOException {
    envelopeSender.close();
    datumAuditor.close();
  }
}
