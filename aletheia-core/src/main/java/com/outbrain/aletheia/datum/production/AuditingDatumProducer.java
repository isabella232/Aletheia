package com.outbrain.aletheia.datum.production;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbDispatcher;
import com.outbrain.aletheia.datum.envelope.DatumEnvelopeBuilder;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.metrics.MoreExceptionUtils;
import com.outbrain.aletheia.metrics.common.Counter;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import com.outbrain.aletheia.metrics.common.Summary;
import com.outbrain.swinfra.metrics.timing.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link com.outbrain.aletheia.breadcrumbs.Breadcrumb} generating, {@link DatumEnvelope} transportation based
 * {@link DatumProducer} implementation.
 */
public class AuditingDatumProducer<TDomainClass> implements DatumProducer<TDomainClass> {

  private static final Logger logger = LoggerFactory.getLogger(AuditingDatumProducer.class);

  private static final String DELIVER_REQUESTS_ATTEMPTS_FAILURES = "Deliver_Requests_Attempts_Failures";
  private static final long LOG_SUPPRESS_INTERVAL_MS = 60 * 1000;

  private final Summary deliverDurationSummary;
  private final Counter deliverRequestSuccessCounter;
  private final Counter filteredCounter;
  private final Counter deliverRequestFailureCounter;

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

    deliverDurationSummary = metricFactory.createSummary("Deliver_Requests_Duration", "Duration of the requests");
    filteredCounter = metricFactory.createCounter("Deliver_Requests_Filtered", "Number of the filtered requests");
    deliverRequestSuccessCounter = metricFactory.createCounter("Deliver_Requests_Attempts_Success", "Number of the successful requests");

    // Create counter for QueueFullExceptions
    deliverRequestFailureCounter = metricFactory.createCounter(
            DELIVER_REQUESTS_ATTEMPTS_FAILURES,
            "Number of failed deliver requests attempts", "root_exception", "non_root_exception"
    );
  }

  public void deliver(final TDomainClass datum) {
    deliver(datum, EmptyCallback.getEmptyCallback());
  }

  @Override
  public void deliver(final TDomainClass datum, final DeliveryCallback deliveryCallback) {
    final Timer timer = deliverDurationSummary.startTimer();

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
      metricFactory.createCounter(Joiner.on("_").join(DELIVER_REQUESTS_ATTEMPTS_FAILURES,
              SilentSenderException.class.getSimpleName()),
              MoreExceptionUtils.getType(e)).inc();

      deliverRequestFailureCounter.inc(SilentSenderException.class.getSimpleName(), MoreExceptionUtils.getType(e));

      // Log with suppression
      final long nowTime = System.currentTimeMillis();
      if (lastExceptionLoggedTime + LOG_SUPPRESS_INTERVAL_MS < nowTime) {
        lastExceptionLoggedTime = nowTime;
        logger.error("Datum send failed with exception:", e);
      }
    } catch (final Exception e) {
      deliverRequestFailureCounter.inc(e.getClass().getSimpleName(), MoreExceptionUtils.getType(e));
      logger.error("Could not deliver datum." + datum, e);
    } finally {
      timer.stop();
    }
  }

  @Override
  public void close() throws Exception {
    envelopeSender.close();
    datumAuditor.close();
  }
}
