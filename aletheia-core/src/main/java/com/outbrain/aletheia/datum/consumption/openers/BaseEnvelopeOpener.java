package com.outbrain.aletheia.datum.consumption.openers;

import com.outbrain.aletheia.breadcrumbs.BreadcrumbDispatcher;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.metrics.common.Counter;
import com.outbrain.aletheia.metrics.common.Histogram;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import com.outbrain.aletheia.metrics.common.TimeWindowAverager;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class BaseEnvelopeOpener<TDomainClass> implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(BaseEnvelopeOpener.class);

  protected final BreadcrumbDispatcher<TDomainClass> datumAuditor;

  private final TimeWindowAverager logicalDelayAverager;
  private final TimeWindowAverager logicalDelayInMillisAverager;
  private final Histogram logicalTimestampDelayHistogram;

  private final Counter futureLogicalMessagesCount;

  private final int LOG_FUTURE_MESSAGES_LAG_THRESHOLD_IN_MILLIS = 5000;

  public BaseEnvelopeOpener(final BreadcrumbDispatcher<TDomainClass> datumAuditor,
                            final MetricsFactory metricFactory) {

    this.datumAuditor = datumAuditor;

    logicalDelayAverager = new TimeWindowAverager(60.0, 15, -1.0);
    logicalDelayInMillisAverager = new TimeWindowAverager(60.0, 15, -1.0);

    futureLogicalMessagesCount = metricFactory.createCounter("timestampLogicalFromTheFuture", "future logical message count");
    logicalTimestampDelayHistogram = metricFactory.createHistogram(
            "Timestamp_Logical_DelayHistogramInSeconds",
            "delay in seconds",
            new double[]{.001, .005, .01, .05, 0.1, 0.5, 1, 5, 10});

    metricFactory.createGauge("timestampLogicalDelayAverageInSeconds", "Datum consumption average delay in seconds", logicalDelayAverager);
    metricFactory.createGauge("timestampLogicalDelayAverageInMillis", "Datum consumption average delay in millis", logicalDelayInMillisAverager);
  }

  @Override
  public void close() throws Exception {
    datumAuditor.close();
  }

  private void updateLagMetrics(final DatumEnvelope envelope) {
    final DateTime datumTime = new DateTime(envelope.getLogicalTimestamp());
    final DateTime now = DateTime.now();

    if (datumTime.isBefore(now)) {
      try {
        final Duration lag = new Interval(datumTime, now).toDuration();

        final long logicalSecondsBehind = lag.getStandardSeconds();
        logicalDelayAverager.addSample((int) logicalSecondsBehind);

        final long logicalMillisBehind = lag.getMillis();
        logicalDelayInMillisAverager.addSample((int) logicalMillisBehind);

        logicalTimestampDelayHistogram.update(logicalSecondsBehind);
      } catch (final Exception e) {
        logger.error("Error while updating lag metric", e);
      }
    } else {
      // Message from the future
      final long logicalTimeAheadInMillis = new Interval(now, datumTime).toDuration().getMillis();
      futureLogicalMessagesCount.inc();

      if (logicalTimeAheadInMillis > LOG_FUTURE_MESSAGES_LAG_THRESHOLD_IN_MILLIS) {
        logger.warn("A message with a future logical timestamp has arrived from {}. Arriving timestamp is: {}, now reference is {}, delta is: {}, skipping lag metrics update.",
                envelope.getSourceHost(),
                envelope.getLogicalTimestamp(),
                now.getMillis(),
                logicalTimeAheadInMillis);
      }
    }
  }

  public TDomainClass open(final DatumEnvelope datumEnvelope) {
    updateLagMetrics(datumEnvelope);
    return openEnvelope(datumEnvelope);
  }

  protected abstract TDomainClass openEnvelope(final DatumEnvelope datumEnvelope);

}
