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

import java.io.IOException;

abstract public class BaseEnvelopeOpener<TDomainClass> {

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

    futureLogicalMessagesCount = metricFactory.createCounter("Timestamp.Logical", "FromTheFuture");
    logicalTimestampDelayHistogram = metricFactory.createHistogram("Timestamp.Logical",
            "DelayHistogramInSeconds",
            true);
    metricFactory.createGauge("Timestamp.Logical", "DelayAverageInSeconds", logicalDelayAverager);
    metricFactory.createGauge("Timestamp.Logical", "DelayAverageInMillis", logicalDelayInMillisAverager);
  }

  public void close() throws IOException {
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
      final long logicalTimeAheadInMillis = new Interval(now, now.plusMillis(1)).toDuration().getMillis();
      futureLogicalMessagesCount.inc();

      if (logicalTimeAheadInMillis > LOG_FUTURE_MESSAGES_LAG_THRESHOLD_IN_MILLIS) {
        logger.warn(String.format("A message with a future logical timestamp has arrived from %s. " +
                        "Arriving timestamp is: %d, now reference is %d, delta is: %d, skipping lag metrics update.",
                envelope.getSourceHost(),
                envelope.getLogicalTimestamp(),
                now.getMillis(),
                envelope.getLogicalTimestamp() - now.getMillis()));
      }
    }
  }

  public TDomainClass open(final DatumEnvelope datumEnvelope){
    updateLagMetrics(datumEnvelope);
    return openEnvelope(datumEnvelope);
  }

  protected abstract TDomainClass openEnvelope(final DatumEnvelope datumEnvelope);

}
