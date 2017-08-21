package com.outbrain.aletheia.datum.consumption;

import com.outbrain.aletheia.breadcrumbs.BreadcrumbDispatcher;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import com.outbrain.aletheia.datum.serialization.DatumTypeVersion;
import com.outbrain.aletheia.datum.serialization.SerializedDatum;
import com.outbrain.aletheia.metrics.common.Counter;
import com.outbrain.aletheia.metrics.common.Histogram;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import com.outbrain.aletheia.metrics.common.TimeWindowAverager;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatumEnvelopeOpener<TDomainClass> {

  private static final Logger logger = LoggerFactory.getLogger(DatumEnvelopeOpener.class);

  private final BreadcrumbDispatcher<TDomainClass> datumAuditor;
  private final DatumSerDe<TDomainClass> datumSerDe;

  private final TimeWindowAverager logicalDelayAverager;
  private final Histogram logicalTimestampDelayHistogram;

  private final Counter futureLogicalMessagesCount;

  private final int LOG_FUTURE_MESSAGES_LAG_THRESHOLD_IN_MILLIS = 5000;

  public DatumEnvelopeOpener(final BreadcrumbDispatcher<TDomainClass> datumAuditor,
                             final DatumSerDe<TDomainClass> datumSerDe,
                             final MetricsFactory metricFactory) {

    this.datumAuditor = datumAuditor;
    this.datumSerDe = datumSerDe;

    logicalDelayAverager = new TimeWindowAverager(60.0, 15, -1.0);

    futureLogicalMessagesCount = metricFactory.createCounter("Timestamp.Logical", "FromTheFuture");
    logicalTimestampDelayHistogram = metricFactory.createHistogram("Timestamp.Logical",
            "DelayHistogramInSeconds",
            true);
    metricFactory.createGauge("Timestamp.Logical", "DelayAverageInSeconds", logicalDelayAverager);
  }

  private void updateLagMetrics(final DatumEnvelope envelope) {
    final DateTime datumTime = new DateTime(envelope.getLogicalTimestamp());
    final DateTime now = DateTime.now();

    if (datumTime.isBefore(now)) {
      try {
        final long logicalSecondsBehind = new Interval(datumTime, now).toDuration().getStandardSeconds();
        logicalDelayAverager.addSample((int) logicalSecondsBehind);
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

  public TDomainClass open(final DatumEnvelope datumEnvelope) {

    updateLagMetrics(datumEnvelope);

    final TDomainClass datum =
            datumSerDe.deserializeDatum(new SerializedDatum(datumEnvelope.getDatumBytes(),
                    new DatumTypeVersion(
                            datumEnvelope.getDatumTypeId().toString(),
                            datumEnvelope.getDatumSchemaVersion())));
    datumAuditor.report(datum);

    return datum;
  }
}

