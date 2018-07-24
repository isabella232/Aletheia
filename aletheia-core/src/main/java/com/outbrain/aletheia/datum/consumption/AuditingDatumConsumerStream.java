package com.outbrain.aletheia.datum.consumption;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.outbrain.aletheia.datum.consumption.openers.BaseEnvelopeOpener;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.metrics.common.Counter;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * A breadcrumb generating {@link DatumConsumerStream} implementation.
 */
public class AuditingDatumConsumerStream<TDomainClass> implements DatumConsumerStream<TDomainClass> {

  private class DatumIterator implements Iterator<TDomainClass> {

    private final Iterable<DatumEnvelope> datumEnvelopes;

    private DatumIterator(final DatumEnvelopeFetcher datumEnvelopeFetcher) {
      datumEnvelopes = datumEnvelopeFetcher.datumEnvelopes();
    }

    private final Function<DatumEnvelope, TDomainClass> toDatum = new Function<DatumEnvelope, TDomainClass>() {
      @Override
      public TDomainClass apply(final DatumEnvelope datumEnvelope) {
        return datumEnvelopeOpener.open(datumEnvelope);
      }
    };

    private final Predicate<TDomainClass> satisfiesDatumFilter = new Predicate<TDomainClass>() {
      @Override
      public boolean apply(final TDomainClass datum) {
        if (datumFilter.apply(datum)) {
          consumedDatumCount.inc();
          return true;
        } else {
          filteredCounter.inc();
          return false;
        }
      }
    };

    @Override
    public boolean hasNext() {
      return datumEnvelopes.iterator().hasNext();
    }

    @Override
    public TDomainClass next() {
      try {
        return FluentIterable
                .from(datumEnvelopes)
                .transform(toDatum)
                .firstMatch(satisfiesDatumFilter)
                .get();
      } catch (final Exception e) {
        consumeFailureCount.inc();
        logger.error("Error while consuming...", e);
        throw new RuntimeException(e);
      }
    }

    @Override
    public void remove() {

    }

  }

  private static final Logger logger = LoggerFactory.getLogger(AuditingDatumConsumerStream.class);

  private final BaseEnvelopeOpener<TDomainClass> datumEnvelopeOpener;
  private final Predicate<TDomainClass> datumFilter;
  private final Counter consumedDatumCount;
  private final Counter consumeFailureCount;
  private final Counter filteredCounter;
  private final Iterable<TDomainClass> datumIterable;
  private final DatumEnvelopeFetcher datumEnvelopeFetcher;

  public AuditingDatumConsumerStream(final DatumEnvelopeFetcher datumEnvelopeFetcher,
                                     final BaseEnvelopeOpener<TDomainClass> datumEnvelopeOpener,
                                     final Predicate<TDomainClass> datumFilter,
                                     final MetricsFactory metricsFactory) {

    consumedDatumCount = metricsFactory.createCounter("Consume_Requests_Attempts_Success", "Consumer requests successful attempts");
    consumeFailureCount = metricsFactory.createCounter("Consume_Requests_Attempts_Failures", "Consumer requests failed attempts");
    filteredCounter = metricsFactory.createCounter("Consume_Requests_Filtered", "Consumer requests filtered attempts");

    this.datumEnvelopeOpener = datumEnvelopeOpener;
    this.datumFilter = datumFilter;
    this.datumEnvelopeFetcher = datumEnvelopeFetcher;

    final DatumIterator datumIterator = new DatumIterator(datumEnvelopeFetcher);
    datumIterable = new Iterable<TDomainClass>() {
      @Override
      public Iterator<TDomainClass> iterator() {
        return datumIterator;
      }
    };
  }

  @Override
  public void close() throws Exception {
    datumEnvelopeFetcher.close();
    datumEnvelopeOpener.close();
  }

  @Override
  public Iterable<TDomainClass> datums() {
    return datumIterable;
  }

  @Override
  public void commitConsumedOffsets() {
    datumEnvelopeFetcher.commitConsumedOffsets();
  }
}

