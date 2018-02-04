package com.outbrain.aletheia.breadcrumbs;

import com.google.common.base.Preconditions;

import com.outbrain.aletheia.datum.DatumType;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A hit counter that uses buckets in order to aggregate incoming hits.
 * Each bucket represents a logical timeslot to which incoming hits are assigned according to their (logical) timestamp.
 */
public abstract class BucketBasedBreadcrumbDispatcher<T> implements BreadcrumbDispatcher<T> {

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  protected static class HitsPerInterval {

    protected static final HitsPerInterval EMPTY = new HitsPerInterval(new DateTime(0).toInstant(), 0);

    protected final AtomicLong hitCount;
    protected final Instant bucketStart;

    HitsPerInterval(final Instant bucketStart, final long hitCount) {
      this.bucketStart = bucketStart;
      this.hitCount = new AtomicLong(hitCount);
    }

    boolean isEmpty() {
      return this == HitsPerInterval.EMPTY;
    }

    boolean nonEmpty() {
      return !isEmpty();
    }
  }

  final Duration bucketDuration;
  final BreadcrumbHandler breadcrumbHandler;
  private final DatumType.TimestampSelector<T> timestampSelector;
  private final Duration preAllocatedInterval;


  public BucketBasedBreadcrumbDispatcher(final Duration bucketDuration,
                                         final DatumType.TimestampSelector<T> timestampSelector,
                                         final BreadcrumbHandler breadcrumbHandler,
                                         final Duration preAllocatedInterval) {
    this.bucketDuration = bucketDuration;
    this.breadcrumbHandler = breadcrumbHandler;
    this.timestampSelector = timestampSelector;
    this.preAllocatedInterval = preAllocatedInterval;
  }

  @Override
  public void close() throws Exception {
    breadcrumbHandler.close();
  }

  long getBucketCount(Duration preAllocatedInterval) {
    final long millisInInterval = preAllocatedInterval.getMillis();
    final long bucketCount = millisInInterval / bucketDuration.getMillis();

    Preconditions.checkState(bucketCount * bucketDuration.getMillis() == millisInInterval,
        "bucket duration must divide the interval without a remainder");
    return bucketCount;
  }

  Instant bucketStart(final T item) {
    return new Instant(
        (timestampSelector.extractDatumDateTime(item).getMillis() /
            bucketDuration.getMillis()) * bucketDuration.getMillis());
  }

  long bucketId(final T item) {
    final long millis = timestampSelector.extractDatumDateTime(item).getMillis() % preAllocatedInterval.getMillis();
    return millis / bucketDuration.getMillis();
  }

  boolean isBucketCollision(final Instant existingBucketStart, final Instant incomingBucketStart) {
    if (!existingBucketStart.equals(incomingBucketStart)) {
      logger.error("Possible bucket collision between existing bucket start: {} and incoming bucket start: {}, ignoring current item.",
          incomingBucketStart.getMillis(),
          existingBucketStart.getMillis());
      return true;
    }
    return false;
  }
}
