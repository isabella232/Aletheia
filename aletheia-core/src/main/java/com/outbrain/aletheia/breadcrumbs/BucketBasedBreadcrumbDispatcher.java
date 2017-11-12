package com.outbrain.aletheia.breadcrumbs;

import com.google.common.base.Preconditions;
import com.outbrain.aletheia.datum.DatumType;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A hit counter that uses buckets in order to aggregate incoming hits.
 * Each bucket represents a logical timeslot to which incoming hits are assigned according to their (logical) timestamp.
 */
public class BucketBasedBreadcrumbDispatcher<T> implements BreadcrumbDispatcher<T> {

  private static class HitsPerInterval {

    private static final HitsPerInterval EMPTY = new HitsPerInterval(new DateTime(0).toInstant(), 0);

    private final AtomicLong hitCount;
    private final Instant bucketStart;

    private HitsPerInterval(final Instant bucketStart, final long hitCount) {
      this.bucketStart = bucketStart;
      this.hitCount = new AtomicLong(hitCount);
    }

    public boolean isEmpty() {
      return this == HitsPerInterval.EMPTY;
    }

    public boolean nonEmpty() {
      return !isEmpty();
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(BucketBasedBreadcrumbDispatcher.class);

  private final ConcurrentMap<Long, HitsPerInterval> bucketId2hitsPerInterval;
  private final Duration bucketDuration;
  private final BreadcrumbBaker<BucketStartWithDuration> breadcrumbBaker;
  private final BreadcrumbHandler breadcrumbHandler;
  private final DatumType.TimestampSelector<T> timestampSelector;
  private final Duration preAllocatedInterval;

  public BucketBasedBreadcrumbDispatcher(final Duration bucketDuration,
                                         final DatumType.TimestampSelector<T> timestampSelector,
                                         final BreadcrumbBaker<BucketStartWithDuration> breadcrumbBaker,
                                         final BreadcrumbHandler breadcrumbHandler,
                                         final Duration preAllocatedInterval) {
    this.bucketDuration = bucketDuration;
    this.breadcrumbBaker = breadcrumbBaker;
    this.breadcrumbHandler = breadcrumbHandler;
    this.timestampSelector = timestampSelector;
    this.preAllocatedInterval = preAllocatedInterval;

    bucketId2hitsPerInterval = initBucketId2hitCountsMap(preAllocatedInterval);
  }

  @Override
  public void close() {

  }

  private ConcurrentMap<Long, HitsPerInterval> initBucketId2hitCountsMap(final Duration preAllocatedInterval) {

    final long millisInOneHour = preAllocatedInterval.getMillis();
    final long bucketCount = millisInOneHour / bucketDuration.getMillis();

    Preconditions.checkState(bucketCount * bucketDuration.getMillis() == millisInOneHour,
                             "bucket duration must divide an hour without a reminder");

    final NonBlockingHashMapLong<HitsPerInterval> bucketIds2hitCounts = new NonBlockingHashMapLong<>((int) bucketCount);

    for (long bucketId = 0; bucketId < bucketCount; bucketId++) {
      bucketIds2hitCounts.put(bucketId, HitsPerInterval.EMPTY);
    }

    return bucketIds2hitCounts;
  }

  private Instant bucketStart(final T item) {
    return new Instant(
            (timestampSelector.extractDatumDateTime(item).getMillis() /
             bucketDuration.getMillis()) * bucketDuration.getMillis());
  }

  private long bucketId(final T item) {
    final long millis = timestampSelector.extractDatumDateTime(item).getMillis() % preAllocatedInterval.getMillis();
    return millis / bucketDuration.getMillis();
  }

  @Override
  public void report(final T item) {

    HitsPerInterval currentValue;
    HitsPerInterval nextValue = null;

    final long bucketId = bucketId(item);
    final Instant bucketStart = bucketStart(item);
    long currentHitCount = 0;

    do {
      currentValue = bucketId2hitsPerInterval.get(bucketId);
      if (currentValue.isEmpty()) {
        nextValue = new HitsPerInterval(bucketStart, 1);
      } else {
        if (!bucketStart.equals(currentValue.bucketStart)) {
          logger.error(
                  "Possible bucket collision between exiting bucket id: {} and incoming bucket id: {}," +
                  " ignoring current item.",
                  currentValue.bucketStart.getMillis(),
                  bucketStart.getMillis());
          return;
        }
        currentHitCount = currentValue.hitCount.get();
      }
    } while ((currentValue.isEmpty() &&
              !bucketId2hitsPerInterval.replace(bucketId, HitsPerInterval.EMPTY, nextValue))
             ||
             (currentValue.nonEmpty() &&
              !bucketId2hitsPerInterval.get(bucketId).hitCount.compareAndSet(currentHitCount,
                                                                             currentHitCount + 1)));
  }

  @Override
  public void dispatchBreadcrumbs() {

    for (final long bucketId : bucketId2hitsPerInterval.keySet()) {

      final HitsPerInterval hitsPerInterval = bucketId2hitsPerInterval.replace(bucketId, HitsPerInterval.EMPTY);

      if (hitsPerInterval.isEmpty()) continue;

      final Breadcrumb breadcrumb = breadcrumbBaker.bakeBreadcrumb(new BucketStartWithDuration(bucketDuration,
                                                                                               hitsPerInterval.bucketStart),
                                                                   Instant.now(),
                                                                   hitsPerInterval.hitCount.get());
      try {
        breadcrumbHandler.handle(breadcrumb);
      } catch (final Exception e) {
        logger.error(String.format(
                             "Failed to dispatch a breadcrumb for processingTimestamp: [%d], bucketId: [%s], aggregated hit count: [%d]",
                             Instant.now().getMillis(),
                             bucketId,
                             hitsPerInterval.hitCount.get()),
                     e);
      }
    }
  }
}
