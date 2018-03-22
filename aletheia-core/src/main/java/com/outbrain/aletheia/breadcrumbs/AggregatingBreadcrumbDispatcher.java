package com.outbrain.aletheia.breadcrumbs;

import com.outbrain.aletheia.datum.DatumType;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.concurrent.ConcurrentMap;

public class AggregatingBreadcrumbDispatcher<T> extends BucketBasedBreadcrumbDispatcher<T> {

  private final BreadcrumbBaker<BucketStartWithDuration> breadcrumbBaker;
  private final ConcurrentMap<Long, HitsPerInterval> bucketId2hitsPerInterval;

  public AggregatingBreadcrumbDispatcher(final Duration bucketDuration,
                                         final DatumType.TimestampSelector<T> timestampSelector,
                                         final BreadcrumbBaker<BucketStartWithDuration> breadcrumbBaker,
                                         final BreadcrumbHandler breadcrumbHandler,
                                         final Duration preAllocatedInterval) {
    super(bucketDuration, timestampSelector, breadcrumbHandler, preAllocatedInterval);

    this.breadcrumbBaker = breadcrumbBaker;
    bucketId2hitsPerInterval = initBucketId2hitCountsMap(preAllocatedInterval);
  }

  private ConcurrentMap<Long, HitsPerInterval> initBucketId2hitCountsMap(final Duration preAllocatedInterval) {

    final long bucketCount = getBucketCount(preAllocatedInterval);

    final NonBlockingHashMapLong<HitsPerInterval> bucketIds2hitCounts = new NonBlockingHashMapLong<>((int) bucketCount);

    for (long bucketId = 0; bucketId < bucketCount; bucketId++) {
      bucketIds2hitCounts.put(bucketId, HitsPerInterval.EMPTY);
    }

    return bucketIds2hitCounts;
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
        if (isBucketCollision(currentValue.bucketStart, bucketStart)) {
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
  long dispatchBreadcrumbsInternal() {
    long totalHitCount = 0;
    for (final long bucketId : bucketId2hitsPerInterval.keySet()) {

      final HitsPerInterval hitsPerInterval = bucketId2hitsPerInterval.replace(bucketId, HitsPerInterval.EMPTY);

      if (hitsPerInterval.isEmpty()) continue;

      final Breadcrumb breadcrumb = breadcrumbBaker.bakeBreadcrumb(new BucketStartWithDuration(bucketDuration,
              hitsPerInterval.bucketStart),
          Instant.now(),
          hitsPerInterval.hitCount.get());
      totalHitCount += breadcrumb.getCount();
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
    return totalHitCount;
  }
}
