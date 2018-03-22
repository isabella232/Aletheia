package com.outbrain.aletheia.breadcrumbs;

import com.outbrain.aletheia.datum.DatumType;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * A hit counter that aggregates incoming hits according to their (logical) timestamp and their
 * Breadcrumb key.
 */
public class KeyedBreadcrumbDispatcher<T> extends BucketBasedBreadcrumbDispatcher<T> {

  private static final Logger logger = LoggerFactory.getLogger(KeyedBreadcrumbDispatcher.class);

  private final BreadcrumbBaker<KeyedBucket> breadcrumbBaker;
  private final Function<T, BreadcrumbKey> breadcrumbKeyMapper;
  private final ConcurrentMap<Long, ConcurrentMap<BreadcrumbKey, HitsPerInterval>> bucketId2hitsPerInterval;

  public KeyedBreadcrumbDispatcher(final Duration bucketDuration,
                                   final DatumType.TimestampSelector<T> timestampSelector,
                                   final BreadcrumbBaker<KeyedBucket> breadcrumbBaker,
                                   final BreadcrumbHandler breadcrumbHandler,
                                   final Function<T, BreadcrumbKey> breadcrumbKeyMapper,
                                   final Duration preAllocatedInterval) {
    super(bucketDuration, timestampSelector, breadcrumbHandler, preAllocatedInterval);
    this.breadcrumbBaker = breadcrumbBaker;
    this.breadcrumbKeyMapper = breadcrumbKeyMapper;

    bucketId2hitsPerInterval = initBucketId2hitCountsMap(preAllocatedInterval);
  }

  private HitsPerInterval getHits(final long bucketId, final BreadcrumbKey breadcrumbKey) {
    return bucketId2hitsPerInterval.get(bucketId).computeIfAbsent(breadcrumbKey, __ -> HitsPerInterval.EMPTY);
  }

  private ConcurrentMap<Long, ConcurrentMap<BreadcrumbKey, HitsPerInterval>> initBucketId2hitCountsMap(final Duration preAllocatedInterval) {

    final long bucketCount = getBucketCount(preAllocatedInterval);

    final NonBlockingHashMapLong<ConcurrentMap<BreadcrumbKey, HitsPerInterval>> bucketIds2hitCounts = new NonBlockingHashMapLong<>((int) bucketCount);

    for (long bucketId = 0; bucketId < bucketCount; bucketId++) {
      bucketIds2hitCounts.put(bucketId, new ConcurrentHashMap<>());
    }

    return bucketIds2hitCounts;
  }

  @Override
  public void report(final T item) {

    HitsPerInterval currentValue;
    HitsPerInterval nextValue = null;

    final long bucketId = bucketId(item);
    final Instant bucketStart = bucketStart(item);
    final BreadcrumbKey breadcrumbKey = breadcrumbKeyMapper.apply(item);

    long currentHitCount = 0;
    do {
      currentValue = getHits(bucketId, breadcrumbKey);
      if (currentValue.isEmpty()) {
        nextValue = new HitsPerInterval(bucketStart, 1);
      } else {
        if (isBucketCollision(currentValue.bucketStart, bucketStart)) {
          return;
        }
        currentHitCount = currentValue.hitCount.get();
      }
    } while ((currentValue.isEmpty() &&
        !bucketId2hitsPerInterval.get(bucketId).replace(breadcrumbKey, HitsPerInterval.EMPTY, nextValue))
        ||
        (currentValue.nonEmpty() &&
            !getHits(bucketId, breadcrumbKey).hitCount.compareAndSet(currentHitCount,
                currentHitCount + 1)));
  }

  @Override
  long dispatchBreadcrumbsInternal() {
    long totalHitCount = 0;
    for (final long bucketId : bucketId2hitsPerInterval.keySet()) {
      final ConcurrentMap<BreadcrumbKey, HitsPerInterval> hitsMap = bucketId2hitsPerInterval.get(bucketId);
      final Set<BreadcrumbKey> keySet = hitsMap.keySet();

      for (final BreadcrumbKey breadcrumbKey : keySet) {
        final HitsPerInterval hitsPerInterval = hitsMap.replace(breadcrumbKey, HitsPerInterval.EMPTY);
        if (hitsPerInterval.isEmpty()) continue;

        final long hitCount = hitsPerInterval.hitCount.getAndSet(-1);
        totalHitCount += hitCount;
        final Breadcrumb breadcrumb = breadcrumbBaker.bakeBreadcrumb(
            new KeyedBucket(bucketDuration, hitsPerInterval.bucketStart, breadcrumbKey),
            Instant.now(),
            hitCount);
        try {
          breadcrumbHandler.handle(breadcrumb);
        } catch (final Exception e) {
          logger.error(String.format(
              "Failed to dispatch a breadcrumb for processingTimestamp: [%d], bucketId: [%s], breadcrumbKey: [%s], aggregated hit count: [%d]",
              Instant.now().getMillis(),
              bucketId,
              breadcrumbKey.toString(),
              hitCount),
              e);
        }
      }
    }
    return totalHitCount;
  }
}
