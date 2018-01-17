package com.outbrain.aletheia.datum;

import com.outbrain.aletheia.breadcrumbs.BreadcrumbBaker;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbHandler;
import com.outbrain.aletheia.breadcrumbs.BucketBasedBreadcrumbDispatcher;
import com.outbrain.aletheia.breadcrumbs.BucketStartWithDuration;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Keeps aggregated counts of the incoming reports and periodically produces corresponding breadcrumbs.
 */
public class DatumAuditor<TDomainClass> extends BucketBasedBreadcrumbDispatcher<TDomainClass> {

  private static final Logger logger = LoggerFactory.getLogger(DatumAuditor.class);

  private final ScheduledExecutorService scheduledExecutorService;
  private final Duration durationBetweenFlushes;
  private final Runnable flushCommand;
  private ScheduledFuture<?> future;

  public DatumAuditor(final Duration bucketDuration,
                      final DatumType.TimestampSelector<TDomainClass> timestampSelector,
                      final BreadcrumbBaker<BucketStartWithDuration> breadcrumbBaker,
                      final BreadcrumbHandler breadcrumbHandler,
                      final Duration durationBetweenFlushes) {
    this(bucketDuration,
         timestampSelector,
         breadcrumbBaker,
         breadcrumbHandler,
         Executors.newSingleThreadScheduledExecutor(),
         durationBetweenFlushes,
         Duration.standardDays(1));
  }

  public DatumAuditor(final Duration bucketDuration,
                      final DatumType.TimestampSelector<TDomainClass> timestampSelector,
                      final BreadcrumbBaker<BucketStartWithDuration> breadcrumbBaker,
                      final BreadcrumbHandler breadcrumbHandler,
                      final ScheduledExecutorService scheduledExecutorService,
                      final Duration durationBetweenFlushes,
                      final Duration preAllocatedInterval) {

    super(bucketDuration, timestampSelector, breadcrumbBaker, breadcrumbHandler, preAllocatedInterval);

    this.scheduledExecutorService = scheduledExecutorService;
    this.durationBetweenFlushes = durationBetweenFlushes;

    flushCommand = () -> {
      try {
        periodicFlush();
      } catch (final Exception e) {
        logger.error("Periodic flush has failed.", e);
      }
    };

    periodicFlush();
  }

  @Override
  public void close() {
    if (future != null) {
      future.cancel(false);
    }
    scheduledExecutorService.shutdown();
  }

  private void periodicFlush() {
    dispatchBreadcrumbs();
    future = scheduledExecutorService.schedule(flushCommand, durationBetweenFlushes.getMillis(), TimeUnit.MILLISECONDS);
  }
}
