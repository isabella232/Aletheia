package com.outbrain.aletheia.datum.auditing;

import com.outbrain.aletheia.breadcrumbs.BreadcrumbBaker;
import com.outbrain.aletheia.breadcrumbs.BucketBasedBreadcrumbDispatcher;
import com.outbrain.aletheia.breadcrumbs.HitLogger;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DatumAuditor<TDomainClass> extends BucketBasedBreadcrumbDispatcher<TDomainClass, DatumBucketKey> {

  private static final Logger logger = LoggerFactory.getLogger(DatumAuditor.class);

  private final ScheduledExecutorService scheduledExecutorService;
  private final Duration durationBetweenFlushes;
  private final Runnable flushCommand;

  public DatumAuditor(final DatumBucketKeySelector<TDomainClass> datumEnvelopeBucketKeySelector,
                      final BreadcrumbBaker<DatumBucketKey> breadcrumbBaker,
                      final BreadcrumbHandler breadcrumbHandler,
                      final ScheduledExecutorService scheduledExecutorService,
                      final Duration durationBetweenFlushes) {
    this(datumEnvelopeBucketKeySelector,
         breadcrumbBaker,
         breadcrumbHandler,
         scheduledExecutorService,
         durationBetweenFlushes,
         HitLogger.NULL);

  }

  public DatumAuditor(final DatumBucketKeySelector<TDomainClass> datumEnvelopeBucketKeySelector,
                      final BreadcrumbBaker<DatumBucketKey> breadcrumbBaker,
                      final BreadcrumbHandler breadcrumbHandler,
                      final ScheduledExecutorService scheduledExecutorService,
                      final Duration durationBetweenFlushes,
                      final HitLogger hitLogger) {

    super(datumEnvelopeBucketKeySelector, breadcrumbBaker, breadcrumbHandler, hitLogger);

    this.scheduledExecutorService = scheduledExecutorService;
    this.durationBetweenFlushes = durationBetweenFlushes;
    flushCommand = new Runnable() {
      @Override
      public void run() {
        try {
          periodicFlush();
        } catch (final Exception e) {
          logger.error("Periodic flash has failed.", e);
        }
      }
    };

    periodicFlush();
  }

  private void periodicFlush() {
    dispatchBreadcrumbs();
    scheduledExecutorService.schedule(flushCommand, durationBetweenFlushes.getMillis(), TimeUnit.MILLISECONDS);
  }
}
