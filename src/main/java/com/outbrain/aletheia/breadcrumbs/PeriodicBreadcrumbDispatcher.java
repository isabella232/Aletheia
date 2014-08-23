package com.outbrain.aletheia.breadcrumbs;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PeriodicBreadcrumbDispatcher<TElement> implements BreadcrumbDispatcher<TElement> {

  private static final Logger log = LoggerFactory.getLogger(PeriodicBreadcrumbDispatcher.class);

  private final ScheduledExecutorService scheduledExecutorService;
  private final BreadcrumbDispatcher<TElement> breadcrumbDispatcher;
  private final Duration durationBetweenFlushes;
  private final Runnable flushCommand;

  public PeriodicBreadcrumbDispatcher(final BreadcrumbDispatcher<TElement> breadcrumbDispatcher,
                                      final ScheduledExecutorService scheduledExecutorService,
                                      final Duration durationBetweenFlushes) {
    this.scheduledExecutorService = scheduledExecutorService;
    this.breadcrumbDispatcher = breadcrumbDispatcher;
    this.durationBetweenFlushes = durationBetweenFlushes;

    flushCommand = new Runnable() {
      @Override
      public void run() {
        try {
          periodicFlush();
        } catch (final Exception e) {
          log.error("Periodic flash has failed.", e);
        }
      }
    };

    periodicFlush();
  }

  private void periodicFlush() {
    breadcrumbDispatcher.dispatchBreadcrumbs();
    scheduledExecutorService.schedule(flushCommand, durationBetweenFlushes.getMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void report(final TElement item) {
    breadcrumbDispatcher.report(item);
  }

  @Override
  public void dispatchBreadcrumbs() {
    breadcrumbDispatcher.dispatchBreadcrumbs();
  }
}
