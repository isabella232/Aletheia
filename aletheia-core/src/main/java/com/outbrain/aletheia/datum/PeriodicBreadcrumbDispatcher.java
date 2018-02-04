package com.outbrain.aletheia.datum;

import com.outbrain.aletheia.breadcrumbs.BreadcrumbDispatcher;

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
public class PeriodicBreadcrumbDispatcher<TDomainClass> implements BreadcrumbDispatcher<TDomainClass> {

  private static final Logger logger = LoggerFactory.getLogger(PeriodicBreadcrumbDispatcher.class);

  private final BreadcrumbDispatcher<TDomainClass> delegateDispatcher;
  private final ScheduledExecutorService scheduledExecutorService;
  private final Duration durationBetweenFlushes;
  private final Runnable flushCommand;
  private ScheduledFuture<?> future;

  public PeriodicBreadcrumbDispatcher(final BreadcrumbDispatcher<TDomainClass> delegateDispatcher,
                                      final Duration durationBetweenFlushes) {
    this(delegateDispatcher,
        Executors.newSingleThreadScheduledExecutor(),
        durationBetweenFlushes);
  }

  public PeriodicBreadcrumbDispatcher(final BreadcrumbDispatcher<TDomainClass> delegateDispatcher,
                                      final ScheduledExecutorService scheduledExecutorService,
                                      final Duration durationBetweenFlushes) {

    this.delegateDispatcher = delegateDispatcher;
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
  public void close() throws Exception {
    delegateDispatcher.close();
    if (future != null) {
      future.cancel(false);
    }
    scheduledExecutorService.shutdown();
  }

  private void periodicFlush() {
    dispatchBreadcrumbs();
    future = scheduledExecutorService.schedule(flushCommand, durationBetweenFlushes.getMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void report(TDomainClass element) {
    delegateDispatcher.report(element);
  }

  @Override
  public void dispatchBreadcrumbs() {
    delegateDispatcher.dispatchBreadcrumbs();
  }
}
