package com.outbrain.aletheia.datum;

import com.outbrain.aletheia.breadcrumbs.Breadcrumb;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbDispatcher;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbHandler;
import com.outbrain.aletheia.breadcrumbs.StartTimeWithDurationBreadcrumbBaker;
import com.outbrain.aletheia.datum.type.SampleDomainClass;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Interval;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by slevin on 8/28/14.
 */
@Ignore
public class DatumAuditorStressTest {

  @Rule
  public final TestName testName = new TestName();

  private static final String TEST_RESULT_FILE_NAME = "/tmp/breadcrumb_%s.txt";
  private static final int THREAD_COUNT = 500;

  private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  private final AtomicLong hitCount = new AtomicLong(0);
  private final Duration bucketDuration = Duration.standardSeconds(10);
  private final Duration durationBetweenFlushes = Duration.standardSeconds(30);
  private final Duration testDuration = Duration.standardHours(1);

  private final BreadcrumbHandler hitCountUpdater = new BreadcrumbHandler() {
    @Override
    public void handle(final Breadcrumb breadcrumb) {
      hitCount.getAndAdd(breadcrumb.getCount());
    }
  };

  private void doBenchmark(final BreadcrumbDispatcher<SampleDomainClass> breadcrumbDispatcher) throws Exception {

    final Instant start = Instant.now();
    final AtomicLong[] hitReportCounts = new AtomicLong[THREAD_COUNT];

    final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
    final LinkedList<Future<?>> submits = new LinkedList<>();

    for (int j = 0; j < THREAD_COUNT; j++) {
      final AtomicLong atomicLong = new AtomicLong(0);
      hitReportCounts[j] = atomicLong;
      submits.add(executorService.submit(new Runnable() {
        public void run() {
          for (final Instant start = Instant.now();
               new Interval(start, Instant.now()).toDuration().isShorterThan(testDuration); ) {
            breadcrumbDispatcher.report(new SampleDomainClass(1, 1, "", Instant.now(), true));
            atomicLong.incrementAndGet();
          }
        }
      }));
    }

    for (final Future<?> submit : submits) {
      submit.get();
    }

    // some extra waiting to make sure we have flushed it all
    executorService.awaitTermination(durationBetweenFlushes.getStandardSeconds() * 2, TimeUnit.SECONDS);

    final PrintWriter writer = new PrintWriter(String.format(TEST_RESULT_FILE_NAME, testName.getMethodName()),
                                               "UTF-8");

    writer.println(String.format("Test time in millis: %d", new Interval(start, Instant.now()).toDurationMillis()));

    long totalHitReportCount = 0;

    for (int j = 0; j < THREAD_COUNT; j++) {
      writer.println(String.format("Thread [%d] got [%d] incoming hits", j, hitReportCounts[j].get()));
      totalHitReportCount += hitReportCounts[j].get();
    }

    writer.println(String.format("Total incoming hits: %d, Total outgoing hits: %d, status: %B",
                                 totalHitReportCount,
                                 hitCount.get(),
                                 totalHitReportCount == hitCount.get()));
    writer.close();

    assertThat(hitCount.get(), is(totalHitReportCount));
  }

  @Test
  public void test_breadcrumbDispatcher_under_load() throws Exception {

    doBenchmark(
            new DatumAuditor<>(
                    bucketDuration,
                    DatumUtils.getDatumTimestampExtractor(SampleDomainClass.class),
                    new StartTimeWithDurationBreadcrumbBaker("", "", "", "", "", ""),
                    hitCountUpdater,
                    scheduledExecutorService,
                    durationBetweenFlushes,
                    Duration.standardDays(1)));
  }
}
