package com.outbrain.aletheia.breadcrumbs;

import com.outbrain.aletheia.MyDatum;
import com.outbrain.aletheia.datum.DatumUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class BreadcrumbDispatcherTest {

  private static final int BUCKET_INTERVAL_SECONDS = 300;
  private static final int TOTAL_INTERVAL_SECONDS = 3600;
  private static final DateTime referenceTime = new DateTime(1532516400000L);
  private static final int numBuckets = TOTAL_INTERVAL_SECONDS / BUCKET_INTERVAL_SECONDS;

  private BreadcrumbDispatcher<MyDatum> dispatcher;
  private List<Breadcrumb> breadcrumbList;

  @Before
  public void setUp() throws Exception {
    breadcrumbList = new ArrayList<>();
    dispatcher = new AggregatingBreadcrumbDispatcher<MyDatum>(
            Duration.standardSeconds(BUCKET_INTERVAL_SECONDS),
            DatumUtils.getDatumTimestampExtractor(MyDatum.class),
            new StartTimeWithDurationBreadcrumbBaker(
                    "source",
                    "endpoint",
                    "tier",
                    "DC",
                    "Application",
                    DatumUtils.getDatumTypeId(MyDatum.class)),
            new BreadcrumbHandler() {
              @Override
              public void handle(Breadcrumb breadcrumb) {
                breadcrumbList.add(breadcrumb);
              }

              @Override
              public void close() throws Exception {

              }
            },
            Duration.standardSeconds(TOTAL_INTERVAL_SECONDS));
  }

  @After
  public void tearDown() throws Exception {
    dispatcher = null;
    breadcrumbList = null;
  }

  @Test
  public void shouldCountHitsByBuckets() {
    final int expectedCount = 3;
    final MyDatum datum = new MyDatum("testdatum");

    for (int i = 0; i < numBuckets * expectedCount; i++) {
      datum.setTimeCreated(referenceTime.plusSeconds((BUCKET_INTERVAL_SECONDS / expectedCount) * i).getMillis());
      dispatcher.report(datum);
    }

    dispatcher.dispatchBreadcrumbs();

    assertEquals("Number of Breadcrumbs doesn't match number of buckets", numBuckets, breadcrumbList.size());
    breadcrumbList.forEach(breadcrumb -> assertEquals("Hit count doesn't match expected count", expectedCount, breadcrumb.getCount()));
  }

  @Test
  public void whenBucketCollisionThenRejectOldData() {
    final MyDatum datum = new MyDatum("testdatum");

    datum.setTimeCreated(referenceTime.getMillis());
    dispatcher.report(datum);
    dispatcher.report(datum);

    datum.setTimeCreated(referenceTime.minusHours(1).getMillis());
    dispatcher.report(datum);

    dispatcher.dispatchBreadcrumbs();
    assertEquals("Unexpected number of Breadcrumbs", 1, breadcrumbList.size());
    final Breadcrumb breadcrumb = breadcrumbList.get(0);
    assertEquals("Unexpected hit count", 2, breadcrumb.getCount());
    assertEquals("Bucket start time doesn't match reference time", referenceTime.getMillis(), breadcrumb.getBucketStartTime().getMillis());
  }

  @Test
  public void whenBucketCollisionThenNewDataTakesPrecedence() {
    final DateTime expectedBucketTime = referenceTime.plusHours(3);
    final MyDatum datum = new MyDatum("testdatum");

    datum.setTimeCreated(referenceTime.getMillis());
    dispatcher.report(datum);
    dispatcher.report(datum);

    datum.setTimeCreated(expectedBucketTime.getMillis());
    dispatcher.report(datum);

    dispatcher.dispatchBreadcrumbs();
    assertEquals("Unexpected number of Breadcrumbs", 1, breadcrumbList.size());
    final Breadcrumb breadcrumb = breadcrumbList.get(0);
    assertEquals("Unexpected hit count", 1, breadcrumb.getCount());
    assertEquals("Bucket start time doesn't match reference time", expectedBucketTime.getMillis(), breadcrumb.getBucketStartTime().getMillis());
  }
}
