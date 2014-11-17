package com.outbrain.aletheia.breadcrumbs.stats;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.outbrain.aletheia.breadcrumbs.Breadcrumb;
import com.outbrain.aletheia.breadcrumbs.persistence.BreadcrumbSqlDao;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.text.NumberFormat;
import java.util.*;

/**
 * The API level for the audit logic
 */
public class BreadcrumbMetricsCalculator {

  private static final Logger logger = Logger.getLogger(BreadcrumbMetricsCalculator.class);

  private final BreadcrumbSqlDao breadcrumbSqlDao;

  public BreadcrumbMetricsCalculator(final BreadcrumbSqlDao breadcrumbSqlDao) {
    this.breadcrumbSqlDao = breadcrumbSqlDao;
  }

  public Set<String> fetchTopics() {
    return Sets.newHashSet("dwlt_listing");
    //return this.db.fetchTopics();
  }

  public Map<String, TierMetrics> fetchMetrics(final String topic,
                                               final long granularity,
                                               final DateTime begin,
                                               final DateTime end,
                                               final double percentComplete) {

    final long adjBegin = (long) (granularity * Math.ceil(begin.getMillis() / (double) granularity));
    final int numBuckets = (int) Math.ceil((end.getMillis() - adjBegin) / (double) granularity);
    final long[] beginTimes = new long[numBuckets];
    int bucketIdx = 0;
    for (long time = adjBegin; time < end.getMillis(); time += granularity)
      beginTimes[bucketIdx++] = time;
    final Map<String, List<LoadTimes>> loadTimes = calculateCompletionTime(topic, begin, end, granularity);
    final Map<String, TierMetrics> lagTimes = new HashMap<>();
    // for each tier
    for (final Map.Entry<String, List<LoadTimes>> entry : loadTimes.entrySet()) {
      final String tier = entry.getKey();
      final int size = entry.getValue().size();
      final long[] lags = new long[numBuckets];
      final long[] counts = new long[numBuckets];

      // for each bucket calculate lag and total
      for (int i = 0; i < size; i++) {
        final LoadTimes times = entry.getValue().get(i);
        long total = 0;
        for (final long count : times.getCounts())
          total += count;
        long sum = 0;
        final long target = (long) (total * percentComplete);
        for (int j = 0; j < times.getCounts().length; j++) {
          sum += times.getCounts()[j];
          if (sum >= target) {
            // find index of begin time
            final int index = Arrays.binarySearch(beginTimes, times.getBegin());
            lags[index] = times.getTimes()[j] - times.begin;
            counts[index] = total;
            break;
          }
        }
        maybeLogLagTimes(times, total);
      }
      lagTimes.put(tier, new TierMetrics(tier, beginTimes, counts, lags));
    }
    return lagTimes;
  }

  /* Log out info on the full histogram of times */
  private void maybeLogLagTimes(final LoadTimes times, final long totalCount) {
    if (!logger.isDebugEnabled())
      return;

    // begin debug output
    final NumberFormat percent = NumberFormat.getPercentInstance();
    percent.setMaximumFractionDigits(2);
    final NumberFormat format = NumberFormat.getInstance();
    format.setMaximumFractionDigits(0);
    logger.debug("bucket: " + new Date(times.getBegin()));
    final long firstTime = times.getTimes()[0];
    logger.debug("first time: " + new Date(firstTime));
    logger.debug("initial delay: "
                         + format.format((firstTime - times.getBegin()) / (1000 * 60.0)) + " mins");
    logger.debug("data completion: [");
    for (int k = 0; k < times.getTimes().length; k++) {
      logger.debug(percent.format(sum(times.getCounts(), k) / (double) totalCount) + ": ");
      logger.debug("+" + (times.getTimes()[k] - firstTime) / (60.0 * 1000) + " mins, ");
    }
    logger.debug("]");
    logger.debug("-------------------------------------");
  }

  private long sum(final long[] vals, final int length) {
    long sum = 0;
    for (int i = 0; i <= length; i++)
      sum += vals[i];
    return sum;
  }

  public Map<String, List<LoadTimes>> calculateCompletionTime(final String topic,
                                                              final DateTime begin,
                                                              final DateTime end,
                                                              final long granularity) {
    final List<Breadcrumb> data = breadcrumbSqlDao.aggregateBreadcrumbs(topic,
                                                                        null,
                                                                        begin,
                                                                        end,
                                                                        Lists.newArrayList(
                                                                                breadcrumbSqlDao.getBreadcrumbMapper().tier(),
                                                                                breadcrumbSqlDao.getBreadcrumbMapper().bucketStart(),
                                                                                breadcrumbSqlDao.getBreadcrumbMapper().processingTimestamp()),
                                                                        Lists.newArrayList(
                                                                                breadcrumbSqlDao.getBreadcrumbMapper().tier(),
                                                                                breadcrumbSqlDao.getBreadcrumbMapper().bucketStart(),
                                                                                breadcrumbSqlDao.getBreadcrumbMapper().processingTimestamp()));
    final Map<String, List<LoadTimes>> times = new HashMap<>();
    String tier = null;
    Long bucket = null;
    int groupStart = 0;
    for (int i = 0; i < data.size(); i++) {
      final Breadcrumb breadcrumb = data.get(i);
      // start a new grouping by time bucket and tier
      if (!new Long(breadcrumb.getBucketStartTime().getMillis()).equals(bucket) || !breadcrumb.getTier().equals(tier)
              || i == data.size() - 1) {
        final List<Breadcrumb> sublist = data.subList(groupStart, i);
        if (sublist.size() > 0) {
          final long[] counts = new long[sublist.size()];
          final long[] eventTimes = new long[sublist.size()];
          for (int j = 0; j < sublist.size(); j++) {
            final Breadcrumb breadcrumb1 = sublist.get(j);
            counts[j] = breadcrumb1.getCount();
            eventTimes[j] = breadcrumb1.getProcessingTimestamp().getMillis();
          }
          List<LoadTimes> timesForTier = times.get(tier);
          if (timesForTier == null) {
            timesForTier = new ArrayList<>();
            times.put(tier, timesForTier);
          }
          timesForTier.add(new LoadTimes(sublist.get(0).getBucketStartTime().getMillis(),
                                         counts,
                                         eventTimes));
        }
        tier = breadcrumb.getTier();
        bucket = breadcrumb.getBucketStartTime().getMillis();
        groupStart = i;
      }
    }
    return times;
  }

  public static class LoadTimes {

    private final long begin;
    private final long[] counts;
    private final long[] times;

    public LoadTimes(final long begin, final long[] counts, final long[] times) {
      super();
      this.begin = begin;
      this.counts = counts;
      this.times = times;
    }

    public long getBegin() {
      return begin;
    }

    public long[] getCounts() {
      return counts;
    }

    public long[] getTimes() {
      return times;
    }
  }

}
