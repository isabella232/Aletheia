//package com.outbrain.aletheia.breadcrumbs;
//
//import com.google.common.base.Joiner;
//import com.google.common.collect.Lists;
//import org.joda.time.Interval;
//
//import java.util.Arrays;
//import java.util.List;
//
//public class BreadcrumbSqlDao {
//
//  private static final String BREADCRUMB_TABLE_NAME = "dabd_breadcrumb_data";
//  private static final String INSERT_INTO_TABLE_QUERY_TEMPLATE_STRING = "insert into %s (%s) values(%s);";
//
//  private static final String[] COLUMN_NAMES =
//          new String[]{"dabd_hit_type", "dabd_source", "dabd_destination", "dabd_bucket_start_time",
//                  "dabd_bucket_end_time", "dabd_bucket_processing_timestamp", "dabd_bucket_hit_count",
//                  "dabd_tier", "dabd_application", "dabd_data_center"};
//
//  private final String SAVE_BREADCRUMB_QUERY_STRING;
//  private final JdbcTemplate jdbcTemplate;
//
//  public BreadcrumbSqlDao(final JdbcTemplate jdbcTemplate) {
//
//    this.jdbcTemplate = jdbcTemplate;
//    SAVE_BREADCRUMB_QUERY_STRING = getQueryString(COLUMN_NAMES);
//  }
//
//  private String getQueryString(final String[] columns) {
//
//    final String[] questionMarks = new String[columns.length];
//    Arrays.fill(questionMarks, "?");
//    final String columnsString = Joiner.on(',').join(columns);
//    final String questionMarksString = Joiner.on(',').join(questionMarks);
//
//    return String.format(INSERT_INTO_TABLE_QUERY_TEMPLATE_STRING,
//                         BREADCRUMB_TABLE_NAME,
//                         columnsString,
//                         questionMarksString);
//  }
//
//  public void save(final Breadcrumb breadcrumb) {
//
//    jdbcTemplate.update(
//            SAVE_BREADCRUMB_QUERY_STRING,
//            breadcrumb.getType(),
//            breadcrumb.getSource(),
//            breadcrumb.getDestination(),
//            breadcrumb.getBucketStartTime().getMillis(),
//            breadcrumb.getBucketEndTime().getMillis(),
//            breadcrumb.getProcessingTimestamp().getMillis(),
//            breadcrumb.getCount(), breadcrumb.getTier(),
//            breadcrumb.getApplication(), breadcrumb.getDatacenter());
//  }
//
//  public List<Breadcrumb> queryBreadcrumbs(final Interval interval) {
//    return Lists.newArrayList();
//  }
//
//  public List<Breadcrumb> queryBreadcrumbs(final Interval interval, final String source, String destination) {
//    return Lists.newArrayList();
//  }
//}
