//package com.outbrain.aletheia.breadcrumbs;
//
//import com.outbrain.metrics.MetricFactory;
//import org.springframework.jdbc.core.JdbcTemplate;
//
//public class BreadcrumbSqlPersister implements BreadcrumbPersister {
//
//  private final static String COMPONENT = BreadcrumbSqlPersister.class.getSimpleName();
//  private final BreadcrumbSqlDao breadcrumbDao;
//  private final MetricFactory metricFactory;
//
//  public BreadcrumbSqlPersister(final MetricFactory metricFactory, final JdbcTemplate jdbcTemplate) {
//    this.metricFactory = metricFactory;
//    this.breadcrumbDao = new BreadcrumbSqlDao(jdbcTemplate);
//  }
//
//  @Override
//  public void persist(final Breadcrumb breadcrumb) {
//
//    final String hitType = breadcrumb.getType();
//    final long hitCount = breadcrumb.getCount();
//
//    try {
//      metricFactory.createCounter(COMPONENT + ".persist." + hitType, "Attempts.success").inc();
//      metricFactory.createCounter(COMPONENT + ".persisted", hitType).inc(hitCount);
//      metricFactory.createManualUpdateGauge(COMPONENT + ".breadcrumbSize", hitType, hitCount);
//
//      breadcrumbDao.save(breadcrumb);
//    } catch (final Exception e) {
//      metricFactory.createCounter(COMPONENT + ".persist." + hitType, "Attempts.failure").inc();
//      throw new RuntimeException("Could not persist a breadcrumb " + breadcrumb, e);
//    }
//  }
//}
