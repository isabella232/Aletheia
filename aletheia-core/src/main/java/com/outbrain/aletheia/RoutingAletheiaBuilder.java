package com.outbrain.aletheia;

import com.outbrain.aletheia.breadcrumbs.Breadcrumb;
import com.outbrain.aletheia.datum.DatumUtils;
import com.outbrain.aletheia.datum.production.DatumProducer;
import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @param <TDomainClass> The datum type this builder will be building a
 *                       {@link DatumProducer} or {@link com.outbrain.aletheia.datum.consumption.DatumConsumerStream} for.
 * @param <TBuilder>     The concrete type of builder, used for type safety, to be filled in by deriving classes.
 */
abstract class RoutingAletheiaBuilder<TDomainClass, TBuilder extends RoutingAletheiaBuilder<TDomainClass, ?>> {

  private static final Logger logger = LoggerFactory.getLogger(RoutingAletheiaBuilder.class);

  protected AletheiaBuilder<TDomainClass, ?> builder;
  protected final Class<TDomainClass> domainClass;
  protected final Properties properties;

  protected RoutingAletheiaBuilder(final Class<TDomainClass> domainClass, final AletheiaConfig config) {
    this.domainClass = domainClass;
    this.properties = config.getProperties();
  }

  protected BreadcrumbsConfig getBreadcrumbConfig() {
    return new BreadcrumbsConfig(
            Duration.standardSeconds(Integer.parseInt(properties.getProperty("aletheia.breadcrumbs.bucketDurationSec"))),
            Duration.standardSeconds(Integer.parseInt(properties.getProperty("aletheia.breadcrumbs.flushIntervalSec"))),
            properties.getProperty("aletheia.breadcrumbs.fields.application"),
            properties.getProperty("aletheia.breadcrumbs.fields.source"),
            properties.getProperty("aletheia.breadcrumbs.fields.tier"),
            properties.getProperty("aletheia.breadcrumbs.fields.datacenter"));
  }

  protected void saveBuilder(final AletheiaBuilder<TDomainClass, ?> aBuilder) {
    builder = aBuilder;
  }

  protected Properties getBreadcrumbEnvironment() {
    final Properties breadcrumbEnv = new Properties();

    breadcrumbEnv.setProperty("aletheia.endpoints.config.path",
                              properties.getProperty("aletheia.endpoints.config.path"));
    breadcrumbEnv.setProperty("aletheia.routing.config.path",
                              properties.getProperty("aletheia.routing.config.path"));
    breadcrumbEnv.setProperty("aletheia.endpoint.groups.config.path",
                              properties.getProperty("aletheia.endpoint.groups.config.path"));
    breadcrumbEnv.setProperty("aletheia.serdes.config.path",
                              properties.getProperty("aletheia.serdes.config.path"));
    breadcrumbEnv.setProperty("aletheia.breadcrumbs.endpoint.id",
                              properties.getProperty("aletheia.breadcrumbs.endpoint.id", ""));

    return breadcrumbEnv;
  }

  protected void configureBreadcrumbProduction() {
    if (!domainClass.equals(Breadcrumb.class)) {
      final String datumTypeId = DatumUtils.getDatumTypeId(domainClass);
      final ProductionEndPoint breadcrumbsProductionEndPoint =
              new AletheiaConfig(getBreadcrumbEnvironment()).getBreadcrumbsProductionEndPoint(datumTypeId);

      if (breadcrumbsProductionEndPoint != null) {
        saveBuilder(builder.deliverBreadcrumbsTo(breadcrumbsProductionEndPoint, getBreadcrumbConfig()));
        logger.warn("Breadcrumbs endpoint {} has been added to pipeline.", breadcrumbsProductionEndPoint);
      } else {
        logger.warn("Breadcrumbs endpoint was null or illegal. Breadcrumbs will NOT be produced for \"{}\".",
                    datumTypeId);
      }
    }
  }

  protected abstract TBuilder This();

  /**
   * Configures metrics reporting.
   *
   * @param metricFactory A MetricsFactory instance to report metrics to.
   * @return A {@link TBuilder} instance with metrics reporting configured.
   */
  public TBuilder reportMetricsTo(final MetricsFactory metricFactory) {
    builder.reportMetricsTo(metricFactory);
    return This();
  }
}
