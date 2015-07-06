package com.outbrain.aletheia.configuration.routing;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.List;

public class ExtendedRoutingInfo {

  private final List<Route> routes;
  private final List<String> routeGroupIds;
  private final String breadcrumbsEndPointId;
  private final String datumKeySelectorClassName;

  public ExtendedRoutingInfo(@JsonProperty("routes") final List<Route> routes,
                             @JsonProperty("routeGroups") final List<String> routeGroupIds,
                             @JsonProperty("breadcrumbs") final String breadcrumbsEndPointId,
                             @JsonProperty("datum.key.selector.class") final String datumKeySelectorClassName) {
    this.routes = routes != null ? routes : Lists.<Route>newArrayList();
    this.routeGroupIds = routeGroupIds != null ? routeGroupIds : Lists.<String>newArrayList();
    this.breadcrumbsEndPointId = breadcrumbsEndPointId;
    this.datumKeySelectorClassName = datumKeySelectorClassName;
  }

  public List<Route> getRoutes() {
    return routes;
  }

  public List<String> getRouteGroupIds() {
    return routeGroupIds;
  }

  public String getDatumKeySelectorClassName() {
    return datumKeySelectorClassName;
  }

  public String getBreadcrumbsEndPointId() {
    return breadcrumbsEndPointId;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    final ExtendedRoutingInfo that = (ExtendedRoutingInfo) o;

    return EqualsBuilder.reflectionEquals(this, that);
  }

  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}
