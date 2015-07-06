package com.outbrain.aletheia.configuration.routing;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.outbrain.aletheia.datum.DatumKeySelector;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.List;

/**
 * Created by slevin on 5/19/15.
 */
public class RoutingInfo {

  private final List<Route> routes;
  private final String datumKeySelectorClassName;

  public RoutingInfo(final List<Route> routes, final String datumKeySelectorClassName) {
    this.routes = routes != null ? routes : Lists.<Route>newArrayList();
    this.datumKeySelectorClassName = datumKeySelectorClassName;
  }

  public List<Route> getRoutes() {
    return routes;
  }

  public String getDatumKeySelectorClassName() {
    return datumKeySelectorClassName;
  }

  public DatumKeySelector getDatumKeySelector() {
    try {
      return !Strings.isNullOrEmpty(getDatumKeySelectorClassName()) ?
             (DatumKeySelector) Class.forName(getDatumKeySelectorClassName()).newInstance() :
             DatumKeySelector.NULL;
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException(
              String.format("Could not instantiate %s of type %s",
                            DatumKeySelector.class.getSimpleName(),
                            Strings.nullToEmpty(getDatumKeySelectorClassName())),
              e);
    }
  }


  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    final RoutingInfo that = (RoutingInfo) o;

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
