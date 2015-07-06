package com.outbrain.aletheia;

import com.google.common.base.Predicate;
import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Created by slevin on 6/28/15.
 */
class ProductionEndPointInfo<T> {

  private final ProductionEndPoint productionEndPoint;

  private final DatumSerDe<T> datumSerDe;

  private final Predicate<T> filter;

  public ProductionEndPointInfo(final ProductionEndPoint productionEndPoint,
                                final DatumSerDe<T> datumSerDe,
                                final Predicate<T> filter) {
    this.productionEndPoint = productionEndPoint;
    this.datumSerDe = datumSerDe;
    this.filter = filter;
  }

  public Predicate<T> getFilter() {
    return filter;
  }

  public ProductionEndPoint getProductionEndPoint() {
    return productionEndPoint;
  }

  public DatumSerDe<T> getDatumSerDe() {
    return datumSerDe;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    final ProductionEndPointInfo<?> that = (ProductionEndPointInfo<?>) o;

    return EqualsBuilder.reflectionEquals(this, that);
  }

  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }
}
