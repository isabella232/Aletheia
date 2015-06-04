package com.outbrain.aletheia.datum;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Defines the interface to be implemented by the various datum key selection strategies.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface DatumKeySelector<TDomainClass> {

  DatumKeySelector NULL = new DatumKeySelector() {

    @Override
    public String getDatumKey(final Object domainObject) {
      return null;
    }
  };

  String getDatumKey(TDomainClass domainObject);
}
