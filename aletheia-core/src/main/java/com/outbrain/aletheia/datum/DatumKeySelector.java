package com.outbrain.aletheia.datum;

/**
* Defines the interface to be implemented by the various datum key selection strategies.
*/
public interface DatumKeySelector<TDomainClass> {

  static DatumKeySelector NULL = new DatumKeySelector() {

    @Override
    public String getDatumKey(final Object domainObject) {
      return null;
    }
  };

  String getDatumKey(TDomainClass domainObject);
}
