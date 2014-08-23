package com.outbrain.aletheia.datum.type;

import org.joda.time.DateTime;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DatumType {

  interface TimestampExtractor<TDomainClass> {
    DateTime extractDatumDateTime(TDomainClass domainObject);
  }

  String datumTypeId();

  Class<? extends TimestampExtractor> timestampExtractor();
}
