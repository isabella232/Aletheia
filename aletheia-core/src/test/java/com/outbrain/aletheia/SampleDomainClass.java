package com.outbrain.aletheia;

import com.outbrain.aletheia.datum.DatumKeySelector;
import com.outbrain.aletheia.datum.DatumType;
import com.outbrain.aletheia.datum.avro.sample_domain_class;
import com.outbrain.aletheia.datum.serialization.avro.AvroRoundTripProjector;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.joda.time.DateTime;
import org.joda.time.Instant;

@DatumType(datumTypeId = "sample_domain_class", timestampExtractor = SampleDomainClass.SampleDomainClassTimestampSelector.class)
public class SampleDomainClass {

  public static class SampleDomainClassTimestampSelector implements DatumType.TimestampSelector<SampleDomainClass> {
    @Override
    public DateTime extractDatumDateTime(final SampleDomainClass domainObject) {
      return domainObject.getEventTimestamp().toDateTime();
    }
  }

  public static class SampleDomainClassAvroRoundTripProjector implements AvroRoundTripProjector<SampleDomainClass> {

    @Override
    public SpecificRecord toAvro(final SampleDomainClass domainObject) {
      return sample_domain_class.newBuilder()
                                .setId(domainObject.getId())
                                .setMyNumber(domainObject.getMyNumber())
                                .setMyString(domainObject.getMyString())
                                .setEventTimestamp(domainObject.getEventTimestamp().getMillis())
                                .setShouldBeSent(domainObject.isDiscarded())
                                .build();
    }

    @Override
    public SampleDomainClass fromAvro(final SpecificRecord domainObject) {
      final sample_domain_class avroObject = (sample_domain_class) domainObject;
      return new SampleDomainClass(avroObject.getId(),
                                   avroObject.getMyNumber(),
                                   avroObject.getMyString().toString(),
                                   new Instant(avroObject.getEventTimestamp()),
                                   avroObject.getShouldBeSent());
    }
  }

  /**
   * Created by slevin on 6/29/15.
   */
  public static class SampleDatumKeySelector<TDomainClass> implements DatumKeySelector<TDomainClass> {

    public static final String DATUM_KEY = "datumKey";

    @Override
    public String getDatumKey(final TDomainClass domainObject) {
      return DATUM_KEY;
    }
  }

  private int id;
  private double myNumber;
  private String myString;
  private Instant eventTimestamp;
  private boolean discarded;


  private SampleDomainClass() {
  }

  public SampleDomainClass(final int id,
                           final double myNumber,
                           final String myString,
                           final Instant eventTimestamp,
                           final boolean discarded) {
    this.id = id;
    this.myNumber = myNumber;
    this.myString = myString;
    this.eventTimestamp = eventTimestamp;
    this.discarded = discarded;
  }

  public int getId() {
    return id;
  }

  public void setId(final int id) {
    this.id = id;
  }

  public double getMyNumber() {
    return myNumber;
  }

  public void setMyNumber(final double myNumber) {
    this.myNumber = myNumber;
  }

  public String getMyString() {
    return myString;
  }

  public void setMyString(final String myString) {
    this.myString = myString;
  }

  public Instant getEventTimestamp() {
    return eventTimestamp;
  }

  public void setEventTimestamp(final Instant eventTimestamp) {
    this.eventTimestamp = eventTimestamp;
  }

  public boolean isDiscarded() {
    return discarded;
  }

  public void setDiscarded(final boolean discarded) {
    this.discarded = discarded;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    final SampleDomainClass that = (SampleDomainClass) o;

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
