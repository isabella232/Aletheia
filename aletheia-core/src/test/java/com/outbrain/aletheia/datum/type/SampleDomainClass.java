package com.outbrain.aletheia.datum.type;

import com.outbrain.aletheia.datum.DatumType;
import org.joda.time.DateTime;
import org.joda.time.Instant;

@DatumType(datumTypeId = "sample_domain_class",
           timestampExtractor = SampleDomainClass.SampleDomainClassTimestampSelector.class)
public class SampleDomainClass {

  public static class SampleDomainClassTimestampSelector implements DatumType.TimestampSelector<SampleDomainClass> {
    @Override
    public DateTime extractDatumDateTime(final SampleDomainClass domainObject) {
      return domainObject.getEventTimestamp().toDateTime();
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

    if (id != that.id) return false;
    if (Double.compare(that.myNumber, myNumber) != 0) return false;
    if (discarded != that.discarded) return false;
    if (!eventTimestamp.equals(that.eventTimestamp)) return false;
    if (!myString.equals(that.myString)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = id;
    temp = Double.doubleToLongBits(myNumber);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    result = 31 * result + myString.hashCode();
    result = 31 * result + eventTimestamp.hashCode();
    result = 31 * result + (discarded ? 1 : 0);
    return result;
  }
}
