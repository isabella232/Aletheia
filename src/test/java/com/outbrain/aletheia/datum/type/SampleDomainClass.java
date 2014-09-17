package com.outbrain.aletheia.datum.type;

import com.outbrain.aletheia.datum.type.tsv.TabSeparatedLine;
import org.joda.time.DateTime;
import org.joda.time.Instant;

@DatumType(datumTypeId = "test_domain_class",
           timestampExtractor = SampleDomainClass.SampleDomainClassTimestampExtractor.class)
public class SampleDomainClass extends TabSeparatedLine {

  public static class SampleDomainClassTimestampExtractor implements DatumType.TimestampExtractor<SampleDomainClass> {
    @Override
    public DateTime extractDatumDateTime(final SampleDomainClass domainObject) {
      return domainObject.getEventTimestamp().toDateTime();
    }
  }

  private int id;
  private double myNumber;
  private String myString;
  private Instant eventTimestamp;
  private boolean shouldBeSent;

  public SampleDomainClass(final int id,
                           final double myNumber,
                           final String myString,
                           final Instant eventTimestamp,
                           final boolean shouldBeSent) {

    super(id + "\t" + myNumber + "\t" + myString + "\t" + eventTimestamp + "\t" + shouldBeSent);

    this.id = id;
    this.myNumber = myNumber;
    this.myString = myString;
    this.eventTimestamp = eventTimestamp;
    this.shouldBeSent = shouldBeSent;
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

  public boolean shouldBeSent() {
    return shouldBeSent;
  }

  public void setShouldBeSent(final boolean shouldBeSent) {
    this.shouldBeSent = shouldBeSent;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    final SampleDomainClass that = (SampleDomainClass) o;

    if (id != that.id) return false;
    if (Double.compare(that.myNumber, myNumber) != 0) return false;
    if (shouldBeSent != that.shouldBeSent) return false;
    if (!eventTimestamp.equals(that.eventTimestamp)) return false;
    if (!myString.equals(that.myString)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    final long temp;
    result = 31 * result + id;
    temp = Double.doubleToLongBits(myNumber);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    result = 31 * result + myString.hashCode();
    result = 31 * result + eventTimestamp.hashCode();
    result = 31 * result + (shouldBeSent ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return getDomainObjectAsString();
  }
}
