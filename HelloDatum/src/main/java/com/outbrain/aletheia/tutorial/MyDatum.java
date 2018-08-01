package com.outbrain.aletheia.tutorial;

import com.outbrain.aletheia.datum.DatumKeySelector;
import com.outbrain.aletheia.datum.DatumType;

import org.joda.time.DateTime;
import org.joda.time.Instant;

@DatumType(datumTypeId = "my_datum_id", timestampExtractor = MyDatum.TimeSelector.class)
public class MyDatum {

  public static class TimeSelector implements DatumType.TimestampSelector<MyDatum> {
    @Override
    public DateTime extractDatumDateTime(final MyDatum domainObject) {
      return domainObject.getTimestamp().toDateTime();
    }
  }

  public static class MyKeySelector implements DatumKeySelector<MyDatum> {
    @Override
    public String getDatumKey(MyDatum domainObject) {
      return domainObject.getInfo();
    }
  }

  private Instant timestamp;
  private String info;

  MyDatum() {
  }

  public MyDatum(final Instant timestamp, final String info) {
    this.timestamp = timestamp;
    this.info = info;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public String getInfo() {
    return info;
  }

  @Override
  public String toString() {
    return "MyDatum{" +
            "timestamp=" + timestamp +
            ", info='" + info + '\'' +
            '}';
  }
}
