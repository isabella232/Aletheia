package com.outbrain.aletheia.tutorial;

import com.outbrain.aletheia.datum.DatumType;
import org.joda.time.DateTime;
import org.joda.time.Instant;

/**
 * Created by slevin on 9/17/14.
 */
@DatumType(datumTypeId = "my_datum_id", timestampExtractor = MyDatum.TimeExtractor.class)
public class MyDatum {

  public static class TimeExtractor implements DatumType.TimestampExtractor<MyDatum> {
    @Override
    public DateTime extractDatumDateTime(final MyDatum domainObject) {
      return domainObject.getTimestamp().toDateTime();
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
}
