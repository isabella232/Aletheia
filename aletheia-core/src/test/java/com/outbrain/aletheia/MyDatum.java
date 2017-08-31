package com.outbrain.aletheia;

import com.outbrain.aletheia.datum.DatumKeySelector;
import com.outbrain.aletheia.datum.DatumType;

import org.joda.time.DateTime;

@DatumType(datumTypeId = "my.datum", timestampExtractor = MyDatum.TimestampExtractor.class)
public class MyDatum {

  private long timeCreated;
  private String data;

  public MyDatum() {
  }

  public MyDatum(final String data) {
    this.timeCreated = DateTime.now().getMillis();
    this.data = data;
  }

  public long getTimeCreated() {
    return timeCreated;
  }

  public String getData() {
    return data;
  }

  public void setTimeCreated(long timeCreated) {
    this.timeCreated = timeCreated;
  }

  public void setData(String data) {
    this.data = data;
  }

  public static final class TimestampExtractor implements DatumType.TimestampSelector {
    @Override
    public DateTime extractDatumDateTime(Object domainObject) {
      return new DateTime(((MyDatum) domainObject).getTimeCreated());
    }
  }

  public static final class MyDatumKeySelector implements DatumKeySelector<MyDatum> {
    @Override
    public String getDatumKey(MyDatum domainObject) {
      return domainObject.getData();
    }
  }
}
