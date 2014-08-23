package com.outbrain.aletheia.datum.auditing;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Created by slevin on 7/14/14.
 */
public class DatumBucketKey {

  private final Instant timestamp;
  private final String datumTypeId;
  private final Duration bucketDuration;

  public DatumBucketKey(final Instant timestamp, final Duration bucketDuration, final String datumTypeId) {
    this.timestamp = timestamp;
    this.bucketDuration = bucketDuration;
    this.datumTypeId = datumTypeId;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public String getDatumTypeId() {
    return datumTypeId;
  }

  public Duration getBucketDuration() {
    return bucketDuration;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final DatumBucketKey that = (DatumBucketKey) o;

    if (timestamp != that.timestamp) return false;
    if (!bucketDuration.equals(that.bucketDuration)) return false;
    if (!datumTypeId.equals(that.datumTypeId)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = timestamp.hashCode();
    result = 31 * result + datumTypeId.hashCode();
    result = 31 * result + bucketDuration.hashCode();
    return result;
  }

  @Override
  public String toString() {
    final ToStringBuilder toStringBuilder = new ToStringBuilder(this, ToStringStyle.DEFAULT_STYLE);
    toStringBuilder.append("timestamp", getTimestamp());
    toStringBuilder.append("type", getDatumTypeId());

    return toStringBuilder.toString();
  }
}
