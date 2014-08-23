package com.outbrain.aletheia.datum.auditing;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class DatumEnvelopeBucketKey {

  private final Instant timestamp;
  private final String envelopeDatumType;
  private final Duration bucketDuration;

  public DatumEnvelopeBucketKey(final Instant timestamp, final Duration bucketDuration, final String envelopeDatumType) {
    this.timestamp = timestamp;
    this.bucketDuration = bucketDuration;
    this.envelopeDatumType = envelopeDatumType;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public String getEnvelopeDatumType() {
    return envelopeDatumType;
  }

  public Duration getBucketDuration() {
    return bucketDuration;
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    }

    if (!(other instanceof DatumEnvelopeBucketKey)) {
      return false;
    }

    final DatumEnvelopeBucketKey o = (DatumEnvelopeBucketKey) other;
    final EqualsBuilder equalsBuilder = new EqualsBuilder();

    equalsBuilder.append(getTimestamp(), o.getTimestamp());
    equalsBuilder.append(getEnvelopeDatumType(), o.getEnvelopeDatumType());

    return equalsBuilder.isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(getTimestamp()).append(getEnvelopeDatumType()).toHashCode();
  }

  @Override
  public String toString() {
    final ToStringBuilder toStringBuilder = new ToStringBuilder(this, ToStringStyle.DEFAULT_STYLE);
    toStringBuilder.append("timestamp", getTimestamp());
    toStringBuilder.append("type", getEnvelopeDatumType());

    return toStringBuilder.toString();
  }
}
