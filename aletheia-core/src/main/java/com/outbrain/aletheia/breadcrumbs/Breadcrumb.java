package com.outbrain.aletheia.breadcrumbs;

import com.outbrain.aletheia.datum.DatumType;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.joda.time.DateTime;
import org.joda.time.Instant;

/**
 * Metadata containing information (source, destination, time frame, count ) about a particular datum type's
 * transportation from one point to another. Breadcrumbs are aimed to assist at monitor data flow along the pipeline
 * by comparing outgoing and incoming numbers.
 */
@DatumType(datumTypeId = "breadcrumb", timestampExtractor = Breadcrumb.BreadcrumbTimestampSelector.class)
public class Breadcrumb {

  public static class BreadcrumbTimestampSelector implements DatumType.TimestampSelector<Breadcrumb> {
    @Override
    public DateTime extractDatumDateTime(final Breadcrumb domainObject) {
      return domainObject.getProcessingTimestamp().toDateTime();
    }
  }

  private String source;
  private String destination;
  private String type;
  private String datacenter;
  private String tier;
  private Instant bucketStartTime;
  private Instant bucketEndTime;
  private Instant processingTimestamp;
  private long count;

  private String application;

  private Breadcrumb() {
  }

  public Breadcrumb(final String type,
                    final String source,
                    final String destination,
                    final Instant bucketStartTime,
                    final Instant bucketEndTime,
                    final Instant processingTimestamp,
                    final long count,
                    final String datacenter,
                    final String application,
                    final String tier) {

    this.source = source;
    this.destination = destination;
    this.type = type;
    this.datacenter = datacenter;
    this.application = application;
    this.tier = tier;
    this.bucketStartTime = bucketStartTime;
    this.bucketEndTime = bucketEndTime;
    this.processingTimestamp = processingTimestamp;
    this.count = count;
  }

  public Instant getProcessingTimestamp() {
    return processingTimestamp;
  }

  public long getCount() {
    return count;
  }

  public Instant getBucketStartTime() {
    return bucketStartTime;
  }

  public Instant getBucketEndTime() {
    return bucketEndTime;
  }

  public String getSource() {
    return source;
  }

  public String getType() {
    return type;
  }

  public String getDatacenter() {
    return datacenter;
  }

  public String getTier() {
    return tier;
  }

  public String getApplication() {
    return application;
  }

  public String getDestination() {
    return destination;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final Breadcrumb that = (Breadcrumb) o;

    if (count != that.count) return false;
    if (!application.equals(that.application)) return false;
    if (!bucketEndTime.equals(that.bucketEndTime)) return false;
    if (!bucketStartTime.equals(that.bucketStartTime)) return false;
    if (!datacenter.equals(that.datacenter)) return false;
    if (!destination.equals(that.destination)) return false;
    if (!processingTimestamp.equals(that.processingTimestamp)) return false;
    if (!source.equals(that.source)) return false;
    if (!tier.equals(that.tier)) return false;
    if (!type.equals(that.type)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = source.hashCode();
    result = 31 * result + destination.hashCode();
    result = 31 * result + type.hashCode();
    result = 31 * result + datacenter.hashCode();
    result = 31 * result + tier.hashCode();
    result = 31 * result + bucketStartTime.hashCode();
    result = 31 * result + bucketEndTime.hashCode();
    result = 31 * result + processingTimestamp.hashCode();
    result = 31 * result + (int) (count ^ (count >>> 32));
    result = 31 * result + application.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}
