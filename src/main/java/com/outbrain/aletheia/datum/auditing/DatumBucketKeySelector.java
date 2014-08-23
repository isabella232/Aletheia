package com.outbrain.aletheia.datum.auditing;

import com.outbrain.aletheia.breadcrumbs.BucketKeySelector;
import com.outbrain.aletheia.datum.type.DatumType;
import com.outbrain.aletheia.datum.utils.DatumUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Created by slevin on 7/14/14.
 */
public class DatumBucketKeySelector<TDomainClass> implements BucketKeySelector<TDomainClass, DatumBucketKey> {

  private final Duration bucketDuration;

  public DatumBucketKeySelector(final Duration bucketDuration) {

    this.bucketDuration = bucketDuration;
  }

  private Instant getRoundedDatumDateTime(final TDomainClass domainObject) {

    final DatumType.TimestampExtractor<TDomainClass> timestampExtractor =
            DatumUtils.getDatumTimestampExtractor((Class<TDomainClass>) domainObject.getClass());

    final long logicalDateTIme = timestampExtractor.extractDatumDateTime(domainObject).getMillis();

    return new Instant((logicalDateTIme / bucketDuration.getMillis()) * bucketDuration.getMillis());
  }

  @Override
  public DatumBucketKey selectKey(final TDomainClass domainObject) {

    final Instant roundedTimestamp = getRoundedDatumDateTime(domainObject);
    final String datumTypeId = DatumUtils.getDatumTypeId(domainObject.getClass());

    return new DatumBucketKey(roundedTimestamp, bucketDuration, datumTypeId);
  }
}
