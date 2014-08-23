package com.outbrain.aletheia.datum.auditing;

import com.outbrain.aletheia.breadcrumbs.BucketKeySelector;
import com.outbrain.aletheia.datum.envelope.DatumEnvelopeUtils;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class DatumEnvelopeBucketKeySelector implements BucketKeySelector<DatumEnvelope, DatumEnvelopeBucketKey> {

  private final Duration bucketDuration;

  public DatumEnvelopeBucketKeySelector(final Duration bucketDuration) {

    this.bucketDuration = bucketDuration;
  }

  private Instant getLogicalTimestamp(final DatumEnvelope datumEnvelope) {
    return new Instant((datumEnvelope.getLogicalTimestamp() / bucketDuration.getMillis()) * bucketDuration.getMillis());
  }

  @Override
  public DatumEnvelopeBucketKey selectKey(final DatumEnvelope datumEnvelope) {

    final Instant roundedTimestamp = getLogicalTimestamp(datumEnvelope);
    final String datumTypeId = DatumEnvelopeUtils.getDatumTypeId(datumEnvelope);

    return new DatumEnvelopeBucketKey(roundedTimestamp, bucketDuration, datumTypeId);
  }
}
