package com.outbrain.aletheia.datum.envelope;

import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import com.outbrain.aletheia.datum.serialization.SerializedDatum;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.type.DatumType;
import com.outbrain.aletheia.datum.utils.DatumUtils;
import org.joda.time.Instant;

/**
 *
 * Builds an enclosing <code>DatumEnvelope</code>s for a datum, serializing it using
 * the specified <code>DatumSerDe</code>.
 *
 * @param <TDomainClass> The type of datum to build a <code>DatumEnvelope</code> for.
 */
public class DatumEnvelopeBuilder<TDomainClass> {

  private final String hostname;
  private final int incarnation;
  private final DatumSerDe<TDomainClass> datumSerDe;

  private DatumType.TimestampExtractor<TDomainClass> datumTimestampExtractor;
  private String datumTypeId;

  public DatumEnvelopeBuilder(final DatumSerDe<TDomainClass> datumSerDe,
                              final int incarnation,
                              final String hostname) {
    this.datumSerDe = datumSerDe;
    this.hostname = hostname;
    this.incarnation = incarnation;
  }

  private void extractMetaDataIfMissing(final TDomainClass domainObject) {

    if (datumTypeId == null) {
      datumTypeId = DatumUtils.getDatumTypeId(domainObject.getClass());
    }

    if (datumTimestampExtractor == null) {
      datumTimestampExtractor =
              (DatumType.TimestampExtractor<TDomainClass>)
                      DatumUtils.getDatumTimestampExtractor(domainObject.getClass());
    }

  }

  public DatumEnvelope buildEnvelope(final TDomainClass domainObject) {

    extractMetaDataIfMissing(domainObject);

    final long logicalTimestamp = datumTimestampExtractor.extractDatumDateTime(domainObject).getMillis();
    final SerializedDatum serializedDatum = datumSerDe.serializeDatum(domainObject);

    return new DatumEnvelope(datumTypeId,
                             serializedDatum.getVersionedDatumTypeId().getVersion(),
                             logicalTimestamp,
                             incarnation,
                             hostname,
                             Instant.now().getMillis(),
                             serializedDatum.getPayload(),
                             datumSerDe.getClass().getSimpleName());
  }
}
