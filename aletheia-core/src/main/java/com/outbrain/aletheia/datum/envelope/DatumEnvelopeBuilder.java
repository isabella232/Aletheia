package com.outbrain.aletheia.datum.envelope;

import com.outbrain.aletheia.datum.DatumType;
import com.outbrain.aletheia.datum.DatumUtils;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import com.outbrain.aletheia.datum.serialization.SerializedDatum;
import org.joda.time.Instant;

/**
 * Builds an enclosing {@code DatumEnvelope}s for a datum, serializing it using
 * the specified {@code DatumSerDe}.
 *
 * @param <TDomainClass> The type of datum to build a {@code DatumEnvelope} for.
 */
public class DatumEnvelopeBuilder<TDomainClass> {

  private final String hostname;
  private final int incarnation;
  private final DatumSerDe<TDomainClass> datumSerDe;

  private final DatumType.TimestampExtractor<TDomainClass> datumTimestampExtractor;
  private final String datumTypeId;

  public DatumEnvelopeBuilder(final Class<TDomainClass> domainClass,
                              final DatumSerDe<TDomainClass> datumSerDe,
                              final int incarnation,
                              final String hostname) {

    this.datumSerDe = datumSerDe;
    this.hostname = hostname;
    this.incarnation = incarnation;

    datumTimestampExtractor = DatumUtils.getDatumTimestampExtractor(domainClass);
    datumTypeId = DatumUtils.getDatumTypeId(domainClass);
  }

  public DatumEnvelope buildEnvelope(final TDomainClass domainObject) {

    final long logicalTimestamp = datumTimestampExtractor.extractDatumDateTime(domainObject).getMillis();
    final SerializedDatum serializedDatum = datumSerDe.serializeDatum(domainObject);

    return new DatumEnvelope(datumTypeId,
                             serializedDatum.getDatumTypeVersion().getVersion(),
                             logicalTimestamp,
                             incarnation,
                             hostname,
                             Instant.now().getMillis(),
                             serializedDatum.getPayload(),
                             datumSerDe.getClass().getSimpleName());
  }
}
