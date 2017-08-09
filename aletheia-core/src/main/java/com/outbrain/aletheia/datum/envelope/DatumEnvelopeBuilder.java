package com.outbrain.aletheia.datum.envelope;

import com.outbrain.aletheia.datum.DatumKeySelector;
import com.outbrain.aletheia.datum.DatumType;
import com.outbrain.aletheia.datum.DatumUtils;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import com.outbrain.aletheia.datum.serialization.SerializedDatum;
import org.joda.time.Instant;

/**
 * Builds an enclosing {@link DatumEnvelope}s for a datum, serializing it using the specified {@link
 * DatumSerDe}.
 *
 * @param <TDomainClass> The type of datum to build a {@link DatumEnvelope} for.
 */
public class DatumEnvelopeBuilder<TDomainClass> {

  private final String source;
  private final int incarnation;
  private final String datumTypeId;
  private final DatumSerDe<TDomainClass> datumSerDe;
  private final DatumType.TimestampSelector<TDomainClass> datumTimestampSelector;
  private final DatumKeySelector<TDomainClass> datumKeySelector;

  public DatumEnvelopeBuilder(final Class<TDomainClass> domainClass,
                              final DatumSerDe<TDomainClass> datumSerDe,
                              final DatumKeySelector<TDomainClass> datumKeySelector,
                              final int incarnation,
                              final String source) {

    this.datumSerDe = datumSerDe;
    this.source = source;
    this.incarnation = incarnation;

    this.datumKeySelector = datumKeySelector;
    datumTimestampSelector = DatumUtils.getDatumTimestampExtractor(domainClass);
    datumTypeId = DatumUtils.getDatumTypeId(domainClass);
  }

  public DatumEnvelope buildEnvelope(final TDomainClass domainObject) {

    final long logicalTimestamp = datumTimestampSelector.extractDatumDateTime(domainObject).getMillis();
    final String datumKey = datumKeySelector.getDatumKey(domainObject);
    final SerializedDatum serializedDatum = datumSerDe.serializeDatum(domainObject);

    return new DatumEnvelope(datumTypeId,
        serializedDatum.getDatumTypeVersion().getVersion(),
        logicalTimestamp,
        incarnation,
        source,
        Instant.now().getMillis(),
        serializedDatum.getPayload(),
        datumSerDe.getClass().getSimpleName(),
        datumKey);
  }
}
