package com.outbrain.aletheia.datum.serialization.avro.schema;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.outbrain.aletheia.datum.serialization.DatumTypeVersion;
import org.apache.avro.Schema;

/**
 * A base interface for datum schema repositories.
 */
@JsonTypeInfo(use= JsonTypeInfo.Id.CLASS)
public interface DatumSchemaRepository {

  DatumTypeVersion getDatumTypeVersion(Schema schema);

  Schema getSchema(DatumTypeVersion datumTypeVersion);

  Schema getLatestSchema(String datumTypeId);
}