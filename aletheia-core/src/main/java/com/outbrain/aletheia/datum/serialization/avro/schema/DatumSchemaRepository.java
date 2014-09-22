package com.outbrain.aletheia.datum.serialization.avro.schema;

import com.outbrain.aletheia.datum.serialization.DatumTypeVersion;
import org.apache.avro.Schema;

/**
 * A base interface for datum schema repositories.
 */
public interface DatumSchemaRepository {

  DatumTypeVersion getDatumTypeVersion(Schema schema);

  Schema getSchema(DatumTypeVersion datumTypeVersion);

  Schema getLatestSchema(String datumTypeId);
}