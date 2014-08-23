package com.outbrain.aletheia.datum.serialization.avro.schema;

import com.outbrain.aletheia.datum.serialization.VersionedDatumTypeId;
import org.apache.avro.Schema;

public interface DatumSchemaRepository {

  VersionedDatumTypeId retrieveSchemaVersion(Schema schema);

  Schema retrieveSchema(VersionedDatumTypeId versionedDatumTypeId);

  Schema retrieveLatestSchema(String datumTypeId);
}