package com.outbrain.aletheia.datum.serialization.avro.schema;

/**
 * Created by slevin on 5/25/15.
 */
public class DefaultStaticDatumAvroSchemaRepository extends CachedDatumSchemaRepository {

  private DefaultStaticDatumAvroSchemaRepository() {
    super(new StaticDatumAvroSchemaRepository(), DEFAULT_MAX_SIZE, DEFAULT_REFRESH_TIME);
  }
}
