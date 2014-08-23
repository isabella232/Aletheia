package com.outbrain.aletheia.datum.serialization.avro.schema;

import com.outbrain.aletheia.datum.serialization.VersionedDatumTypeId;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

public class StaticDatumAvroSchemaRepository implements DatumSchemaRepository {

  private final String avroClassPackage;

  public StaticDatumAvroSchemaRepository() {
    this("com.outbrain.aletheia.datum.avro");
  }

  public StaticDatumAvroSchemaRepository(final String avroClassPackage) {
    this.avroClassPackage = avroClassPackage;
  }

  @Override
  public VersionedDatumTypeId retrieveSchemaVersion(final Schema schema) {
    return new VersionedDatumTypeId(schema.getName(), -31415927);
  }

  @Override
  public Schema retrieveSchema(final VersionedDatumTypeId versionedDatumTypeId) {
    // RLRL - Currently return own schema as the only available schema, until we create the schema repository
    return retrieveLatestSchema(versionedDatumTypeId.getDatumTypeId());
  }

  @Override
  public Schema retrieveLatestSchema(final String datumTypeId) {
    final Class<? extends SpecificRecord> avroClass = getAvroClassForDatumType(datumTypeId);
    final Schema datumTypeOwnSchema = detectAvroSchema(avroClass);
    return datumTypeOwnSchema;
  }

  public String getFullyQualifiedAvroClassFromDatumTypeId(final String datumTypeId) {
    return String.format("%s.%s", avroClassPackage, datumTypeId);
  }

  public Class<? extends SpecificRecord> getAvroClassForDatumType(final String datumTypeId) {
    try {
      final Class<?> avroClass = Class.forName(getFullyQualifiedAvroClassFromDatumTypeId(datumTypeId));
      if (!SpecificRecord.class.isAssignableFrom(avroClass)) {
        throw new RuntimeException("Expected avro class is not an avro SpecificRecord, which means it has not been generated properly");
      }
      final Class<? extends SpecificRecord> datumTypeAvroClass = (Class<? extends SpecificRecord>) avroClass;
      return datumTypeAvroClass;
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException("Could not find avro class for datum type " + datumTypeId);
    }
  }

  public Schema detectAvroSchema(final Class<? extends SpecificRecord> avroClass) {
    Schema datumBodySchema = null;
    try {
      datumBodySchema = (Schema) avroClass.getDeclaredMethod("getClassSchema").invoke(null);
      return datumBodySchema;
    } catch (final Exception e) {
      return null;
    }

  }

}
