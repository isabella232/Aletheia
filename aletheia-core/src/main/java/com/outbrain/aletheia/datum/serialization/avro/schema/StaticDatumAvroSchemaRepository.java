package com.outbrain.aletheia.datum.serialization.avro.schema;

import com.outbrain.aletheia.datum.serialization.DatumTypeVersion;
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

  private Schema detectAvroSchema(final Class<? extends SpecificRecord> avroClass) {
    try {
      return (Schema) avroClass.getDeclaredMethod("getClassSchema").invoke(null);
    } catch (final Exception e) {
      return null;
    }

  }

  private String getFullyQualifiedAvroClassFromDatumTypeId(final String datumTypeId) {
    return String.format("%s.%s", avroClassPackage, datumTypeId);
  }

  private Class<? extends SpecificRecord> getAvroClassForDatumTypeId(final String datumTypeId) {
    try {
      final Class<?> avroClass = Class.forName(getFullyQualifiedAvroClassFromDatumTypeId(datumTypeId));
      if (!SpecificRecord.class.isAssignableFrom(avroClass)) {
        throw new RuntimeException(
                "Expected avro class is not an avro SpecificRecord, which means it has not been generated properly");
      }
      final Class<? extends SpecificRecord> datumTypeAvroClass = (Class<? extends SpecificRecord>) avroClass;
      return datumTypeAvroClass;
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException("Could not find avro class for datum type " + datumTypeId);
    }
  }

  @Override
  public DatumTypeVersion getDatumTypeVersion(final Schema schema) {
    return new DatumTypeVersion(schema.getName(), -31415927);
  }

  @Override
  public Schema getSchema(final DatumTypeVersion datumTypeVersion) {
    return getLatestSchema(datumTypeVersion.getDatumTypeId());
  }

  @Override
  public Schema getLatestSchema(final String datumTypeId) {
    final Class<? extends SpecificRecord> avroClass = getAvroClassForDatumTypeId(datumTypeId);
    return detectAvroSchema(avroClass);
  }

}
