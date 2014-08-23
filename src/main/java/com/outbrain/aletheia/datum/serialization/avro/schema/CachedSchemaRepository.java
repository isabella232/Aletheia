package com.outbrain.aletheia.datum.serialization.avro.schema;

import com.outbrain.aletheia.datum.serialization.VersionedDatumTypeId;
import org.apache.avro.Schema;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class CachedSchemaRepository implements DatumSchemaRepository {

  private final DatumSchemaRepository innerRepository;
  private final ConcurrentMap<Schema, VersionedDatumTypeId> schemaToVersionedType = new ConcurrentHashMap<>();
  private final ConcurrentMap<VersionedDatumTypeId, Schema> versionedTypeIdToSchema = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Schema> typeIdToSchema = new ConcurrentHashMap<>();

  // this is used to limit the size of the caches (very roughly).
  boolean cachesAreTooBig = false;
  AtomicLong sizesOfAllCaches = new AtomicLong(0);


  public CachedSchemaRepository(final DatumSchemaRepository innerRepository) {
    this.innerRepository = innerRepository;
  }

  @Override
  public VersionedDatumTypeId retrieveSchemaVersion(final Schema schema) {
    VersionedDatumTypeId versionedDatumTypeId = schemaToVersionedType.get(schema);
    if (versionedDatumTypeId == null) {
      versionedDatumTypeId = innerRepository.retrieveSchemaVersion(schema);
      if (!cachesAreTooBig) {
        schemaToVersionedType.put(schema, versionedDatumTypeId);
        final long currentSize = sizesOfAllCaches.incrementAndGet();
        if (currentSize > 100) {
          cachesAreTooBig = true;
        }
      }
    }
    return versionedDatumTypeId;
  }

  @Override
  public Schema retrieveSchema(final VersionedDatumTypeId versionedDatumTypeId) {
    Schema schema = versionedTypeIdToSchema.get(versionedDatumTypeId);
    if (schema == null) {
      schema = innerRepository.retrieveSchema(versionedDatumTypeId);
      if (!cachesAreTooBig) {
        versionedTypeIdToSchema.put(versionedDatumTypeId, schema);
        final long currentSize = sizesOfAllCaches.incrementAndGet();
        if (currentSize > 100) {
          cachesAreTooBig = true;
        }
      }
    }
    return schema;
  }

  @Override
  public Schema retrieveLatestSchema(final String datumTypeId) {
    Schema schema = typeIdToSchema.get(datumTypeId);
    if (schema == null) {
      schema = innerRepository.retrieveLatestSchema(datumTypeId);
      if (!cachesAreTooBig) {
        typeIdToSchema.put(datumTypeId, schema);
        final long currentSize = sizesOfAllCaches.incrementAndGet();
        if (currentSize > 100) {
          cachesAreTooBig = true;
        }
      }
    }
    return schema;
  }
}
