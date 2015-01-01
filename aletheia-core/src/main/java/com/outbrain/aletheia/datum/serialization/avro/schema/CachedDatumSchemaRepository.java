package com.outbrain.aletheia.datum.serialization.avro.schema;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.outbrain.aletheia.datum.serialization.DatumTypeVersion;
import org.apache.avro.Schema;
import org.joda.time.Duration;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A schema caching {@link DatumSchemaRepository}.
 */
public class CachedDatumSchemaRepository implements DatumSchemaRepository {

  private static final int DEFAULT_MAX_SIZE = 100;
  private static final Duration DEFAULT_REFRESH_TIME = Duration.standardHours(1);

  private final LoadingCache<Schema, DatumTypeVersion> schema2datumTypeVersion;
  private final LoadingCache<DatumTypeVersion, Schema> datumTypeVersion2schema;
  private final LoadingCache<String, Schema> datumTypeId2schema;

  private CachedDatumSchemaRepository(final DatumSchemaRepository datumSchemaRepository,
                                      final int maxSize,
                                      final Duration refresh) {

    final CacheBuilder<Object, Object> cacheTemplate = CacheBuilder.newBuilder()
                                                                   .maximumSize(maxSize)
                                                                   .refreshAfterWrite(refresh.getStandardSeconds(),
                                                                                      TimeUnit.SECONDS);

    schema2datumTypeVersion = cacheTemplate.build(new CacheLoader<Schema, DatumTypeVersion>() {
      @Override
      public DatumTypeVersion load(final Schema schema) throws Exception {
        return datumSchemaRepository.getDatumTypeVersion(schema);
      }
    });

    datumTypeVersion2schema = cacheTemplate.build(new CacheLoader<DatumTypeVersion, Schema>() {
      @Override
      public Schema load(final DatumTypeVersion datumTypeVersion) throws Exception {
        return datumSchemaRepository.getSchema(datumTypeVersion);
      }
    });

    datumTypeId2schema = cacheTemplate.build(new CacheLoader<String, Schema>() {
      @Override
      public Schema load(final String datumTypeId) throws Exception {
        return datumSchemaRepository.getLatestSchema(datumTypeId);
      }
    });
  }

  public static DatumSchemaRepository from(final DatumSchemaRepository innerRepository) {
    return new CachedDatumSchemaRepository(innerRepository, DEFAULT_MAX_SIZE, DEFAULT_REFRESH_TIME);
  }

  public static DatumSchemaRepository from(final DatumSchemaRepository datumSchemaRepository,
                                           final int maxSize,
                                           final Duration refreshTime) {
    return new CachedDatumSchemaRepository(datumSchemaRepository, maxSize, refreshTime);
  }

  private <K, V> V getOrThrow(final K key, final LoadingCache<K, V> cache) {
    try {
      return cache.get(key);
    } catch (final ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DatumTypeVersion getDatumTypeVersion(final Schema schema) {
    return getOrThrow(schema, schema2datumTypeVersion);
  }

  @Override
  public Schema getSchema(final DatumTypeVersion datumTypeVersion) {
    return getOrThrow(datumTypeVersion, datumTypeVersion2schema);
  }

  @Override
  public Schema getLatestSchema(final String datumTypeId) {
    return getOrThrow(datumTypeId, datumTypeId2schema);
  }
}
