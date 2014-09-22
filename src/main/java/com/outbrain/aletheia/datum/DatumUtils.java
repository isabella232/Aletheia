package com.outbrain.aletheia.datum;

/**
 * A utility class for extracting various information provided via by the <code>DatumType</code> annotation.
 */
public final class DatumUtils {

  private DatumUtils() {
  }

  private static DatumType getDatumAnnotationOrThrow(final Class<?> domainClass) {
    final DatumType datumTypeAnnotation = domainClass.getAnnotation(DatumType.class);

    if (datumTypeAnnotation == null) {
      throw new UnknownDatumTypeException(String.format("The provided class %s did not contain a @%s annotation.",
                                                        domainClass.getSimpleName(),
                                                        DatumType.class.getSimpleName()));
    }

    return datumTypeAnnotation;
  }

  private static <T> T newInstanceOrThrow(final Class<T> clazz) {
    try {
      return clazz.newInstance();
    } catch (final Exception e) {
      throw new RuntimeException("Could not instantiate class:" + clazz.getSimpleName(), e);
    }
  }

  public static String getDatumTypeId(final Class<?> domainClass) {
    final DatumType datumTypeAnnotation = getDatumAnnotationOrThrow(domainClass);
    return datumTypeAnnotation.datumTypeId();
  }

  public static <T> DatumType.TimestampExtractor<T> getDatumTimestampExtractor(final Class<T> domainClass) {
    final DatumType datumTypeAnnotation = getDatumAnnotationOrThrow(domainClass);
    return newInstanceOrThrow(datumTypeAnnotation.timestampExtractor());
  }
}
