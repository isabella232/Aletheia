package com.outbrain.aletheia.configuration;

import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.Properties;

/**
 * Created by slevin on 6/11/15.
 */
public class PropertyUtils {

  /**
   * Created by slevin on 6/17/15.
   */
  private static class ImmutableProperties extends Properties {

    public ImmutableProperties(final Properties properties) {
      for (final Map.Entry<Object, Object> pair : properties.entrySet()) {
        super.put(pair.getKey(), pair.getValue());
      }
    }

    @Override
    public synchronized Object put(final Object key, final Object value) {
      throw new UnsupportedOperationException("These properties are immutable and cannot be changed.");
    }
  }

  private final Properties source;
  private Properties target;

  public PropertyUtils(final Properties source) {
    this.source = source;
  }

  private static Properties copy(final Properties from, final Properties to, final boolean intersectingOnly) {
    Preconditions.checkNotNull(from, "cannot copy from a null source");
    Preconditions.checkNotNull(to, "cannot copy to a null target");

    for (final Map.Entry<Object, Object> pair : from.entrySet()) {
      if (!intersectingOnly || to.getProperty((String) pair.getKey()) != null) {
        to.setProperty((String) pair.getKey(), ((String) pair.getValue()));
      }
    }

    return to;
  }

  public static PropertyUtils override(final Properties source) {
    return new PropertyUtils(source);
  }

  public PropertyUtils with(final Properties target) {
    this.target = target;
    return this;
  }

  public Properties intersectingOnly() {
    return copy(target, copy(source, new Properties(), false), true);
  }

  public Properties all() {
    return copy(target, copy(source, new Properties(), false), false);
  }

  public static Properties makeImmutable(final Properties properties) {
    return new ImmutableProperties(properties);
  }
}
