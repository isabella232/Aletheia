package com.outbrain.aletheia.breadcrumbs;


import java.util.Objects;

/**
 * An object used as a HashMap key for aggregating breadcrumb counts by datum attributes.
 */
public class BreadcrumbKey {
  private final String type;
  private final String source;
  private final String destination;
  private final String application;

  public BreadcrumbKey(final String type, final String source, final String destination, final String application) {
    this.type = type;
    this.source = source;
    this.destination = destination;
    this.application = application;
  }

  public String getType() {
    return type;
  }

  public String getSource() {
    return source;
  }

  public String getDestination() {
    return destination;
  }
  public String getApplication() {
    return application;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BreadcrumbKey that = (BreadcrumbKey) o;
    return Objects.equals(type, that.type) &&
        Objects.equals(source, that.source) &&
        Objects.equals(destination, that.destination) &&
        Objects.equals(application, that.application);
  }

  @Override
  public int hashCode() {

    return Objects.hash(type, source, destination, application);
  }

  @Override
  public String toString() {
    return "BreadcrumbKey{" +
        "type='" + type + '\'' +
        ", source='" + source + '\'' +
        ", destination='" + destination + '\'' +
        ", application='" + application + '\'' +
        '}';
  }
}
