package com.outbrain.aletheia.datum.type.tsv;

/**
 * A default implementation for tab separated line based datum types.
 */
public class TabSeparatedLine {

  private final String domainObjectAsString;

  public TabSeparatedLine(final String domainObjectAsString) {
    this.domainObjectAsString = domainObjectAsString;
  }

  public String getDomainObjectAsString() {
    return domainObjectAsString;
  }

  @Override
  public String toString() {
    return getDomainObjectAsString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TabSeparatedLine)) {
      return false;
    }

    final TabSeparatedLine that = (TabSeparatedLine) o;

    return !(domainObjectAsString != null ? !domainObjectAsString.equals(that.domainObjectAsString) : that.domainObjectAsString != null);

  }

  @Override
  public int hashCode() {
    return domainObjectAsString != null ? domainObjectAsString.hashCode() : 0;
  }
}
