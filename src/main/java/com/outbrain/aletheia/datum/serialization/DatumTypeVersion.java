package com.outbrain.aletheia.datum.serialization;

/**
 * A container class for datum type id and version information.
 */
public class DatumTypeVersion {

  private final String datumTypeId;
  private final int version;

  public DatumTypeVersion(final String datumTypeId, final int version) {
    this.datumTypeId = datumTypeId;
    this.version = version;
  }

  public String getDatumTypeId() {
    return datumTypeId;
  }

  public int getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "VersionDatumTypeId{" +
            "datumTypeId='" + datumTypeId + '\'' +
            ", datumSchemaVersion=" + version +
            '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final DatumTypeVersion that = (DatumTypeVersion) o;

    if (version != that.version) return false;
    if (!datumTypeId.equals(that.datumTypeId)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = datumTypeId.hashCode();
    result = 31 * result + version;
    return result;
  }
}
