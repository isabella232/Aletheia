package com.outbrain.aletheia.datum.type;

public class UnknownDatumTypeException extends RuntimeException {
  public UnknownDatumTypeException(final String type) {
    super(type);
  }
}
