package com.outbrain.aletheia.datum;

public class UnknownDatumTypeException extends RuntimeException {
  public UnknownDatumTypeException(final String type) {
    super(type);
  }
}
