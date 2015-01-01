package com.outbrain.aletheia.datum.production;

import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;

import java.io.UnsupportedEncodingException;

/**
 * A special case of a {@code NamedSender&lt;DatumEnvelope&gt;}, that extracts the datum part of the
 * incoming {@code DatumEnvelope}, and decodes it using {@code UTF-8} encoding. It is assumed that
 * the incoming {@code DatumEnvelopes} do indeed have a serialized, {@code UTF-8} encoded, string datum.
 */
public class DatumEnvelopePeelingStringSender implements NamedSender<DatumEnvelope> {

  public static final String UTF_8 = "UTF-8";

  private final DatumKeyAwareNamedSender<String> stringTransporter;

  public DatumEnvelopePeelingStringSender(final DatumKeyAwareNamedSender<String> stringTransporter) {
    this.stringTransporter = stringTransporter;
  }

  private String getDataAsString(final byte[] bytes) {
    try {
      return new String(bytes, UTF_8);
    } catch (final UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void send(final DatumEnvelope datumEnvelope) throws SilentSenderException {
    final String dataAsString = getDataAsString(datumEnvelope.getDatumBytes().array());
    final String key = datumEnvelope.getDatumKey() != null ? datumEnvelope.getDatumKey().toString() : null;
    stringTransporter.send(dataAsString, key);
  }

  @Override
  public String getName() {
    return stringTransporter.getName();
  }
}
