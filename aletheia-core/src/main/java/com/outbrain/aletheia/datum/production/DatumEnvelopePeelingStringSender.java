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

  private final NamedSender<String> stringTransporter;

  public DatumEnvelopePeelingStringSender(final NamedSender<String> stringTransporter) {
    this.stringTransporter = stringTransporter;
  }

  @Override
  public void send(final DatumEnvelope datumEnvelope) throws SilentSenderException {
    final byte[] bytes = datumEnvelope.getDatumBytes().array();
    try {
      final String data = new String(bytes, UTF_8);
      stringTransporter.send(data);
    } catch (final UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getName() {
    return stringTransporter.getName();
  }
}
