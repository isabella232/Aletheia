package com.outbrain.aletheia.datum.production;

import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;

import java.io.UnsupportedEncodingException;

/**
 * A special case of a {@link NamedSender&lt;DatumEnvelope&gt;}, that extracts the datum part of the
 * incoming {@link DatumEnvelope}, and decodes it using {@code UTF-8} encoding. It is assumed that
 * the incoming {@link DatumEnvelope} do indeed have a serialized, {@code UTF-8} encoded, string datum.
 *
 * Deliver with callback api support depends on whether the provided stringSender supports the api.
 */
public class DatumEnvelopePeelingStringSender implements NamedSender<DatumEnvelope> {

  public static final String UTF_8 = "UTF-8";

  private final DatumKeyAwareNamedSender<String> stringSender;

  public DatumEnvelopePeelingStringSender(final DatumKeyAwareNamedSender<String> stringSender) {
    this.stringSender = stringSender;
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
    send(datumEnvelope, EmptyCallback.getEmptyCallback());
  }

  @Override
  public void send(final DatumEnvelope datumEnvelope, final DeliveryCallback deliveryCallback) throws SilentSenderException {
    final String dataAsString = getDataAsString(datumEnvelope.getDatumBytes().array());
    final String key = datumEnvelope.getDatumKey() != null ? datumEnvelope.getDatumKey().toString() : null;
    stringSender.send(dataAsString, key, deliveryCallback);
  }

  @Override
  public String getName() {
    return stringSender.getName();
  }
}
