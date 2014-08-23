package com.outbrain.aletheia.datum.production;

import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

/**
* Created by slevin on 7/27/14.
*/
public class DatumEnvelopePeelingStringSender implements NamedSender<DatumEnvelope> {

  private static final Logger logger = LoggerFactory.getLogger(DatumEnvelopePeelingStringSender.class);

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
