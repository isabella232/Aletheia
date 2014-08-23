package com.outbrain.aletheia.datum.consumption;

import java.nio.ByteBuffer;

/**
 * Created by slevin on 7/24/14.
 */
public interface DatumInbox<TDomainClass> {
  TDomainClass post(final ByteBuffer datumEnvelopeBytes);
}
