package com.outbrain.aletheia.datum.consumption;

import com.outbrain.aletheia.datum.EndPoint;

import java.io.Serializable;

/**
 * Represents a source a {@link DatumConsumerStream} can be built on top of.
 */
public interface ConsumptionEndPoint extends EndPoint, Serializable {

}

