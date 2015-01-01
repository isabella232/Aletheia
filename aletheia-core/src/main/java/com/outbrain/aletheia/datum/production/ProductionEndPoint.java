package com.outbrain.aletheia.datum.production;

import com.outbrain.aletheia.datum.EndPoint;

import java.io.Serializable;

/**
 * Represents a destination a {@link DatumProducer} can produce data to.
 */
public interface ProductionEndPoint extends EndPoint, Serializable {

}

