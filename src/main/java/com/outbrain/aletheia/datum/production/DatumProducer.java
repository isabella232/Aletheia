package com.outbrain.aletheia.datum.production;

public interface DatumProducer<TDomainClass> {

    void deliver(TDomainClass datum);

}
