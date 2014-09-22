package com.outbrain.aletheia.metrics.common;

public interface RatioHolder {

  void addDeltas(int nominatorDelta, int denominatorDelta);

  Double resetAndReturnLastValue();

}
