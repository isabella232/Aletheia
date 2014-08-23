package com.outbrain.aletheia.breadcrumbs;

public interface HitLogger {

  public static HitLogger NULL = new HitLogger() {
    public void logHit(final Object obj) {
    }
  };

  void logHit(Object msg);

}
