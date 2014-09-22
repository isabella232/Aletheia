package com.outbrain.aletheia.metrics.codahale3;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


public class Timer implements com.outbrain.aletheia.metrics.common.Timer {

  private final com.codahale.metrics.Timer timer;

  private static class Context implements com.outbrain.aletheia.metrics.common.Timer.Context {
    private final com.codahale.metrics.Timer.Context context;

    private Context(final com.codahale.metrics.Timer.Context context) {
      this.context = context;
    }

    @Override
    public void stop() {
      this.context.stop();
    }
  }

  public Timer(final com.codahale.metrics.Timer timer) {
    this.timer = timer;
  }

  @Override
  public void update(final long duration, final TimeUnit unit) {
    timer.update(duration, unit);
  }

  @Override
  public <T> T time(final Callable<T> event) throws Exception {
    return timer.time(event);
  }

  @Override
  public Context time() {
    return new Context(timer.time());
  }
}